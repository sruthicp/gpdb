package cli

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	ClusterParam    ClusterParams
	primarySegments []Segment
	Gparray         gpArray
	execCommand     = exec.Command
)

type InputConfig struct {
	ClusterName     string              `mapstructure:"cluster-name"`
	Encoding        string              `mapstructure:"encoding"`
	HbaHostnames    bool                `mapstructure:"hba-hostnames"`
	SuPassword      string              `mapstructure:"su-password"`
	Locale          Locale              `mapstructure:"locale"`
	CommonConfig    map[string]string   `mapstructure:"common-config"`
	CordConfig      map[string]string   `mapstructure:"coordinator-config"`
	SegmentConfig   map[string]string   `mapstructure:"segment-config"`
	Coordinator     map[string]string   `mapstructure:"coordinator"`
	PrimarySegments []map[string]string `mapstructure:"primary-segments-array"`
}

func initCmd() *cobra.Command {
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize cluster, segments",
	}

	initCmd.AddCommand(initClusterCmd())

	return initCmd
}

var (
	InitClusterService = InitClusterServiceFn
	RunInitCluster     = RunInitClusterFn
)

func initClusterCmd() *cobra.Command {
	initClusterCmd := &cobra.Command{
		Use:     "cluster",
		Short:   "Initialize the cluster",
		PreRunE: InitializeCommand,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunInitCluster(cmd, args)
		},
	}
	// TODO `gp init <file>` command should also work. Current `gp init cluster <file>` is implemented
	// TODO Add forced flag and populate to MakeClusterRequest
	// TODO Check behavior when provided with no input file. Should give proper error message. Currently giving exception.

	return initClusterCmd
}

func InitClusterServiceFn(hubConfig *hub.Config, inputConfigFile string) error {
	if _, err := os.Stat(inputConfigFile); err != nil {
		return err
	}

	if err := readInputConfig(inputConfigFile); err != nil {
		return err
	}

	if err := validateInputConfig(); err != nil {
		return err
	}

	// TODO Make call to MakeCluster RPC and wait for results
	client, err := ConnectToHub(Conf)
	if err != nil {
		return err
	}
	// TODO Populate clusterReq with data
	clusterReq := LoadToIdl(Gparray, ClusterParam)

	stream, err := client.MakeCluster(context.Background(), clusterReq)
	done := make(chan bool)
	errChan := make(chan error)
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				gplog.Debug("MakeCluster connection closed. Done.")
				fmt.Println("MakeCluster connection closed. Done.")
				done <- true
				return
			}
			if err != nil {
				fmt.Printf("Error receiving data from stream:%v\n", err)
				errChan <- err
				close(errChan)
				return
			}
			reply := resp.MakeClusterReply
			switch reply.(type) {
			case *idl.MakeClusterReply_Message:
				fmt.Printf("Got RPC Message:%s\n", resp.GetMessage())
			case *idl.MakeClusterReply_Progress:
				fmt.Printf("Got RPC Progress, Title:%s Progreess:%d\n", resp.GetProgress().GetTitle(), resp.GetProgress().GetPercentProgress())
			}
		}
	}()
	<-done
	/*
		fmt.Printf("Checking for error.")
		err = <-errChan
		if err != nil {
			fmt.Printf("Error receiving response:%v", err)
			close(errChan)
		}
	*/
	fmt.Println("Done with Make Cluster. Exiting.")
	return nil
}

func LoadToIdl(gparray gpArray, param ClusterParams) *idl.MakeClusterRequest {
	fmt.Printf("Starting LoadToIdl Coordinator:%v\n", gparray.Coordinator)
	clusterReq := idl.MakeClusterRequest{}
	clusterReq.GpArray = &idl.GpArray{}
	clusterReq.GpArray.Coordinator = &idl.Segment{}
	clusterReq.ClusterParams = &idl.ClusterParams{}
	clusterReq.ClusterParams.Locale = &idl.Locale{}
	clusterReq.ClusterParams.CoordinatorConfig = make(map[string]string)
	clusterReq.ClusterParams.SegmentConfig = make(map[string]string)
	clusterReq.ClusterParams.CommonConfig = make(map[string]string)

	//GPArray
	//Coordinator
	clusterReq.GpArray.Coordinator.HostAddress = gparray.Coordinator.hostAddress
	clusterReq.GpArray.Coordinator.HostName = gparray.Coordinator.hostName
	clusterReq.GpArray.Coordinator.Port = int32(gparray.Coordinator.port)
	clusterReq.GpArray.Coordinator.DataDirectory = gparray.Coordinator.dataDirectory

	//Primaries
	for _, seg := range gparray.PrimarySegments {
		newSeg := new(idl.Segment)
		newSeg.HostName = seg.hostName
		newSeg.HostAddress = seg.hostAddress
		newSeg.Port = int32(seg.port)
		newSeg.DataDirectory = seg.dataDirectory
		clusterReq.GpArray.Primaries = append(clusterReq.GpArray.Primaries, newSeg)
	}

	// ClusterParams
	clusterReq.ClusterParams.HbaHostnames = param.hbaHostname
	clusterReq.ClusterParams.SuPassword = param.suPassword
	clusterReq.ClusterParams.Encoding = param.encoding

	clusterReq.ClusterParams.DbName = param.dbname
	clusterReq.ClusterParams.CommonConfig = param.CommonConfig
	clusterReq.ClusterParams.CoordinatorConfig = param.CoordinatorConfig
	clusterReq.ClusterParams.SegmentConfig = param.SegmentConfig

	// Locale
	clusterReq.ClusterParams.Locale.LcTime = param.Locale.LcTime
	clusterReq.ClusterParams.Locale.LcMonetory = param.Locale.LcTime
	clusterReq.ClusterParams.Locale.LcNumeric = param.Locale.LcNumeric
	clusterReq.ClusterParams.Locale.LcMessages = param.Locale.LcMessages
	clusterReq.ClusterParams.Locale.LcCollate = param.Locale.LcCollate
	clusterReq.ClusterParams.Locale.LcAll = param.Locale.LcAll
	clusterReq.ClusterParams.Locale.LcCtype = param.Locale.LcCtype

	// TODO : Get the forced flag
	clusterReq.ForceFlag = false

	return &clusterReq
}
func RunInitClusterFn(cmd *cobra.Command, args []string) error {
	err := InitClusterService(Conf, args[0])
	if err != nil {
		return err
	}
	err = WaitAndRetryHubConnect()
	if err != nil {
		return err
	}
	gplog.Info("Cluster initialized started successfully")

	return nil
}

func readInputConfig(inputConfigFile string) error {
	viper.AddConfigPath(filepath.Dir(inputConfigFile))
	viper.SetConfigName(filepath.Base(inputConfigFile))      // Register config file name (no extension)
	viper.SetConfigType((filepath.Ext(inputConfigFile))[1:]) // Look for specific type
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
	}
	var t InputConfig

	if err := viper.Unmarshal(&t); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
	}

	ClusterParam.CoordinatorConfig = t.CordConfig
	ClusterParam.SegmentConfig = t.SegmentConfig
	ClusterParam.CommonConfig = t.CommonConfig
	ClusterParam.Locale = t.Locale
	ClusterParam.hbaHostname = t.HbaHostnames
	ClusterParam.encoding = t.Encoding
	ClusterParam.suPassword = t.SuPassword

	// Copy primary segments
	for _, ps := range t.PrimarySegments {
		var segment Segment
		segment.hostName = ps["hostname"]
		segment.hostAddress = ps["address"]
		segment.dataDirectory = ps["data-directory"]
		port, err := strconv.Atoi(ps["port"])
		if err != nil {
			gplog.Error("Error converting port number from config file. Error:%v", err)
			return err
		}
		segment.port = port
		primarySegments = append(primarySegments, segment)
	}
	// copy coordinator segment
	port, err := strconv.Atoi(t.Coordinator["port"])
	if err != nil {
		gplog.Error("Error converting port number from config file. Error:%v", err)
		return err
	}
	coordinator := Segment{
		hostName:      t.Coordinator["hostname"],
		hostAddress:   t.Coordinator["address"],
		dataDirectory: t.Coordinator["data-directory"],
		port:          port,
	}
	Gparray.Coordinator = coordinator
	Gparray.PrimarySegments = primarySegments
	mapstructure.Decode(t.Coordinator, &Gparray.Coordinator)

	return nil
}

func validateInputConfig() error {
	if ClusterParam.CoordinatorConfig == nil {
		ClusterParam.CoordinatorConfig = make(map[string]string)
	}
	if ClusterParam.SegmentConfig == nil {
		ClusterParam.SegmentConfig = make(map[string]string)
	}
	if ClusterParam.CommonConfig == nil {
		ClusterParam.CommonConfig = make(map[string]string)
	}

	if _, ok := ClusterParam.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		ClusterParam.CommonConfig["heap-checksum"] = "True"
	}

	// Check if length of Gparray.PimarySegments is 0
	if len(Gparray.PrimarySegments) == 0 {
		gplog.Error("No primary segments are provided in input config file")
	}

	if ClusterParam.dbname == "" {
		ClusterParam.dbname = constants.DefaultDB
		gplog.Info("Database name is not set, will set to default template1")
	}

	if ClusterParam.encoding == "" {
		gplog.Info("Could not find encoding in cluster config, defaulting to UTF-8.")
		ClusterParam.encoding = "UTF-8"
	}

	if ClusterParam.encoding == "SQL_ASCII" {
		gplog.Error("SQL_ASCII is no longer supported as a server encoding")
	}

	if _, ok := ClusterParam.CoordinatorConfig["maxconnections"]; !ok {
		gplog.Info("COORDINATOR_MAX_CONNECT not set, will set to default value 150")
		ClusterParam.CoordinatorConfig["maxconnections"] = string(constants.DefaultQdMaxConnect)
	}
	coordinatorMaxConnect, _ := strconv.Atoi(ClusterParam.CoordinatorConfig["maxconnections"])
	if coordinatorMaxConnect < 1 {
		gplog.Error("COORDINATOR_MAX_CONNECT less than 1")
	}
	if _, ok := ClusterParam.SegmentConfig["maxconnections"]; !ok {
		ClusterParam.SegmentConfig["maxconnections"] = string(coordinatorMaxConnect * constants.QeConnectFactor)
	}

	// check for shared_buffers if not provided in config then set the COORDINATOR_SHARED_BUFFERS and QE_SHARED_BUFFERS to DEFAULT_BUFFERS (128000 kB)
	if _, ok := ClusterParam.CommonConfig["shared-buffer"]; !ok {
		ClusterParam.CommonConfig["shared-buffer"] = constants.DefaultBuffer
		gplog.Info("shared_buffer is not set, will set to default value 128000kB")
	}

	// check coordinator open file values
	coordinatorOpenFileLimit, _ := execCommand("ulimit", "-n").CombinedOutput()
	if val, _ := strconv.Atoi(string(coordinatorOpenFileLimit)); val < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %s should be >= %d", string(coordinatorOpenFileLimit), constants.OsOpenFiles))
	}

	return nil
}

func getAllAvailableLocales() string {
	availableLocales, err := execCommand("locale", "-a").CombinedOutput()
	if err != nil {
		fmt.Errorf("failed to get the available locales on this system: %w", err)
	}

	return string(availableLocales)
}

func IsLocaleAvailable(locale_type string) bool {
	allAvailableLocales := getAllAvailableLocales()
	locales := strings.Split(allAvailableLocales, "\n")

	for _, v := range locales {
		if locale_type == v {
			return true
		}
	}
	return false
}
