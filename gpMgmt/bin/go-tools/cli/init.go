package cli

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

var (
	ClusterParam    ClusterParams
	primarySegments []Segment
	Gparray         gpArray
	execCommand     = exec.Command
	force           bool
)

type InputConfig struct {
	ClusterName     string              `mapstructure:"cluster-name"`
	DbName          string              `mapstructure:"db-name"`
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

func SegmentToIdl(input Segment) *idl.Segment {
	segment := idl.Segment{}
	segment.HostName = input.hostName
	segment.HostAddress = input.hostAddress
	segment.DataDirectory = input.dataDirectory
	segment.Port = int32(input.port)
	return &segment
}

func LoadClusterParams(input ClusterParams) *idl.ClusterParams {
	clusterParam := new(idl.ClusterParams)
	// Populate ClusterParams
	clusterParam.HbaHostnames = input.hbaHostname
	clusterParam.SuPassword = input.suPassword
	clusterParam.Encoding = input.encoding

	clusterParam.DbName = input.dbname
	clusterParam.CommonConfig = input.CommonConfig
	clusterParam.CoordinatorConfig = input.CoordinatorConfig
	clusterParam.SegmentConfig = input.SegmentConfig

	// Load Locacle
	clusterParam.Locale = LoadLocale(input.Locale)
	return clusterParam
}
func LoadLocale(param Locale) *idl.Locale {
	locale := new(idl.Locale)
	locale.LcCollate = param.LcCollate
	locale.LcAll = param.LcAll
	locale.LcCtype = param.LcCtype
	locale.LcMessages = param.LcMessages
	locale.LcMonetory = param.LcMonetary
	locale.LcTime = param.LcTime
	locale.LcNumeric = param.LcNumeric
	return locale
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
		RunE:    RunInitClusterCmd,
	}
	initClusterCmd.LocalFlags().Bool("force", false, "Create cluster forcefully by overwriting existing directories")
	//force, _ := initClusterCmd.LocalFlags().GetBool("force")
	// TODO `gp init <file>` command should also work. Current `gp init cluster <file>` is implemented
	// TODO Add forced flag and populate to MakeClusterRequest
	// TODO Check behavior when provided with no input file. Should give proper error message. Currently giving exception.

	return initClusterCmd
}
func RunInitClusterCmd(cmd *cobra.Command, args []string) error {
	return RunInitCluster(cmd, args)
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
	if err != nil {
		gplog.Error("Error calling hub to create cluster:%v", err)
		return fmt.Errorf("error calling hub to create cluster:%v", err)
	}
	done := make(chan bool)
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
				fmt.Printf("Error:%v\n", err)
				done <- true
				return
			}
			reply := resp.MakeClusterReply
			switch reply.(type) {
			case *idl.MakeClusterReply_Message:
				fmt.Printf("Message:%s\n", resp.GetMessage())
			case *idl.MakeClusterReply_Progress:
				fmt.Printf("Progress, Title:%s Progreess:%d\n", resp.GetProgress().GetTitle(), resp.GetProgress().GetPercentProgress())
			}
		}
	}()
	<-done

	fmt.Println("Done with Make Cluster. Exiting.")
	return nil
}

func LoadToIdl(gparray gpArray, param ClusterParams) *idl.MakeClusterRequest {
	clusterReq := idl.MakeClusterRequest{}
	clusterReq.GpArray = &idl.GpArray{}
	clusterReq.ClusterParams = &idl.ClusterParams{}
	clusterReq.ClusterParams.Locale = &idl.Locale{}
	clusterReq.ClusterParams.CoordinatorConfig = make(map[string]string)
	clusterReq.ClusterParams.SegmentConfig = make(map[string]string)
	clusterReq.ClusterParams.CommonConfig = make(map[string]string)

	//Populate GPArray
	clusterReq.GpArray.Coordinator = SegmentToIdl(gparray.Coordinator)

	for _, seg := range gparray.PrimarySegments {
		newSeg := SegmentToIdl(seg)
		clusterReq.GpArray.Primaries = append(clusterReq.GpArray.Primaries, newSeg)
	}

	// Populate ClusterParams
	clusterReq.ClusterParams = LoadClusterParams(param)

	// TODO : Get the forced flag
	clusterReq.ForceFlag = false

	return &clusterReq
}
func RunInitClusterFn(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("please provide config file for cluster initialization")
	}
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
	var input InputConfig

	if err := viper.Unmarshal(&input); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
	}

	ClusterParam.CoordinatorConfig = input.CordConfig
	ClusterParam.SegmentConfig = input.SegmentConfig
	ClusterParam.CommonConfig = input.CommonConfig
	ClusterParam.Locale = input.Locale
	ClusterParam.hbaHostname = input.HbaHostnames
	ClusterParam.encoding = input.Encoding
	ClusterParam.suPassword = input.SuPassword

	// Copy primary segments
	for _, ps := range input.PrimarySegments {
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
	Gparray.PrimarySegments = primarySegments

	// copy coordinator segment
	port, err := strconv.Atoi(input.Coordinator["port"])
	if err != nil {
		gplog.Error("Error converting port number from config file. Error:%v", err)
		return err
	}
	coordinator := Segment{
		hostName:      input.Coordinator["hostname"],
		hostAddress:   input.Coordinator["address"],
		dataDirectory: input.Coordinator["data-directory"],
		port:          port,
	}
	Gparray.Coordinator = coordinator

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

	/* TODO Check this checksum
	if _, ok := ClusterParam.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		ClusterParam.CommonConfig["heap-checksum"] = "True"
	}
	*/

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

	if _, ok := ClusterParam.CoordinatorConfig["max_connections"]; !ok {
		gplog.Info("COORDINATOR max_connectionsT not set, will set to default value 150")
		ClusterParam.CoordinatorConfig["max_connections"] = string(constants.DefaultQdMaxConnect)
	}

	coordinatorMaxConnect, err := strconv.Atoi(ClusterParam.CoordinatorConfig["max_connections"])
	if err != nil {
		return fmt.Errorf("error parsing max_connections from json: %v", err)
	}

	if coordinatorMaxConnect < 1 {
		gplog.Error("COORDINATOR_MAX_CONNECT less than 1")
	}
	if _, ok := ClusterParam.SegmentConfig["max_connections"]; !ok {
		ClusterParam.SegmentConfig["max_connections"] = string(coordinatorMaxConnect * constants.QeConnectFactor)
	}

	// check for shared_buffers if not provided in config then set the COORDINATOR_SHARED_BUFFERS and QE_SHARED_BUFFERS to DEFAULT_BUFFERS (128000 kB)
	if _, ok := ClusterParam.CommonConfig["shared_buffers"]; !ok {
		ClusterParam.CommonConfig["shared_buffers"] = constants.DefaultBuffer
		gplog.Info("shared_buffers is not set, will set to default value 128000kB")
	}

	// check coordinator open file values
	coordinatorOpenFileLimit, _ := execCommand("ulimit", "-n").CombinedOutput()
	if val, _ := strconv.Atoi(string(coordinatorOpenFileLimit)); val < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %d should be >= %d", coordinatorOpenFileLimit, constants.OsOpenFiles))
	}

	return nil
}
