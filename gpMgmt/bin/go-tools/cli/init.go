package cli

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/common"
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
	execCommand = exec.Command
	force       bool
)

type InputConfig struct {
	ClusterName     string              `mapstructure:"cluster-name"`
	DbName          string              `mapstructure:"db-name"`
	Encoding        string              `mapstructure:"encoding"`
	HbaHostnames    bool                `mapstructure:"hba-hostnames"`
	SuPassword      string              `mapstructure:"su-password"`
	Locale          common.Locale       `mapstructure:"locale"`
	CommonConfig    map[string]string   `mapstructure:"common-config"`
	CordConfig      map[string]string   `mapstructure:"coordinator-config"`
	SegmentConfig   map[string]string   `mapstructure:"segment-config"`
	Coordinator     map[string]string   `mapstructure:"coordinator"`
	PrimarySegments []map[string]string `mapstructure:"primary-segments-array"`
}

func SegmentToIdl(input common.Segment) *idl.Segment {
	segment := idl.Segment{}
	segment.HostName = input.HostName
	segment.HostAddress = input.HostAddress
	segment.DataDirectory = input.DataDirectory
	segment.Port = int32(input.Port)
	return &segment
}

func LoadClusterParams(input *common.ClusterParams) *idl.ClusterParams {
	clusterParam := new(idl.ClusterParams)
	// Populate ClusterParams
	clusterParam.HbaHostnames = input.HbaHostname
	clusterParam.SuPassword = input.SuPassword
	clusterParam.Encoding = input.Encoding

	clusterParam.DbName = input.DbName
	clusterParam.CommonConfig = input.CommonConfig
	clusterParam.CoordinatorConfig = input.CoordinatorConfig
	clusterParam.SegmentConfig = input.SegmentConfig

	// Load Locale
	clusterParam.Locale = LoadLocale(input.Locale)
	return clusterParam
}
func LoadLocale(param common.Locale) *idl.Locale {
	locale := new(idl.Locale)
	locale.LcCollate = param.Lc_collate
	locale.LcAll = param.Lc_all
	locale.LcCtype = param.Lc_ctype
	locale.LcMessages = param.Lc_messages
	locale.LcMonetory = param.Lc_monetory
	locale.LcTime = param.Lc_time
	locale.LcNumeric = param.Lc_numeric
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

	inputClusterParams, inputGpArray, err := readInputConfig(inputConfigFile)

	if err := validateInputConfig(inputClusterParams, inputGpArray); err != nil {
		return err
	}

	// TODO Make call to MakeCluster RPC and wait for results
	client, err := ConnectToHub(Conf)
	if err != nil {
		return err
	}

	// TODO Populate clusterReq with data
	clusterReq := LoadToIdl(inputGpArray, inputClusterParams)

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

func LoadToIdl(gparray *common.GpArray, param *common.ClusterParams) *idl.MakeClusterRequest {
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

func readInputConfig(inputConfigFile string) (*common.ClusterParams, *common.GpArray, error) {
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

	var inputClusterParam common.ClusterParams
	inputClusterParam.CoordinatorConfig = input.CordConfig
	inputClusterParam.SegmentConfig = input.SegmentConfig
	inputClusterParam.CommonConfig = input.CommonConfig
	inputClusterParam.Locale = input.Locale
	inputClusterParam.HbaHostname = input.HbaHostnames
	inputClusterParam.Encoding = input.Encoding
	inputClusterParam.SuPassword = input.SuPassword

	// Copy primary segments
	var primarySegments []common.Segment
	for _, ps := range input.PrimarySegments {
		var segment common.Segment
		segment.HostName = ps["hostname"]
		segment.HostAddress = ps["address"]
		segment.DataDirectory = ps["data-directory"]
		port, err := strconv.Atoi(ps["port"])
		if err != nil {
			gplog.Error("Error converting port number from config file. Error:%v", err)
			return nil, nil, err
		}
		segment.Port = port
		primarySegments = append(primarySegments, segment)
	}
	var inputGpArray common.GpArray

	inputGpArray.PrimarySegments = primarySegments

	// copy coordinator segment
	port, err := strconv.Atoi(input.Coordinator["port"])
	if err != nil {
		gplog.Error("Error converting port number from config file. Error:%v", err)
		return nil, nil, err
	}
	coordinator := common.Segment{
		HostName:      input.Coordinator["hostname"],
		HostAddress:   input.Coordinator["address"],
		DataDirectory: input.Coordinator["data-directory"],
		Port:          port,
	}
	inputGpArray.Coordinator = coordinator

	return &inputClusterParam, &inputGpArray, nil
}

func validateInputConfig(inputClusterParams *common.ClusterParams, inputGpArray *common.GpArray) error {
	if inputClusterParams.CoordinatorConfig == nil {
		inputClusterParams.CoordinatorConfig = make(map[string]string)
	}
	if inputClusterParams.SegmentConfig == nil {
		inputClusterParams.SegmentConfig = make(map[string]string)
	}
	if inputClusterParams.CommonConfig == nil {
		inputClusterParams.CommonConfig = make(map[string]string)
	}

	/* TODO Check this checksum
	if _, ok := inputClusterParams.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		inputClusterParams.CommonConfig["heap-checksum"] = "True"
	}
	*/

	// Check if length of Gparray.PimarySegments is 0
	if len(inputGpArray.PrimarySegments) == 0 {
		gplog.Error("No primary segments are provided in input config file")
	}

	if inputClusterParams.DbName == "" {
		inputClusterParams.DbName = constants.DefaultDB
		gplog.Info("Database name is not set, will set to default template1")
	}

	if inputClusterParams.Encoding == "" {
		gplog.Info("Could not find encoding in cluster config, defaulting to UTF-8.")
		inputClusterParams.Encoding = "UTF-8"
	}

	if inputClusterParams.Encoding == "SQL_ASCII" {
		gplog.Error("SQL_ASCII is no longer supported as a server encoding")
	}

	if _, ok := inputClusterParams.CoordinatorConfig["max_connections"]; !ok {
		gplog.Info("COORDINATOR max_connections not set, will set to default value 150")
		inputClusterParams.CoordinatorConfig["max_connections"] = string(constants.DefaultQdMaxConnect)
	}

	coordinatorMaxConnect, err := strconv.Atoi(inputClusterParams.CoordinatorConfig["max_connections"])
	if err != nil {
		return fmt.Errorf("error parsing max_connections from json: %v", err)
	}

	if coordinatorMaxConnect < 1 {
		gplog.Error("COORDINATOR_MAX_CONNECT less than 1")
	}
	if _, ok := inputClusterParams.SegmentConfig["max_connections"]; !ok {
		inputClusterParams.SegmentConfig["max_connections"] = string(coordinatorMaxConnect * constants.QeConnectFactor)
	}

	// check for shared_buffers if not provided in config then set the COORDINATOR_SHARED_BUFFERS and QE_SHARED_BUFFERS to DEFAULT_BUFFERS (128000 kB)
	if _, ok := inputClusterParams.CommonConfig["shared_buffers"]; !ok {
		inputClusterParams.CommonConfig["shared_buffers"] = constants.DefaultBuffer
		gplog.Info("shared_buffers is not set, will set to default value 128000kB")
	}

	// check coordinator open file values
	coordinatorOpenFileLimit, _ := execCommand("ulimit", "-n").CombinedOutput()
	if val, _ := strconv.Atoi(string(coordinatorOpenFileLimit)); val < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %d should be >= %d", coordinatorOpenFileLimit, constants.OsOpenFiles))
	}

	return nil
}
