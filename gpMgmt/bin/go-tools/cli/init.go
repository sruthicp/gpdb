package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

type Locale struct {
	LcAll      string `mapstructure:"lc-all"`
	LcCollate  string `mapstructure:"lc-collate"`
	LcCtype    string `mapstructure:"lc-ctype"`
	LcMessages string `mapstructure:"lc-messages"`
	LcMonetary string `mapstructure:"lc-monetary"`
	LcNumeric  string `mapstructure:"lc-numeric"`
	LcTime     string `mapstructure:"lc-time"`
}

type Segment struct {
	Hostname      string `mapstructure:"hostname"`
	Address       string `mapstructure:"address"`
	Port          int    `mapstructure:"port"`
	DataDirectory string `mapstructure:"data-directory"`
}

type InitConfig struct {
	ClusterName          string            `mapstructure:"cluster-name"`
	DbName               string            `mapstructure:"db-name"`
	Encoding             string            `mapstructure:"encoding"`
	HbaHostnames         bool              `mapstructure:"hba-hostnames"`
	SuPassword           string            `mapstructure:"su-password"`
	Locale               Locale            `mapstructure:"locale"`
	CommonConfig         map[string]string `mapstructure:"common-config"`
	CoordinatorConfig    map[string]string `mapstructure:"coordinator-config"`
	SegmentConfig        map[string]string `mapstructure:"segment-config"`
	Coordinator          Segment           `mapstructure:"coordinator"`
	PrimarySegmentsArray []Segment         `mapstructure:"primary-segments-array"`
}

func SegmentToIdl(seg Segment) *idl.Segment {
	return &idl.Segment{
		Port:          int32(seg.Port),
		DataDirectory: seg.DataDirectory,
		HostName:      seg.Hostname,
		HostAddress:   seg.Address,
	}
}

func ClusterParamsToIdl(config *InitConfig) *idl.ClusterParams {
	return &idl.ClusterParams{
		CoordinatorConfig: config.CoordinatorConfig,
		SegmentConfig:     config.SegmentConfig,
		CommonConfig:      config.CommonConfig,
		Locale: &idl.Locale{
			LcAll:      config.Locale.LcAll,
			LcCollate:  config.Locale.LcCollate,
			LcCtype:    config.Locale.LcCtype,
			LcMessages: config.Locale.LcMessages,
			LcMonetory: config.Locale.LcMonetary,
			LcNumeric:  config.Locale.LcNumeric,
			LcTime:     config.Locale.LcTime,
		},
		HbaHostnames: config.HbaHostnames,
		Encoding:     config.Encoding,
		SuPassword:   config.SuPassword,
		DbName:       config.DbName,
	}
}

func CreateMakeClusterReq(config *InitConfig, forceFlag bool) *idl.MakeClusterRequest {
	primarySegs := []*idl.Segment{}
	for _, seg := range config.PrimarySegmentsArray {
		primarySegs = append(primarySegs, SegmentToIdl(seg))
	}

	return &idl.MakeClusterRequest{
		GpArray: &idl.GpArray{
			Coordinator: SegmentToIdl(config.Coordinator),
			Primaries:   primarySegs,
		},
		ClusterParams: ClusterParamsToIdl(config),
		ForceFlag:     forceFlag,
	}
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
	initClusterCmd.PersistentFlags().Bool("force", false, "Create cluster forcefully by overwriting existing directories")

	return initClusterCmd
}
func RunInitClusterCmd(cmd *cobra.Command, args []string) error {
	force, err := cmd.PersistentFlags().GetBool("force")
	if err != nil {
		gplog.Error("Could not get value of force flag %v", err)
		return err
	}
	return RunInitCluster(cmd, args, force)
}

func InitClusterServiceFn(hubConfig *hub.Config, inputConfigFile string, force bool) error {
	if _, err := os.Stat(inputConfigFile); err != nil {
		return err
	}

	clusterReq, err := LoadInputConfigToIdl(inputConfigFile, force)

	if err := validateInputConfigAndSetDefaults(clusterReq); err != nil {
		return err
	}

	// Make call to MakeCluster RPC and wait for results
	client, err := ConnectToHub(Conf)
	if err != nil {
		return err
	}

	stream, err := client.MakeCluster(context.Background(), clusterReq)
	if err != nil {
		return utils.FormatGrpcError(err)
	}

	err = ParseStreamResponse(stream)
	if err != nil {
		return err
	}

	return nil
}

func RunInitClusterFn(cmd *cobra.Command, args []string, force bool) error {
	if len(args) == 0 {
		return fmt.Errorf("please provide config file for cluster initialization")
	}
	err := InitClusterService(Conf, args[0], force)
	if err != nil {
		return err
	}
	gplog.Info("Cluster initialized started successfully")

	return nil
}

func LoadInputConfigToIdl(inputConfigFile string, force bool) (*idl.MakeClusterRequest, error) {
	viper.SetConfigFile(inputConfigFile)
	viper.SetConfigType("json")
	viper.SetDefault("common-config", make(map[string]string))
	viper.SetDefault("coordinator-config", make(map[string]string))
	viper.SetDefault("segment-config", make(map[string]string))

	if err := viper.ReadInConfig(); err != nil {
		return &idl.MakeClusterRequest{}, fmt.Errorf("reading config file: %w", err)
	}

	var config InitConfig
	if err := viper.Unmarshal(&config); err != nil {
		return &idl.MakeClusterRequest{}, fmt.Errorf("reading config file: %w", err)
	}

	return CreateMakeClusterReq(&config, force), nil
}

func validateInputConfigAndSetDefaults(request *idl.MakeClusterRequest) error {
	/* TODO Check this checksum
	if _, ok := inputClusterParams.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		inputClusterParams.CommonConfig["heap-checksum"] = "True"
	}
	*/

	// Check if length of Gparray.PimarySegments is 0
	if len(request.GpArray.Primaries) == 0 {
		return fmt.Errorf("No primary segments are provided in input config file")
	}

	// TODO: Current gpinitsystem does not create a database if not provided by the user.
	// To decide if we want the same behaviour
	// if request.ClusterParams.DbName == "" {
	// 	gplog.Info(fmt.Sprintf("Database name is not set, will set to default %v", constants.DefaultDbName))
	// 	request.ClusterParams.DbName = constants.DefaultDbName
	// }

	if request.ClusterParams.Encoding == "" {
		gplog.Info(fmt.Sprintf("Could not find encoding in cluster config, defaulting to %v", constants.DefaultEncoding))
		request.ClusterParams.Encoding = "UTF-8"
	}

	if request.ClusterParams.Encoding == "SQL_ASCII" {
		return fmt.Errorf("SQL_ASCII is no longer supported as a server encoding")
	}

	if _, ok := request.ClusterParams.CoordinatorConfig["max_connections"]; !ok {
		gplog.Info(fmt.Sprintf("COORDINATOR max_connections not set, will set to default value %v", constants.DefaultQdMaxConnect))
		request.ClusterParams.CoordinatorConfig["max_connections"] = strconv.Itoa(constants.DefaultQdMaxConnect)
	}

	coordinatorMaxConnect, err := strconv.Atoi(request.ClusterParams.CoordinatorConfig["max_connections"])
	if err != nil {
		return fmt.Errorf("error parsing max_connections from json: %v", err)
	}

	if coordinatorMaxConnect < 1 {
		return fmt.Errorf("COORDINATOR_MAX_CONNECT less than 1")
	}
	if _, ok := request.ClusterParams.SegmentConfig["max_connections"]; !ok {
		request.ClusterParams.SegmentConfig["max_connections"] = strconv.Itoa(coordinatorMaxConnect * constants.QeConnectFactor)
	}

	// check for shared_buffers if not provided in config then set the COORDINATOR_SHARED_BUFFERS and QE_SHARED_BUFFERS to DEFAULT_BUFFERS (128000 kB)
	if _, ok := request.ClusterParams.CommonConfig["shared_buffers"]; !ok {
		gplog.Info(fmt.Sprintf("shared_buffers is not set, will set to default value %v", constants.DefaultBuffer))
		request.ClusterParams.CommonConfig["shared_buffers"] = constants.DefaultBuffer
	}

	// check coordinator open file values
	out, err := utils.System.ExecCommand("ulimit", "-n").CombinedOutput()
	if err != nil {
		return err
	}

	coordinatorOpenFileLimit, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return fmt.Errorf("could not convert the ulimit value: %w", err)
	}

	if coordinatorOpenFileLimit < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %d should be >= %d", coordinatorOpenFileLimit, constants.OsOpenFiles))
	}

	return nil
}
