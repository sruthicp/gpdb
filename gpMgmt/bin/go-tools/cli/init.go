package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/common"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

var (
	execCommand = exec.Command
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

func SegmentToIdl(input map[string]string) *idl.Segment {
	segment := idl.Segment{}
	segment.HostName = input["hostname"]
	segment.HostAddress = input["address"]
	segment.DataDirectory = input["data-directory"]
	port, _ := strconv.Atoi(input["port"])
	segment.Port = int32(port)
	return &segment
}

func LoadClusterParams(input *InputConfig) *idl.ClusterParams {
	clusterParam := new(idl.ClusterParams)
	// Populate ClusterParams
	clusterParam.HbaHostnames = input.HbaHostnames
	clusterParam.SuPassword = input.SuPassword
	clusterParam.Encoding = input.Encoding

	clusterParam.DbName = input.DbName
	clusterParam.CommonConfig = input.CommonConfig
	clusterParam.CoordinatorConfig = input.CordConfig
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

	// TODO Make call to MakeCluster RPC and wait for results
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
	viper.AddConfigPath(filepath.Dir(inputConfigFile))
	viper.SetConfigName(filepath.Base(inputConfigFile))      // Register config file name (no extension)
	viper.SetConfigType((filepath.Ext(inputConfigFile))[1:]) // Look for specific type
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
	}
	var input InputConfig

	if err := viper.Unmarshal(&input); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
		return nil, err
	}

	clusterReq := idl.MakeClusterRequest{}
	clusterReq.GpArray = &idl.GpArray{}
	clusterReq.ClusterParams = &idl.ClusterParams{}
	clusterReq.ClusterParams.Locale = &idl.Locale{}
	clusterReq.ClusterParams.CoordinatorConfig = make(map[string]string)
	clusterReq.ClusterParams.SegmentConfig = make(map[string]string)
	clusterReq.ClusterParams.CommonConfig = make(map[string]string)

	//Populate GPArray
	clusterReq.GpArray.Coordinator = SegmentToIdl(input.Coordinator)

	for _, seg := range input.PrimarySegments {
		newSeg := SegmentToIdl(seg)
		clusterReq.GpArray.Primaries = append(clusterReq.GpArray.Primaries, newSeg)
	}

	// Populate ClusterParams
	clusterReq.ClusterParams = LoadClusterParams(&input)

	// TODO : Get the forced flag
	clusterReq.ForceFlag = force

	return &clusterReq, nil
}

func validateInputConfigAndSetDefaults(request *idl.MakeClusterRequest) error {
	if request.ClusterParams.CoordinatorConfig == nil {
		request.ClusterParams.CoordinatorConfig = make(map[string]string)
	}
	if request.ClusterParams.SegmentConfig == nil {
		request.ClusterParams.SegmentConfig = make(map[string]string)
	}
	if request.ClusterParams.CommonConfig == nil {
		request.ClusterParams.CommonConfig = make(map[string]string)
	}

	/* TODO Check this checksum
	if _, ok := inputClusterParams.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		inputClusterParams.CommonConfig["heap-checksum"] = "True"
	}
	*/

	// Check if length of Gparray.PimarySegments is 0
	if len(request.GpArray.Primaries) == 0 {
		gplog.Error("No primary segments are provided in input config file")
	}

	if request.ClusterParams.DbName == "" {
		gplog.Info(fmt.Sprintf("Database name is not set, will set to default %v", constants.DefaultDbName))
		request.ClusterParams.DbName = constants.DefaultDbName
	}

	if request.ClusterParams.Encoding == "" {
		gplog.Info(fmt.Sprintf("Could not find encoding in cluster config, defaulting to %v", constants.DefaultEncoding))
		request.ClusterParams.Encoding = "UTF-8"
	}

	if request.ClusterParams.Encoding == "SQL_ASCII" {
		gplog.Error("SQL_ASCII is no longer supported as a server encoding")
	}

	if _, ok := request.ClusterParams.CoordinatorConfig["maxconnections"]; !ok {
		gplog.Info(fmt.Sprintf("COORDINATOR max_connections not set, will set to default value %v", constants.DefaultQdMaxConnect))
		request.ClusterParams.CoordinatorConfig["max_connections"] = strconv.Itoa(constants.DefaultQdMaxConnect)
	}

	coordinatorMaxConnect, err := strconv.Atoi(request.ClusterParams.CoordinatorConfig["max_connections"])
	if err != nil {
		return fmt.Errorf("error parsing max_connections from json: %v", err)
	}

	if coordinatorMaxConnect < 1 {
		gplog.Error("COORDINATOR_MAX_CONNECT less than 1")
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
	coordinatorOpenFileLimit, _ := execCommand("ulimit", "-n").CombinedOutput()
	val, err := strconv.Atoi(string(coordinatorOpenFileLimit)[:len(string(coordinatorOpenFileLimit))-1])
	if err != nil {
		fmt.Printf("could not covert the ulimit %s", err)
	}
	if val < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %d should be >= %d", val, constants.OsOpenFiles))
	}

	return nil
}
