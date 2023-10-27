package cli

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	Clusterparams   ClusterParams
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
	return nil
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

	Clusterparams.CoordinatorConfig = t.CordConfig
	Clusterparams.SegmentConfig = t.SegmentConfig
	Clusterparams.CommonConfig = t.CommonConfig
	Clusterparams.Locale = t.Locale
	Clusterparams.hbaHostname = t.HbaHostnames
	Clusterparams.encoding = t.Encoding
	Clusterparams.suPassword = t.SuPassword

	for _, ps := range t.PrimarySegments {
		var segment Segment
		mapstructure.Decode(ps, &segment)
		primarySegments = append(primarySegments, segment)
	}

	Gparray.PrimarySegments = primarySegments
	mapstructure.Decode(t.Coordinator, &Gparray.Coordinator)

	return nil
}

func validateInputConfig() error {
	if _, ok := Clusterparams.CommonConfig["heap-checksum"]; !ok {
		gplog.Info("Could not find HEAP_CHECKSUM in cluster config, defaulting to on.")
		Clusterparams.CommonConfig["heap-checksum"] = "True"
	}

	// Check if length of Gparray.PimarySegments is 0
	if len(Gparray.PrimarySegments) == 0 {
		gplog.Error("No primary segments are provided in input config file")
	}

	if Clusterparams.dbname == "" {
		Clusterparams.dbname = constants.DefaultDB
		gplog.Info("Database name is not set, will set to default template1")
	}

	if Clusterparams.encoding == "" {
		gplog.Info("Could not find encoding in cluster config, defaulting to UTF-8.")
		Clusterparams.encoding = "UTF-8"
	}

	if Clusterparams.encoding == "SQL_ASCII" {
		gplog.Error("SQL_ASCII is no longer supported as a server encoding")
	}

	if _, ok := Clusterparams.CoordinatorConfig["maxconnections"]; !ok {
		gplog.Info("COORDINATOR_MAX_CONNECT not set, will set to default value 150")
		Clusterparams.CoordinatorConfig["maxconnections"] = string(constants.DefaultQdMaxConnect)
	}
	coordinatorMaxConnect, _ := strconv.Atoi(Clusterparams.CoordinatorConfig["maxconnections"])
	if coordinatorMaxConnect < 1 {
		gplog.Error("COORDINATOR_MAX_CONNECT less than 1")
	}
	if _, ok := Clusterparams.SegmentConfig["maxconnections"]; !ok {
		Clusterparams.SegmentConfig["maxconnections"] = string(coordinatorMaxConnect * constants.QeConnectFactor)
	}

	// check for shared_buffers if not provided in config then set the COORDINATOR_SHARED_BUFFERS and QE_SHARED_BUFFERS to DEFAULT_BUFFERS (128000 kB)
	if _, ok := Clusterparams.CommonConfig["shared-buffer"]; !ok {
		Clusterparams.CommonConfig["shared-buffer"] = constants.DefaultBuffer
		gplog.Info("shared_buffer is not set, will set to default value 128000kB")
	}

	// check coordinator open file values
	coordinatorOpenFileLimit, _ := execCommand("ulimit", "-n").CombinedOutput()
	if val, _ := strconv.Atoi(string(coordinatorOpenFileLimit)); val < constants.OsOpenFiles {
		gplog.Warn(fmt.Sprintf("Coordinator open file limit is %d should be >= %d", coordinatorOpenFileLimit, constants.OsOpenFiles))
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
