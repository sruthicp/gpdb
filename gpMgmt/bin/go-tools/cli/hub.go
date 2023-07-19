package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	platform                 = utils.GetPlatform()
	defaultHubLogDir  string = "/tmp"
	defaultHubPort    int    = 4242
	defaultAgentPort  int    = 8000
	defaultServiceDir string = platform.GetDefaultServiceDir()

	agentPort      int
	caCertPath     string
	caKeyPath      string
	gphome         string
	hubLogDir      string
	hubPort        int
	hostnames      []string
	hostfilePath   string
	serverCertPath string
	serverKeyPath  string
	serviceDir     string // Provide the service file's directory and name separately so users can name different files for different clusters
	serviceName    string
	serviceUser    string
)

func hubCmd() *cobra.Command {
	hubCmd := &cobra.Command{
		Use:    "hub",
		Short:  "Start a gp process in hub mode",
		Long:   "Start a gp process in hub mode",
		Hidden: true, // Should only be invoked by systemd
		PreRun: InitializeCommand,
		RunE:   RunHub,
	}

	return hubCmd
}

func RunHub(cmd *cobra.Command, args []string) (err error) {
	h := hub.New(conf, grpc.DialContext)
	err = h.Start()
	if err != nil {
		return fmt.Errorf("Could not start hub: %w", err)
	}
	return nil
}

func installCmd() *cobra.Command {
	installCmd := &cobra.Command{
		Use:    "install",
		Short:  "Install gp as a systemd daemon",
		PreRun: InitializeGplog,
		RunE:   RunInstall,
	}
	installCmd.Flags().IntVar(&agentPort, "agent-port", defaultAgentPort, `Port on which the agents should listen`)
	installCmd.Flags().StringVar(&gphome, "gphome", os.Getenv("GPHOME"), `Path to GPDB installation`)
	installCmd.Flags().IntVar(&hubPort, "hub-port", defaultHubPort, `Port on which the hub should listen`)
	installCmd.Flags().StringVar(&hubLogDir, "log-dir", defaultHubLogDir, `Path to gp hub log directory`)
	installCmd.Flags().StringVar(&serviceName, "service-name", "gp", `Name for the generated systemd service file`)
	installCmd.Flags().StringVar(&serviceDir, "service-dir", fmt.Sprintf(defaultServiceDir, os.Getenv("USER")), `Path to service file directory`)
	installCmd.Flags().StringVar(&serviceUser, "service-user", os.Getenv("USER"), `User for whom to install the service`)
	// TLS credentials are deliberately left blank if not provided, and need to be filled in by the user
	installCmd.Flags().StringVar(&caCertPath, "ca-certificate", "", `Path to SSL/TLS CA certificate`)
	installCmd.Flags().StringVar(&caKeyPath, "ca-key", "", `Path to SSL/TLS CA private key`)
	installCmd.Flags().StringVar(&serverCertPath, "server-certificate", "", `Path to hub SSL/TLS server certificate`)
	installCmd.Flags().StringVar(&serverKeyPath, "server-key", "", `Path to hub SSL/TLS server private key`)
	// Allow passing a hostfile for "real" use cases or a few host names for tests, but not both
	installCmd.Flags().StringArrayVar(&hostnames, "host", []string{}, `Segment hostname`)
	installCmd.Flags().StringVar(&hostfilePath, "hostfile", "", `Path to file containing a list of segment hostnames`)
	installCmd.MarkFlagsMutuallyExclusive("host", "hostfile")
	return installCmd
}
func RunInstall(cmd *cobra.Command, args []string) (err error) {
	if gphome == "" {
		return fmt.Errorf("GPHOME environment variable not set and --gphome flag not provided\n")
	}

	// Regenerate default flag values if a custom GPHOME or username is passed
	if cmd.Flags().Lookup("gphome").Changed && !cmd.Flags().Lookup("config-file").Changed {
		configFilePath = fmt.Sprintf(defaultConfigFilePath, gphome)
	}
	if cmd.Flags().Lookup("service-user").Changed && !cmd.Flags().Lookup("service-dir").Changed {
		serviceDir = fmt.Sprintf(defaultServiceDir, os.Getenv("USER"))
	}

	if !cmd.Flags().Lookup("host").Changed && !cmd.Flags().Lookup("hostfile").Changed {
		gplog.Error("At least one hostname must be provided using either --host or --hostfile")
		return errors.New("At least one hostname must be provided using either --host or --hostfile")
	}

	// Convert file/directory paths to absolute path before writing to gp.conf file
	err = resolveAbsolutePaths(cmd)
	if err != nil {
		return err
	}

	hostnames, err := getHostnames(hostnames, hostfilePath)
	if err != nil {
		gplog.Error("Error while fetching the hostname from hostfile: %s\nError:%s", hostfilePath, err.Error())
		return err
	}

	err = CreateConfigFile(caCertPath, caKeyPath, serverCertPath, serverKeyPath, hubPort, agentPort, hostnames, hubLogDir, serviceName, serviceDir)
	if err != nil {
		gplog.Error("Error creating config file: %s", err.Error())
		return err
	}

	err = platform.CreateServiceDir(hostnames, serviceDir, gphome)
	if err != nil {
		return err
	}

	err = platform.CreateAndInstallHubServiceFile(gphome, serviceDir, serviceName)
	if err != nil {
		gplog.Error("Error in CreateAndInstallHubServiceFile on Coordinator: %s", err.Error())
		return err
	}

	err = platform.CreateAndInstallAgentServiceFile(hostnames, gphome, serviceDir, serviceName)
	if err != nil {
		gplog.Error("Error in CreateAndInstallAgentServiceFile on Coordinator: %s", err.Error())
		return err
	}

	err = platform.EnableUserLingering(hostnames, gphome, serviceUser)
	if err != nil {
		gplog.Error("Error in EnableUserLingering on Coordinator: %s", err.Error())
		return err
	}
	return nil
}

func resolveAbsolutePaths(cmd *cobra.Command) error {
	paths := []*string{&caCertPath, &caKeyPath, &serverCertPath, &serverKeyPath, &defaultHubLogDir, &gphome}
	for _, path := range paths {
		p, err := filepath.Abs(*path)
		if err != nil {
			return fmt.Errorf("Error resolving absolute path for %s: %w", *path, err)
		}
		*path = p
	}
	return nil
}

func CreateConfigFile(caCertPath, caKeyPath, serverCertPath, serverKeyPath string, hubPort, agentPort int, hostnames []string, hubLogDir, serviceName string, serviceDir string) error {
	creds := utils.GpCredentials{CACertPath: caCertPath, CAKeyPath: caKeyPath, ServerCertPath: serverCertPath, ServerKeyPath: serverKeyPath}
	conf = &hub.Config{Port: hubPort, AgentPort: agentPort, Hostnames: hostnames, LogDir: hubLogDir, ServiceName: serviceName, GpHome: gphome, Credentials: creds}
	configContents, err := json.MarshalIndent(conf, "", "\t")
	if err != nil {
		gplog.Error("Could not generate configuration file: %s", err.Error())
		return fmt.Errorf("Could not generate configuration file: %w", err)
	}
	configHandle, err := os.OpenFile(configFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		gplog.Error("Could not create configuration file %s: %s", configFilePath, err.Error())
		return fmt.Errorf("Could not create configuration file %s: %w\n", configFilePath, err)
	}
	defer configHandle.Close()

	_, err = configHandle.Write(configContents)
	if err != nil {
		gplog.Error("Could not write to configuration file %s: %s", configFilePath, err.Error())
		return fmt.Errorf("Could not write to configuration file %s: %w\n", configFilePath, err)
	}
	gplog.Debug("Wrote configuration file to %s", configFilePath)

	hostList := make([]string, 0)
	for _, host := range hostnames {
		hostList = append(hostList, "-h", host)
	}
	remoteCmd := append(hostList, configFilePath, fmt.Sprintf("=:%s", configFilePath))
	err = exec.Command("/bin/bash", "-c", fmt.Sprintf("source %s/greenplum_path.sh; gpsync %s", gphome, strings.Join(remoteCmd, " "))).Run()
	if err != nil {
		return fmt.Errorf("Could not copy gp.conf file to segment hosts: %w", err)
	}
	gplog.Info("Copied gp.conf file to segment hosts")
	return nil
}

func getHostnames(hostnames []string, hostfilePath string) ([]string, error) {
	if len(hostnames) > 0 {
		return hostnames, nil
	}

	contents, err := os.ReadFile(hostfilePath)
	if err != nil {
		gplog.Error("Could not read hostfile: %s", err.Error())
		return []string{}, fmt.Errorf("Could not read hostfile: %w", err)
	}
	return strings.Fields(string(contents)), nil
}
