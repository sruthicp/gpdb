package configure

import (
	"fmt"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestConfigureHelp(t *testing.T) {
	Testcases := []struct {
		name        string
		cliParams   []string
		expectedOut []string
	}{
		{
			name:        "service configure shows help with --help",
			cliParams:   []string{"--help"},
			expectedOut: helpTxt,
		},
		{
			name:        "service configure shows help with -h",
			cliParams:   []string{"-h"},
			expectedOut: helpTxt,
		},
	}
	for _, tc := range Testcases {
		t.Run(tc.name, func(t *testing.T) {
			// Running the gp configure command
			result, err := testutils.RunConfigure(tc.cliParams...)
			// check for command result
			if err != nil {
				t.Errorf("\nUnexpected error: %#v", err)
			}
			if result.ExitCode != 0 {
				t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
			}
			for _, item := range tc.expectedOut {
				if !strings.Contains(result.OutputMsg, item) {
					t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
				}
			}
		})
	}
}

func TestConfigureSuccess(t *testing.T) {
	hosts := testutils.GetHostListFromFile(*hostfile)
	agentFile := fmt.Sprintf("%s/%s_%s.%s", defaultServiceDir, constants.DefaultServiceName, "agent", serviceExt)
	hubFile := fmt.Sprintf("%s/%s_%s.%s", defaultServiceDir, constants.DefaultServiceName, "hub", serviceExt)

	t.Run("configure service with --host option", func(t *testing.T) {
		var hostParams []string
		for _, h := range hosts {
			hostParams = append(hostParams, "--host", h)
		}
		tc := TestCase{
			cliParams:   hostParams,
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)

	})

	t.Run("configure service with --hostfile option", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)

	})

	t.Run("configure service with host and agent_port option", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--agent-port", "8001"},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		testConfig.AgentPort = 8001
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with host and hub_port option", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--hub-port", "8001"},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		testConfig.Port = 8001
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with --service-user option", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--service-user", os.Getenv("USER")},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with server and client certificates", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--ca-certificate", "/tmp/certificates/ca-cert.pem",
				"--ca-key", "/tmp/certificates/ca-key.pem",
				"--server-certificate", "/tmp/certificates/server-cert.pem",
				"--server-key", "/tmp/certificates/server-key.pem",
				"--hostfile", *hostfile,
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		cred := &utils.GpCredentials{
			CACertPath:     "/tmp/certificates/ca-cert.pem",
			CAKeyPath:      "/tmp/certificates/ca-key.pem",
			ServerCertPath: "/tmp/certificates/server-cert.pem",
			ServerKeyPath:  "/tmp/certificates/server-key.pem",
		}
		testConfig.Credentials = cred
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with verbose option", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--verbose",
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with config-file option", func(t *testing.T) {
		configFile := "/tmp/gp.conf"
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--config-file", configFile,
			},
			expectedOut: successOutput,
		}
		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(configFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(configFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with changing gphome value", func(t *testing.T) {
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--gphome", os.Getenv("GPHOME"),
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with log_dir option", func(t *testing.T) {
		logDir := "/tmp/log"
		_ = os.MkdirAll(logDir, 0777)
		logFile := fmt.Sprintf("%s/gp_configure.log", logDir)
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--log-dir", logDir,
			},
			expectedOut: successOutput,
		}
		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		testConfig.LogDir = logDir
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		testutils.FilesExistOnHub(t, hubFile, logFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, logFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with service-dir option", func(t *testing.T) {
		serviceDir := "/tmp"
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--service-dir", serviceDir,
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)
		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		agentFile := fmt.Sprintf("%s/%s_%s.%s", serviceDir, config.ServiceName, "agent", serviceExt)
		hubFile := fmt.Sprintf("%s/%s_%s.%s", serviceDir, config.ServiceName, "hub", serviceExt)
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure create service directory if directory given in service-dir option doesn't exist", func(t *testing.T) {
		serviceDir := "/tmp/ServiceDir"
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--service-dir", serviceDir,
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)

		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		agentFile := fmt.Sprintf("%s/%s_%s.%s", serviceDir, config.ServiceName, "agent", serviceExt)
		hubFile := fmt.Sprintf("%s/%s_%s.%s", serviceDir, config.ServiceName, "hub", serviceExt)
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})

	t.Run("configure service with service-name option", func(t *testing.T) {
		svcName := "dummySvc"
		tc := TestCase{
			cliParams: []string{
				"--hostfile", *hostfile,
				"--service-name", svcName,
			},
			expectedOut: successOutput,
		}

		runConfigureAndCheckOutput(t, tc)

		// verify generated configuration
		testConfig := defaultGPConf
		testConfig.Hostnames = hosts
		config := testutils.ParseConfig(testutils.DefaultConfigurationFile)
		testConfig.ServiceName = svcName
		if !reflect.DeepEqual(testConfig, config) {
			t.Errorf("\nExpected: %v \nGot: %v",
				testutils.StructToString(testConfig),
				testutils.StructToString(config))
		}

		// check if log file and service files are created
		agentFile := fmt.Sprintf("%s/%s_%s.%s", defaultServiceDir, config.ServiceName, "agent", serviceExt)
		hubFile := fmt.Sprintf("%s/%s_%s.%s", defaultServiceDir, config.ServiceName, "hub", serviceExt)
		testutils.FilesExistOnHub(t, hubFile, defaultLogFile)
		testutils.FilesExistsOnAgents(t, agentFile, hosts)

		// clean up files after each test cases
		testutils.CleanupFilesOnHub(testutils.DefaultConfigurationFile, defaultLogFile, hubFile)
		testutils.CleanupFilesOnAgents(agentFile, hosts)
	})
}

func runConfigureAndCheckOutput(t *testing.T, tc TestCase) {
	// Running the gp configure command
	result, err := testutils.RunConfigure(tc.cliParams...)
	// check for command result
	if err != nil {
		t.Errorf("\nUnexpected error: %#v", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
	}
	for _, item := range tc.expectedOut {
		if !strings.Contains(result.OutputMsg, item) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
		}
	}
}
