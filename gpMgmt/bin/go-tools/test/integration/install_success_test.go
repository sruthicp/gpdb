package integration

import (
	"fmt"
	"os"
	"testing"

	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
)

func TestInstallSuccess(t *testing.T) {
	host, _ := os.Hostname()
	testutils.CreateHostfile([]byte(host))
	testcases := []struct {
		name        string
		option      []string
		logFile     string
		serviceDir  string
		cofigFile   string
		checkConfig func(config testutils.GpConfig) testutils.GpConfig
	}{
		{
			name:      "install service with --host option",
			option:    []string{"install", "--host", "localhost"},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with multiple --host option",
			option: []string{"install",
				"--host", "localhost",
				"--host", host,
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost", host}
				return testgpConf
			},
		},
		{
			name: "install service with --hostfile option",
			option: []string{"install",
				"--hostfile", "hosts",
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{host}
				return testgpConf
			},
		},
		{
			name: "install service with host and agent_port option",
			option: []string{"install",
				"--host", "localhost",
				"--agent-port", "8001"},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.AgentPort = 8001
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with host and hub_port option",
			option: []string{"install",
				"--host", "localhost",
				"--hub-port", "8001"},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Port = 8001
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with server and client certificates",
			option: []string{"install",
				"--ca-certificate", "certificates/ca-cert.pem",
				"--ca-key", "certificates/ca-key.pem",
				"--server-certificate", "certificates/server-cert.pem",
				"--server-key", "certificates/server-key.pem",
				"--service-user", "user",
				"--host", "localhost",
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				path, _ := os.Getwd()
				testgpConf.Hostnames = []string{"localhost"}
				cred := testutils.Cred{
					CaCert:     fmt.Sprintf("%s/%s", path, "certificates/ca-cert.pem"),
					CaKey:      fmt.Sprintf("%s/%s", path, "certificates/ca-key.pem"),
					ServerCert: fmt.Sprintf("%s/%s", path, "certificates/server-cert.pem"),
					ServerKey:  fmt.Sprintf("%s/%s", path, "certificates/server-key.pem"),
				}
				testgpConf.Credentials = cred
				return testgpConf
			},
		},
		{
			name: "install service with verbose option",
			option: []string{"install",
				"--host", "localhost",
				"--verbose",
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with config-file option",
			option: []string{"install",
				"--host", "localhost",
				"--config-file", "./gp.conf",
			},
			cofigFile: "./gp.conf",
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with changing gphome value",
			option: []string{"install",
				"--host", "localhost",
				"--gphome", os.Getenv("GPHOME"),
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with log_dir option",
			option: []string{"install",
				"--host", "localhost",
				"--log-dir", ".",
			},
			logFile:   "./gp_install.log",
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.LogDir = "."
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with service-dir option",
			option: []string{"install",
				"--host", "localhost",
				"--service-dir", "/tmp",
			},
			cofigFile:  defaultConfigurationFile,
			serviceDir: "/tmp",
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				return testgpConf
			},
		},
		{
			name: "install service with service-name option",
			option: []string{"install",
				"--host", "localhost",
				"--service-name", "dummySvc",
			},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf testutils.GpConfig) testutils.GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				testgpConf.ServiceName = "dummySvc"
				return testgpConf
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// Running the gp install command
			out, rc, err := testutils.RunInstall(tc.option...)
			// check for command result
			testutils.Equal(t, nil, err)
			testutils.Equal(t, 0, rc)
			testutils.Contains(t, successOutput, out)
			// check for configuration changes
			config := testutils.ParseConfig(tc.cofigFile)
			testConfig := defaultGPConf
			testutils.EqualValues(t, config, tc.checkConfig(testConfig))
			// check if log file is created
			tc.logFile = testutils.SetDefault(tc.logFile, defaultLogFile)
			testutils.FileExists(t, tc.logFile)

			// check if service files are created
			tc.serviceDir = testutils.SetDefault(tc.serviceDir, defaultServiceDir)
			agentFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "agent")
			hubFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "hub")
			testutils.ServiceFilesExist(t, agentFile, hubFile)

			// clean up files after each test cases
			testutils.CleanupFiles(tc.cofigFile, tc.logFile, agentFile, hubFile)
		})
	}
}
