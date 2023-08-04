package integration

/*
import (
	"fmt"
	"github.com/greenplum-db/gpdb/gp/test/integration/assertions"
	"os"
	"os/exec"
	"testing"
)

func TestInstallSuccess(t *testing.T) {
	host, _ := os.Hostname()
	testcases := []struct {
		name        string
		option      []string
		logFile     string
		serviceDir  string
		cofigFile   string
		checkConfig func(config GpConfig) GpConfig
	}{
		{
			name:      "install service with --host option",
			option:    []string{"install", "--host", "localhost"},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
				testgpConf.Hostnames = []string{"localhost", host}
				return testgpConf
			},
		},
		{
			name: "install service with host and agent_port option",
			option: []string{"install",
				"--host", "localhost",
				"--agent-port", "8001"},
			cofigFile: defaultConfigurationFile,
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
				path, _ := os.Getwd()
				testgpConf.Hostnames = []string{"localhost"}
				cred := Cred{
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
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
			checkConfig: func(testgpConf GpConfig) GpConfig {
				testgpConf.Hostnames = []string{"localhost"}
				testgpConf.ServiceName = "dummySvc"
				return testgpConf
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("gp", tc.option...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("unexpected error: %#v in test %s", err.Error(), tc.name)
			}
			assertions.AssertEqual(t, 0, cmd.ProcessState.ExitCode())
			assertions.AssertContains(t, successOutput, string(out))
			config := parseConfig(tc.cofigFile)
			testConfig := defaultGPConf
			assertions.AssertEqualValues(t, config, tc.checkConfig(testConfig))
			if tc.logFile == "" {
				tc.logFile = defaultLogFile
			}
			assertions.AssertFileExists(t, tc.logFile)
			if tc.serviceDir == "" {
				tc.serviceDir = defaultServiceDir
			}
			agentFile := fmt.Sprintf("%s/%s_agent.%s", tc.serviceDir, config.ServiceName, serviceExt)
			hubFile := fmt.Sprintf("%s/%s_hub.%s", tc.serviceDir, config.ServiceName, serviceExt)
			for _, file := range []string{agentFile, hubFile} {
				assertions.AssertFileExists(t, file)
			}
			cleanupFiles(tc.cofigFile, tc.logFile, agentFile, hubFile)
		})
	}
}
*/
