package integration

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/assertions"
	"os/exec"
	"testing"
)

func TestInstallFailure(t *testing.T) {
	testcases := []struct {
		name     string
		option   []string
		output   []string
		exitcode int
	}{
		{
			name:   "install service with empty value for --host option",
			option: []string{"install", "--host", ""},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 1,
		},
		{
			name:   "install service with invalid value for --host option",
			option: []string{"install", "--host", "invalid"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 1,
		},
		{
			name:   "install service with one valid host and invalid host for --host option",
			option: []string{"install", "--host", "localhost", "--host", "invalid"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 1,
		},
		{
			name:   "install service without any option",
			option: []string{"install"},
			output: []string{
				"At least one hostname must be provided using either --host or --hostfile",
			},
			exitcode: 1,
		},
		{
			name:   "install service with invalid option",
			option: []string{"install", "--invalid"},
			output: []string{
				helpTxt,
			},
			exitcode: 1,
		},
		//		{
		//			name: "install gp with host and agent_port option",
		//			option: []string{"install",
		//				"--host", "localhost",
		//				"--agent-port", "8001"},
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				testgpConf.AgentPort = 8001
		//				testgpConf.Hostnames = []string{"localhost"}
		//				return testgpConf
		//			},
		//		},
		//		{
		//			name: "install gp with host and hub_port option",
		//			option: []string{"install",
		//				"--host", "localhost",
		//				"--hub-port", "8001"},
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				testgpConf.Port = 8001
		//				testgpConf.Hostnames = []string{"localhost"}
		//				return testgpConf
		//			},
		//		},
		//		{
		//			name: "install gp with server and client certificates",
		//			option: []string{"install",
		//				"--ca-certificate", "certificates/ca-cert.pem",
		//				"--ca-key", "certificates/ca-key.pem",
		//				"--server-certificate", "certificates/server-cert.pem",
		//				"--server-key", "certificates/server-key.pem",
		//				"--service-user", "user",
		//				"--host", "localhost",
		//			},
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				path, _ := os.Getwd()
		//				testgpConf.Hostnames = []string{"localhost"}
		//				cred := Cred{
		//					CaCert:     fmt.Sprintf("%s/%s", path, "certificates/ca-cert.pem"),
		//					CaKey:      fmt.Sprintf("%s/%s", path, "certificates/ca-key.pem"),
		//					ServerCert: fmt.Sprintf("%s/%s", path, "certificates/server-cert.pem"),
		//					ServerKey:  fmt.Sprintf("%s/%s", path, "certificates/server-key.pem"),
		//				}
		//				testgpConf.Credentials = cred
		//				return testgpConf
		//			},
		//		},
		//		{
		//			name: "install gp with verbose option",
		//			option: []string{"install",
		//				"--host", "localhost",
		//				"--verbose",
		//			},
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				testgpConf.Hostnames = []string{"localhost"}
		//				return testgpConf
		//			},
		//		},
		//		{
		//			name: "install gp with log_dir option",
		//			option: []string{"install",
		//				"--host", "localhost",
		//				"--log-dir", ".",
		//			},
		//			logFile: "./gp_install.log",
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				testgpConf.LogDir = "."
		//				testgpConf.Hostnames = []string{"localhost"}
		//				return testgpConf
		//			},
		//		},
		//		{
		//			name: "install gp with service-dir option",
		//			option: []string{"install",
		//				"--host", "localhost",
		//				"--service-dir", "/tmp",
		//			},
		//			logFile:    "./gp_install.log",
		//			serviceDir: "/tmp",
		//			checkConfig: func(testgpConf GpConfig) GpConfig {
		//				testgpConf.Hostnames = []string{"localhost"}
		//				return testgpConf
		//			},
		//		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("gp", tc.option...)
			out, _ := cmd.CombinedOutput()

			assertions.AssertEqual(t, tc.exitcode, cmd.ProcessState.ExitCode())
			assertions.AssertContains(t, tc.output, string(out))
		})
	}
}
