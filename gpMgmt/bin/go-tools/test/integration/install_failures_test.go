package integration

import (
	"testing"

	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
)

func TestInstallFailure(t *testing.T) {
	testutils.CreateHostfile([]byte(""))
	testcases := []struct {
		name     string
		option   []string
		output   []string
		exitcode int
		//additional_testsetup func()
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
			name:   "install service with no value for --host option",
			option: []string{"install", "--host"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 2,
		},
		{
			name:   "install service with empty file for --hostfile option",
			option: []string{"install", "--hostfile", testutils.Hostfile},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 1,
		},
		{
			name:   "install service with no value for --hostfile option",
			option: []string{"install", "--hostfile"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 2,
		},
		{
			name:   "install service with non-existing host for --host option",
			option: []string{"install", "--host", "host"},
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
		{
			name: "install service with both host and hostfile options",
			option: []string{"install", "--host", "localhost",
				"--hostfile", "abc"},
			output: []string{
				"[ERROR] if any flags in the group [host hostfile] are set none of the others can be; [host hostfile] were all set",
			},
			exitcode: 1,
		},
		{
			name: "install service with string value for --agent-port option",
			option: []string{"install", "--host", "localhost",
				"--agent-port", "abc"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 2,
		},
		{
			name: "install service with string value for --hub-port option",
			option: []string{"install", "--host", "localhost",
				"--hub-port", "abc"},
			output: []string{
				"Error creating config file: Could not copy gp.conf file to segment hosts",
				"Could not copy gp.conf file to segment hosts",
			},
			exitcode: 2,
		},
		{
			name: "install service with non-existing directory as service-dir value",
			option: []string{"install",
				"--host", "localhost",
				"--service-dir", "/newDir/Service-dir",
			},
			output: []string{
				"Could not create service directory /newDir/Service-dir on hosts",
			},
			exitcode: 1,
		},
		{
			name: "install service with no value for service-dir option",
			option: []string{"install",
				"--host", "localhost",
				"--service-dir",
			},
			exitcode: 2,
		},
		{
			name: "install service with non-existing directory as log-dir value",
			option: []string{"install",
				"--host", "localhost",
				"--log-dir", "/newDir/log_dir",
			},
			output: []string{
				"no such file or directory",
			},
			exitcode: 2,
		},
		{
			name: "install service with no value for log-dir option",
			option: []string{"install",
				"--host", "localhost",
				"--log-dir",
			},
			output: []string{
				"no such file or directory",
			},
			exitcode: 2,
		},
		{
			name: "install fails when value for both --agent-port and --hub-port are same",
			option: []string{"install",
				"--host", "localhost",
				"--agent-port", "2000",
				"--hub-port", "2000",
			},
			exitcode: 1,
		},
		{
			name: "install service fails when --gphome value is invalid",
			option: []string{"install",
				"--host", "localhost",
				"--gphome", "invalid",
			},
			exitcode: 1,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			out, rc, err := testutils.RunInstall(tc.option...)
			testutils.NotNil(t, err)
			testutils.Equal(t, tc.exitcode, rc)
			testutils.Contains(t, tc.output, out)
		})
	}
}
