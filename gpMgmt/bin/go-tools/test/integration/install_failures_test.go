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
		//{
		//	name: "install service with string value for --agent-port option",
		//	option: []string{"install", "--host", "localhost",
		//		"--agent-port", "abc"},
		//	output: []string{
		//		"Error creating config file: Could not copy gp.conf file to segment hosts",
		//		"Could not copy gp.conf file to segment hosts",
		//	},
		//	exitcode: 1,
		//},
		//{
		//	name: "install service with string value for --hub-port option",
		//	option: []string{"install", "--host", "localhost",
		//		"--hub-port", "abc"},
		//	output: []string{
		//		"Error creating config file: Could not copy gp.conf file to segment hosts",
		//		"Could not copy gp.conf file to segment hosts",
		//	},
		//	exitcode: 1,
		//},
		{
			name: "install service with both host and hostfile options",
			option: []string{"install", "--host", "localhost",
				"--hostfile", "abc"},
			output: []string{
				"[ERROR] if any flags in the group [host hostfile] are set none of the others can be; [host hostfile] were all set",
			},
			exitcode: 1,
		},
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
