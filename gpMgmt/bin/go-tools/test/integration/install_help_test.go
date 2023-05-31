package integration

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/assertions"
	"os/exec"
	"testing"
)

func TestInstallHelp(t *testing.T) {
	testcases := []struct {
		name        string
		option      []string
		expectedOut string
	}{
		{
			name:        "service install shows help with --help",
			option:      []string{"install", "--help"},
			expectedOut: helpTxt,
		},
		{
			name:        "service install shows help with -h",
			option:      []string{"install", "-h"},
			expectedOut: helpTxt,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("gp", tc.option...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}
			assertions.AssertEqual(t, 0, cmd.ProcessState.ExitCode())
			assertions.AssertEqual(t, tc.expectedOut, string(out))
		})
	}
}
