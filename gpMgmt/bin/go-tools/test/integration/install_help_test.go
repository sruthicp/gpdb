package integration

import (
	"testing"

	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
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
			out, rc, err := testutils.RunInstall(tc.option...)
			testutils.Equal(t, nil, err)
			testutils.Equal(t, 0, rc)
			testutils.Equal(t, tc.expectedOut, out)
		})
	}
}
