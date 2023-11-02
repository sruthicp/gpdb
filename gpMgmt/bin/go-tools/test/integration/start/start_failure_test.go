package start

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"os"
	"strings"
	"testing"
)

func TestStartFailWithoutConfig(t *testing.T) {
	t.Run("starting services without configuration file will fail", func(t *testing.T) {
		_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		tc := TestCase{
			cliParams: []string{
				"services",
			},
			expectedOut: []string{
				"could not open config file",
				"no such file or directory",
			},
		}
		result, err := testutils.RunStart(tc.cliParams...)
		if err == nil {
			t.Errorf("\nExpected error Got: %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		for _, item := range tc.expectedOut {
			if !strings.Contains(result.OutputMsg, item) {
				t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
			}
		}
	})

	t.Run("starting hub without service file", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		testutils.DisableandDeleteServiceFiles(p)
		tc := TestCase{
			cliParams: []string{
				"hub",
			},
			expectedOut: []string{
				"failed to start hub service",
			},
		}
		result, err := testutils.RunStart(tc.cliParams...)
		if err == nil {
			t.Errorf("\nExpected error Got: %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		for _, item := range tc.expectedOut {
			if !strings.Contains(result.OutputMsg, item) {
				t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
			}
		}
	})

	t.Run("starting agents without certificates", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_ = testutils.CpCfgWithoutCertificates(configCopy)
		tc := TestCase{
			cliParams: []string{
				"agents", "--config-file", configCopy,
			},
			expectedOut: []string{
				"error while loading server certificate",
			},
		}
		result, err := testutils.RunStart(tc.cliParams...)
		if err == nil {
			t.Errorf("\nExpected error Got: %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		for _, item := range tc.expectedOut {
			if !strings.Contains(result.OutputMsg, item) {
				t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
			}
		}
	})

	t.Run("starting services without ca-certificates", func(t *testing.T) {
		_, _ = testutils.RunConfigure(append(
			[]string{
				"--hostfile", *hostfile,
			},
			testutils.CertificateParams[4:]...)...)
		tc := TestCase{
			cliParams: []string{
				"agents",
			},
			expectedOut: []string{
				"error while loading server certificate",
			},
		}
		result, err := testutils.RunStart(tc.cliParams...)
		if err == nil {
			t.Errorf("\nExpected error Got: %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		for _, item := range tc.expectedOut {
			if !strings.Contains(result.OutputMsg, item) {
				t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
			}
		}
	})

	t.Run("starting services without server-certificates", func(t *testing.T) {
		_, _ = testutils.RunConfigure(append(
			[]string{
				"--hostfile", *hostfile,
			},
			testutils.CertificateParams[:4]...)...)
		tc := TestCase{
			cliParams: []string{
				"agents",
			},
			expectedOut: []string{
				"error while loading server certificate",
			},
		}
		result, err := testutils.RunStart(tc.cliParams...)
		if err == nil {
			t.Errorf("\nExpected error Got: %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		for _, item := range tc.expectedOut {
			if !strings.Contains(result.OutputMsg, item) {
				t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
			}
		}
	})
}

func TestStartGlobalFlagsFailures(t *testing.T) {
	failTestCases := []struct {
		name        string
		cliParams   []string
		expectedOut []string
	}{
		{
			name: "starting agents without starting hub will fail",
			cliParams: []string{
				"agents",
			},
			expectedOut: []string{
				"could not connect to hub",
			},
		},
		{
			name: "starting services with no value for --config-file will fail",
			cliParams: []string{
				"services", "--config-file",
			},
			expectedOut: []string{
				"flag needs an argument: --config-file",
			},
		},
		{
			name: "starting services with non-existing file for --config-file will fail",
			cliParams: []string{
				"services", "--config-file", "file",
			},
			expectedOut: []string{
				"no such file or directory",
			},
		},
		{
			name: "starting services with empty string for --config-file will fail",
			cliParams: []string{
				"services", "--config-file", "",
			},
			expectedOut: []string{
				"no such file or directory",
			},
		},
	}

	for _, tc := range failTestCases {
		t.Run(tc.name, func(t *testing.T) {
			testutils.InitService(*hostfile, testutils.CertificateParams)

			result, err := testutils.RunStart(tc.cliParams...)
			if err == nil {
				t.Errorf("\nExpected error Got: %#v", err)
			}
			if result.ExitCode != testutils.ExitCode1 {
				t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
			}
			for _, item := range tc.expectedOut {
				if !strings.Contains(result.OutputMsg, item) {
					t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
				}
			}
		})
		_, _ = testutils.RunStop("services")
	}
}

//func testFailureCases(t *testing.T, tc TestCase) {
//	result, err := testutils.RunStart(tc.cliParams...)
//	if err == nil {
//		t.Errorf("\nExpected error Got: %#v", err)
//	}
//	if result.ExitCode != testutils.ExitCode1 {
//		t.Errorf("\nExpected: %#v \nGot: %v", testutils.ExitCode1, result.ExitCode)
//	}
//	for _, item := range tc.expectedOut {
//		if !strings.Contains(result.OutputMsg, item) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, result.OutputMsg)
//		}
//	}
//}
