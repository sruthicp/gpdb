package status

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"os"
	"strings"
	"testing"
)

func TestStatusFailures(t *testing.T) {
	t.Run("checking service status without configuration file will fail", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")
		_ = testutils.CopyFile(testutils.DefaultConfigurationFile, "/tmp/config.conf")
		_ = os.RemoveAll(testutils.DefaultConfigurationFile)

		tc := TestCase{
			cliParams: []string{
				"services",
			},
			expectedOut: []string{
				"could not open config file",
			},
		}

		result, err := testutils.RunStatus(tc.cliParams...)
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

		_, _ = testutils.RunStop("services", "--config-file", "/tmp/config.conf")
	})

	t.Run("checking status of agents will fail if hub is not running", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		tc := TestCase{
			cliParams: []string{
				"agents",
			},
			expectedOut: []string{
				"could not connect to hub",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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

	t.Run("checking status of services after stopping hub will fail", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		tc := TestCase{
			cliParams: []string{
				"services",
			},
			expectedOut: []string{
				"Hub", "not running", "0",
				"could not connect to hub",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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

	t.Run("checking status of agents without certificates", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")
		_ = testutils.CpCfgWithoutCertificates(configCopy)

		tc := TestCase{
			cliParams: []string{
				"agents", "--config-file", configCopy,
			},
			expectedOut: []string{
				"error while loading server certificate",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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
		_, _ = testutils.RunStop("services")
	})

	t.Run("checking status of services without certificates", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")
		_ = testutils.CpCfgWithoutCertificates(configCopy)

		tc := TestCase{
			cliParams: []string{
				"services", "--config-file", configCopy,
			},
			expectedOut: []string{
				"error while loading server certificate",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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
		_, _ = testutils.RunStop("services")
	})

	t.Run("checking service status with no value for --config-file will fail", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")

		tc := TestCase{
			cliParams: []string{
				"services", "--config-file",
			},
			expectedOut: []string{
				"flag needs an argument: --config-file",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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
		_, _ = testutils.RunStop("services")
	})

	t.Run("checking service status with non-existing file for --config-file will fail", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")

		tc := TestCase{
			cliParams: []string{
				"services", "--config-file", "file",
			},
			expectedOut: []string{
				"no such file or directory",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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
		_, _ = testutils.RunStop("services")
	})

	t.Run("checking service status with empty string for --config-file will fail", func(t *testing.T) {
		testutils.InitService(*hostfile, testutils.CertificateParams)
		_, _ = testutils.RunStart("services")

		tc := TestCase{
			cliParams: []string{
				"services", "--config-file", "",
			},
			expectedOut: []string{
				"no such file or directory",
			},
		}
		result, err := testutils.RunStatus(tc.cliParams...)
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
		_, _ = testutils.RunStop("services")
	})
}
