package start

import (
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"strings"
	"testing"
)

var gpCfg hub.Config

type StartSuccessTC struct {
	name            string
	cliParams       []string
	expectedOut     []string
	serviceName     []string
	additionalSetup func()
	cleanup         func()
	IsSingleHost    bool
	IsMultiHost     bool
}

var StartSuccessTestCases = []StartSuccessTC{
	{
		name: "start hub successfully",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"[INFO] Hub gp started successfully",
		},
		serviceName: []string{"gp_hub"},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		cleanup: func() {
			_, _, _ = testutils.RunStop("hub")
		},
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start hub and agents successfully",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"[INFO] Hub gp started successfully",
			"[INFO] Agents gp started successfully",
		},
		serviceName: []string{"gp_agent", "gp_hub"},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		cleanup: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start hub after gp configure with --service-name param",
		cliParams: []string{
			"services",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunConfigure(append(
				[]string{
					"--hostfile", testutils.Hostfile,
					"--service-name", "dummySvc",
				},
				testutils.CertificateParams...)...)
		},
		serviceName: []string{"dummySvc_hub", "dummySvc_agent"},
		expectedOut: []string{
			"[INFO] Hub dummySvc started successfully",
			"[INFO] Agents dummySvc started successfully",
		},
		cleanup: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start command with invalid params shows help",
		cliParams: []string{
			"invalid",
		},
		expectedOut: append([]string{
			"Start hub, agents services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start command without additional cli params shows help",
		expectedOut: append([]string{
			"Start hub, agents services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start command with --help params shows help",
		cliParams: []string{
			"--help",
		},
		expectedOut: append([]string{
			"Start hub, agents services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start command with -h params shows help",
		cliParams: []string{
			"-h",
		},
		expectedOut: append([]string{
			"Start hub, agents services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start hub with -h params shows help",
		cliParams: []string{
			"hub", "-h",
		},
		expectedOut: append([]string{
			"Start the hub",
		}, testutils.CommonHelpText...),
	},
	{
		name: "start hub with --help params shows help",
		cliParams: []string{
			"hub", "--help",
		},
		expectedOut: append([]string{
			"Start the hub",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start agents with -h params shows help",
		cliParams: []string{
			"agents", "-h",
		},
		expectedOut: append([]string{
			"Start the agents",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start agents with --help params shows help",
		cliParams: []string{
			"agents", "--help",
		},
		expectedOut: append([]string{
			"Start the agents",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start services with -h params shows help",
		cliParams: []string{
			"services", "-h",
		},
		expectedOut: append([]string{
			"Start hub and agent services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start services with --help params shows help",
		cliParams: []string{
			"services", "--help",
		},
		expectedOut: append([]string{
			"Start hub and agent services",
		}, testutils.CommonHelpText...),
		IsMultiHost:  true,
		IsSingleHost: true,
	},
	{
		name: "start services with --verbose param shows sevice status",
		cliParams: []string{
			"services", "--verbose",
		},
		serviceName: []string{"gp_agent", "gp_hub"},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiHost:  true,
		IsSingleHost: true,
		expectedOut: []string{
			"[INFO] Hub gp started successfully",
			"[INFO] Agents gp started successfully",
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
		},
		cleanup: func() {
			_, _, _ = testutils.RunStop("services")
		},
	},
	{
		name: "start hub with --verbose param shows hub status",
		cliParams: []string{
			"hub", "--verbose",
		},
		serviceName: []string{"gp_hub"},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiHost:  true,
		IsSingleHost: true,
		expectedOut: []string{
			"[INFO] Hub gp started successfully",
			"Hub", "running",
		},
		cleanup: func() {
			_, _, _ = testutils.RunStop("hub")
		},
	},
}

func TestSingleHostStartSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StartSuccessTestCases {
		if tc.IsSingleHost {
			runSuccessTestcases(t, tc, strings.Split(testutils.DefaultHost, "\n"))
		}
	}
}

func TestMultiHostStartSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StartSuccessTestCases {
		if tc.IsMultiHost {
			runSuccessTestcases(t, tc, strings.Split(testutils.MultiHosts, "\n"))
		}
	}
}

func runSuccessTestcases(t *testing.T, tc StartSuccessTC, hosts []string) {
	t.Run(tc.name, func(t *testing.T) {
		var listeningPort int
		if tc.additionalSetup != nil {
			tc.additionalSetup()
		}
		// Running the gp start command
		out, rc, err := testutils.RunStart(tc.cliParams...)
		// check for command result
		testutils.Equal(t, nil, err)
		testutils.Equal(t, 0, rc)
		testutils.Contains(t, tc.expectedOut, out)

		gpCfg = testutils.ParseConfig(testutils.DefaultConfigurationFile)

		// check if service is running
		for _, svc := range tc.serviceName {
			hostList := make([]string, 0)
			if strings.Contains(svc, "_hub") {
				listeningPort = gpCfg.Port
				hostList = hosts[:1]
			} else if strings.Contains(svc, "_agent") {
				listeningPort = gpCfg.AgentPort
				hostList = hosts
			}
			for _, host := range hostList {
				status, _, _ := testutils.GetSvcStatusOnHost(p.(utils.GpPlatform), svc, host)
				testutils.VerifyServicePIDOnPort(t, status, listeningPort, host)
			}
		}
		if tc.cleanup != nil {
			tc.cleanup()
		}
	})
}
