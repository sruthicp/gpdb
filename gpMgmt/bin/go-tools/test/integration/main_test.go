package integration

import (
	"fmt"
	"os"
	"testing"

	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
)

const (
	defaultAgentPort   = 8000
	defaultHubPort     = 4242
	defaultLogDir      = "/tmp"
	defaultServiceName = "gp"
	defaultLogFile     = "/tmp/gp_install.log"
)

var (
	defaultConfigurationFile, defaultServiceDir string
	serviceExt, helpTxt                         string
	successOutput                               []string
	defaultGPConf                               testutils.GpConfig
)

func TestMain(m *testing.M) {
	setupTest()
	exitVal := m.Run()
	tearDownTest()

	os.Exit(exitVal)
}

func setupTest() {
	gpHome := os.Getenv("GPHOME")
	path, _ := os.Getwd()
	p := utils.GetPlatform()
	defaultServiceDir = fmt.Sprintf(p.GetDefaultServiceDir(), os.Getenv("USER"))
	serviceExt = p.(utils.GpPlatform).ServiceExt
	defaultConfigurationFile = fmt.Sprintf("%s/gp.conf", gpHome)
	cred := testutils.Cred{
		CaCert:     path,
		CaKey:      path,
		ServerCert: path,
		ServerKey:  path,
	}
	defaultGPConf = testutils.GpConfig{
		Port:        defaultHubPort,
		AgentPort:   defaultAgentPort,
		Hostnames:   []string{},
		LogDir:      defaultLogDir,
		ServiceName: defaultServiceName,
		GpHome:      gpHome,
		Credentials: cred,
	}
	successOutput = []string{
		"[INFO] Copied gp.conf file to segment hosts",
		"[INFO] Created service file directory",
		"[INFO] Wrote hub service file",
		"[INFO] Wrote agent service file",
	}
	helpTemplate := `
Install gp as a systemd daemon

Usage:
  gp install [flags]

Flags:
  --agent-port int              Port on which the agents should listen (default %d)
  --ca-certificate string       Path to SSL/TLS CA certificate
  --ca-key string               Path to SSL/TLS CA private key
  --gphome string               Path to GPDB installation (default %q)
  -h, --help                        help for install
  --host stringArray            Segment hostname
  --hostfile string             Path to file containing a list of segment hostnames
  --hub-port int                Port on which the hub should listen (default %d)
  --log-dir string              Path to gp hub log directory (default %q)
  --server-certificate string   Path to hub SSL/TLS server certificate
  --server-key string           Path to hub SSL/TLS server private key
  --service-dir string          Path to service file directory (default %q)
  --service-name string         Name for the generated systemd service file (default %q)
  --service-user string         User for whom to install the service (default %q)

Global Flags:
  --config-file string   Path to gp configuration file (default %q)
  --verbose              Provide verbose output
`
	helpTxt = fmt.Sprintf(helpTemplate,
		defaultAgentPort,
		defaultConfigurationFile,
		defaultHubPort,
		defaultLogDir,
		defaultServiceDir,
		defaultServiceName,
		os.Getenv("USER"),
		defaultConfigurationFile)
}

func tearDownTest() {
	testutils.RemoveHostfile()
}
