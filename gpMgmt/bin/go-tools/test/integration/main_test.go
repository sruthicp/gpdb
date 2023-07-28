package integration

import (
	"encoding/json"
	"fmt"
	"github.com/greenplum-db/gpdb/gp/utils"
	"io"
	"os"
	"testing"
)

var (
	defaultConfigurationFile, defaultLogFile, defaultServiceDir string
	serviceExt, helpTxt                                         string
	successOutput                                               []string
	defaultGPConf                                               GpConfig
)

type Cred struct {
	CaCert     string `json:"caCert"`
	CaKey      string `json:"caKey"`
	ServerCert string `json:"serverCert"`
	ServerKey  string `json:"serverKey"`
}
type GpConfig struct {
	Port        int      `json:"hubPort"`
	AgentPort   int      `json:"agentPort"`
	Hostnames   []string `json:"hostnames"`
	LogDir      string   `json:"hubLogDir"`
	ServiceName string   `json:"serviceName"`
	GpHome      string   `json:"gphome"`
	Credentials Cred     `json:"Credentials"`
}

func TestMain(m *testing.M) {
	SetupTest()
	exitVal := m.Run()
	//TearDownTest()

	os.Exit(exitVal)
}

func SetupTest() {
	gpHome := os.Getenv("GPHOME")
	path, _ := os.Getwd()
	p := utils.GetPlatform()
	defaultServiceDir = fmt.Sprintf(p.GetDefaultServiceDir(), os.Getenv("USER"))
	serviceExt = p.(utils.GpPlatform).ServiceExt
	defaultConfigurationFile = fmt.Sprintf("%s/gp.conf", gpHome)
	cred := Cred{
		CaCert:     path,
		CaKey:      path,
		ServerCert: path,
		ServerKey:  path,
	}
	defaultGPConf = GpConfig{
		Port:        4242,
		AgentPort:   8000,
		Hostnames:   []string{},
		LogDir:      "/tmp",
		ServiceName: "gp",
		GpHome:      gpHome,
		Credentials: cred,
	}
	successOutput = []string{
		"[INFO] Copied gp.conf file to segment hosts",
		"[INFO] Created service file directory",
		"[INFO] Wrote hub service file",
		"[INFO] Wrote agent service file",
	}
	defaultLogFile = "/tmp/gp_install.log"
	helpTemplate := `Install gp as a systemd daemon

Usage:
  gp install [flags]

Flags:
      --agent-port int              Port on which the agents should listen (default 8000)
      --ca-certificate string       Path to SSL/TLS CA certificate
      --ca-key string               Path to SSL/TLS CA private key
      --gphome string               Path to GPDB installation (default "/usr/local/gpdb")
  -h, --help                        help for install
      --host stringArray            Segment hostname
      --hostfile string             Path to file containing a list of segment hostnames
      --hub-port int                Port on which the hub should listen (default 4242)
      --log-dir string              Path to gp hub log directory (default "/tmp")
      --server-certificate string   Path to hub SSL/TLS server certificate
      --server-key string           Path to hub SSL/TLS server private key
      --service-dir string          Path to service file directory (default %q)
      --service-name string         Name for the generated systemd service file (default "gp")
      --service-user string         User for whom to install the service (default %q)

Global Flags:
      --config-file string   Path to gp configuration file (default %q)
      --verbose              Provide verbose output
`
	helpTxt = fmt.Sprintf(helpTemplate, defaultServiceDir, os.Getenv("USER"), defaultConfigurationFile)
}

//func TearDownTest() {
//}

func parseConfig(cofig_file string) (gp_config GpConfig) {
	config, _ := os.Open(cofig_file)
	byteValue, _ := io.ReadAll(config)
	_ = json.Unmarshal(byteValue, &gp_config)
	return
}

func cleanupFiles(file ...string) {
	for _, f := range file {
		os.Remove(f)
	}
}
