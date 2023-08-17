package testutils

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
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

const (
	Hostfile = "hostlist"
)

func RunInstall(option ...string) (string, int, error) {
	cmd := exec.Command("gp", option...)
	out, err := cmd.CombinedOutput()
	return string(out), cmd.ProcessState.ExitCode(), err
}

func ParseConfig(configFile string) (gpConfig GpConfig) {
	config, _ := os.Open(configFile)
	byteValue, _ := io.ReadAll(config)
	_ = json.Unmarshal(byteValue, &gpConfig)
	return
}

func SetDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

func CleanupFiles(files ...string) {
	for _, f := range files {
		os.Remove(f)
	}
}

func GenerateFilePath(serviceDir, serviceName, serviceExt, fileType string) string {
	return fmt.Sprintf("%s/%s_%s.%s", serviceDir, serviceName, fileType, serviceExt)
}

func CreateHostfile(content []byte) {
	_ = os.WriteFile(Hostfile, content, 0644)
}

func RemoveHostfile() {
	_ = os.Remove(Hostfile)
}
