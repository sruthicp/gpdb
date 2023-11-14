package greenplum

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

const (
	gpstart = "gpstart"
	gpstop  = "gpstop"
)

type GpCommand interface {
	buildGpCommand(gphome string) *exec.Cmd
}

func NewGpCommand(gpCmd GpCommand, gphome string) *exec.Cmd {
	cmd := gpCmd.buildGpCommand(gphome)
	gpSourceFile := filepath.Join(gphome, "greenplum_path.sh")

	return utils.System.ExecCommand("bash", "-c", fmt.Sprintf("source %s && %s", gpSourceFile, cmd.String()))
}

func RunGpCommand(gpCmd GpCommand, gphome string) (*bytes.Buffer, error) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	cmd := NewGpCommand(gpCmd, gphome)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	gplog.Debug("Executing command: %s", cmd.String())
	err := cmd.Run()
	if err != nil {
		return stderr, err
	} else {
		return stdout, err
	}
}

type GpStop struct {
	DataDirectory   string
	CoordinatorOnly bool
}

func (cmd *GpStop) buildGpCommand(gphome string) *exec.Cmd {
	utility := postgres.GetGphomeUtilityPath(gphome, gpstop)
	args := []string{"-a"}

	args = postgres.AppendIfNotEmpty(args, "-d", cmd.DataDirectory)
	args = postgres.AppendIfNotEmpty(args, "--coordinator_only", cmd.CoordinatorOnly)

	return utils.System.ExecCommand(utility, args...)
}

type GpStart struct {
	DataDirectory   string
}

func (cmd *GpStart) buildGpCommand(gphome string) *exec.Cmd {
	utility := postgres.GetGphomeUtilityPath(gphome, gpstart)
	args := []string{"-a"}

	args = postgres.AppendIfNotEmpty(args, "-d", cmd.DataDirectory)

	return utils.System.ExecCommand(utility, args...)
}
