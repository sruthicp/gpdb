package greenplum

import (
	"os/exec"

	"github.com/greenplum-db/gpdb/gp/utils"
)

const (
	gpstart = "gpstart"
	gpstop  = "gpstop"
)

type GpStop struct {
	DataDirectory   string
	CoordinatorOnly bool
}

func (cmd *GpStop) BuildExecCommand(gphome string) *exec.Cmd {
	utility := utils.GetGphomeUtilityPath(gphome, gpstop)
	args := []string{"-a"}

	args = utils.AppendIfNotEmpty(args, "-d", cmd.DataDirectory)
	args = utils.AppendIfNotEmpty(args, "--coordinator_only", cmd.CoordinatorOnly)

	return utils.System.ExecCommand(utility, args...)
}

type GpStart struct {
	DataDirectory   string
}

func (cmd *GpStart) BuildExecCommand(gphome string) *exec.Cmd {
	utility := utils.GetGphomeUtilityPath(gphome, gpstart)
	args := []string{"-a"}

	args = utils.AppendIfNotEmpty(args, "-d", cmd.DataDirectory)

	return utils.System.ExecCommand(utility, args...)
}
