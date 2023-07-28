package integration

import (
	"context"
	"errors"
	"github.com/cucumber/godog"
	"os/exec"
	"strings"
)

type installSvc struct {
	ctx context.Context
}

func (i *installSvc) gpInstallShouldReturnReturnCode(arg1 int) error {
	cmd := i.ctx.Value("command").(*exec.Cmd)
	if arg1 != cmd.ProcessState.ExitCode() {
		return errors.New("return code mismatch")
	}
	return nil
}

func (i *installSvc) userRun(arg1 string) error {
	args := strings.Split(arg1, " ")
	cmd := exec.Command(args[0], args[1:]...)
	_, err := cmd.CombinedOutput()
	i.ctx = context.WithValue(i.ctx, "command", cmd)
	return err
}

func (i *installSvc) gpInstallShouldPrint(arg1 string) error {
	return godog.ErrPending
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	iSvc := &installSvc{ctx: context.Background()}
	ctx.Step(`^gp install should return return code (\d+)$`, iSvc.gpInstallShouldReturnReturnCode)
	ctx.Step(`^user run "([^"]*)"$`, iSvc.userRun)
}
