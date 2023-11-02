package postgres

import (
	"bytes"
	"os/exec"
	"path"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/utils"
)

const (
	initdbUtility = "initdb"
	pgCtlUtility  = "pg_ctl"
)

type PgCommand interface {
	buildPgCommand(gphome string) *exec.Cmd
}

func NewPgCommand(pgCmd PgCommand, gphome string) *exec.Cmd {
	return pgCmd.buildPgCommand(gphome)
}

func RunPgCommand(pgCmd PgCommand, gphome string) (*bytes.Buffer, error) {
	stdout := new(bytes.Buffer)
    stderr := new(bytes.Buffer)

	cmd := NewPgCommand(pgCmd, gphome)
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

type Initdb struct {
	PgData         string
	Encoding       string
	Locale         string
	LcCollate      string
	LcCtype        string
	LcMessages     string
	LcMonetory     string
	LcNumeric      string
	LcTime         string
	SharedBuffers  string
	MaxConnections int
	DataChecksums   bool
}

func (cmd *Initdb) buildPgCommand(gphome string) *exec.Cmd {
	utility := GetGphomeUtilityPath(gphome, initdbUtility)
	args := []string{}

	args = AppendIfNotEmpty(args, "--pgdata", cmd.PgData)
	args = AppendIfNotEmpty(args, "--encoding", cmd.Encoding)
	args = AppendIfNotEmpty(args, "--lc-collate", cmd.LcCollate)
	args = AppendIfNotEmpty(args, "--lc-ctype", cmd.LcCtype)
	args = AppendIfNotEmpty(args, "--lc-messages", cmd.LcMessages)
	args = AppendIfNotEmpty(args, "--lc-monetary", cmd.LcMonetory)
	args = AppendIfNotEmpty(args, "--lc-numeric", cmd.LcNumeric)
	args = AppendIfNotEmpty(args, "--lc-time", cmd.LcTime)
	args = AppendIfNotEmpty(args, "--max_connections", cmd.MaxConnections)
	args = AppendIfNotEmpty(args, "--shared_buffers", cmd.SharedBuffers)
	args = AppendIfNotEmpty(args, "--data-checksums", cmd.DataChecksums)

	return utils.System.ExecCommand(utility, args...)
}

type PgCtlStart struct {
	PgData  string
	Timeout int
	Wait    bool
	NoWait  bool
	Logfile string
	Options string
	Mode    string
}

func (cmd *PgCtlStart) buildPgCommand(gphome string) *exec.Cmd {
	utility := GetGphomeUtilityPath(gphome, pgCtlUtility)
	args := []string{"start"}

	args = AppendIfNotEmpty(args, "--pgdata", cmd.PgData)
	args = AppendIfNotEmpty(args, "--timeout", cmd.Timeout)
	args = AppendIfNotEmpty(args, "--wait", cmd.Wait)
	args = AppendIfNotEmpty(args, "--no-wait", cmd.NoWait)
	args = AppendIfNotEmpty(args, "--log", cmd.Logfile)
	args = AppendIfNotEmpty(args, "--options", cmd.Options)
	args = AppendIfNotEmpty(args, "--mode", cmd.Mode)

	return utils.System.ExecCommand(utility, args...)
}

func GetGphomeUtilityPath(gphome, utility string) string {
	return path.Join(gphome, "bin", utility)
}

func AppendIfNotEmpty(args []string, flag string, value interface{}) []string {
	switch value := value.(type) {
	case int:
		if value != 0 {
			args = append(args, flag, strconv.Itoa(value))
		}
	case float64:
		if value != 0 {
			args = append(args, flag, strconv.FormatFloat(value, 'f', -1, 64))
		}
	case string:
		if value != "" {
			args = append(args, flag, value)
		}
	case bool:
		if value {
			args = append(args, flag)
		}
	}

	return args
}
