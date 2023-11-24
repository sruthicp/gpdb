package agent_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/agent"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/testutils"
	"github.com/greenplum-db/gpdb/gp/testutils/exectest"
	"github.com/greenplum-db/gpdb/gp/utils"
)

func init() {
	exectest.RegisterMains()
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func TestMakeSegment(t *testing.T) {
	testhelper.SetupTestLogger()

	agentServer := agent.New(agent.Config{
		GpHome: "gphome",
	})

	request := &idl.MakeSegmentRequest{
		Segment: &idl.Segment{
			Port:          1234,
			DataDirectory: "/data/gpseg",
			HostName:      "sdw",
			HostAddress:   "sdw",
			Contentid:     0,
			Dbid:          1,
		},
		Locale: &idl.Locale{},
		SegConfig: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		HbaHostNames: true,
	}

	t.Run("successfully creates a coordinator segment", func(t *testing.T) {
		var initdbCalled bool
		var initdbArgs []string

		utils.System.ExecCommand = exectest.NewCommandWithVerifier(exectest.Success, func(utility string, args ...string) {
			initdbCalled = true
			expectedUtility := "gphome/bin/initdb"
			if utility != expectedUtility {
				t.Fatalf("got %s, want %s", utility, expectedUtility)
			}
			
			initdbArgs = args
		})
		defer utils.ResetSystemFunctions()
		
		utils.System.CurrentUser = func() (*user.User, error) {
			return &user.User{Username: "gpadmin"}, nil
		}
		defer utils.ResetSystemFunctions()

		dname, err := os.MkdirTemp("", "gpseg")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.RemoveAll(dname)

		postgresqlConf := creatTempConfFile(t, dname, "postgresql.conf", 0644)
		pgHbaConf := creatTempConfFile(t, dname, "pg_hba.conf", 0644)
		pgInternalConf := filepath.Join(dname, "internal.auto.conf")

		request.Segment.DataDirectory = dname
		request.Segment.Contentid = -1
		_, err = agentServer.MakeSegment(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		
		if !initdbCalled {
			t.Fatalf("expected initdb to be called")
		}
		expectedArgs := []string{"--pgdata", dname, "--data-checksums"}
		if !reflect.DeepEqual(initdbArgs, expectedArgs) {
			t.Fatalf("got %+v, want %+v", initdbArgs, expectedArgs)
		}
		
		expectedPostgresqlConf := fmt.Sprintf(`listen_addresses = '*'
gp_contentid = %d
log_statement = 'all'
key1 = 'value1'
key2 = 'value2'
port = %d
`, request.Segment.Contentid, request.Segment.Port)

		expectedPgHbaConf := fmt.Sprintf(`local	all	gpadmin	ident
host	all	gpadmin	localhost	trust
host	all	gpadmin	%[1]s	trust
local	replication	gpadmin	ident
host	replication	gpadmin	samehost	trust
host	replication	gpadmin	%[1]s	trust
`, request.Segment.HostName)

		expectedPgInternalConf := fmt.Sprintf("gp_dbid = %d", request.Segment.Dbid)

		testutils.AssertFileContentsUnordered(t, postgresqlConf, expectedPostgresqlConf)
		testutils.AssertFileContents(t, pgInternalConf, expectedPgInternalConf)
		testutils.AssertFileContents(t, pgHbaConf, expectedPgHbaConf)
	})
	
	t.Run("errors out when initdb fails to run", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()
		
		expectedErrPrefix := "executing initdb:"
		_, err := agentServer.MakeSegment(context.Background(), request)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %v", err, expectedErrPrefix)
		}
		
		if e, ok := err.(*exec.ExitError); ok && !e.Success() {
			t.Fatalf("got %v, want non zero exit code", err)
		}
	})

	t.Run("errors out when it fails to update the postgresql.conf", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()
		
		dname, err := os.MkdirTemp("", "gpseg")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.RemoveAll(dname)

		creatTempConfFile(t, dname, "postgresql.conf", 0000)
		
		request.Segment.DataDirectory = dname
		_, err = agentServer.MakeSegment(context.Background(), request)
		
		expectedErrPrefix := "updating postgresql.conf:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %v", err, expectedErrPrefix)
		}
		
		expectedErr := os.ErrPermission
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
	})

	t.Run("errors out when it fails to create internal.auto.conf", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()
		
		dname, err := os.MkdirTemp("", "gpseg")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.RemoveAll(dname)

		creatTempConfFile(t, dname, "postgresql.conf", 0644)
		creatTempConfFile(t, dname, "internal.auto.conf", 0000)
		
		request.Segment.DataDirectory = dname
		_, err = agentServer.MakeSegment(context.Background(), request)
		
		expectedErrPrefix := "creating internal.auto.conf:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %v", err, expectedErrPrefix)
		}
		
		expectedErr := os.ErrPermission
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
	})

	t.Run("errors out when it fails to update pg_hba.conf", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()
		
		dname, err := os.MkdirTemp("", "gpseg")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.RemoveAll(dname)

		creatTempConfFile(t, dname, "postgresql.conf", 0644)
		creatTempConfFile(t, dname, "pg_hba.conf", 0000)
		
		request.Segment.DataDirectory = dname
		_, err = agentServer.MakeSegment(context.Background(), request)
		
		expectedErrPrefix := "updating pg_hba.conf:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %v", err, expectedErrPrefix)
		}
		
		expectedErr := os.ErrPermission
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
	})
}

func creatTempConfFile(t *testing.T, dname string, filename string, perm os.FileMode) string {
	t.Helper()
	
	confPath := filepath.Join(dname, filename)
	err := os.WriteFile(confPath, []byte(""), perm)
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	
	return confPath
}
