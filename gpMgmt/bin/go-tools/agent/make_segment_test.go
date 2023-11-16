package agent_test

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	agent "github.com/greenplum-db/gpdb/gp/agent"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/testutils/exectest"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func init() {
	exectest.RegisterMains(
		InitdbFailure,
	)
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func setMocks() {
	agent.UpdatePostgresqlConf = nil
	agent.CreatePostgresInternalConf = nil
	agent.UpdateCoordinatorPgHbaConf = nil
	agent.UpdateSegmentPgHbaConf = nil
}

func resetMocks() {
	agent.UpdatePostgresqlConf = postgres.UpdatePostgresqlConf
	agent.CreatePostgresInternalConf = postgres.CreatePostgresInternalConf
	agent.UpdateCoordinatorPgHbaConf = postgres.UpdateCoordinatorPgHbaConf
	agent.UpdateSegmentPgHbaConf = postgres.UpdateSegmentPgHbaConf
}

func TestMakeSegment(t *testing.T) {
	testhelper.SetupTestLogger()
	
	agentServer := agent.New(agent.Config{
		GpHome: "gphome",
	})
	
	request := &idl.MakeSegmentRequest{
		Segment: &idl.Segment{
			Port: 1234,
			DataDirectory: "/data/gpseg",
			HostName: "sdw",
			HostAddress: "sdw",
			Contentid: -1,
			Dbid: 1,
			
		},
		Locale: &idl.Locale{},
		SegConfig: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	t.Run("successfully creates a coordinator segment", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()
		
		setMocks()
		defer resetMocks()
		agent.UpdatePostgresqlConf = func(pgdata string, configParams map[string]string, overwrite bool) error {
			expectedConfigParams := map[string]string{
				"port": strconv.Itoa(int(request.Segment.Port)),
				"listen_addresses": "*",
				"gp_contentid": strconv.Itoa(int(request.Segment.Contentid)),
				"log_statement": "all",
				"key1": "value1",
				"key2": "value2",
			}
			
			if !reflect.DeepEqual(configParams, expectedConfigParams) {
				t.Fatalf("got %+v, want %+v", configParams, expectedConfigParams)
			}
			
			return nil
		}
		agent.CreatePostgresInternalConf = func(pgdata string, dbid int) error {
			if dbid != int(request.Segment.Dbid) {
				t.Fatalf("got %v, want %v", dbid, request.Segment.Dbid)
			}
			return nil
		}
		agent.UpdateCoordinatorPgHbaConf = func(pgdata string, hbaHostnames bool, hostname string) error {
			if hostname != request.Segment.HostName {
				t.Fatalf("got %v, want %v", hostname, request.Segment.HostName)
			}
			return nil
		}
		_, err := agentServer.MakeSegment(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func InitdbFailure() {
	os.Stdout.WriteString("error")
	os.Exit(1)
}
