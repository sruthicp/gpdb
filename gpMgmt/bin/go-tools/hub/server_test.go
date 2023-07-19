package hub_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/hub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type MockCredentials struct {
	TlsConnection credentials.TransportCredentials
	err           error
}

func (s MockCredentials) LoadServerCredentials() (credentials.TransportCredentials, error) {
	return s.TlsConnection, s.err
}

func (s MockCredentials) LoadClientCredentials() (credentials.TransportCredentials, error) {
	return s.TlsConnection, s.err
}

func TestStartServer(t *testing.T) {

	testhelper.SetupTestLogger()
	host, _ := os.Hostname()
	gpHome := os.Getenv("GPHOME")

	t.Run("successfully starts the hub server", func(t *testing.T) {

		credCmd := MockCredentials{nil, nil}

		conf := &hub.Config{
			1234,
			8080,
			[]string{host},
			"/tmp/logDir",
			"gp",
			gpHome,
			credCmd,
		}

		hubServer := hub.New(conf, grpc.DialContext)

		errChan := make(chan error, 1)
		go func() {
			errChan <- hubServer.Start()
		}()

		defer hubServer.Shutdown()

		select {
		case err := <-errChan:
			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}
		case <-time.After(1 * time.Second):
			t.Log("hub server started listening")
		}

	})

	t.Run("failed to start if the load credential fail", func(t *testing.T) {

		credCmd := &MockCredentials{nil, errors.New("")}

		conf := &hub.Config{
			1235,
			8080,
			[]string{host},
			"/tmp/logDir",
			"gp",
			gpHome,
			credCmd,
		}
		hubServer := hub.New(conf, grpc.DialContext)

		errChan := make(chan error, 1)
		go func() {
			errChan <- hubServer.Start()
		}()
		defer hubServer.Shutdown()

		select {
		case err := <-errChan:
			if err == nil || !strings.Contains(err.Error(), "Could not load credentials") {
				t.Fatalf("want \"Could not load credentials\" but get: %q", err.Error())
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Failed to raise error if load credential fail")
		}
	})
}

func TestStartAgents(t *testing.T) {

	testhelper.SetupTestLogger()
	host, _ := os.Hostname()
	gpHome := os.Getenv("GPHOME")

	t.Run("successfully starts the agents from hub", func(t *testing.T) {

		credCmd := &MockCredentials{nil, nil}

		conf := &hub.Config{
			1234,
			8080,
			[]string{host},
			"/tmp/logDir",
			"gp",
			gpHome,
			credCmd,
		}

		hubServer := hub.New(conf, grpc.DialContext)

		err := hubServer.StartAllAgents()
		defer hubServer.Shutdown()

		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

	})

	t.Run("failed to start if the host is not reachable", func(t *testing.T) {

		credCmd := &MockCredentials{nil, nil}

		conf := &hub.Config{
			1234,
			8080,
			[]string{"test"},
			"/tmp/logDir",
			"gp",
			gpHome,
			credCmd,
		}
		hubServer := hub.New(conf, grpc.DialContext)

		err := hubServer.StartAllAgents()
		defer hubServer.Shutdown()

		if err == nil || !strings.Contains(err.Error(), "unable to login") {
			t.Fatalf("expected connection error")
		}
	})

	t.Run("failed to start if the gphome is not set", func(t *testing.T) {

		credCmd := &MockCredentials{nil, nil}

		conf := &hub.Config{
			1234,
			8080,
			[]string{host},
			"/tmp/logDir",
			"gp",
			"gphome",
			credCmd,
		}
		hubServer := hub.New(conf, grpc.DialContext)

		err := hubServer.StartAllAgents()
		defer hubServer.Shutdown()

		if err == nil || !strings.Contains(err.Error(), "No such file or directory") {
			t.Fatalf("expected path greenplum_path not found error")
		}
	})
}
