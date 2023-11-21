package postgres_test

import (
	"errors"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func TestUpdatePostgresqlConf(t *testing.T) {
	testhelper.SetupTestLogger()

	cases := []struct {
		overwrite    bool
		configParams map[string]string
		confContent  string
		expected     string
	}{
		{
			overwrite: true,
			configParams: map[string]string{
				"guc_1": "new_value_1",
			},
			confContent: `
guc_1 = value_1
guc_2 = value_2
# comment`,
			expected: `
guc_1 = 'new_value_1'
guc_2 = value_2
# comment`,
		},
		{
			overwrite: false,
			configParams: map[string]string{
				"guc_1": "new_value_1",
			},
			confContent: `
guc_1 = value_1
guc_2 = value_2
# comment`,
			expected: `
# guc_1 = value_1
guc_1 = 'new_value_1'
guc_2 = value_2
# comment`,
		},
		{
			overwrite: true,
			configParams: map[string]string{
				"guc_1": "new_value_1",
				"guc_3": "new_value_3",
			},
			confContent: `
guc_1 = value_1
guc_2 = value_2`,
			expected: `
guc_1 = 'new_value_1'
guc_2 = value_2
guc_3 = 'new_value_3'`,
		},
		{
			overwrite: true,
			configParams: map[string]string{
				"guc_1": "new_value_1",
				"guc_3": "1234",
			},
			confContent: `
guc_1 value_1
guc_2 value_2`,
			expected: `
guc_1 = 'new_value_1'
guc_2 value_2
guc_3 = 1234`,
		},
		{
			overwrite: true,
			configParams: map[string]string{
				"guc_1": "new_value_1",
			},
			confContent: `
guc_1 value_1
#guc_1 value_1
guc_1a value_1
guc_1_a value_1
guc_1a = value_1
guc_1_a=value_1
guc_2 value_2`,
			expected: `
guc_1 = 'new_value_1'
#guc_1 value_1
guc_1a value_1
guc_1_a value_1
guc_1a = value_1
guc_1_a=value_1
guc_2 value_2`,
		},
	}

	for _, tc := range cases {
		t.Run("correctly updates the postgresql.conf file", func(t *testing.T) {
			dname, confPath := createTempConfFile(t, "postgresql.conf", tc.confContent)
			defer os.RemoveAll(dname)

			err := postgres.UpdatePostgresqlConf(dname, tc.configParams, tc.overwrite)
			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}

			testutils.AssertFileContents(t, confPath, tc.expected)
		})
	}

	t.Run("errors out when there is no file present", func(t *testing.T) {
		dname, _ := createTempConfFile(t, "", "")
		defer os.RemoveAll(dname)

		expectedErr := os.ErrNotExist
		err := postgres.UpdatePostgresqlConf(dname, map[string]string{}, true)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
	})

	t.Run("returns appropriate error when fails to update the conf file", func(t *testing.T) {
		dname, _ := createTempConfFile(t, "postgresql.conf", "")
		defer os.RemoveAll(dname)

		expectedErr := errors.New("error")
		utils.System.Create = func(name string) (*os.File, error) {
			return nil, expectedErr
		}
		defer utils.ResetSystemFunctions()

		err := postgres.UpdatePostgresqlConf(dname, map[string]string{}, true)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
	})
}

func TestCreatePostgresInternalConf(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("successfully creates the internal.auto.conf", func(t *testing.T) {
		dname, _ := createTempConfFile(t, "", "")
		defer os.RemoveAll(dname)

		confPath := filepath.Join(dname, "internal.auto.conf")
		_, err := os.Stat(confPath)
		if !os.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", confPath)
		}

		err = postgres.CreatePostgresInternalConf(dname, -1)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		expected := "gp_dbid = -1"
		testutils.AssertFileContents(t, confPath, expected)
	})
}

func TestBuildPgHbaConf(t *testing.T) {
	testhelper.SetupTestLogger()

	cases := []struct {
		hbaHostnames bool
		hostname     string
		confContent  string
		expected     string
	}{
		{
			hbaHostnames: false,
			hostname:     "cdw",
			confContent:  ``,
			expected: `local	all	gpadmin	ident
host	all	gpadmin	localhost	trust
host	all	gpadmin	1.2.3.4	trust
local	replication	gpadmin	ident
host	replication	gpadmin	samehost	trust
host	replication	gpadmin	1.2.3.4	trust`,
		},
		{
			hbaHostnames: true,
			hostname:     "cdw",
			confContent:  ``,
			expected: `local	all	gpadmin	ident
host	all	gpadmin	localhost	trust
host	all	gpadmin	cdw	trust
local	replication	gpadmin	ident
host	replication	gpadmin	samehost	trust
host	replication	gpadmin	cdw	trust`,
		},
		{
			hbaHostnames: true,
			hostname:     "cdw",
			confContent: `# foo
foobar
# bar`,
			expected: `# foo
# bar
local	all	gpadmin	ident
host	all	gpadmin	localhost	trust
host	all	gpadmin	cdw	trust
local	replication	gpadmin	ident
host	replication	gpadmin	samehost	trust
host	replication	gpadmin	cdw	trust`,
		},
	}

	for _, tc := range cases {
		t.Run("correctly builds the coordinator pg_hba.conf file", func(t *testing.T) {
			dname, confPath := createTempConfFile(t, "pg_hba.conf", tc.confContent)
			defer os.RemoveAll(dname)

			utils.System.CurrentUser = func() (*user.User, error) {
				return &user.User{Username: "gpadmin"}, nil
			}
			utils.System.InterfaceAddrs = func() ([]net.Addr, error) {
				ip := net.IPv4(1, 2, 3, 4)
				addr := &net.IPAddr{IP: ip}
				return []net.Addr{addr}, nil
			}
			defer utils.ResetSystemFunctions()

			err := postgres.UpdateCoordinatorPgHbaConf(dname, tc.hbaHostnames, tc.hostname)
			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}

			testutils.AssertFileContents(t, confPath, tc.expected)
		})
	}
}

func createTempConfFile(t *testing.T, filename, content string) (string, string) {
	t.Helper()

	dname, err := os.MkdirTemp("", "gpseg")
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	filepath := filepath.Join(dname, filename)
	if filename != "" {
		os.WriteFile(filepath, []byte(content), 0644)
	}

	return dname, filepath
}
