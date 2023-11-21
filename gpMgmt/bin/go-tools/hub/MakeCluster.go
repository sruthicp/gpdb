package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
	"golang.org/x/exp/maps"
)

func (s *Server) MakeCluster(request *idl.MakeClusterRequest, stream idl.Hub_MakeClusterServer) error {
	gplog.Debug("Starting MakeCluster")

	streamLogMsg(stream, "Starting MakeCluster")
	err := s.ValidateEnvironment(stream, request)
	if err != nil {
		gplog.Error("Error during validation:%v", err)
		return err
	}

	streamLogMsg(stream, "Creating coordinator segment")
	err = s.CreateAndStartCoordinator(request.GpArray.Coordinator, request.ClusterParams)
	if err != nil {
		return err
	}
	streamLogMsg(stream, "Successfully created coordinator segment")

	streamLogMsg(stream, "Starting to register primary segments with the coordinator")
	conn, err := greenplum.ConnectDatabase(request.GpArray.Coordinator.HostName, int(request.GpArray.Coordinator.Port))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	err = greenplum.RegisterCoordinator(request.GpArray.Coordinator, conn)
	if err != nil {
		return err
	}

	err = greenplum.RegisterPrimaries(request.GpArray.Primaries, conn)
	if err != nil {
		return err
	}
	streamLogMsg(stream, "Successfully registered primary segments with the coordinator")

	gpArray := greenplum.NewGpArray()
	err = gpArray.ReadGpSegmentConfig(conn)
	if err != nil {
		return err
	}
	conn.Close()

	primarySegs, err := gpArray.GetPrimarySegments()
	if err != nil {
		return err
	}

	streamLogMsg(stream, "Creating primary segments")
	err = s.CreateSegments(stream, primarySegs, request.ClusterParams, []string{})
	if err != nil {
		return err
	}
	streamLogMsg(stream, "Successfully created primary segments")

	// We do not start the primary segments, only the coordinator.
	// Only in case of mirrors, we start them at the end after gpstop/gpstart
	// err = StartSegments(s.Conns, primarySegs, "-c gp_role=utility")

	streamLogMsg(stream, "Restarting the Greenplum cluster in production mode")
	streamLogMsg(stream, "Shutting down coordinator segment")
	pgCtlStopCmd := &postgres.PgCtlStop{
		PgData: request.GpArray.Coordinator.DataDirectory,
	}
	out, err := utils.RunExecCommand(pgCtlStopCmd, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing pg_ctl stop: %s, %w", out, err)
	}
	streamLogMsg(stream, "Successfully shut down coordinator segment")

	gpstartOptions := &greenplum.GpStart{
		DataDirectory: request.GpArray.Coordinator.DataDirectory,
	}
	cmd := utils.NewGpSourcedCommand(gpstartOptions, s.GpHome)
	err = streamExecCommand(stream, cmd, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing gpstart: %w", err)
	}
	streamLogMsg(stream, "Completed restart of Greenplum cluster in production mode")

	streamLogMsg(stream, "Creating core GPDB extensions")
	err = CreateGpToolkitExt(conn)
	if err != nil {
		return err
	}
	streamLogMsg(stream, "Successfully created core GPDB extensions")

	streamLogMsg(stream, "Importing system collations")
	err = ImportCollation(conn)
	if err != nil {
		return err
	}

	if request.ClusterParams.DbName != "" {
		streamLogMsg(stream, fmt.Sprintf("Creating database %q", request.ClusterParams.DbName))
		err = CreateDatabase(conn, request.ClusterParams.DbName)
		if err != nil {
			return err
		}
	}

	streamLogMsg(stream, "Setting Greenplum superuser password")
	err = SetGpUserPasswd(conn, request.ClusterParams.SuPassword)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) ValidateEnvironment(stream idl.Hub_MakeClusterServer, request *idl.MakeClusterRequest) error {
	gparray := request.GpArray
	hostDirMap := make(map[string][]string)
	// Add coordinator to the map
	hostDirMap[gparray.Coordinator.HostAddress] = append(hostDirMap[gparray.Coordinator.HostAddress], gparray.Coordinator.DataDirectory)

	// Add primaries to the map
	for _, seg := range gparray.Primaries {
		hostDirMap[seg.HostAddress] = append(hostDirMap[seg.HostAddress], seg.DataDirectory)
	}
	progressLabel := "Validating Hosts:"
	progressTotal := len(hostDirMap)
	streamProgressMsg(stream, progressLabel, progressTotal)
	validateFn := func(conn *Connection) error {
		dirList := hostDirMap[conn.Hostname]
		validateReq := idl.ValidateHostEnvRequest{DirectoryList: dirList, Forced: request.ForceFlag}
		_, err := conn.AgentClient.ValidateHostEnv(context.Background(), &validateReq)
		if err != nil {
			return err
		}
		streamProgressMsg(stream, progressLabel, progressTotal)
		return nil
	}
	return ExecuteRPC(s.Conns, validateFn)
}

func CreateSingleSegment(conn *Connection, seg *idl.Segment, clusterParams *idl.ClusterParams, addressList []string) error {
	pgConfig := make(map[string]string)
	maps.Copy(pgConfig, clusterParams.CommonConfig)
	if seg.Contentid == -1 {
		maps.Copy(pgConfig, clusterParams.CoordinatorConfig)
	} else {
		maps.Copy(pgConfig, clusterParams.SegmentConfig)
	}

	makeSegmentReq := &idl.MakeSegmentRequest{
		Segment:      seg,
		Locale:       clusterParams.Locale,
		Encoding:     clusterParams.Encoding,
		SegConfig:    pgConfig,
		IPList:       addressList,
		HbaHostNames: clusterParams.HbaHostnames,
	}

	_, err := conn.AgentClient.MakeSegment(context.Background(), makeSegmentReq)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) CreateAndStartCoordinator(seg *idl.Segment, clusterParams *idl.ClusterParams) error {
	coordinatorConn := getConnByHost(s.Conns, []string{seg.HostName})

	seg.Contentid = -1
	seg.Dbid = 1
	request := func(conn *Connection) error {
		err := CreateSingleSegment(conn, seg, clusterParams, []string{})
		if err != nil {
			return err
		}

		startSegReq := &idl.StartSegmentRequest{
			DataDir: seg.DataDirectory,
			Wait:    true,
			Options: "-c gp_role=utility",
		}
		_, err = conn.AgentClient.StartSegment(context.Background(), startSegReq)

		return err
	}

	return ExecuteRPC(coordinatorConn, request)
}

func (s *Server) CreateSegments(stream idl.Hub_MakeClusterServer, segs []greenplum.Segment, clusterParams *idl.ClusterParams, addressList []string) error {
	hostSegmentMap := map[string][]*idl.Segment{}
	for _, seg := range segs {
		segReq := &idl.Segment{
			Port:          int32(seg.Port),
			DataDirectory: seg.DataDirectory,
			HostName:      seg.HostName,
			HostAddress:   seg.HostAddress,
			Contentid:     int32(seg.ContentId),
			Dbid:          int32(seg.Dbid),
		}

		if _, ok := hostSegmentMap[seg.HostName]; !ok {
			hostSegmentMap[seg.HostName] = []*idl.Segment{segReq}
		} else {
			hostSegmentMap[seg.HostName] = append(hostSegmentMap[seg.HostName], segReq)
		}
	}

	progressLabel := "Initializing segments:"
	progressTotal := len(segs)
	streamProgressMsg(stream, progressLabel, progressTotal)

	request := func(conn *Connection) error {
		var wg sync.WaitGroup

		segs := hostSegmentMap[conn.Hostname]
		errs := make(chan error, len(segs))
		for _, seg := range segs {
			seg := seg
			wg.Add(1)
			go func(seg *idl.Segment) {
				defer wg.Done()

				err := CreateSingleSegment(conn, seg, clusterParams, addressList)
				if err != nil {
					errs <- utils.FormatGrpcError(err)
				} else {
					streamProgressMsg(stream, progressLabel, progressTotal)
				}
			}(seg)
		}

		wg.Wait()
		close(errs)

		var err error
		for e := range errs {
			err = errors.Join(err, e)
		}
		return err
	}

	return ExecuteRPC(s.Conns, request)
}

func StartSegments(conns []*Connection, segs []greenplum.Segment, options string) error {
	hostSegmentMap := map[string][]greenplum.Segment{}
	for _, seg := range segs {
		if _, ok := hostSegmentMap[seg.HostName]; !ok {
			hostSegmentMap[seg.HostName] = []greenplum.Segment{seg}
		} else {
			hostSegmentMap[seg.HostName] = append(hostSegmentMap[seg.HostName], seg)
		}
	}

	request := func(conn *Connection) error {
		var wg sync.WaitGroup

		segs := hostSegmentMap[conn.Hostname]
		errs := make(chan error, len(segs))
		for _, seg := range segs {
			seg := seg
			startReq := &idl.StartSegmentRequest{
				DataDir: seg.DataDirectory,
				Wait:    true,
				Timeout: 600,
				Options: options,
			}
			wg.Add(1)
			go func(seg greenplum.Segment) {
				defer wg.Done()

				_, err := conn.AgentClient.StartSegment(context.Background(), startReq)
				if err != nil {
					errs <- utils.FormatGrpcError(err)
				}
			}(seg)
		}

		wg.Wait()
		close(errs)

		var err error
		for e := range errs {
			err = errors.Join(err, e)
		}
		return err
	}

	return ExecuteRPC(conns, request)
}

func execOnDatabase(conn *dbconn.DBConn, dbname string, query string) error {
	conn.DBName = dbname
	if err := conn.Connect(1); err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Exec(query); err != nil {
		return err
	}

	return nil
}

func CreateGpToolkitExt(conn *dbconn.DBConn) error {
	createExtensionQuery := "CREATE EXTENSION gp_toolkit"

	for _, dbname := range []string{"template1", "postgres"} {
		if err := execOnDatabase(conn, dbname, createExtensionQuery); err != nil {
			return err
		}
	}

	return nil
}

func ImportCollation(conn *dbconn.DBConn) error {
	importCollationQuery := "SELECT pg_import_system_collations('pg_catalog'); ANALYZE;"

	if err := execOnDatabase(conn, "postgres", "ALTER DATABASE template0 ALLOW_CONNECTIONS on"); err != nil {
		return err
	}

	if err := execOnDatabase(conn, "template0", importCollationQuery); err != nil {
		return err
	}
	if err := execOnDatabase(conn, "template0", "VACUUM FREEZE"); err != nil {
		return err
	}

	if err := execOnDatabase(conn, "postgres", "ALTER DATABASE template0 ALLOW_CONNECTIONS off"); err != nil {
		return err
	}

	for _, dbname := range []string{"template1", "postgres"} {
		if err := execOnDatabase(conn, dbname, importCollationQuery); err != nil {
			return err
		}

		if err := execOnDatabase(conn, dbname, "VACUUM FREEZE"); err != nil {
			return err
		}
	}

	return nil
}

func CreateDatabase(conn *dbconn.DBConn, dbname string) error {
	createDbQuery := fmt.Sprintf("CREATE DATABASE %s", dbname)
	if err := execOnDatabase(conn, "template1", createDbQuery); err != nil {
		return err
	}

	return nil
}

func SetGpUserPasswd(conn *dbconn.DBConn, passwd string) error {
	user, err := utils.System.CurrentUser()
	if err != nil {
		return err
	}

	alterPasswdQuery := fmt.Sprintf("ALTER USER %s WITH PASSWORD '%s'", user.Username, passwd)
	if err := execOnDatabase(conn, "template1", alterPasswdQuery); err != nil {
		return err
	}

	return nil
}
