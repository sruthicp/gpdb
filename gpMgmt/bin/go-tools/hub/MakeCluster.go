package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
)

func (s *Server) MakeCluster(request *idl.MakeClusterRequest, stream idl.Hub_MakeClusterServer) error {
	gplog.Debug("Starting MakeCluster")

	streamLogMsg(stream, "Starting MakeCluster")
	err := s.NewValidateEnvironment(stream, request)
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
	err = greenplum.RegisterCoordinator(request.GpArray.Coordinator)
	if err != nil {
		return err
	}

	err = greenplum.RegisterPrimaries(request.GpArray.Primaries, request.GpArray.Coordinator.HostName, int(request.GpArray.Coordinator.Port))
	if err != nil {
		return err
	}
	streamLogMsg(stream, "Successfully registered primary segments with the coordinator")

	gpArray := greenplum.NewGpArray()
	err = gpArray.ReadGpSegmentConfig(request.GpArray.Coordinator.HostName, int(request.GpArray.Coordinator.Port))
	if err != nil {
		return err
	}

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
	gpstopOptions := &greenplum.GpStop{
		DataDirectory:   request.GpArray.Coordinator.DataDirectory,
		CoordinatorOnly: true,
	}
	cmd := utils.NewGpSourcedCommand(gpstopOptions, s.GpHome)
	err = streamExecCommand(stream, cmd, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing gpstop: %w", err)
	}

	gpstartOptions := &greenplum.GpStart{
		DataDirectory: request.GpArray.Coordinator.DataDirectory,
	}
	cmd = utils.NewGpSourcedCommand(gpstartOptions, s.GpHome)
	err = streamExecCommand(stream, cmd, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing gpstart: %w", err)
	}
	streamLogMsg(stream, "Completed restart of Greenplum cluster in production mode")

	// TODO
	// 1. CREATE_GPEXTENSIONS
	// 2. IMPORT_COLLATION
	// 3. CREATE_DATABASE
	// 4. SET_GP_USER_PW

	return err
}
func (s *Server) NewValidateEnvironment(stream idl.Hub_MakeClusterServer, request *idl.MakeClusterRequest) error {
	gparray := request.GpArray
	hostDirMap := make(map[string][]string)
	// Add coordinator to the map
	hostDirMap[gparray.Coordinator.HostAddress] = append(hostDirMap[gparray.Coordinator.HostAddress], gparray.Coordinator.DataDirectory)

	// Add primaries to the map
	for _, seg := range gparray.Primaries {
		hostDirMap[seg.HostAddress] = append(hostDirMap[seg.HostAddress], seg.DataDirectory)
	}
	progressLabel := "Validating segments:"
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
		/*var wg sync.WaitGroup
		errs := make(chan error, len(hostDirMap))
		for _, dirList := range hostDirMap {
			wg.Add(1)
			go func(dirList []string) {
				defer wg.Done()
				validateReq := idl.ValidateHostEnvRequest{DirectoryList: dirList, Forced: request.ForceFlag}
				_, err := conn.AgentClient.ValidateHostEnv(context.Background(), &validateReq)
				if err != nil {
					errs <- utils.FormatGrpcError(err)
				} else {
					streamProgressMsg(stream, progressLabel, progressTotal)
				}
			}(dirList)
		}

		wg.Wait()
		close(errs)
		var err error
		for e := range errs {
			err = errors.Join(err, e)
		}


		return err

		*/
	}
	return ExecuteRPC(s.Conns, validateFn)
}

/*
	func (s *Server) ValidateEnvironment(request *idl.MakeClusterRequest) error {
		// Check and validate environment for each segment
		gparray := request.GpArray
		gplog.Info("Starting ValidateEnvironment")
		// TODO Validate Parameters and gpArray populated

		hostDirMap := make(map[string][]string)
		// Add coordinator to the map
		hostDirMap[gparray.Coordinator.HostAddress] = append(hostDirMap[gparray.Coordinator.HostAddress], gparray.Coordinator.DataDirectory)

		// Add primaries to the map
		for _, seg := range gparray.Primaries {
			hostDirMap[seg.HostAddress] = append(hostDirMap[seg.HostAddress], seg.DataDirectory)
		}

		// Connect to all agents
		err := s.DialAllAgents()
		if err != nil {
			gplog.Error("Error in DialAllAgents:%v", err)
			return fmt.Errorf("error while connecting Agents:%v", err)
		}
		err = s.ValidateOnAllHost(hostDirMap, request.ForceFlag)
		if err != nil {
			return err
		}
		gplog.Info("Done with ValidateEnvironment")

		return nil
	}

	func (s *Server) ValidateOnAllHost(hostDirMap map[string][]string, forced bool) error {
		// Get ready to validate host env
		gplog.Debug("Starting validation on all hosts")
		ctx, cancelFunc := context.WithTimeout(context.Background(), DialTimeout)
		defer cancelFunc()

		creds, err := s.Credentials.LoadClientCredentials()
		if err != nil {
			cancelFunc()
			return err
		}
		gplog.Debug("Done with initial gRPC setup")
		var wg sync.WaitGroup
		errs := make(chan error, len(hostDirMap))
		for hostname, dirList := range hostDirMap {
			wg.Add(1)
			go func(hostname string) {
				defer wg.Done()
				err = s.ValidateOnHost(hostname, creds, ctx, dirList, forced)
				errs <- err
			}(hostname)
			if err != nil {
				cancelFunc()
				return err
			}
		}
		wg.Wait()
		close(errs)
		var err1 error
		for e := range errs {
			err1 = errors.Join(err1, e)
		}
		return err1
	}
*/
func (s *Server) ValidateOnHost(hostname string, credentials credentials.TransportCredentials, ctx context.Context, dirList []string, forced bool) error {
	address := fmt.Sprintf("%s:%d", hostname, s.AgentPort)
	gplog.Debug("Calling RPC on:%s", address)
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials),
		grpc.WithReturnConnectionError(),
	}
	if s.grpcDialer != nil {
		opts = append(opts, grpc.WithContextDialer(s.grpcDialer))
	}
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		gplog.Error("could not connect to agent on host %s: %v", hostname, err)
		return fmt.Errorf("could not connect to agent on host %s: %w", hostname, err)
	}
	gplog.Debug("Getting AgentClient")
	AgentClient := idl.NewAgentClient(conn)
	gplog.Debug("Calling RPC on AgentClient")
	validateReq := idl.ValidateHostEnvRequest{DirectoryList: dirList, Forced: forced}
	_, err = AgentClient.ValidateHostEnv(ctx, &validateReq)
	if err != nil {
		gplog.Error("Error from ValidateHostEnv:%v", err)
		gplog.Error("Error:%v", err)
		return err
	}
	return nil
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
