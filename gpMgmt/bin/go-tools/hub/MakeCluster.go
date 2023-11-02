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
	"github.com/greenplum-db/gpdb/gp/common"
	"github.com/greenplum-db/gpdb/gp/errorlist"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
)

func (s *Server) MakeActualCluster(gparray common.GpArray, params common.ClusterParams, forced bool) error {
	//Makes actual cluster
	gplog.Info("Starting MakeActualCluster")
	gplog.Debug("GPArray:%#v", gparray)
	gplog.Debug("ClusterParams:%#v", params)
	gplog.Debug("Forced:%t", forced)
	// TODO Validate Parameters and gpArray

	// Check and validate environment for each segment
	// create list of directories per host
	gplog.Debug("Building hostDirMap. PrimarySegs:%d", len(gparray.PrimarySegments))
	hostDirMap := make(map[string][]string)
	// Add coordinator to the map
	hostDirMap[gparray.Coordinator.HostAddress] = append(hostDirMap[gparray.Coordinator.HostAddress], gparray.Coordinator.DataDirectory)

	// Add primaries to the map
	for _, seg := range gparray.PrimarySegments {
		gplog.Debug("HostAddress: %s, Dir:%s", seg.HostAddress, seg.DataDirectory)
		hostDirMap[seg.HostAddress] = append(hostDirMap[seg.HostAddress], seg.DataDirectory)
	}
	gplog.Debug("Created Host-Dir Map")
	err := s.DialAllAgents()
	if err != nil {
		gplog.Error("Error in DialAllAgents:%v", err)
		return err
	}
	gplog.Debug("Done with DialAllAgents successfully")
	err = s.ValidateOnAllHost(hostDirMap, forced)
	if err != nil {
		return err
	}
	//
	gplog.Info("Exiting MakeActualCluster")

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
		err1 = errorlist.Append(err1, e)
	}
	return err1
}

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
		Segment: seg,
		Locale: clusterParams.Locale,
		Encoding: clusterParams.Encoding,
		SegConfig: pgConfig,
		IPList: addressList,
		HbaHostNames: clusterParams.HbaHostnames,
	}

	_, err := conn.AgentClient.MakeSegment(context.Background(), makeSegmentReq)
	if err != nil {
		return err
	}

	return nil
}

func CreateAndStartCoordinator(conns []*Connection, seg *idl.Segment, clusterParams *idl.ClusterParams) error {
	coordinatorConn := getConnByHost(conns, []string{seg.HostName})

	seg.Contentid = -1
	seg.Dbid = 1
	request := func(conn *Connection) error {
		err := CreateSingleSegment(conn, seg, clusterParams, []string{})
		if err != nil {
			return err
		}

		startSegReq := &idl.StartSegmentRequest{
			DataDir: seg.DataDirectory,
			Wait: true,
			Options: "-c gp_role=utility",
		}
		_, err = conn.AgentClient.StartSegment(context.Background(), startSegReq)

		return err
	}

	return ExecuteRPC(coordinatorConn, request)
}

func CreateSegments(conns []*Connection, segs []greenplum.Segment, clusterParams *idl.ClusterParams, addressList []string) error {
	hostSegmentMap := map[string][]*idl.Segment{}
	for _, seg := range segs {
		segReq := &idl.Segment{
			Port: int32(seg.Port),
			DataDirectory: seg.DataDirectory,
			HostName: seg.HostName,
			HostAddress: seg.HostAddress,
			Contentid: int32(seg.ContentId),
			Dbid: int32(seg.Dbid),
		}

		if _, ok := hostSegmentMap[seg.HostName]; !ok {
			hostSegmentMap[seg.HostName] = []*idl.Segment{segReq}
		} else {
			hostSegmentMap[seg.HostName] = append(hostSegmentMap[seg.HostName], segReq)
		}
	}

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
				Wait: true,
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
