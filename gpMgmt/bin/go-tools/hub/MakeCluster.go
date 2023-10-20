package hub

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/common"
	"github.com/greenplum-db/gpdb/gp/errorlist"
	"github.com/greenplum-db/gpdb/gp/idl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sync"
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
