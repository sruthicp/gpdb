package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/reflection"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/common"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/testutils/exectest"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
)

var (
	platform                      = utils.GetPlatform()
	DialTimeout                   = 3 * time.Second
	ensureConnectionsAreReadyFunc = ensureConnectionsAreReady
	execCommand                   = exec.Command
)

type Dialer func(context.Context, string) (net.Conn, error)

type Config struct {
	Port        int      `json:"hubPort"`
	AgentPort   int      `json:"agentPort"`
	Hostnames   []string `json:"hostnames"`
	LogDir      string   `json:"hubLogDir"` // log directory for the hub itself; utilities might go somewhere else
	ServiceName string   `json:"serviceName"`
	GpHome      string   `json:"gphome"`

	Credentials utils.Credentials
}

type Server struct {
	*Config
	Conns      []*Connection
	grpcDialer Dialer

	mutex      sync.Mutex
	grpcServer *grpc.Server
	listener   net.Listener
	finish     chan struct{}
}

func ReplyLogMsg(msg string, server idl.Hub_MakeClusterServer) error {
	logResponse := idl.MakeClusterReply_Message{
		msg,
	}
	response := idl.MakeClusterReply{MakeClusterReply: &logResponse}
	err := server.Send(&response)
	if err != nil {
		gplog.Error("Error replying MakeCluster RPC:%v", err)
		return err
	}
	return nil
}

func ReplyProgress(msg string, percent int, server idl.Hub_MakeClusterServer) error {

	progressMsg := idl.ProgressMessage{Title: msg, PercentProgress: int32(percent)}
	progress := idl.MakeClusterReply_Progress{Progress: &progressMsg}
	response := idl.MakeClusterReply{
		MakeClusterReply: &progress,
	}
	err := server.Send(&response)
	if err != nil {
		gplog.Error("Error replying MakeCluster RPC:%v", err)
		return err
	}
	return nil
}

func (s *Server) MakeCluster(request *idl.MakeClusterRequest, server idl.Hub_MakeClusterServer) error {
	gplog.Debug("Starting TestRPC")

	err := ReplyLogMsg("Starting MakeCluster", server)
	if err != nil {
		return err
	}

	err = ReplyProgress("Validation", 0, server)
	if err != nil {
		return err
	}

	var clusterParams common.ClusterParams
	var gparray common.GpArray
	clusterParams.LoadFromIdl(request.ClusterParams)
	gparray.LoadFromIdl(request.GpArray)

	err = s.MakeActualCluster(gparray, clusterParams, request.ForceFlag)

	err = CreateAndStartCoordinator(s.Conns, request.GpArray.Coordinator, request.ClusterParams)
	if err != nil {
		return err
	}

	greenplum.RegisterCoordinator(request.GpArray.Coordinator)
	greenplum.RegisterPrimaries(request.GpArray.Primaries, request.GpArray.Coordinator.HostName, int(request.GpArray.Coordinator.Port))

	gpArray := greenplum.NewGpArray()
	err = gpArray.ReadGpSegmentConfig(request.GpArray.Coordinator.HostName, int(request.GpArray.Coordinator.Port))
	if err != nil {
		return err
	}

	primarySegs, err := gpArray.GetPrimarySegments()
	if err != nil {
		return err
	}

	err = CreateSegments(s.Conns, primarySegs, request.ClusterParams, []string{})
	if err != nil {
		return err
	}

	// We do not start the primary segments, only the coordinator.
	// Only in case of mirrors, we start them at the end after gpstop/gpstart
	// err = StartSegments(s.Conns, primarySegs, "-c gp_role=utility")

	gpstopOptions := greenplum.GpStop{
		DataDirectory:   request.GpArray.Coordinator.DataDirectory,
		CoordinatorOnly: true,
	}
	out, err := greenplum.RunGpCommand(&gpstopOptions, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing gpstop: %s, %w", out, err)
	}

	gpstartOptions := greenplum.GpStart{
		DataDirectory: request.GpArray.Coordinator.DataDirectory,
	}
	out, err = greenplum.RunGpCommand(&gpstartOptions, s.GpHome)
	if err != nil {
		return fmt.Errorf("executing gpstart: %s, %w", out, err)
	}

	// TODO
	// 1. CREATE_GPEXTENSIONS
	// 2. IMPORT_COLLATION
	// 3. CREATE_DATABASE
	// 4. SET_GP_USER_PW

	return err
}

type Connection struct {
	Conn          *grpc.ClientConn
	AgentClient   idl.AgentClient
	Hostname      string
	CancelContext func()
}

func New(conf *Config, grpcDialer Dialer) *Server {
	h := &Server{
		Config:     conf,
		grpcDialer: grpcDialer,
		finish:     make(chan struct{}, 1),
	}
	return h
}

func (s *Server) Start() error {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.Port)) // TODO: make this "hostname:port" so it can be started from somewhere other than the coordinator host
	if err != nil {
		return fmt.Errorf("could not listen on port %d: %w", s.Port, err)
	}

	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// handle stuff here if needed
		return handler(ctx, req)
	}

	credentials, err := s.Credentials.LoadServerCredentials()
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials),
		grpc.UnaryInterceptor(interceptor),
	)

	s.mutex.Lock()
	s.grpcServer = grpcServer
	s.listener = listener
	s.mutex.Unlock()

	idl.RegisterHubServer(grpcServer, s)
	reflection.Register(grpcServer)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-s.finish
		gplog.Info("Received stop command, attempting graceful shutdown")
		s.grpcServer.GracefulStop()
		gplog.Info("gRPC server has shut down")
		cancel()
		wg.Done()
	}()

	err = grpcServer.Serve(listener)
	if err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	wg.Wait()
	return nil
}

func (s *Server) Stop(ctx context.Context, in *idl.StopHubRequest) (*idl.StopHubReply, error) {
	s.Shutdown()
	return &idl.StopHubReply{}, nil
}

func (s *Server) Shutdown() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.grpcServer != nil {
		s.finish <- struct{}{}
	}
}

func (s *Server) StartAgents(ctx context.Context, in *idl.StartAgentsRequest) (*idl.StartAgentsReply, error) {
	err := s.StartAllAgents()
	if err != nil {
		return &idl.StartAgentsReply{}, err
	}

	// Make sure service has started :
	err = s.DialAllAgents()
	if err != nil {
		return &idl.StartAgentsReply{}, err
	}
	return &idl.StartAgentsReply{}, nil
}

func (s *Server) StartAllAgents() error {
	remoteCmd := make([]string, 0)
	for _, host := range s.Hostnames {
		remoteCmd = append(remoteCmd, "-h", host)
	}
	remoteCmd = append(remoteCmd, platform.GetStartAgentCommandString(s.ServiceName)...)
	greenplumPathSh := filepath.Join(s.GpHome, "greenplum_path.sh")
	cmd := execCommand(constants.ShellPath, "-c", fmt.Sprintf("source %s && gpssh %s", greenplumPathSh, strings.Join(remoteCmd, " ")))
	output, err := cmd.CombinedOutput()
	strOutput := string(output)
	if err != nil {
		return fmt.Errorf("could not start agents: %s", output)
	}
	// As command is run through gpssh, even if actual command returns error, gpssh still returns as success.
	// to overcome this we have added check the command output.
	if strings.Contains(strOutput, "ERROR") || strings.Contains(strOutput, "No such file or directory") {
		return fmt.Errorf("could not start agents: %s", strOutput)
	}

	return nil
}

func (s *Server) DialAllAgents() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Conns != nil {
		err := ensureConnectionsAreReadyFunc(s.Conns)
		if err != nil {
			return err
		}

		return nil
	}

	for _, host := range s.Hostnames {
		ctx, cancelFunc := context.WithTimeout(context.Background(), DialTimeout)

		credentials, err := s.Credentials.LoadClientCredentials()
		if err != nil {
			cancelFunc()
			return err
		}

		address := fmt.Sprintf("%s:%d", host, s.AgentPort)
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
			cancelFunc()
			return fmt.Errorf("could not connect to agent on host %s: %w", host, err)
		}
		s.Conns = append(s.Conns, &Connection{
			Conn:          conn,
			AgentClient:   idl.NewAgentClient(conn),
			Hostname:      host,
			CancelContext: cancelFunc,
		})
	}

	err := ensureConnectionsAreReadyFunc(s.Conns)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) StopAgents(ctx context.Context, in *idl.StopAgentsRequest) (*idl.StopAgentsReply, error) {
	request := func(conn *Connection) error {
		_, err := conn.AgentClient.Stop(context.Background(), &idl.StopAgentRequest{})
		if err == nil { // no error -> didn't stop
			return fmt.Errorf("failed to stop agent on host %s", conn.Hostname)
		}

		errStatus := grpcStatus.Convert(err)
		if errStatus.Code() != codes.Unavailable {
			return fmt.Errorf("failed to stop agent on host %s: %w", conn.Hostname, err)
		}

		return nil
	}

	err := s.DialAllAgents()
	if err != nil {
		return &idl.StopAgentsReply{}, err
	}

	err = ExecuteRPC(s.Conns, request)
	s.Conns = nil

	return &idl.StopAgentsReply{}, err
}

func (s *Server) StatusAgents(ctx context.Context, in *idl.StatusAgentsRequest) (*idl.StatusAgentsReply, error) {
	statusChan := make(chan *idl.ServiceStatus, len(s.Conns))

	request := func(conn *Connection) error {
		status, err := conn.AgentClient.Status(context.Background(), &idl.StatusAgentRequest{})
		if err != nil {
			return fmt.Errorf("failed to get agent status on host %s", conn.Hostname)
		}
		s := idl.ServiceStatus{
			Host:   conn.Hostname,
			Status: status.Status,
			Uptime: status.Uptime,
			Pid:    status.Pid,
		}
		statusChan <- &s

		return nil
	}

	err := s.DialAllAgents()
	if err != nil {
		return &idl.StatusAgentsReply{}, err
	}
	err = ExecuteRPC(s.Conns, request)
	if err != nil {
		return &idl.StatusAgentsReply{}, err
	}
	close(statusChan)

	statuses := make([]*idl.ServiceStatus, 0)
	for status := range statusChan {
		statuses = append(statuses, status)
	}

	return &idl.StatusAgentsReply{Statuses: statuses}, err
}

func ensureConnectionsAreReady(conns []*Connection) error {
	hostnames := []string{}
	for _, conn := range conns {
		if conn.Conn.GetState() != connectivity.Ready {
			hostnames = append(hostnames, conn.Hostname)
		}
	}

	if len(hostnames) > 0 {
		return fmt.Errorf("could not ensure connections were ready: unready hosts: %s", strings.Join(hostnames, ","))
	}

	return nil
}

func ExecuteRPC(agentConns []*Connection, executeRequest func(conn *Connection) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(agentConns))

	for _, conn := range agentConns {
		conn := conn
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := executeRequest(conn)
			errs <- err
		}()
	}

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = e
		break
	}

	return err
}

func (conf *Config) Load(ConfigFilePath string) error {
	//Loads config from the configFilePath
	conf.Credentials = &utils.GpCredentials{}
	contents, err := os.ReadFile(ConfigFilePath)
	if err != nil {
		return fmt.Errorf("could not open config file: %w", err)
	}

	err = json.Unmarshal(contents, &conf)
	if err != nil {
		return fmt.Errorf("could not parse config file: %w", err)
	}

	return nil
}

func (conf *Config) Write(ConfigFilePath string) error {
	// Updates config to the conf file
	configHandle, err := os.OpenFile(ConfigFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("could not create configuration file %s: %w\n", ConfigFilePath, err)
	}
	defer configHandle.Close()
	configContents, err := json.MarshalIndent(conf, "", "\t")
	if err != nil {
		return fmt.Errorf("could not parse configuration file %s: %w\n", ConfigFilePath, err)
	}

	_, err = configHandle.Write(configContents)
	if err != nil {
		return fmt.Errorf("could not write to configuration file %s: %w\n", ConfigFilePath, err)
	}
	gplog.Debug("Wrote configuration file to %s", ConfigFilePath)

	err = copyConfigFileToAgents(conf, ConfigFilePath)
	if err != nil {
		return err
	}

	return err
}

func copyConfigFileToAgents(conf *Config, ConfigFilePath string) error {
	hostList := make([]string, 0)

	for _, host := range conf.Hostnames {
		hostList = append(hostList, "-h", host)
	}
	greenplumPathSh := filepath.Join(conf.GpHome, "greenplum_path.sh")
	if len(hostList) < 1 {
		return fmt.Errorf("hostlist should not be empty. No hosts to copy files.")
	}

	remoteCmd := append(hostList, ConfigFilePath, fmt.Sprintf("=:%s", ConfigFilePath))
	cmd := execCommand(constants.ShellPath, "-c", fmt.Sprintf("source %s && gpsync %s", greenplumPathSh, strings.Join(remoteCmd, " ")))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not copy gp.conf file to segment hosts: %w, Command Output: %s", err, string(output))
	}

	return nil
}

func getConnByHost(conns []*Connection, hostnames []string) []*Connection {
	result := []*Connection{}
	for _, conn := range conns {
		if slices.Contains(hostnames, conn.Hostname) {
			result = append(result, conn)
		}
	}

	return result
}

// used only for testing
func SetEnsureConnectionsAreReady(customFunc func(conns []*Connection) error) {
	ensureConnectionsAreReadyFunc = customFunc
}

func ResetEnsureConnectionsAreReady() {
	ensureConnectionsAreReadyFunc = ensureConnectionsAreReady
}

func SetExecCommand(command exectest.Command) {
	execCommand = command
}

func ResetExecCommand() {
	execCommand = exec.Command
}
