package node

import (
	"context"
	"net"
	"testing"
	"time"

	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stubNodeService struct {
	nodev1.UnimplementedNodeServiceServer
	ReplicateFn       func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error)
	ForceRecoverFn    func(context.Context, *nodev1.ForceRecoverRequest) (*nodev1.ForceRecoverResponse, error)
	RequestVoteFn     func(context.Context, *nodev1.VoteRequest) (*nodev1.VoteResponse, error)
	LeaderHeartbeatFn func(context.Context, *nodev1.LeaderHeartbeatRequest) (*nodev1.LeaderHeartbeatResponse, error)
	RecoverFn         func(context.Context, *nodev1.RecoverRequest) (*nodev1.RecoverResponse, error)
}

func (s *stubNodeService) Replicate(ctx context.Context, req *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
	if s.ReplicateFn != nil {
		return s.ReplicateFn(ctx, req)
	}
	return &nodev1.ReplicateResponse{Success: true}, nil
}

func (s *stubNodeService) ForceRecover(ctx context.Context, req *nodev1.ForceRecoverRequest) (*nodev1.ForceRecoverResponse, error) {
	if s.ForceRecoverFn != nil {
		return s.ForceRecoverFn(ctx, req)
	}
	return &nodev1.ForceRecoverResponse{}, nil
}

func (s *stubNodeService) RequestVote(ctx context.Context, req *nodev1.VoteRequest) (*nodev1.VoteResponse, error) {
	if s.RequestVoteFn != nil {
		return s.RequestVoteFn(ctx, req)
	}
	return &nodev1.VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (s *stubNodeService) LeaderHeartbeat(ctx context.Context, req *nodev1.LeaderHeartbeatRequest) (*nodev1.LeaderHeartbeatResponse, error) {
	if s.LeaderHeartbeatFn != nil {
		return s.LeaderHeartbeatFn(ctx, req)
	}
	return &nodev1.LeaderHeartbeatResponse{Term: req.Term}, nil
}

func (s *stubNodeService) Recover(ctx context.Context, req *nodev1.RecoverRequest) (*nodev1.RecoverResponse, error) {
	if s.RecoverFn != nil {
		return s.RecoverFn(ctx, req)
	}
	return &nodev1.RecoverResponse{}, nil
}

type stubCoordinatorService struct {
	coordinatorv1.UnimplementedCoordinatorServiceServer
	NotifyLeaderFn func(context.Context, *coordinatorv1.NotifyLeaderRequest) (*coordinatorv1.NotifyLeaderResponse, error)
}

func (s *stubCoordinatorService) NotifyLeader(ctx context.Context, req *coordinatorv1.NotifyLeaderRequest) (*coordinatorv1.NotifyLeaderResponse, error) {
	if s.NotifyLeaderFn != nil {
		return s.NotifyLeaderFn(ctx, req)
	}
	return &coordinatorv1.NotifyLeaderResponse{Accepted: true}, nil
}

func startTestNodeGRPC(t *testing.T, srv *Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gs := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(gs, &grpcServer{s: srv})

	go func() {
		_ = gs.Serve(lis)
	}()
	addr := lis.Addr().String()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return addr
}

func startStubNodeGRPC(t *testing.T, stub nodev1.NodeServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gs := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(gs, stub)

	go func() {
		_ = gs.Serve(lis)
	}()
	addr := lis.Addr().String()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return addr
}

func startStubCoordinatorGRPC(t *testing.T, stub coordinatorv1.CoordinatorServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gs := grpc.NewServer()
	coordinatorv1.RegisterCoordinatorServiceServer(gs, stub)

	go func() {
		_ = gs.Serve(lis)
	}()
	addr := lis.Addr().String()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return addr
}

func dialNodeClient(t *testing.T, addr string) nodev1.NodeServiceClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return nodev1.NewNodeServiceClient(conn)
}

func dialCoordinatorClient(t *testing.T, addr string) coordinatorv1.CoordinatorServiceClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return coordinatorv1.NewCoordinatorServiceClient(conn)
}

func grpcContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithTimeout(context.Background(), time.Second)
	}
	return context.WithTimeout(context.Background(), timeout)
}
