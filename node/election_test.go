package node

import (
	"context"
	"testing"
	"time"

	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newElectionTestServer builds a Server suitable for election handler tests.
// It initialises the server with a single shard-0 and marks the replica as a
// follower unless leaderID == nodeID.
func newElectionTestServer(nodeID, leaderID string, peers []string) *Server {
	addrs := make(map[string]string, len(peers)+1)
	addrs[nodeID] = "127.0.0.1:0"
	for _, p := range peers {
		addrs[p] = "127.0.0.1:0"
	}
	replicas := append([]string{nodeID}, peers...)
	srv := NewServer(testNodeCfg(nodeID), nil)
	srv.InitShards(testShardMapResp("shard-0", leaderID, replicas, addrs))
	return srv
}

// setVersion sets the version on shard-0's replica directly (test-only).
func (s *Server) setVersion(shardID string, version uint64) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r != nil {
		r.mu.Lock()
		r.Version = version
		r.mu.Unlock()
	}
}

// --- RequestVote ---

func TestHandleRequestVote_GrantVote(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.VoteRequest{Term: 2, CandidateId: "leader", Version: 0, ShardId: "shard-0"}
	resp := doVoteRequest(t, client, req)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(2), resp.Term)
}

func TestHandleRequestVote_RejectStaleTerm(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setTerm("shard-0", 5)
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.VoteRequest{Term: 3, CandidateId: "leader", Version: 0, ShardId: "shard-0"}
	resp := doVoteRequest(t, client, req)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
}

func TestHandleRequestVote_GrantIdempotent(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.VoteRequest{Term: 2, CandidateId: "leader", Version: 0, ShardId: "shard-0"}
	r1 := doVoteRequest(t, client, req)
	r2 := doVoteRequest(t, client, req)
	assert.True(t, r1.VoteGranted)
	assert.True(t, r2.VoteGranted)
}

func TestHandleRequestVote_RejectDoubleVote(t *testing.T) {
	follower := newElectionTestServer("follower", "candidate-a", []string{"candidate-a", "candidate-b"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	r1 := doVoteRequest(t, client, &nodev1.VoteRequest{Term: 2, CandidateId: "candidate-a", Version: 0, ShardId: "shard-0"})
	r2 := doVoteRequest(t, client, &nodev1.VoteRequest{Term: 2, CandidateId: "candidate-b", Version: 0, ShardId: "shard-0"})
	assert.True(t, r1.VoteGranted)
	assert.False(t, r2.VoteGranted)
}

func TestHandleRequestVote_RejectStalerVersion(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setVersion("shard-0", 10)
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.VoteRequest{Term: 2, CandidateId: "leader", Version: 5, ShardId: "shard-0"}
	resp := doVoteRequest(t, client, req)
	assert.False(t, resp.VoteGranted)
}

func TestHandleRequestVote_HigherTermResetsVote(t *testing.T) {
	follower := newElectionTestServer("follower", "candidate-a", []string{"candidate-a", "candidate-b"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	r1 := doVoteRequest(t, client, &nodev1.VoteRequest{Term: 2, CandidateId: "candidate-a", Version: 0, ShardId: "shard-0"})
	require.True(t, r1.VoteGranted)

	r2 := doVoteRequest(t, client, &nodev1.VoteRequest{Term: 3, CandidateId: "candidate-b", Version: 0, ShardId: "shard-0"})
	assert.True(t, r2.VoteGranted)
}

// --- LeaderHeartbeat ---

func TestHandleLeaderHeartbeat_AcceptsValid(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setTerm("shard-0", 1)
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.LeaderHeartbeatRequest{Term: 1, Version: 0, LeaderId: "leader", ShardId: "shard-0"}
	resp := doLeaderHeartbeat(t, client, req)
	assert.Equal(t, uint64(1), resp.Term)

	follower.mu.RLock()
	r := follower.replicas["shard-0"]
	follower.mu.RUnlock()
	r.mu.RLock()
	contact := r.LastLeaderContact
	r.mu.RUnlock()
	assert.False(t, contact.IsZero())
}

func TestHandleLeaderHeartbeat_RejectsStaleTerm(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setTerm("shard-0", 5)
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	req := &nodev1.LeaderHeartbeatRequest{Term: 3, Version: 0, LeaderId: "leader", ShardId: "shard-0"}
	resp := doLeaderHeartbeat(t, client, req)
	assert.Equal(t, uint64(5), resp.Term)
}

func TestHandleLeaderHeartbeat_HigherTermDemotes(t *testing.T) {
	leader := newElectionTestServer("node-a", "node-a", []string{"node-b"})
	leader.setTerm("shard-0", 2)
	leader.mu.RLock()
	r := leader.replicas["shard-0"]
	leader.mu.RUnlock()
	r.mu.Lock()
	r.Role = RoleLeader
	r.mu.Unlock()

	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	req := &nodev1.LeaderHeartbeatRequest{Term: 5, Version: 0, LeaderId: "node-b", ShardId: "shard-0"}
	resp := doLeaderHeartbeat(t, client, req)
	assert.Equal(t, uint64(5), resp.Term)

	r.mu.RLock()
	role := r.Role
	term := r.Term
	r.mu.RUnlock()
	assert.Equal(t, RoleFollower, role)
	assert.Equal(t, uint64(5), term)
}

// --- startElection ---

func TestStartElection_WinsWithMajority(t *testing.T) {
	votes := 0
	peerAddr := startStubNodeGRPC(t, &stubNodeService{RequestVoteFn: func(context.Context, *nodev1.VoteRequest) (*nodev1.VoteResponse, error) {
		votes++
		return &nodev1.VoteResponse{Term: 2, VoteGranted: true}, nil
	}})

	coordAddr := startStubCoordinatorGRPC(t, &stubCoordinatorService{NotifyLeaderFn: func(context.Context, *coordinatorv1.NotifyLeaderRequest) (*coordinatorv1.NotifyLeaderResponse, error) {
		return &coordinatorv1.NotifyLeaderResponse{Accepted: true}, nil
	}})

	cfg := &config.NodeConfig{
		Node:               config.NodeSpec{ID: "node-a", Address: "127.0.0.1:0"},
		CoordinatorAddress: coordAddr,
		HeartbeatInterval:  100 * time.Millisecond,
		QuorumTimeout:      500 * time.Millisecond,
	}
	srv := NewServer(cfg, clock.Real{})
	srv.InitShards(testShardMapResp("shard-0", "node-b",
		[]string{"node-a", "node-b"},
		map[string]string{"node-a": "127.0.0.1:0", "node-b": peerAddr},
	))
	srv.mu.Lock()
	srv.nodeAddresses["node-b"] = peerAddr
	srv.mu.Unlock()

	srv.mu.RLock()
	replica := srv.replicas["shard-0"]
	srv.mu.RUnlock()

	replica.mu.Lock()
	replica.Term = 1
	replica.mu.Unlock()

	srv.startElection(context.Background(), replica)

	replica.mu.RLock()
	role := replica.Role
	term := replica.Term
	replica.mu.RUnlock()

	assert.Equal(t, RoleLeader, role)
	assert.Equal(t, uint64(2), term)
	assert.Equal(t, 1, votes)
}

func TestStartElection_FailsWithoutQuorum(t *testing.T) {
	peerAddr := startStubNodeGRPC(t, &stubNodeService{RequestVoteFn: func(context.Context, *nodev1.VoteRequest) (*nodev1.VoteResponse, error) {
		return &nodev1.VoteResponse{Term: 2, VoteGranted: false}, nil
	}})
	peer2Addr := startStubNodeGRPC(t, &stubNodeService{RequestVoteFn: func(context.Context, *nodev1.VoteRequest) (*nodev1.VoteResponse, error) {
		return &nodev1.VoteResponse{Term: 2, VoteGranted: false}, nil
	}})

	coordAddr := startStubCoordinatorGRPC(t, &stubCoordinatorService{})

	cfg := &config.NodeConfig{
		Node:               config.NodeSpec{ID: "node-a", Address: "127.0.0.1:0"},
		CoordinatorAddress: coordAddr,
		HeartbeatInterval:  100 * time.Millisecond,
		QuorumTimeout:      500 * time.Millisecond,
	}
	srv := NewServer(cfg, clock.Real{})
	srv.InitShards(testShardMapResp("shard-0", "node-b",
		[]string{"node-a", "node-b", "node-c"},
		map[string]string{
			"node-a": "127.0.0.1:0",
			"node-b": peerAddr,
			"node-c": peer2Addr,
		},
	))
	srv.mu.Lock()
	srv.nodeAddresses["node-b"] = peerAddr
	srv.nodeAddresses["node-c"] = peer2Addr
	srv.mu.Unlock()

	srv.mu.RLock()
	replica := srv.replicas["shard-0"]
	srv.mu.RUnlock()
	replica.mu.Lock()
	replica.Term = 1
	replica.mu.Unlock()

	srv.startElection(context.Background(), replica)

	replica.mu.RLock()
	role := replica.Role
	replica.mu.RUnlock()

	assert.Equal(t, RoleFollower, role)
}

// --- helpers ---

func doVoteRequest(t *testing.T, client nodev1.NodeServiceClient, req *nodev1.VoteRequest) *nodev1.VoteResponse {
	t.Helper()
	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.RequestVote(ctx, req)
	require.NoError(t, err)
	return resp
}

func doLeaderHeartbeat(t *testing.T, client nodev1.NodeServiceClient, req *nodev1.LeaderHeartbeatRequest) *nodev1.LeaderHeartbeatResponse {
	t.Helper()
	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.LeaderHeartbeat(ctx, req)
	require.NoError(t, err)
	return resp
}
