package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// --- handleRequestVote ---

func TestHandleRequestVote_GrantVote(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := VoteRequest{Term: 2, CandidateID: "leader", Version: 0}
	resp := doVoteRequest(t, ts.URL, "shard-0", req)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(2), resp.Term)
}

func TestHandleRequestVote_RejectStaleTerm(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setTerm("shard-0", 5)
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := VoteRequest{Term: 3, CandidateID: "leader", Version: 0}
	resp := doVoteRequest(t, ts.URL, "shard-0", req)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term) // returns current term
}

func TestHandleRequestVote_GrantIdempotent(t *testing.T) {
	// Granting the same vote twice should succeed (idempotent).
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := VoteRequest{Term: 2, CandidateID: "leader", Version: 0}
	r1 := doVoteRequest(t, ts.URL, "shard-0", req)
	r2 := doVoteRequest(t, ts.URL, "shard-0", req)
	assert.True(t, r1.VoteGranted)
	assert.True(t, r2.VoteGranted)
}

func TestHandleRequestVote_RejectDoubleVote(t *testing.T) {
	// After voting for candidate-a, deny candidate-b in the same term.
	follower := newElectionTestServer("follower", "candidate-a", []string{"candidate-a", "candidate-b"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	r1 := doVoteRequest(t, ts.URL, "shard-0", VoteRequest{Term: 2, CandidateID: "candidate-a", Version: 0})
	r2 := doVoteRequest(t, ts.URL, "shard-0", VoteRequest{Term: 2, CandidateID: "candidate-b", Version: 0})
	assert.True(t, r1.VoteGranted)
	assert.False(t, r2.VoteGranted)
}

func TestHandleRequestVote_RejectStalerVersion(t *testing.T) {
	// Candidate version behind follower → reject.
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setVersion("shard-0", 10)
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := VoteRequest{Term: 2, CandidateID: "leader", Version: 5}
	resp := doVoteRequest(t, ts.URL, "shard-0", req)
	assert.False(t, resp.VoteGranted)
}

func TestHandleRequestVote_HigherTermResetsVote(t *testing.T) {
	// Vote granted in term 2 for candidate-a, then term 3 arrives — follower
	// should be able to vote for a different candidate in term 3.
	follower := newElectionTestServer("follower", "candidate-a", []string{"candidate-a", "candidate-b"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	r1 := doVoteRequest(t, ts.URL, "shard-0", VoteRequest{Term: 2, CandidateID: "candidate-a", Version: 0})
	require.True(t, r1.VoteGranted)

	r2 := doVoteRequest(t, ts.URL, "shard-0", VoteRequest{Term: 3, CandidateID: "candidate-b", Version: 0})
	assert.True(t, r2.VoteGranted)
}

// --- handleLeaderHeartbeat ---

func TestHandleLeaderHeartbeat_AcceptsValid(t *testing.T) {
	follower := newElectionTestServer("follower", "leader", []string{"leader"})
	follower.setTerm("shard-0", 1)
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := LeaderHeartbeatRequest{Term: 1, Version: 0, LeaderID: "leader", ShardID: "shard-0"}
	resp := doLeaderHeartbeat(t, ts.URL, "shard-0", req)
	assert.Equal(t, uint64(1), resp.Term)

	// Verify LastLeaderContact was set.
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
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := LeaderHeartbeatRequest{Term: 3, Version: 0, LeaderID: "leader", ShardID: "shard-0"}
	resp := doLeaderHeartbeat(t, ts.URL, "shard-0", req)
	// Should still return 200 with our current term so the stale leader knows to step down.
	assert.Equal(t, uint64(5), resp.Term)
}

func TestHandleLeaderHeartbeat_HigherTermDemotes(t *testing.T) {
	// A node that thinks it is leader receives a heartbeat with a higher term.
	leader := newElectionTestServer("node-a", "node-a", []string{"node-b"})
	leader.setTerm("shard-0", 2)
	// Manually promote to leader.
	leader.mu.RLock()
	r := leader.replicas["shard-0"]
	leader.mu.RUnlock()
	r.mu.Lock()
	r.Role = RoleLeader
	r.mu.Unlock()

	ts := httptest.NewServer(leader.Handler())
	defer ts.Close()

	req := LeaderHeartbeatRequest{Term: 5, Version: 0, LeaderID: "node-b", ShardID: "shard-0"}
	resp := doLeaderHeartbeat(t, ts.URL, "shard-0", req)
	assert.Equal(t, uint64(5), resp.Term)

	r.mu.RLock()
	role := r.Role
	term := r.Term
	r.mu.RUnlock()
	assert.Equal(t, RoleFollower, role, "should have stepped down")
	assert.Equal(t, uint64(5), term)
}

// --- startElection ---

func TestStartElection_WinsWithMajority(t *testing.T) {
	// 3-node shard: node-a is follower with two peers that both grant votes.
	// node-a should win and become leader.
	votes := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		votes++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: 2, VoteGranted: true})
	}))
	defer peer.Close()
	peerAddr := peer.Listener.Addr().String()

	// Build a fake coordinator that never responds (election doesn't depend on it).
	fakeCoord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer fakeCoord.Close()

	cfg := &config.NodeConfig{
		Node:               config.NodeSpec{ID: "node-a", Address: "127.0.0.1:0"},
		CoordinatorAddress: fakeCoord.Listener.Addr().String(),
		HeartbeatInterval:  100 * time.Millisecond,
		QuorumTimeout:      500 * time.Millisecond,
	}
	srv := NewServer(cfg, clock.Real{})
	srv.InitShards(testShardMapResp("shard-0", "node-b",
		[]string{"node-a", "node-b"},
		map[string]string{"node-a": "127.0.0.1:0", "node-b": peerAddr},
	))
	// Manually set node-b's address.
	srv.mu.Lock()
	srv.nodeAddresses["node-b"] = peerAddr
	srv.mu.Unlock()

	srv.mu.RLock()
	replica := srv.replicas["shard-0"]
	srv.mu.RUnlock()

	// Set term to 1 so election bumps to 2.
	replica.mu.Lock()
	replica.Term = 1
	replica.mu.Unlock()

	srv.startElection(context.Background(), replica)

	replica.mu.RLock()
	role := replica.Role
	term := replica.Term
	replica.mu.RUnlock()

	assert.Equal(t, RoleLeader, role, "node-a should have won election")
	assert.Equal(t, uint64(2), term)
	assert.Equal(t, 1, votes, "peer should have received exactly one vote request")
}

func TestStartElection_FailsWithoutQuorum(t *testing.T) {
	// Peer rejects the vote — no quorum.
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: 2, VoteGranted: false})
	}))
	defer peer.Close()

	fakeCoord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fakeCoord.Close()

	cfg := &config.NodeConfig{
		Node:               config.NodeSpec{ID: "node-a", Address: "127.0.0.1:0"},
		CoordinatorAddress: fakeCoord.Listener.Addr().String(),
		HeartbeatInterval:  100 * time.Millisecond,
		QuorumTimeout:      500 * time.Millisecond,
	}
	srv := NewServer(cfg, clock.Real{})
	// 3 replicas → quorum = 2; need 1 follower ACK + self; peer rejects so no quorum.
	peer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: 2, VoteGranted: false})
	}))
	defer peer2.Close()

	srv.InitShards(testShardMapResp("shard-0", "node-b",
		[]string{"node-a", "node-b", "node-c"},
		map[string]string{
			"node-a": "127.0.0.1:0",
			"node-b": peer.Listener.Addr().String(),
			"node-c": peer2.Listener.Addr().String(),
		},
	))
	srv.mu.Lock()
	srv.nodeAddresses["node-b"] = peer.Listener.Addr().String()
	srv.nodeAddresses["node-c"] = peer2.Listener.Addr().String()
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

	assert.Equal(t, RoleFollower, role, "node-a should remain follower without quorum")
}

// --- helpers ---

func doVoteRequest(t *testing.T, baseURL, shardID string, req VoteRequest) VoteResponse {
	t.Helper()
	resp, err := http.Post(
		baseURL+"/internal/request_vote/"+shardID,
		"application/json",
		jsonBody(req),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var vr VoteResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&vr))
	return vr
}

func doLeaderHeartbeat(t *testing.T, baseURL, shardID string, req LeaderHeartbeatRequest) LeaderHeartbeatResponse {
	t.Helper()
	resp, err := http.Post(
		baseURL+"/internal/leader_heartbeat/"+shardID,
		"application/json",
		jsonBody(req),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var hr LeaderHeartbeatResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&hr))
	return hr
}
