package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test helpers ---

func testNodeCfg(id string) *config.NodeConfig {
	return &config.NodeConfig{
		Node:               config.NodeSpec{ID: id, Address: "127.0.0.1:0"},
		CoordinatorAddress: "127.0.0.1:9999",
		HeartbeatInterval:  100 * time.Millisecond,
		QuorumTimeout:      500 * time.Millisecond,
	}
}

func testShardMapResp(shardID, leaderID string, replicas []string, addrs map[string]string) coordinator.ShardMapResponse {
	return coordinator.ShardMapResponse{
		Version: 1,
		Shards:  []shardmap.ShardInfo{{ID: shardID, Leader: leaderID, Replicas: replicas}},
		NodeAddresses: addrs,
	}
}

// newTestServer builds a Server and initialises it with a single-shard map.
// The server is pre-seeded as leader or follower depending on leaderID == nodeID.
func newTestServer(nodeID, leaderID string, replicas []string) *Server {
	addrs := make(map[string]string, len(replicas))
	for _, r := range replicas {
		addrs[r] = "127.0.0.1:0"
	}
	srv := NewServer(testNodeCfg(nodeID), nil)
	srv.InitShards(testShardMapResp("shard-0", leaderID, replicas, addrs))
	return srv
}

func jsonBody(v any) *strings.Reader {
	b, _ := json.Marshal(v)
	return strings.NewReader(string(b))
}

// getKV reads a key from the named shard's KV store directly (test-only).
func (s *Server) getKV(shardID, key string) (string, bool) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r == nil {
		return "", false
	}
	return r.KV.Get(key)
}

// setKV writes a key into the named shard's KV store directly (test-only).
func (s *Server) setKV(shardID, key, value string) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r != nil {
		r.KV.Put(key, value)
	}
}

// setTerm sets the term on the named shard's replica (test-only).
func (s *Server) setTerm(shardID string, term uint64) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r != nil {
		r.mu.Lock()
		r.Term = term
		r.mu.Unlock()
	}
}

// --- fanOutReplicate ---

func TestFanOutReplicate_NoFollowers(t *testing.T) {
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 1, Op: "put", Key: "k", Value: "v",
	}, nil, time.Second)
	assert.Equal(t, 0, acks)
}

func TestFanOutReplicate_AllAck(t *testing.T) {
	makeFollower := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: true, Term: 1})
		}))
	}
	s1, s2 := makeFollower(), makeFollower()
	defer s1.Close()
	defer s2.Close()

	peers := []string{s1.Listener.Addr().String(), s2.Listener.Addr().String()}
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 2, Op: "put", Key: "x", Value: "y",
	}, peers, time.Second)
	assert.Equal(t, 2, acks)
}

func TestFanOutReplicate_PartialAck(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: true, Term: 1})
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Error: "term mismatch"})
	}))
	defer bad.Close()

	peers := []string{good.Listener.Addr().String(), bad.Listener.Addr().String()}
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 3, Op: "delete", Key: "x",
	}, peers, time.Second)
	assert.Equal(t, 1, acks)
}

func TestFanOutReplicate_Timeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	peers := []string{slow.Listener.Addr().String()}
	start := time.Now()
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 1, Op: "put", Key: "k", Value: "v",
	}, peers, 80*time.Millisecond)
	elapsed := time.Since(start)

	assert.Equal(t, 0, acks)
	assert.Less(t, elapsed, time.Second, "should return quickly after timeout")
}

// --- fanOutCommit ---

func TestFanOutCommit_AllAck(t *testing.T) {
	makeFollower := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(CommitResponse{Success: true, Term: 1})
		}))
	}
	s1, s2 := makeFollower(), makeFollower()
	defer s1.Close()
	defer s2.Close()

	peers := []string{s1.Listener.Addr().String(), s2.Listener.Addr().String()}
	acks := fanOutCommit(context.Background(), "shard-0", CommitRequest{
		Term: 1, Version: 2,
	}, peers, time.Second)
	assert.Equal(t, 2, acks)
}

func TestFanOutCommit_PartialAck(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CommitResponse{Success: true, Term: 1})
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CommitResponse{Success: false, Error: "missing"})
	}))
	defer bad.Close()

	peers := []string{good.Listener.Addr().String(), bad.Listener.Addr().String()}
	acks := fanOutCommit(context.Background(), "shard-0", CommitRequest{
		Term: 1, Version: 3,
	}, peers, time.Second)
	assert.Equal(t, 1, acks)
}

// --- handleReplicate / handleCommit ---

func TestHandleReplicate_StagesWrite(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	req := ReplicateRequest{Term: 1, Version: 1, Op: "put", Key: "hello", Value: "world"}
	resp, err := http.Post(ts.URL+"/internal/replicate/shard-0", "application/json", jsonBody(req))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var rr ReplicateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rr))
	assert.True(t, rr.Success)

	val, ok := follower.getKV("shard-0", "hello")
	assert.False(t, ok, "write should not be applied before commit")
	assert.Empty(t, val)

	follower.mu.RLock()
	replica := follower.replicas["shard-0"]
	follower.mu.RUnlock()
	require.NotNil(t, replica)
	replica.mu.RLock()
	_, pending := replica.Pending[1]
	replica.mu.RUnlock()
	assert.True(t, pending, "pending entry should be staged")
}

func TestHandleCommit_AppliesWrite(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	appendReq := ReplicateRequest{Term: 1, Version: 1, Op: "put", Key: "hello", Value: "world"}
	resp, err := http.Post(ts.URL+"/internal/replicate/shard-0", "application/json", jsonBody(appendReq))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	commitReq := CommitRequest{Term: 1, Version: 1}
	commitResp, err := http.Post(ts.URL+"/internal/commit/shard-0", "application/json", jsonBody(commitReq))
	require.NoError(t, err)
	defer func() { _ = commitResp.Body.Close() }()
	require.Equal(t, http.StatusOK, commitResp.StatusCode)

	var cr CommitResponse
	require.NoError(t, json.NewDecoder(commitResp.Body).Decode(&cr))
	assert.True(t, cr.Success)

	val, ok := follower.getKV("shard-0", "hello")
	require.True(t, ok)
	assert.Equal(t, "world", val)
}

func TestHandleCommit_Delete(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	follower.setKV("shard-0", "gone", "value")
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	appendReq := ReplicateRequest{Term: 1, Version: 1, Op: "delete", Key: "gone"}
	resp, err := http.Post(ts.URL+"/internal/replicate/shard-0", "application/json", jsonBody(appendReq))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	commitReq := CommitRequest{Term: 1, Version: 1}
	commitResp, err := http.Post(ts.URL+"/internal/commit/shard-0", "application/json", jsonBody(commitReq))
	require.NoError(t, err)
	defer func() { _ = commitResp.Body.Close() }()
	require.Equal(t, http.StatusOK, commitResp.StatusCode)

	var cr CommitResponse
	require.NoError(t, json.NewDecoder(commitResp.Body).Decode(&cr))
	assert.True(t, cr.Success)

	_, ok := follower.getKV("shard-0", "gone")
	assert.False(t, ok, "key should have been deleted")
}

func TestHandleReplicate_StaleTerm(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	follower.setTerm("shard-0", 5) // follower is at term 5

	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	// Send a replicate with an older term.
	req := ReplicateRequest{Term: 2, Version: 1, Op: "put", Key: "k", Value: "v"}
	resp, err := http.Post(ts.URL+"/internal/replicate/shard-0", "application/json", jsonBody(req))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var rr ReplicateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rr))
	assert.False(t, rr.Success)
	assert.Equal(t, uint64(5), rr.Term, "should echo the follower's current term")
}

func TestHandleReplicate_UnknownShard(t *testing.T) {
	srv := newTestServer("node", "node", []string{"node"})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req := ReplicateRequest{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v"}
	resp, err := http.Post(ts.URL+"/internal/replicate/no-such-shard", "application/json", jsonBody(req))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- handleSync ---

func TestHandleSync_ReturnsSnapshot(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	leader.setKV("shard-0", "foo", "bar")
	leader.setKV("shard-0", "baz", "qux")

	ts := httptest.NewServer(leader.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/internal/sync/shard-0")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var sr SyncResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sr))
	assert.Equal(t, "bar", sr.KV["foo"])
	assert.Equal(t, "qux", sr.KV["baz"])
}

func TestHandleSync_NonLeaderRejects(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/internal/sync/shard-0")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// --- handleKV ---

func TestHandleKV_PutAndGet_SingleReplica(t *testing.T) {
	// Single-replica shard: no followers needed for quorum.
	leader := newTestServer("leader", "leader", []string{"leader"})
	ts := httptest.NewServer(leader.Handler())
	defer ts.Close()

	// Put
	putResp, err := http.Post(ts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "put", Key: "name", Value: "alice"}))
	require.NoError(t, err)
	defer func() { _ = putResp.Body.Close() }()
	require.Equal(t, http.StatusOK, putResp.StatusCode)

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(putResp.Body).Decode(&kvr))
	assert.True(t, kvr.OK)

	// Get
	getResp, err := http.Post(ts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "get", Key: "name"}))
	require.NoError(t, err)
	defer func() { _ = getResp.Body.Close() }()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var getKvr KVResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&getKvr))
	assert.True(t, getKvr.OK)
	assert.Equal(t, "alice", getKvr.Value)
}

func TestHandleKV_GetMissing(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	ts := httptest.NewServer(leader.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "get", Key: "missing"}))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&kvr))
	assert.False(t, kvr.OK)
}

func TestHandleKV_DeleteSingleReplica(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	leader.setKV("shard-0", "del", "me")
	ts := httptest.NewServer(leader.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "delete", Key: "del"}))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&kvr))
	assert.True(t, kvr.OK)

	_, ok := leader.getKV("shard-0", "del")
	assert.False(t, ok)
}

func TestHandleKV_NotLeaderReturnsError(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	ts := httptest.NewServer(follower.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "put", Key: "k", Value: "v"}))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusMisdirectedRequest, resp.StatusCode)

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&kvr))
	assert.False(t, kvr.OK)
	assert.Equal(t, "NOT_LEADER", kvr.Error)
	assert.Equal(t, "leader", kvr.LeaderID)
}

func TestHandleKV_PutWithFollowers_QuorumMet(t *testing.T) {
	// Start two real follower servers using their own handlers.
	f1 := newTestServer("f1", "leader", []string{"leader", "f1", "f2"})
	f2 := newTestServer("f2", "leader", []string{"leader", "f1", "f2"})
	ts1 := httptest.NewServer(f1.Handler())
	defer ts1.Close()
	ts2 := httptest.NewServer(f2.Handler())
	defer ts2.Close()

	// Build leader that knows about the followers' actual addresses.
	leaderCfg := testNodeCfg("leader")
	leader := NewServer(leaderCfg, nil)
	leader.InitShards(coordinator.ShardMapResponse{
		Version: 1,
		Shards: []shardmap.ShardInfo{
			{ID: "shard-0", Leader: "leader", Replicas: []string{"leader", "f1", "f2"}},
		},
		NodeAddresses: map[string]string{
			"leader": "leader-addr",
			"f1":     ts1.Listener.Addr().String(),
			"f2":     ts2.Listener.Addr().String(),
		},
	})
	lts := httptest.NewServer(leader.Handler())
	defer lts.Close()

	resp, err := http.Post(lts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "put", Key: "x", Value: "42"}))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&kvr))
	assert.True(t, kvr.OK, "write should succeed with quorum")
}

func TestHandleKV_PutWithFollowers_QuorumUnavailable(t *testing.T) {
	// Use an address that nothing is listening on — both followers are "dead".
	leaderCfg := testNodeCfg("leader")
	leaderCfg.QuorumTimeout = 100 * time.Millisecond
	leader := NewServer(leaderCfg, nil)
	leader.InitShards(coordinator.ShardMapResponse{
		Version: 1,
		Shards: []shardmap.ShardInfo{
			{ID: "shard-0", Leader: "leader", Replicas: []string{"leader", "f1", "f2"}},
		},
		NodeAddresses: map[string]string{
			"leader": "leader-addr",
			"f1":     "127.0.0.1:1", // nothing listening here
			"f2":     "127.0.0.1:2", // nothing listening here
		},
	})
	lts := httptest.NewServer(leader.Handler())
	defer lts.Close()

	resp, err := http.Post(lts.URL+"/kv/shard-0", "application/json",
		jsonBody(KVRequest{Op: "put", Key: "k", Value: "v"}))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var kvr KVResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&kvr))
	assert.False(t, kvr.OK)
}
