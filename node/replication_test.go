package node

import (
	"context"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/replicationlog"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		Version:       1,
		Shards:        []shardmap.ShardInfo{{ID: shardID, Leader: leaderID, Replicas: replicas}},
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

// getKV reads a key from the named shard's KV store directly (test-only).
func (s *Server) getKV(shardID, key string) (string, bool) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r == nil {
		return "", false
	}
	return r.SM.Get(key)
}

// setKV writes a key into the named shard's KV store directly (test-only).
func (s *Server) setKV(shardID, key, value string) {
	s.mu.RLock()
	r := s.replicas[shardID]
	s.mu.RUnlock()
	if r != nil {
		r.mu.Lock()
		snap := r.SM.Snapshot()
		if snap.KV == nil {
			snap.KV = make(map[string]string)
		}
		snap.KV[key] = value
		_ = r.SM.ApplySnapshot(snap, SnapshotNoPersist)
		r.mu.Unlock()
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

func replicateOp(op, key, value string) *commonv1.Operation {
	switch op {
	case "put":
		return &commonv1.Operation{Type: commonv1.Operation_TYPE_PUT, Key: []byte(key), Value: []byte(value)}
	case "delete":
		return &commonv1.Operation{Type: commonv1.Operation_TYPE_DELETE, Key: []byte(key)}
	default:
		return &commonv1.Operation{Type: commonv1.Operation_TYPE_UNSPECIFIED, Key: []byte(key), Value: []byte(value)}
	}
}

// --- fanOutReplicate ---

func TestFanOutReplicate_NoFollowers(t *testing.T) {
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 1, Op: "put", Key: "k", Value: "v",
	}, nil, time.Second)
	assert.Len(t, acks, 0)
}

func TestFanOutReplicate_AllAck(t *testing.T) {
	s1 := startStubNodeGRPC(t, &stubNodeService{ReplicateFn: func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
		return &nodev1.ReplicateResponse{Success: true, Term: 1}, nil
	}})
	s2 := startStubNodeGRPC(t, &stubNodeService{ReplicateFn: func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
		return &nodev1.ReplicateResponse{Success: true, Term: 1}, nil
	}})

	peers := map[string]string{
		"p1": s1,
		"p2": s2,
	}
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 2, Op: "put", Key: "x", Value: "y",
	}, peers, time.Second)
	assert.Len(t, acks, 2)
}

func TestFanOutReplicate_PartialAck(t *testing.T) {
	good := startStubNodeGRPC(t, &stubNodeService{ReplicateFn: func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
		return &nodev1.ReplicateResponse{Success: true, Term: 1}, nil
	}})
	bad := startStubNodeGRPC(t, &stubNodeService{ReplicateFn: func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
		return &nodev1.ReplicateResponse{Success: false, Term: 1}, nil
	}})

	peers := map[string]string{
		"good": good,
		"bad":  bad,
	}
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 3, Op: "delete", Key: "x",
	}, peers, time.Second)
	assert.Len(t, acks, 1)
}

func TestFanOutReplicate_Timeout(t *testing.T) {
	slow := startStubNodeGRPC(t, &stubNodeService{ReplicateFn: func(context.Context, *nodev1.ReplicateRequest) (*nodev1.ReplicateResponse, error) {
		time.Sleep(2 * time.Second)
		return &nodev1.ReplicateResponse{Success: true, Term: 1}, nil
	}})

	peers := map[string]string{"slow": slow}
	start := time.Now()
	acks := fanOutReplicate(context.Background(), "shard-0", ReplicateRequest{
		Term: 1, Version: 1, Op: "put", Key: "k", Value: "v",
	}, peers, 80*time.Millisecond)
	elapsed := time.Since(start)

	assert.Len(t, acks, 0)
	assert.Less(t, elapsed, time.Second, "should return quickly after timeout")
}

// --- Replicate ---

func TestHandleReplicate_AppliesWrite(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Replicate(ctx, &nodev1.ReplicateRequest{
		ShardId: "shard-0",
		Term:    1,
		Version: 1,
		Op:      replicateOp("put", "hello", "world"),
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	val, ok := follower.getKV("shard-0", "hello")
	require.True(t, ok)
	assert.Equal(t, "world", val)
}

func TestHandleReplicate_Delete(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	follower.setKV("shard-0", "gone", "value")
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	_, err := client.Replicate(ctx, &nodev1.ReplicateRequest{
		ShardId: "shard-0",
		Term:    1,
		Version: 1,
		Op:      replicateOp("delete", "gone", ""),
	})
	require.NoError(t, err)

	_, ok := follower.getKV("shard-0", "gone")
	assert.False(t, ok, "key should have been deleted")
}

func TestHandleReplicate_StaleTerm(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	follower.setTerm("shard-0", 5)
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Replicate(ctx, &nodev1.ReplicateRequest{
		ShardId: "shard-0",
		Term:    2,
		Version: 1,
		Op:      replicateOp("put", "k", "v"),
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
}

func TestHandleReplicate_GapTriggersRecovery(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Replicate(ctx, &nodev1.ReplicateRequest{
		ShardId: "shard-0",
		Term:    1,
		Version: 3,
		Op:      replicateOp("put", "k", "v"),
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)

	follower.mu.RLock()
	replica := follower.replicas["shard-0"]
	follower.mu.RUnlock()
	require.NotNil(t, replica)
	replica.mu.RLock()
	isReady := replica.IsReady
	replica.mu.RUnlock()
	assert.False(t, isReady, "gap should mark replica not-ready")
}

func TestHandleForceRecover_MarksNotReadyAndResetsLog(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	follower.mu.RLock()
	replica := follower.replicas["shard-0"]
	follower.mu.RUnlock()
	require.NotNil(t, replica)
	replica.mu.Lock()
	replica.IsReady = true
	err := replica.SM.Apply(replicationlog.Entry{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v"}, ApplyWithoutWAL)
	require.NoError(t, err)
	replica.mu.Unlock()

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	_, err = client.ForceRecover(ctx, &nodev1.ForceRecoverRequest{ShardId: "shard-0"})
	require.NoError(t, err)

	replica.mu.RLock()
	isReady := replica.IsReady
	logLen := replica.SM.LogLen()
	replica.mu.RUnlock()
	assert.False(t, isReady)
	assert.Equal(t, 0, logLen)
}

func TestHandleRecover_FollowerAheadGetsSnapshot(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	leader.mu.RLock()
	replica := leader.replicas["shard-0"]
	leader.mu.RUnlock()
	require.NotNil(t, replica)
	replica.mu.Lock()
	err := replica.SM.Apply(replicationlog.Entry{
		Term: 1, Version: 1, Op: "put", Key: "k", Value: "v",
	}, ApplyWithoutWAL)
	replica.mu.Unlock()
	require.NoError(t, err)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	recov, err := client.Recover(ctx, &nodev1.RecoverRequest{ShardId: "shard-0", SinceVersion: 2})
	require.NoError(t, err)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_SNAPSHOT, recov.Type)
	assert.Equal(t, uint64(1), recov.Version)
	assert.Equal(t, "v", string(recov.Kv[0].Value))
}

func TestHandleReplicate_UnknownShard(t *testing.T) {
	srv := newTestServer("node", "node", []string{"node"})
	addr := startTestNodeGRPC(t, srv)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	_, err := client.Replicate(ctx, &nodev1.ReplicateRequest{
		ShardId: "no-such-shard",
		Term:    1,
		Version: 1,
		Op:      replicateOp("put", "k", "v"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestRecover_ReturnsSnapshot(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	leader.setKV("shard-0", "foo", "bar")
	leader.setKV("shard-0", "baz", "qux")
	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	recov, err := client.Recover(ctx, &nodev1.RecoverRequest{ShardId: "shard-0", SinceVersion: 999})
	require.NoError(t, err)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_SNAPSHOT, recov.Type)
	kv := make(map[string]string)
	for _, e := range recov.Kv {
		kv[string(e.Key)] = string(e.Value)
	}
	assert.Equal(t, "bar", kv["foo"])
	assert.Equal(t, "qux", kv["baz"])
}

func TestRecover_NonLeaderRejects(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	_, err := client.Recover(ctx, &nodev1.RecoverRequest{ShardId: "shard-0", SinceVersion: 0})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// --- KV operations ---

func TestHandleKV_PutAndGet_SingleReplica(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	putResp, err := client.Put(ctx, &nodev1.PutRequest{ShardId: "shard-0", Key: []byte("name"), Value: []byte("alice")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.PutResponse_RESULT_OK, putResp.Result)

	getResp, err := client.Get(ctx, &nodev1.GetRequest{ShardId: "shard-0", Key: []byte("name")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.GetResponse_RESULT_OK, getResp.Result)
	assert.Equal(t, "alice", string(getResp.Value))
}

func TestHandleKV_GetMissing(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Get(ctx, &nodev1.GetRequest{ShardId: "shard-0", Key: []byte("missing")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.GetResponse_RESULT_NOT_FOUND, resp.Result)
}

func TestHandleKV_DeleteSingleReplica(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	leader.setKV("shard-0", "del", "me")
	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	delResp, err := client.Delete(ctx, &nodev1.DeleteRequest{ShardId: "shard-0", Key: []byte("del")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.DeleteResponse_RESULT_OK, delResp.Result)

	_, ok := leader.getKV("shard-0", "del")
	assert.False(t, ok)
}

func TestHandleKV_NotLeaderReturnsError(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Put(ctx, &nodev1.PutRequest{ShardId: "shard-0", Key: []byte("k"), Value: []byte("v")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.PutResponse_RESULT_NOT_LEADER, resp.Result)
	assert.Equal(t, "leader", resp.LeaderHint)
}

func TestHandleKV_PutWithFollowers_QuorumMet(t *testing.T) {
	f1 := newTestServer("f1", "leader", []string{"leader", "f1", "f2"})
	f2 := newTestServer("f2", "leader", []string{"leader", "f1", "f2"})
	addr1 := startTestNodeGRPC(t, f1)
	addr2 := startTestNodeGRPC(t, f2)

	leaderCfg := testNodeCfg("leader")
	leader := NewServer(leaderCfg, nil)
	leader.InitShards(coordinator.ShardMapResponse{
		Version: 1,
		Shards: []shardmap.ShardInfo{
			{ID: "shard-0", Leader: "leader", Replicas: []string{"leader", "f1", "f2"}},
		},
		NodeAddresses: map[string]string{
			"leader": "leader-addr",
			"f1":     addr1,
			"f2":     addr2,
		},
	})
	leaderAddr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, leaderAddr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Put(ctx, &nodev1.PutRequest{ShardId: "shard-0", Key: []byte("x"), Value: []byte("42")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)
}

func TestHandleKV_PutWithFollowers_QuorumUnavailable(t *testing.T) {
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
			"f1":     "127.0.0.1:1",
			"f2":     "127.0.0.1:2",
		},
	})
	leaderAddr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, leaderAddr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Put(ctx, &nodev1.PutRequest{ShardId: "shard-0", Key: []byte("k"), Value: []byte("v")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.PutResponse_RESULT_QUORUM_UNAVAILABLE, resp.Result)
}

func TestHandleKV_DeleteMapsToQuorumUnavailable(t *testing.T) {
	leaderCfg := testNodeCfg("leader")
	leaderCfg.QuorumTimeout = 100 * time.Millisecond
	leader := NewServer(leaderCfg, nil)
	leader.InitShards(coordinator.ShardMapResponse{
		Version: 1,
		Shards: []shardmap.ShardInfo{
			{ID: "shard-0", Leader: "leader", Replicas: []string{"leader", "f1"}},
		},
		NodeAddresses: map[string]string{
			"leader": "leader-addr",
			"f1":     "127.0.0.1:1",
		},
	})
	leaderAddr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, leaderAddr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Delete(ctx, &nodev1.DeleteRequest{ShardId: "shard-0", Key: []byte("k")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.DeleteResponse_RESULT_QUORUM_UNAVAILABLE, resp.Result)
}

func TestHandleKV_LeaderHintForDelete(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Delete(ctx, &nodev1.DeleteRequest{ShardId: "shard-0", Key: []byte("k")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.DeleteResponse_RESULT_NOT_LEADER, resp.Result)
	assert.Equal(t, "leader", resp.LeaderHint)
}

func TestHandleKV_LeaderHintForGet(t *testing.T) {
	follower := newTestServer("follower", "leader", []string{"leader", "follower"})
	addr := startTestNodeGRPC(t, follower)
	client := dialNodeClient(t, addr)

	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	resp, err := client.Get(ctx, &nodev1.GetRequest{ShardId: "shard-0", Key: []byte("k")})
	require.NoError(t, err)
	assert.Equal(t, nodev1.GetResponse_RESULT_NOT_LEADER, resp.Result)
	assert.Equal(t, "leader", resp.LeaderHint)
}

func TestHandleKV_PutAndGetWithReplicationLog(t *testing.T) {
	leader := newTestServer("leader", "leader", []string{"leader"})
	leader.mu.RLock()
	replica := leader.replicas["shard-0"]
	leader.mu.RUnlock()
	before := replica.SM.LogLen()

	addr := startTestNodeGRPC(t, leader)
	client := dialNodeClient(t, addr)
	ctx, cancel := grpcContext(time.Second)
	defer cancel()
	_, err := client.Put(ctx, &nodev1.PutRequest{ShardId: "shard-0", Key: []byte("k"), Value: []byte("v")})
	require.NoError(t, err)

	replica.mu.RLock()
	after := replica.SM.LogLen()
	replica.mu.RUnlock()
	assert.Greater(t, after, before)
}
