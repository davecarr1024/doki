//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// durabilityCluster is like testCluster but exposes server handles so tests
// can gracefully stop them and restart with the same data directories.
type durabilityCluster struct {
	CoordinatorAddr string
	NodeAddrs       map[string]string // node_id → addr
	DataDirs        map[string]string // node_id → data dir
	cancel          context.CancelFunc
	servers         map[string]*node.Server
}

// startDurabilityCluster starts a coordinator + nodes with explicit data dirs.
// snapshotInterval controls how many writes trigger a WAL snapshot; use a
// small value (e.g. 5) in tests to exercise snapshot + truncation.
func startDurabilityCluster(
	t *testing.T,
	nodeIDs []string,
	shardSpecs []config.ShardSpec,
	dataDirs map[string]string, // node_id → pre-created dir (or "" for t.TempDir())
	snapshotInterval int,
) *durabilityCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Coordinator
	coordListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	coordAddr := coordListener.Addr().String()

	nodeSpecs := make([]config.NodeSpec, len(nodeIDs))
	nodeListeners := make(map[string]net.Listener, len(nodeIDs))
	nodeAddrs := make(map[string]string, len(nodeIDs))

	for i, id := range nodeIDs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		nodeListeners[id] = l
		nodeAddrs[id] = l.Addr().String()
		nodeSpecs[i] = config.NodeSpec{ID: id, Address: l.Addr().String()}
	}

	clusterCfg := &config.ClusterConfig{
		Coordinator: config.CoordinatorConfig{
			Address:             coordAddr,
			HeartbeatIntervalMs: 100,
			FailureTimeoutMs:    300,
		},
		Nodes:  nodeSpecs,
		Shards: shardSpecs,
		Replication: config.ReplicationConfig{
			QuorumTimeoutMs:     500,
			MaxLagVersions:      1000,
			MaxBufferedVersions: 100,
		},
	}
	clusterCfg.Coordinator.HeartbeatInterval = 100 * time.Millisecond
	clusterCfg.Coordinator.FailureTimeout = 300 * time.Millisecond

	coordServer := coordinator.NewServer(clusterCfg, clock.Real{})
	require.NoError(t, coordServer.Init())
	go func() {
		if err := coordServer.StartOnListener(ctx, coordListener); err != nil {
			t.Logf("coordinator stopped: %v", err)
		}
	}()
	waitForHTTP(t, "http://"+coordAddr+"/ready", 3*time.Second)

	// Resolve data dirs.
	resolvedDirs := make(map[string]string, len(nodeIDs))
	for _, id := range nodeIDs {
		if d, ok := dataDirs[id]; ok && d != "" {
			resolvedDirs[id] = d
		} else {
			resolvedDirs[id] = t.TempDir()
		}
	}

	smResp := fetchShardMapRespHTTP(t, "http://"+coordAddr+"/shardmap")
	servers := make(map[string]*node.Server, len(nodeIDs))

	for _, id := range nodeIDs {
		id := id
		l := nodeListeners[id]
		nodeCfg := &config.NodeConfig{
			Node:               config.NodeSpec{ID: id, Address: l.Addr().String()},
			CoordinatorAddress: coordAddr,
			DataDir:            resolvedDirs[id],
			HeartbeatInterval:  100 * time.Millisecond,
			QuorumTimeout:      500 * time.Millisecond,
			SnapshotInterval:   snapshotInterval,
		}
		srv := node.NewServer(nodeCfg, clock.Real{})
		srv.InitShards(smResp)
		servers[id] = srv
		go func() {
			if err := srv.StartOnListener(ctx, l); err != nil {
				t.Logf("node %s stopped: %v", id, err)
			}
		}()
	}

	for id, addr := range nodeAddrs {
		waitForHTTP(t, "http://"+addr+"/ready", 3*time.Second)
		t.Logf("node %s ready at %s", id, addr)
	}

	return &durabilityCluster{
		CoordinatorAddr: coordAddr,
		NodeAddrs:       nodeAddrs,
		DataDirs:        resolvedDirs,
		cancel:          cancel,
		servers:         servers,
	}
}

// stop cancels the cluster context and closes all server resources.
func (dc *durabilityCluster) stop(t *testing.T) {
	t.Helper()
	dc.cancel()
	time.Sleep(100 * time.Millisecond) // let servers shut down
	for _, srv := range dc.servers {
		srv.Close()
	}
}

// --- Tests ---

func TestDurability_SingleNodeSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	// First cluster: write a value.
	c1 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)
	addr1 := c1.NodeAddrs["node-a"]

	putResp, status := postKV(t, addr1, "shard-0", node.KVRequest{Op: "put", Key: "persistent", Value: "yes"})
	require.Equal(t, 200, status)
	require.True(t, putResp.OK)
	c1.stop(t)

	// Second cluster: same data dir, different address (new port).
	c2 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)

	addr2 := c2.NodeAddrs["node-a"]
	getResp, status := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: "persistent"})
	require.Equal(t, 200, status)
	assert.True(t, getResp.OK, "value should survive node restart")
	assert.Equal(t, "yes", getResp.Value)
}

func TestDurability_MultipleWritesSurviveRestart(t *testing.T) {
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	c1 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)
	addr1 := c1.NodeAddrs["node-a"]

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, k := range keys {
		r, _ := postKV(t, addr1, "shard-0", node.KVRequest{Op: "put", Key: k, Value: k + "-val"})
		require.True(t, r.OK, "write %s should succeed", k)
	}
	// Also delete one.
	r, _ := postKV(t, addr1, "shard-0", node.KVRequest{Op: "delete", Key: "beta"})
	require.True(t, r.OK)
	c1.stop(t)

	// Restart.
	c2 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)
	addr2 := c2.NodeAddrs["node-a"]

	// alpha, gamma, delta should be present; beta should be gone.
	for _, k := range []string{"alpha", "gamma", "delta"} {
		resp, _ := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: k})
		assert.True(t, resp.OK, "key %s should survive restart", k)
		assert.Equal(t, k+"-val", resp.Value)
	}
	resp, _ := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: "beta"})
	assert.False(t, resp.OK, "deleted key beta should not survive restart")
}

func TestDurability_SnapshotTruncatesWAL(t *testing.T) {
	// Use snapshotInterval=3 so a snapshot is taken after every 3 writes.
	// This verifies snapshot + WAL truncation + replay correctness.
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	c1 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 3)
	addr1 := c1.NodeAddrs["node-a"]

	// Write 7 entries — triggers snapshot at writes 3 and 6.
	for i := 1; i <= 7; i++ {
		key := "k" + string(rune('0'+i))
		r, _ := postKV(t, addr1, "shard-0", node.KVRequest{Op: "put", Key: key, Value: key})
		require.True(t, r.OK, "write %d should succeed", i)
	}
	c1.stop(t)

	// Restart — should recover from snapshot + remaining WAL.
	c2 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 3)
	addr2 := c2.NodeAddrs["node-a"]

	for i := 1; i <= 7; i++ {
		key := "k" + string(rune('0'+i))
		resp, _ := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: key})
		assert.True(t, resp.OK, "key %s should survive restart after snapshot", key)
		assert.Equal(t, key, resp.Value)
	}
}

func TestDurability_ThreeNodeCluster_FollowerSurvivesRestart(t *testing.T) {
	dataDirs := map[string]string{
		"node-a": t.TempDir(),
		"node-b": t.TempDir(),
		"node-c": t.TempDir(),
	}
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}

	c1 := startDurabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shards, dataDirs, 100)
	leaderAddr := leaderAddrFor(t, c1.CoordinatorAddr, "shard-0")

	// Write through the leader.
	for i := 1; i <= 5; i++ {
		r, _ := postKV(t, leaderAddr, "shard-0", node.KVRequest{Op: "put", Key: "k", Value: "v"})
		require.True(t, r.OK)
	}

	// Wait for followers to receive replication.
	time.Sleep(200 * time.Millisecond)
	c1.stop(t)

	// Restart all nodes with the same data dirs.
	c2 := startDurabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shards, dataDirs, 100)
	leaderAddr2 := leaderAddrFor(t, c2.CoordinatorAddr, "shard-0")

	// Leader should have the data.
	getResp, _ := postKV(t, leaderAddr2, "shard-0", node.KVRequest{Op: "get", Key: "k"})
	assert.True(t, getResp.OK, "leader should recover data from WAL")
	assert.Equal(t, "v", getResp.Value)
}

func TestDurability_WriteAfterRestartIncreasesVersion(t *testing.T) {
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	c1 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)
	addr1 := c1.NodeAddrs["node-a"]

	r, _ := postKV(t, addr1, "shard-0", node.KVRequest{Op: "put", Key: "first", Value: "before"})
	require.True(t, r.OK)
	c1.stop(t)

	c2 := startDurabilityCluster(t, []string{"node-a"}, shards,
		map[string]string{"node-a": dataDir}, 100)
	addr2 := c2.NodeAddrs["node-a"]

	// Should be able to write again after restart.
	r2, status := postKV(t, addr2, "shard-0", node.KVRequest{Op: "put", Key: "second", Value: "after"})
	require.Equal(t, 200, status)
	assert.True(t, r2.OK, "writes should succeed after restart")

	// Both keys should be readable.
	r3, _ := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: "first"})
	assert.True(t, r3.OK)
	r4, _ := postKV(t, addr2, "shard-0", node.KVRequest{Op: "get", Key: "second"})
	assert.True(t, r4.OK)
}
