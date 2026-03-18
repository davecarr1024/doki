//go:build integration

// Package integration contains end-to-end tests that start real HTTP servers
// in-process. No Docker is required. These tests verify that the coordinator
// and nodes can communicate correctly over the network.
//
// Run with: go test ./test/integration/... -tags=integration -v
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCluster is a fully in-process cluster for integration testing.
type testCluster struct {
	CoordinatorAddr string
	NodeAddrs       map[string]string // node_id → address
	cancel          context.CancelFunc
}

// startCluster creates and starts a coordinator and the given nodes.
// All servers bind to random OS-assigned ports.
func startCluster(t *testing.T, nodeIDs []string, shardSpecs []config.ShardSpec) *testCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Start coordinator
	coordListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for coordinator")
	coordAddr := coordListener.Addr().String()

	nodeSpecs := make([]config.NodeSpec, len(nodeIDs))
	nodeListeners := make(map[string]net.Listener, len(nodeIDs))
	nodeAddrs := make(map[string]string, len(nodeIDs))

	for i, id := range nodeIDs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err, "listen for node %s", id)
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

	// Wait for coordinator to be ready
	waitForHTTP(t, "http://"+coordAddr+"/ready", 3*time.Second)

	// Start each node
	for _, id := range nodeIDs {
		id := id
		l := nodeListeners[id]
		nodeCfg := &config.NodeConfig{
			Node:                config.NodeSpec{ID: id, Address: l.Addr().String()},
			CoordinatorAddress:  coordAddr,
			DataDir:             t.TempDir(),
			HeartbeatIntervalMs: 100,
			HeartbeatInterval:   100 * time.Millisecond,
		}
		// Fetch shard map from coordinator
		shardInfos := fetchShardMapHTTP(t, "http://"+coordAddr+"/shardmap")

		srv := node.NewServer(nodeCfg, clock.Real{})
		srv.InitShards(shardInfos)
		go func() {
			if err := srv.StartOnListener(ctx, l); err != nil {
				t.Logf("node %s stopped: %v", id, err)
			}
		}()
	}

	// Wait for all nodes to be ready
	for id, addr := range nodeAddrs {
		waitForHTTP(t, "http://"+addr+"/ready", 3*time.Second)
		t.Logf("node %s ready at %s", id, addr)
	}

	return &testCluster{
		CoordinatorAddr: coordAddr,
		NodeAddrs:       nodeAddrs,
		cancel:          cancel,
	}
}

// --- Tests ---

func TestCluster_CoordinatorHealthy(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	resp := getJSON(t, "http://"+cluster.CoordinatorAddr+"/status")

	assert.Contains(t, resp, "shard_map_version")
	assert.Contains(t, resp, "nodes")
	assert.Contains(t, resp, "shards")
}

func TestCluster_AllNodesHeartbeat(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	// Wait for all nodes to heartbeat
	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/status", &status) {
			return false
		}
		aliveCount := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				aliveCount++
			}
		}
		return aliveCount == 3
	}, 3*time.Second, 100*time.Millisecond, "all 3 nodes should heartbeat within 3s")
}

func TestCluster_NodeStatusEndpoint(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	for id, addr := range cluster.NodeAddrs {
		resp := getJSON(t, "http://"+addr+"/status")
		assert.Contains(t, resp, "node_id", "node %s status should contain node_id", id)
		assert.Contains(t, resp, "shards", "node %s status should contain shards", id)
	}
}

func TestCluster_ShardMapContainsShard(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	var sm coordinator.ShardMapResponse
	require.True(t, getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/shardmap", &sm))

	require.Len(t, sm.Shards, 1)
	assert.Equal(t, "shard-0", sm.Shards[0].ID)
	assert.Equal(t, "node-a", sm.Shards[0].Leader)
	assert.ElementsMatch(t, []string{"node-a", "node-b", "node-c"}, sm.Shards[0].Replicas)
}

func TestCluster_NodeShowsShardRole(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	var status node.NodeStatusResponse
	require.True(t, getJSONDecoded(t, "http://"+cluster.NodeAddrs["node-a"]+"/status", &status))

	require.Len(t, status.Shards, 1, "node-a should host shard-0")
	assert.Equal(t, "shard-0", status.Shards[0].ShardID)
	assert.Equal(t, node.RoleLeader, status.Shards[0].Role)

	var followerStatus node.NodeStatusResponse
	require.True(t, getJSONDecoded(t, "http://"+cluster.NodeAddrs["node-b"]+"/status", &followerStatus))
	require.Len(t, followerStatus.Shards, 1, "node-b should host shard-0")
	assert.Equal(t, node.RoleFollower, followerStatus.Shards[0].Role)
}

// --- Helpers ---

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not become ready within %s: %s", timeout, url)
}

func getJSON(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return buf.String()
}

func getJSONDecoded(t *testing.T, url string, out any) bool {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Logf("GET %s failed: %v", url, err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Logf("decode response from %s: %v", url, err)
		return false
	}
	return true
}

func fetchShardMapHTTP(t *testing.T, url string) []shardmap.ShardInfo {
	t.Helper()
	var sm coordinator.ShardMapResponse
	require.True(t, getJSONDecoded(t, url, &sm))
	return sm.Shards
}
