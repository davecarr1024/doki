//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"context"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// electionCluster is like testCluster but each node has its own cancel so
// individual nodes can be stopped without affecting the rest.
type electionCluster struct {
	CoordinatorAddr string
	NodeAddrs       map[string]string // node_id → address
	coordCancel     context.CancelFunc
	nodeCancels     map[string]context.CancelFunc
}

// StopNode stops a single node without affecting the coordinator or siblings.
func (c *electionCluster) StopNode(id string) {
	if cancel, ok := c.nodeCancels[id]; ok {
		cancel()
		delete(c.nodeCancels, id)
	}
}

// StopAll stops the coordinator and all remaining nodes.
func (c *electionCluster) StopAll() {
	for _, cancel := range c.nodeCancels {
		cancel()
	}
	c.coordCancel()
}

// startElectionCluster starts a coordinator and nodes with fast election
// timeouts so tests complete quickly.
func startElectionCluster(t *testing.T, nodeIDs []string, shardSpecs []config.ShardSpec) *electionCluster {
	t.Helper()

	coordCtx, coordCancel := context.WithCancel(context.Background())
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
			QuorumTimeoutMs:     300,
			MaxLagVersions:      1000,
			MaxBufferedVersions: 100,
		},
	}
	clusterCfg.Coordinator.HeartbeatInterval = 100 * time.Millisecond
	clusterCfg.Coordinator.FailureTimeout = 300 * time.Millisecond

	coordServer := coordinator.NewServer(clusterCfg, clock.Real{})
	require.NoError(t, coordServer.Init())
	go func() {
		if err := coordServer.StartOnListener(coordCtx, coordListener); err != nil {
			t.Logf("coordinator stopped: %v", err)
		}
	}()
	waitForHTTP(t, "http://"+coordAddr+"/ready", 3*time.Second)

	nodeCancels := make(map[string]context.CancelFunc, len(nodeIDs))
	nodeServers := make(map[string]*node.Server, len(nodeIDs))

	for _, id := range nodeIDs {
		id := id
		l := nodeListeners[id]
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		nodeCancels[id] = nodeCancel

		// Use short election timeouts so tests complete quickly.
		nodeCfg := &config.NodeConfig{
			Node:                 config.NodeSpec{ID: id, Address: l.Addr().String()},
			CoordinatorAddress:   coordAddr,
			HeartbeatIntervalMs:  100,
			HeartbeatInterval:    100 * time.Millisecond,
			QuorumTimeoutMs:      300,
			QuorumTimeout:        300 * time.Millisecond,
			ElectionTimeoutMinMs: 300,
			ElectionTimeoutMaxMs: 500,
			LeaderHeartbeatMs:    100,
		}
		nodeCfg.EnsureDefaults()

		smResp := fetchShardMapRespHTTP(t, "http://"+coordAddr+"/shardmap")
		srv := node.NewServer(nodeCfg, clock.Real{})
		srv.InitShards(smResp)
		nodeServers[id] = srv
		go func() {
			if err := srv.StartOnListener(nodeCtx, l); err != nil {
				t.Logf("node %s stopped: %v", id, err)
			}
		}()
	}

	for id, addr := range nodeAddrs {
		waitForHTTP(t, "http://"+addr+"/ready", 5*time.Second)
		t.Logf("node %s ready at %s", id, addr)
	}

	ec := &electionCluster{
		CoordinatorAddr: coordAddr,
		NodeAddrs:       nodeAddrs,
		coordCancel:     coordCancel,
		nodeCancels:     nodeCancels,
	}
	t.Cleanup(ec.StopAll)
	return ec
}

// currentLeaderAddr returns the address of the current leader for shardID,
// polling until one is found or the deadline passes.
func currentLeaderAddr(t *testing.T, coordAddr, shardID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lr coordinator.LeaderQueryResponse
		if getJSONDecoded(t, "http://"+coordAddr+"/leader/"+shardID, &lr) && lr.Address != "" {
			return lr.Address
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no leader found for shard %s within %s", shardID, timeout)
	return ""
}

// nodeStatusShardRole returns the role of the given shard on the given node,
// or "" if unavailable.
func nodeStatusShardRole(addr, shardID string) node.Role {
	resp, err := http.Get("http://" + addr + "/status") //nolint:noctx
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var status node.NodeStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return ""
	}
	for _, s := range status.Shards {
		if s.ShardID == shardID {
			return s.Role
		}
	}
	return ""
}

// --- Tests ---

// TestElection_LeaderDies_FollowerElected verifies that when the initial leader
// is killed one of the followers is promoted to leader within a reasonable time.
func TestElection_LeaderDies_FollowerElected(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	ec := startElectionCluster(t, []string{"node-a", "node-b", "node-c"}, shardSpec)

	// Verify initial state.
	initialLeaderAddr := currentLeaderAddr(t, ec.CoordinatorAddr, "shard-0", 3*time.Second)
	require.NotEmpty(t, initialLeaderAddr)

	// Write a key to confirm the cluster is healthy.
	r, status := postKV(t, initialLeaderAddr, "shard-0", node.KVRequest{
		Op: "put", Key: "before-election", Value: "yes",
	})
	require.Equal(t, http.StatusOK, status)
	require.True(t, r.OK)

	// Kill node-a (initial leader).
	ec.StopNode("node-a")

	// A new leader should be elected within a few seconds.
	// Election timeout is 300–500ms; coordinator failure detection is 300ms.
	var newLeaderAddr string
	require.Eventually(t, func() bool {
		var lr coordinator.LeaderQueryResponse
		if !getJSONDecoded(t, "http://"+ec.CoordinatorAddr+"/leader/shard-0", &lr) {
			return false
		}
		if lr.Address == "" || lr.Address == initialLeaderAddr {
			return false
		}
		newLeaderAddr = lr.Address
		return true
	}, 5*time.Second, 100*time.Millisecond, "a new leader should be elected after node-a dies")

	t.Logf("new leader elected at %s", newLeaderAddr)
	assert.NotEqual(t, initialLeaderAddr, newLeaderAddr)

	// New leader must have role=LEADER.
	require.Eventually(t, func() bool {
		return nodeStatusShardRole(newLeaderAddr, "shard-0") == node.RoleLeader
	}, 3*time.Second, 100*time.Millisecond, "new leader should report LEADER role")
}

// TestElection_NewLeaderServesWrites verifies that after a leader election the
// newly elected leader can serve client writes.
func TestElection_NewLeaderServesWrites(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	ec := startElectionCluster(t, []string{"node-a", "node-b", "node-c"}, shardSpec)

	initialLeaderAddr := currentLeaderAddr(t, ec.CoordinatorAddr, "shard-0", 3*time.Second)

	// Kill the current leader.
	ec.StopNode("node-a")

	// Wait for a new leader.
	var newLeaderAddr string
	require.Eventually(t, func() bool {
		var lr coordinator.LeaderQueryResponse
		if !getJSONDecoded(t, "http://"+ec.CoordinatorAddr+"/leader/shard-0", &lr) {
			return false
		}
		if lr.Address == "" || lr.Address == initialLeaderAddr {
			return false
		}
		newLeaderAddr = lr.Address
		return true
	}, 5*time.Second, 100*time.Millisecond, "new leader should be elected")

	// Wait until the new leader actually reports LEADER role (election timer + promotion).
	require.Eventually(t, func() bool {
		return nodeStatusShardRole(newLeaderAddr, "shard-0") == node.RoleLeader
	}, 3*time.Second, 100*time.Millisecond, "new leader should report LEADER role")

	// Write a key via the new leader.
	require.Eventually(t, func() bool {
		resp, status := postKV(t, newLeaderAddr, "shard-0", node.KVRequest{
			Op: "put", Key: fmt.Sprintf("post-election-%d", time.Now().UnixNano()), Value: "ok",
		})
		return status == http.StatusOK && resp.OK
	}, 3*time.Second, 200*time.Millisecond, "new leader should serve writes")
}

// TestElection_CoordinatorDown_ElectionStillWorks verifies that an election
// can complete even when the coordinator is unreachable. Nodes communicate
// directly via /internal/request_vote. The coordinator notification will fail
// but the elected leader is still recognised by peers.
func TestElection_CoordinatorDown_ElectionStillWorks(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	ec := startElectionCluster(t, []string{"node-a", "node-b", "node-c"}, shardSpec)

	// Confirm healthy before chaos.
	initialLeaderAddr := currentLeaderAddr(t, ec.CoordinatorAddr, "shard-0", 3*time.Second)
	require.NotEmpty(t, initialLeaderAddr)

	// Kill the coordinator first, then the leader.
	ec.coordCancel()
	ec.StopNode("node-a")

	// Wait for one of the remaining nodes to self-elect.
	// We can't ask the coordinator, so we poll the surviving nodes directly.
	survivorAddrs := make([]string, 0, 2)
	for id, addr := range ec.NodeAddrs {
		if id != "node-a" {
			survivorAddrs = append(survivorAddrs, addr)
		}
	}

	require.Eventually(t, func() bool {
		for _, addr := range survivorAddrs {
			if nodeStatusShardRole(addr, "shard-0") == node.RoleLeader {
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "a survivor should elect itself leader without coordinator")
}
