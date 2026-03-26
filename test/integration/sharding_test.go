//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addNodeToCluster registers a new node with the coordinator via POST /admin/add_node,
// then starts the node process and waits for it to become ready.
// It returns the new node's address.
func addNodeToCluster(t *testing.T, ctx context.Context, coordAddr, nodeID string) string {
	t.Helper()

	// Bind a port for the new node.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for new node %s", nodeID)
	nodeAddr := l.Addr().String()

	// Register with coordinator first so heartbeats are accepted.
	body, err := json.Marshal(coordinator.AddNodeRequest{NodeID: nodeID, Address: nodeAddr})
	require.NoError(t, err)
	resp, err := http.Post("http://"+coordAddr+"/admin/add_node", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "add_node should succeed")

	// Fetch the current shard map so the node can initialise its replicas.
	smResp := fetchShardMapRespHTTP(t, "http://"+coordAddr+"/shardmap")

	nodeCfg := &config.NodeConfig{
		Node:                config.NodeSpec{ID: nodeID, Address: nodeAddr},
		CoordinatorAddress:  coordAddr,
		DataDir:             t.TempDir(),
		HeartbeatIntervalMs: 100,
		HeartbeatInterval:   100 * time.Millisecond,
	}
	srv := node.NewServer(nodeCfg, clock.Real{})
	srv.InitShards(smResp)
	go func() {
		if err := srv.StartOnListener(ctx, l); err != nil {
			t.Logf("node %s stopped: %v", nodeID, err)
		}
	}()

	// Wait for the node's HTTP server to start (may have no shards yet, so /ready
	// returns 200 immediately — just wait for HTTP to be up).
	waitForHTTP(t, "http://"+nodeAddr+"/status", 3*time.Second)
	t.Logf("node %s ready at %s", nodeID, nodeAddr)
	return nodeAddr
}

// adminMigrateShard calls POST /admin/migrate_shard on the coordinator.
func adminMigrateShard(t *testing.T, coordAddr, shardID string, newReplicas []string) {
	t.Helper()
	body, err := json.Marshal(coordinator.MigrateShardRequest{
		ShardID:     shardID,
		NewReplicas: newReplicas,
	})
	require.NoError(t, err)
	resp, err := http.Post("http://"+coordAddr+"/admin/migrate_shard", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "migrate_shard should be accepted")
}

// adminSplitShard calls POST /admin/split_shard on the coordinator.
func adminSplitShard(t *testing.T, coordAddr, sourceShardID, newShardID string, newReplicas []string) {
	t.Helper()
	body, err := json.Marshal(coordinator.SplitShardRequest{
		SourceShardID: sourceShardID,
		NewShardID:    newShardID,
		NewReplicas:   newReplicas,
	})
	require.NoError(t, err)
	resp, err := http.Post("http://"+coordAddr+"/admin/split_shard", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "split_shard should be accepted")
}

// --- Tests ---

// TestDynamic_AddNode verifies that a new node can join the cluster, register
// successfully, and appears as alive in the coordinator status.
func TestDynamic_AddNode(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")

	// Coordinator should see the new node as alive within the failure-detection window.
	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/status", &status) {
			return false
		}
		for _, n := range status.Nodes {
			if n.NodeID == "node-d" && n.IsAlive {
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "node-d should appear as alive")
}

// TestDynamic_MigrateShard verifies the full migration lifecycle:
//  1. Write some data to the original shard.
//  2. Add a new node and migrate the shard to it.
//  3. Confirm all data is readable after migration.
func TestDynamic_MigrateShard(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	// Wait for initial cluster.
	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/status", &status) {
			return false
		}
		alive := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				alive++
			}
		}
		return alive == 3
	}, 5*time.Second, 100*time.Millisecond, "initial 3 nodes alive")

	// Write some data to the original shard.
	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 5; i++ {
		kvr, _ := postKV(t, leaderAddr, "shard-0", node.KVRequest{
			Op: "put", Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("val%d", i),
		})
		require.True(t, kvr.OK, "pre-migration write should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Add two new nodes.
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-e")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-f")

	// Trigger migration to the new nodes.
	adminMigrateShard(t, cluster.CoordinatorAddr, "shard-0", []string{"node-d", "node-e", "node-f"})

	// Wait for migration to complete: shard map should show only new replicas.
	require.Eventually(t, func() bool {
		var sm coordinator.ShardMapResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/shardmap", &sm) {
			return false
		}
		for _, sh := range sm.Shards {
			if sh.ID == "shard-0" {
				if len(sh.IncomingReplicas) != 0 {
					return false // still migrating
				}
				// Must have exactly the new nodes and a leader.
				if sh.Leader == "" {
					return false
				}
				for _, r := range sh.Replicas {
					if r != "node-d" && r != "node-e" && r != "node-f" {
						return false // old node still in replica set
					}
				}
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "shard-0 should migrate to new nodes")

	// All pre-migration writes should be readable on the new leader.
	newLeaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 5; i++ {
		kvr, _ := postKV(t, newLeaderAddr, "shard-0", node.KVRequest{
			Op: "get", Key: fmt.Sprintf("key%d", i),
		})
		assert.True(t, kvr.OK, "key%d should be readable after migration", i)
		assert.Equal(t, fmt.Sprintf("val%d", i), kvr.Value)
	}

	// New writes to the migrated shard should succeed.
	kvr, _ := postKV(t, newLeaderAddr, "shard-0", node.KVRequest{
		Op: "put", Key: "post-migration", Value: "ok",
	})
	assert.True(t, kvr.OK, "post-migration write should succeed")
}

// TestDynamic_SplitShard verifies that a shard can be split into a new shard
// that bootstraps from the original, and that both shards are independently writable.
func TestDynamic_SplitShard(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	// Wait for initial cluster.
	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/status", &status) {
			return false
		}
		alive := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				alive++
			}
		}
		return alive == 3
	}, 5*time.Second, 100*time.Millisecond, "initial 3 nodes alive")

	// Seed some data in the original shard.
	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 3; i++ {
		kvr, _ := postKV(t, leaderAddr, "shard-0", node.KVRequest{
			Op: "put", Key: fmt.Sprintf("seed%d", i), Value: fmt.Sprintf("v%d", i),
		})
		require.True(t, kvr.OK, "seed write should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Add new nodes for the new shard.
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-e")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-f")

	// Split shard-0 into shard-1 hosted on the new nodes.
	adminSplitShard(t, cluster.CoordinatorAddr, "shard-0", "shard-1", []string{"node-d", "node-e", "node-f"})

	// Wait for shard-1 to appear in the shard map with a ready leader.
	var shard1LeaderAddr string
	require.Eventually(t, func() bool {
		var sm coordinator.ShardMapResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/shardmap", &sm) {
			return false
		}
		for _, sh := range sm.Shards {
			if sh.ID != "shard-1" || sh.Leader == "" {
				continue
			}
			leaderAddr := sm.NodeAddresses[sh.Leader]
			if leaderAddr == "" {
				return false
			}
			// Confirm the leader node reports shard-1 as IsReady.
			var nodeStatus node.NodeStatusResponse
			if !getJSONDecoded(t, "http://"+leaderAddr+"/status", &nodeStatus) {
				return false
			}
			for _, rep := range nodeStatus.Shards {
				if rep.ShardID == "shard-1" && rep.IsReady {
					shard1LeaderAddr = leaderAddr
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "shard-1 should be ready with a leader")
	require.NotEmpty(t, shard1LeaderAddr)
	for i := 0; i < 3; i++ {
		kvr, _ := postKV(t, shard1LeaderAddr, "shard-1", node.KVRequest{
			Op: "get", Key: fmt.Sprintf("seed%d", i),
		})
		assert.True(t, kvr.OK, "seed%d should be bootstrapped into shard-1", i)
		assert.Equal(t, fmt.Sprintf("v%d", i), kvr.Value)
	}

	// Both shards should accept independent writes.
	kvr0, _ := postKV(t, leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0"),
		"shard-0", node.KVRequest{Op: "put", Key: "shard0-only", Value: "x"})
	assert.True(t, kvr0.OK, "shard-0 write should succeed after split")

	kvr1, _ := postKV(t, shard1LeaderAddr,
		"shard-1", node.KVRequest{Op: "put", Key: "shard1-only", Value: "y"})
	assert.True(t, kvr1.OK, "shard-1 write should succeed after split")
}

// TestDynamic_ClientsContinueDuringMigration verifies that writes issued to the
// original shard leader continue to succeed while migration is in progress.
func TestDynamic_ClientsContinueDuringMigration(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/status", &status) {
			return false
		}
		alive := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				alive++
			}
		}
		return alive == 3
	}, 5*time.Second, 100*time.Millisecond, "initial nodes alive")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Track writes that succeed before, during and after migration.
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-e")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-f")

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	// Write before migration starts.
	kvr, _ := postKV(t, leaderAddr, "shard-0", node.KVRequest{Op: "put", Key: "before", Value: "1"})
	require.True(t, kvr.OK)

	// Start migration.
	adminMigrateShard(t, cluster.CoordinatorAddr, "shard-0", []string{"node-d", "node-e", "node-f"})

	// Write during migration window (old leader still serves).
	kvr, _ = postKV(t, leaderAddr, "shard-0", node.KVRequest{Op: "put", Key: "during", Value: "2"})
	require.True(t, kvr.OK, "writes to old leader should succeed during migration")

	// Wait for migration to complete.
	require.Eventually(t, func() bool {
		var sm coordinator.ShardMapResponse
		if !getJSONDecoded(t, "http://"+cluster.CoordinatorAddr+"/shardmap", &sm) {
			return false
		}
		for _, sh := range sm.Shards {
			if sh.ID == "shard-0" && len(sh.IncomingReplicas) == 0 && sh.Leader != "" {
				for _, r := range sh.Replicas {
					if r != "node-d" && r != "node-e" && r != "node-f" {
						return false
					}
				}
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "migration should complete")

	// All keys should be readable on the new shard.
	newLeader := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for _, key := range []string{"before", "during"} {
		kvr, _ := postKV(t, newLeader, "shard-0", node.KVRequest{Op: "get", Key: key})
		assert.True(t, kvr.OK, "key %q should survive migration", key)
	}
}
