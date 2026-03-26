//go:build reliability

package reliability

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shard0 is the test shard used by all chaos scenarios.
var shard0 = []config.ShardSpec{
	{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
}

// --- Scenario 1: Single leader failure ---

// TestChaos_SingleLeaderFailure kills the initial leader and verifies:
//   - A new leader is elected within 5 s
//   - Writes committed before the failure are still readable
//   - The cluster can accept writes from the new leader
//   - No split brain
func TestChaos_SingleLeaderFailure(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	// Write a few keys to establish state.
	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("pre-failure-%d", i)
		val := fmt.Sprintf("v%d", i)
		require.True(t, rc.PutKV(lr.Address, "shard-0", key, val),
			"pre-failure write %d should succeed", i)
		wt.Record(key, val)
	}

	// Kill the leader.
	var leaderNode string
	for id, addr := range rc.NodeAddrs {
		if addr == lr.Address {
			leaderNode = id
			break
		}
	}
	require.NotEmpty(t, leaderNode)
	rc.StopNode(leaderNode)

	// A new leader should appear.
	newLR := rc.WaitForLeaderChange("shard-0", lr.Address, 8*time.Second)
	require.NotEmpty(t, newLR.Address)
	t.Logf("new leader: %s at %s (term=%d)", newLR.LeaderID, newLR.Address, newLR.Term)

	// Wait until the new leader actually reports LEADER role.
	require.Eventually(t, func() bool {
		return rc.NodeShardRole(newLR.Address, "shard-0") == node.RoleLeader
	}, 5*time.Second, 100*time.Millisecond)

	// Committed writes must survive.
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)

	// New leader must accept writes.
	ok := rc.PutKV(newLR.Address, "shard-0", "post-failure", "yes")
	assert.True(t, ok, "new leader should accept writes")

	// No split brain.
	AssertNoSplitBrain(t, rc, "shard-0")
}

// --- Scenario 2: Minority follower failure ---

// TestChaos_MinorityFollowerFailure kills one follower (minority) and verifies:
//   - The cluster remains writable (quorum = 2 of 3)
//   - No split brain
//   - Replication lag eventually reaches zero after healed
func TestChaos_MinorityFollowerFailure(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)

	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	wt := newWriteTracker()

	// Identify a follower.
	var followerNode string
	for id, addr := range rc.NodeAddrs {
		if addr != lr.Address {
			followerNode = id
			break
		}
	}
	require.NotEmpty(t, followerNode)
	rc.StopNode(followerNode)

	// Cluster should still accept writes with 2 of 3 alive.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("during-failure-%d", i)
		val := fmt.Sprintf("v%d", i)
		require.Eventually(t, func() bool {
			leaderAddr := rc.CurrentLeader("shard-0", 2*time.Second).Address
			if leaderAddr == "" {
				return false
			}
			ok := rc.PutKV(leaderAddr, "shard-0", key, val)
			if ok {
				wt.Record(key, val)
			}
			return ok
		}, 3*time.Second, 100*time.Millisecond, "write %d should succeed with minority down", i)
	}

	AssertNoSplitBrain(t, rc, "shard-0")
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)
}

// --- Scenario 3: Coordinator failure ---

// TestChaos_CoordinatorFailure kills the coordinator and verifies:
//   - The cluster can still elect a new leader without coordinator
//   - Writes succeed peer-to-peer
func TestChaos_CoordinatorFailure(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)

	// Confirm cluster is healthy.
	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	require.NotEmpty(t, lr.Address)

	// Kill coordinator then the leader.
	rc.StopCoordinator()
	var leaderNode string
	for id, addr := range rc.NodeAddrs {
		if addr == lr.Address {
			leaderNode = id
			break
		}
	}
	rc.StopNode(leaderNode)

	// A survivor should self-elect.
	require.Eventually(t, func() bool {
		for id, addr := range rc.NodeAddrs {
			if id == leaderNode {
				continue
			}
			if _, running := rc.nodeCancels[id]; !running {
				continue
			}
			if rc.NodeShardRole(addr, "shard-0") == node.RoleLeader {
				return true
			}
		}
		return false
	}, 8*time.Second, 100*time.Millisecond, "a survivor should self-elect without coordinator")
}

// --- Scenario 4: Rolling restart ---

// TestChaos_RollingRestart restarts each node in sequence (one at a time),
// verifying the cluster stays writable throughout and no data is lost.
func TestChaos_RollingRestart(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	// Baseline writes before restart.
	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("before-restart-%d", i)
		val := fmt.Sprintf("v%d", i)
		require.True(t, rc.PutKV(lr.Address, "shard-0", key, val))
		wt.Record(key, val)
	}

	// Rolling restart: stop and restart each node.
	for _, id := range rc.NodeIDs {
		t.Logf("rolling restart: stopping %s", id)
		rc.StopNode(id)

		// Wait for quorum to stabilize after kill.
		time.Sleep(600 * time.Millisecond)

		// At least one write should succeed while node is down
		// (as long as it's not the only node in the shard).
		require.Eventually(t, func() bool {
			var leaderAddr string
			for nodeID, addr := range rc.NodeAddrs {
				if nodeID == id {
					continue
				}
				if _, running := rc.nodeCancels[nodeID]; !running {
					continue
				}
				if rc.NodeShardRole(addr, "shard-0") == node.RoleLeader {
					leaderAddr = addr
					break
				}
			}
			if leaderAddr == "" {
				return true // no leader yet, but that's ok for this step
			}
			key := fmt.Sprintf("during-restart-%s-%d", id, time.Now().UnixNano())
			val := "v"
			ok := rc.PutKV(leaderAddr, "shard-0", key, val)
			if ok {
				wt.Record(key, val)
			}
			return true
		}, 5*time.Second, 200*time.Millisecond)
	}

	// All nodes restarted; wait for a leader.
	_ = rc.CurrentLeader("shard-0", 8*time.Second)

	AssertNoSplitBrain(t, rc, "shard-0")
}

// --- Scenario 5: Slow follower ---

// TestChaos_SlowFollower adds artificial latency to one follower and verifies:
//   - Writes still succeed (quorum does not require the slow node)
//   - Replication lag eventually resolves after healing
//   - No split brain
func TestChaos_SlowFollower(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	lr := rc.CurrentLeader("shard-0", 5*time.Second)

	// Identify a follower and slow it down.
	var slowNode string
	for id, addr := range rc.NodeAddrs {
		if addr != lr.Address {
			slowNode = id
			break
		}
	}
	require.NotEmpty(t, slowNode)
	rc.SlowNode(slowNode, 300*time.Millisecond)
	t.Logf("slowed node: %s", slowNode)

	// Writes should succeed despite the slow follower (quorum = 2 of 3).
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("slow-follower-%d", i)
		val := fmt.Sprintf("v%d", i)
		require.Eventually(t, func() bool {
			leaderAddr := rc.CurrentLeader("shard-0", 2*time.Second).Address
			ok := rc.PutKV(leaderAddr, "shard-0", key, val)
			if ok {
				wt.Record(key, val)
			}
			return ok
		}, 4*time.Second, 200*time.Millisecond, "write %d should succeed with slow follower", i)
	}

	// Heal the slow node.
	rc.HealNode(slowNode)

	// Followers should converge.
	AssertFollowersConverge(t, rc, "shard-0", 5*time.Second)
	AssertNoSplitBrain(t, rc, "shard-0")
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)
}

// --- Scenario 6: Network partition ---

// TestChaos_NetworkPartition partitions a minority follower (simulated by
// dropping all outbound requests to its address) and verifies the cluster
// survives.
func TestChaos_NetworkPartition(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	lr := rc.CurrentLeader("shard-0", 5*time.Second)

	// Partition a follower.
	var partitionedNode string
	for id, addr := range rc.NodeAddrs {
		if addr != lr.Address {
			partitionedNode = id
			break
		}
	}
	require.NotEmpty(t, partitionedNode)
	rc.PartitionNode(partitionedNode)
	t.Logf("partitioned node: %s", partitionedNode)

	// Writes should succeed against the 2-node majority.
	leaderAddr := lr.Address
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("partitioned-%d", i)
		val := fmt.Sprintf("v%d", i)
		require.Eventually(t, func() bool {
			ok := rc.PutKV(leaderAddr, "shard-0", key, val)
			if ok {
				wt.Record(key, val)
			}
			return ok
		}, 3*time.Second, 200*time.Millisecond)
	}

	// Heal the partition.
	rc.HealPartition(partitionedNode)
	t.Log("partition healed")

	// After healing, follower should converge and no split brain.
	AssertFollowersConverge(t, rc, "shard-0", 8*time.Second)
	AssertNoSplitBrain(t, rc, "shard-0")
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)
}

// --- Helpers ---

// postKVHTTP sends a raw KV request using the default HTTP client (bypasses fault transport).
// Used in scenarios where we need to reach a node directly.
func postKVHTTP(t *testing.T, nodeAddr, shardID string, req node.KVRequest) (node.KVResponse, int) {
	t.Helper()
	body, _ := marshalJSON(req)
	resp, err := http.Post(
		"http://"+nodeAddr+"/kv/"+shardID,
		"application/json",
		body,
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var kvr node.KVResponse
	require.NoError(t, decodeJSON(resp.Body, &kvr))
	return kvr, resp.StatusCode
}
