//go:build reliability

package reliability

import (
	"fmt"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertNoSplitBrain verifies that at most one node believes it is the leader
// for shardID across all alive node addresses.
func AssertNoSplitBrain(t *testing.T, rc *reliabilityCluster, shardID string) {
	t.Helper()
	leaders := []string{}
	for id, addr := range rc.NodeAddrs {
		if _, running := rc.nodeCancels[id]; !running {
			continue
		}
		if rc.NodeShardRole(addr, shardID) == node.RoleLeader {
			leaders = append(leaders, id)
		}
	}
	assert.LessOrEqual(t, len(leaders), 1,
		"split brain detected: multiple nodes claim LEADER for shard %s: %v", shardID, leaders)
}

// AssertQuorumAvailable verifies that the coordinator reports quorum available
// for all shards.
func AssertQuorumAvailable(t *testing.T, rc *reliabilityCluster) {
	t.Helper()
	var status coordinator.CoordinatorStatusResponse
	require.True(t, getJSONDecoded(t, rc.httpClient, "http://"+rc.CoordinatorAddr+"/status", &status),
		"could not fetch coordinator status")
	for _, sh := range status.Shards {
		assert.True(t, sh.QuorumAlive,
			"quorum not available for shard %s (alive replicas below majority)", sh.ID)
	}
}

// AssertFollowersConverge waits until all alive replicas of shardID have the
// same version as the leader, or fails after timeout.
func AssertFollowersConverge(t *testing.T, rc *reliabilityCluster, shardID string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		var status coordinator.CoordinatorStatusResponse
		if !getJSONDecoded(t, rc.httpClient, "http://"+rc.CoordinatorAddr+"/status", &status) {
			return false
		}
		for _, sh := range status.Shards {
			if sh.ID != shardID {
				continue
			}
			if sh.MaxReplicationLag > 0 {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond,
		"followers for shard %s did not converge within %s", shardID, timeout)
}

// AssertAllCommittedWritesSurvive reads every key recorded in WriteTracker
// from the current leader and verifies the values match.
func AssertAllCommittedWritesSurvive(t *testing.T, rc *reliabilityCluster, shardID string, wt *WriteTracker) {
	t.Helper()

	// Find current leader.
	lr := rc.CurrentLeader(shardID, 5*time.Second)
	require.NotEmpty(t, lr.Address, "need a leader to verify writes")

	committed := wt.Committed()
	failures := []string{}
	for key, wantVal := range committed {
		gotVal, ok := rc.GetKV(lr.Address, shardID, key)
		if !ok || gotVal != wantVal {
			failures = append(failures, fmt.Sprintf("key=%s want=%q got=%q ok=%v", key, wantVal, gotVal, ok))
		}
	}
	assert.Empty(t, failures,
		"committed writes not readable after failover (%d missing):\n%v",
		len(failures), failures)
}

// AssertReplicationLagBelow asserts that maximum replication lag across all
// shards is at or below maxLag.
func AssertReplicationLagBelow(t *testing.T, rc *reliabilityCluster, maxLag uint64) {
	t.Helper()
	var status coordinator.CoordinatorStatusResponse
	require.True(t, getJSONDecoded(t, rc.httpClient, "http://"+rc.CoordinatorAddr+"/status", &status))
	for _, sh := range status.Shards {
		assert.LessOrEqual(t, sh.MaxReplicationLag, maxLag,
			"shard %s replication lag %d exceeds threshold %d", sh.ID, sh.MaxReplicationLag, maxLag)
	}
}
