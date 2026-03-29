//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControlPlane_CoordinatorStatusHealthy(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	status := coordinatorStatus(t, cluster.CoordinatorAddr)
	assert.GreaterOrEqual(t, len(status.Nodes), 3)
	assert.GreaterOrEqual(t, len(status.Shards), 1)
}

func TestControlPlane_AllNodesHeartbeat(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	require.Eventually(t, func() bool {
		status := coordinatorStatus(t, cluster.CoordinatorAddr)
		aliveCount := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				aliveCount++
			}
		}
		return aliveCount == 3
	}, 3*time.Second, 100*time.Millisecond, "all 3 nodes should heartbeat within 3s")
}

func TestControlPlane_NodeStatusVisible(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	for id, addr := range cluster.NodeAddrs {
		status := nodeStatus(t, addr)
		assert.Equal(t, id, status.NodeId)
		assert.NotEmpty(t, status.Shards)
	}
}

func TestControlPlane_ShardMapContainsShard(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	sm := shardMapResponse(t, cluster.CoordinatorAddr)
	require.NotNil(t, sm.ShardMap)
	require.Len(t, sm.ShardMap.Shards, 1)
	assert.Equal(t, "shard-0", sm.ShardMap.Shards[0].ShardId)
	assert.Equal(t, "node-a", sm.ShardMap.Shards[0].Leader)
	assert.ElementsMatch(t, []string{"node-a", "node-b", "node-c"}, sm.ShardMap.Shards[0].Replicas)
}

func TestControlPlane_NodeShowsShardRole(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderStatus := nodeStatus(t, cluster.NodeAddrs["node-a"])
	require.Len(t, leaderStatus.Shards, 1)
	assert.Equal(t, "shard-0", leaderStatus.Shards[0].ShardId)
	assert.Equal(t, "LEADER", leaderStatus.Shards[0].Role)

	followerStatus := nodeStatus(t, cluster.NodeAddrs["node-b"])
	require.Len(t, followerStatus.Shards, 1)
	assert.Equal(t, "shard-0", followerStatus.Shards[0].ShardId)
	assert.Equal(t, "FOLLOWER", followerStatus.Shards[0].Role)
}
