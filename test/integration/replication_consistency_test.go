//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/require"
)

func TestReplication_ApplyOnFollowers(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	resp := putKV(t, cluster.NodeAddrs["node-a"], "shard-0", "hello", "world")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)

	for _, follower := range []string{"node-b", "node-c"} {
		follower := follower
		require.Eventually(t, func() bool {
			status := nodeStatus(t, cluster.NodeAddrs[follower])
			if len(status.Shards) != 1 {
				return false
			}
			return status.Shards[0].Version == 1 && status.Shards[0].IsReady
		}, 2*time.Second, 50*time.Millisecond)
	}
}

func TestReplication_ForceRecoverRestoresFollower(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b"}, InitialLeader: "node-a"},
	})

	resp := putKV(t, cluster.NodeAddrs["node-a"], "shard-0", "k", "v")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)

	client := nodeClient(t, cluster.NodeAddrs["node-b"])
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.ForceRecover(ctx, &nodev1.ForceRecoverRequest{ShardId: "shard-0"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status := nodeStatus(t, cluster.NodeAddrs["node-b"])
		if len(status.Shards) != 1 {
			return false
		}
		return status.Shards[0].IsReady && status.Shards[0].Version == 1
	}, 2*time.Second, 50*time.Millisecond)
}
