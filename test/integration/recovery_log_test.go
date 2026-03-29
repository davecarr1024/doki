//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryLog_IncrementalRecovery_SmallGap(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	for i := 0; i < 5; i++ {
		r := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)
	}

	resp := recoverKV(t, leaderAddr, "shard-0", 0)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	assert.Equal(t, 5, len(resp.Entries))
	assert.Equal(t, uint64(5), resp.Version)

	for i, e := range resp.Entries {
		assert.Equal(t, commonv1.Operation_TYPE_PUT, e.Op.Type)
		assert.Equal(t, fmt.Sprintf("k%d", i), string(e.Op.Key))
		assert.Equal(t, fmt.Sprintf("v%d", i), string(e.Op.Value))
		assert.Equal(t, uint64(i+1), e.Version)
	}
}

func TestRecoveryLog_IncrementalRecovery_AlreadyUpToDate(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	})

	leaderAddr := cluster.NodeAddrs["node-a"]

	r := putKV(t, leaderAddr, "shard-0", "x", "1")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)

	resp := recoverKV(t, leaderAddr, "shard-0", 1)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	assert.Empty(t, resp.Entries)
	assert.Equal(t, uint64(1), resp.Version)
}

func TestRecoveryLog_IncrementalRecovery_LargeGapFallback(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	})
	leaderAddr := cluster.NodeAddrs["node-a"]

	for i := 0; i < 10; i++ {
		r := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)
	}

	resp := recoverKV(t, leaderAddr, "shard-0", 0)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	assert.Equal(t, 10, len(resp.Entries))
}

func TestRecoveryLog_Recover_MidRange(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	for i := 0; i < 8; i++ {
		r := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("k%d", i), "v")
		require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)
	}

	resp := recoverKV(t, leaderAddr, "shard-0", 5)
	assert.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	assert.Equal(t, 3, len(resp.Entries))
	assert.Equal(t, uint64(6), resp.Entries[0].Version)
	assert.Equal(t, uint64(8), resp.Entries[2].Version)
}

func TestRecoveryLog_Recover_DeleteOp(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	r1 := putKV(t, leaderAddr, "shard-0", "gone", "x")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, r1.Result)
	r2 := deleteKV(t, leaderAddr, "shard-0", "gone")
	require.Equal(t, nodev1.DeleteResponse_RESULT_OK, r2.Result)

	resp := recoverKV(t, leaderAddr, "shard-0", 0)
	require.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	require.Len(t, resp.Entries, 2)
	assert.Equal(t, commonv1.Operation_TYPE_PUT, resp.Entries[0].Op.Type)
	assert.Equal(t, commonv1.Operation_TYPE_DELETE, resp.Entries[1].Op.Type)
	assert.Equal(t, "gone", string(resp.Entries[1].Op.Key))
}

func TestRecoveryLog_NonLeaderRejectsRecover(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	var followerAddr string
	for _, addr := range cluster.NodeAddrs {
		if addr != leaderAddr {
			followerAddr = addr
			break
		}
	}
	require.NotEmpty(t, followerAddr)

	client := nodeClient(t, followerAddr)
	_, err := client.Recover(context.Background(), &nodev1.RecoverRequest{ShardId: "shard-0", SinceVersion: 0})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestRecoveryLog_FollowerCatchesUpViaLog(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	const n = 10
	for i := 0; i < n; i++ {
		r := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)
	}

	require.Eventually(t, func() bool {
		for _, addr := range cluster.NodeAddrs {
			status := nodeStatus(t, addr)
			if len(status.Shards) != 1 || !status.Shards[0].IsReady {
				return false
			}
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	resp := recoverKV(t, leaderAddr, "shard-0", 0)
	require.Equal(t, nodev1.RecoverResponse_TYPE_ENTRIES, resp.Type)
	require.Equal(t, n, len(resp.Entries))
	assert.Equal(t, uint64(n), resp.Version)
}
