//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putKV(t *testing.T, addr, shardID, key, value string) *nodev1.PutResponse {
	t.Helper()
	client := nodeClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Put(ctx, &nodev1.PutRequest{ShardId: shardID, Key: []byte(key), Value: []byte(value)})
	require.NoError(t, err)
	return resp
}

func getKV(t *testing.T, addr, shardID, key string) *nodev1.GetResponse {
	t.Helper()
	client := nodeClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Get(ctx, &nodev1.GetRequest{ShardId: shardID, Key: []byte(key)})
	require.NoError(t, err)
	return resp
}

func deleteKV(t *testing.T, addr, shardID, key string) *nodev1.DeleteResponse {
	t.Helper()
	client := nodeClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Delete(ctx, &nodev1.DeleteRequest{ShardId: shardID, Key: []byte(key)})
	require.NoError(t, err)
	return resp
}

func recoverKV(t *testing.T, addr, shardID string, sinceVersion uint64) *nodev1.RecoverResponse {
	t.Helper()
	client := nodeClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Recover(ctx, &nodev1.RecoverRequest{ShardId: shardID, SinceVersion: sinceVersion})
	require.NoError(t, err)
	return resp
}

func TestKV_PutAndGet_Leader(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	putResp := putKV(t, leaderAddr, "shard-0", "greeting", "hello")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, putResp.Result)

	getResp := getKV(t, leaderAddr, "shard-0", "greeting")
	require.Equal(t, nodev1.GetResponse_RESULT_OK, getResp.Result)
	assert.Equal(t, "hello", string(getResp.Value))
}

func TestKV_GetMissing(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	getResp := getKV(t, leaderAddr, "shard-0", "no-such-key")
	assert.Equal(t, nodev1.GetResponse_RESULT_NOT_FOUND, getResp.Result)
}

func TestKV_Delete(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	_ = putKV(t, leaderAddr, "shard-0", "temp", "bye")
	delResp := deleteKV(t, leaderAddr, "shard-0", "temp")
	require.Equal(t, nodev1.DeleteResponse_RESULT_OK, delResp.Result)

	getResp := getKV(t, leaderAddr, "shard-0", "temp")
	assert.Equal(t, nodev1.GetResponse_RESULT_NOT_FOUND, getResp.Result)
}

func TestKV_NonLeaderRejectsWrite(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	var followerAddr string
	for id, addr := range cluster.NodeAddrs {
		if addr != leaderAddr && id != "node-a" {
			followerAddr = addr
			break
		}
	}
	require.NotEmpty(t, followerAddr)

	putResp := putKV(t, followerAddr, "shard-0", "k", "v")
	assert.Equal(t, nodev1.PutResponse_RESULT_NOT_LEADER, putResp.Result)
	assert.NotEmpty(t, putResp.LeaderHint)
}

func TestKV_FollowerReceivesReplication(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	putResp := putKV(t, leaderAddr, "shard-0", "replicated", "yes")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, putResp.Result)

	require.Eventually(t, func() bool {
		for _, addr := range cluster.NodeAddrs {
			status := nodeStatus(t, addr)
			if len(status.Shards) != 1 || !status.Shards[0].IsReady || status.Shards[0].Version < 1 {
				return false
			}
		}
		return true
	}, 2*time.Second, 100*time.Millisecond)
}

func TestKV_FollowerRecoverySnapshot(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	})

	nodeAddr := cluster.NodeAddrs["node-a"]
	putResp := putKV(t, nodeAddr, "shard-0", "state", "ok")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, putResp.Result)

	rec := recoverKV(t, nodeAddr, "shard-0", 999)
	require.Equal(t, nodev1.RecoverResponse_TYPE_SNAPSHOT, rec.Type)
	found := false
	for _, e := range rec.Kv {
		if string(e.Key) == "state" && string(e.Value) == "ok" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestKV_MultipleWrites(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")

	for i := 0; i < 5; i++ {
		resp := putKV(t, leaderAddr, "shard-0", "key", "val")
		require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)
	}

	getResp := getKV(t, leaderAddr, "shard-0", "key")
	assert.Equal(t, nodev1.GetResponse_RESULT_OK, getResp.Result)
	assert.Equal(t, "val", string(getResp.Value))
}
