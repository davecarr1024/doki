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
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func addNodeToCluster(t *testing.T, ctx context.Context, coordAddr, nodeID string) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for new node %s", nodeID)
	nodeAddr := l.Addr().String()

	body, err := json.Marshal(coordinator.AddNodeRequest{NodeID: nodeID, Address: nodeAddr})
	require.NoError(t, err)
	resp, err := http.Post("http://"+coordAddr+"/admin/add_node", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	smResp := fetchShardMapRespGRPC(t, coordAddr)

	nodeCfg := &config.NodeConfig{
		Node:               config.NodeSpec{ID: nodeID, Address: nodeAddr},
		CoordinatorAddress: coordAddr,
		DataDir:            t.TempDir(),
		HeartbeatInterval:  100 * time.Millisecond,
	}
	nodeCfg.EnsureDefaults()
	srv := node.NewServer(nodeCfg, clock.Real{})
	srv.InitShards(smResp)
	go func() {
		if err := srv.StartOnListener(ctx, l); err != nil {
			t.Logf("node %s stopped: %v", nodeID, err)
		}
	}()

	waitForHTTP(t, "http://"+nodeAddr+"/ready", 3*time.Second)
	t.Logf("node %s ready at %s", nodeID, nodeAddr)
	return nodeAddr
}

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
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

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
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDynamic_AddNode(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")

	require.Eventually(t, func() bool {
		status := coordinatorStatus(t, cluster.CoordinatorAddr)
		for _, n := range status.Nodes {
			if n.NodeId == "node-d" && n.IsAlive {
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)
}

func TestDynamic_MigrateShard(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	require.Eventually(t, func() bool {
		status := coordinatorStatus(t, cluster.CoordinatorAddr)
		alive := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				alive++
			}
		}
		return alive == 3
	}, 5*time.Second, 100*time.Millisecond)

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 5; i++ {
		resp := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-e")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-f")

	adminMigrateShard(t, cluster.CoordinatorAddr, "shard-0", []string{"node-d", "node-e", "node-f"})

	require.Eventually(t, func() bool {
		sm := shardMapResponse(t, cluster.CoordinatorAddr)
		if sm.ShardMap == nil {
			return false
		}
		for _, sh := range sm.ShardMap.Shards {
			if sh.ShardId == "shard-0" {
				if sh.Leader == "" {
					return false
				}
				for _, r := range sh.Replicas {
					if r != "node-d" && r != "node-e" && r != "node-f" {
						return false
					}
				}
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond)

	newLeaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 5; i++ {
		resp := getKV(t, newLeaderAddr, "shard-0", fmt.Sprintf("key%d", i))
		assert.Equal(t, nodev1.GetResponse_RESULT_OK, resp.Result)
		assert.Equal(t, fmt.Sprintf("val%d", i), string(resp.Value))
	}

	post := putKV(t, newLeaderAddr, "shard-0", "post-migration", "ok")
	assert.Equal(t, nodev1.PutResponse_RESULT_OK, post.Result)
}

func TestDynamic_SplitShard(t *testing.T) {
	cluster := StartCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	require.Eventually(t, func() bool {
		status := coordinatorStatus(t, cluster.CoordinatorAddr)
		alive := 0
		for _, n := range status.Nodes {
			if n.IsAlive {
				alive++
			}
		}
		return alive == 3
	}, 5*time.Second, 100*time.Millisecond)

	leaderAddr := leaderAddrFor(t, cluster.CoordinatorAddr, "shard-0")
	for i := 0; i < 3; i++ {
		resp := putKV(t, leaderAddr, "shard-0", fmt.Sprintf("seed%d", i), fmt.Sprintf("v%d", i))
		require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-d")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-e")
	addNodeToCluster(t, ctx, cluster.CoordinatorAddr, "node-f")

	adminSplitShard(t, cluster.CoordinatorAddr, "shard-0", "shard-1", []string{"node-d", "node-e", "node-f"})

	var shard1LeaderAddr string
	require.Eventually(t, func() bool {
		sm := shardMapResponse(t, cluster.CoordinatorAddr)
		if sm.ShardMap == nil {
			return false
		}
		for _, sh := range sm.ShardMap.Shards {
			if sh.ShardId == "shard-1" {
				if sh.Leader == "" {
					return false
				}
				shard1LeaderAddr = leaderAddrFor(t, cluster.CoordinatorAddr, "shard-1")
				return shard1LeaderAddr != ""
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond)

	respA := putKV(t, leaderAddr, "shard-0", "original", "ok")
	assert.Equal(t, nodev1.PutResponse_RESULT_OK, respA.Result)

	respB := putKV(t, shard1LeaderAddr, "shard-1", "split", "ok")
	assert.Equal(t, nodev1.PutResponse_RESULT_OK, respB.Result)
}
