//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startElectionCluster(t *testing.T, nodeIDs []string, shardSpecs []config.ShardSpec) *ClusterControl {
	return StartCluster(t, nodeIDs, shardSpecs,
		WithNodeConfig(func(cfg *config.NodeConfig) {
			cfg.HeartbeatInterval = 100 * time.Millisecond
			cfg.QuorumTimeout = 300 * time.Millisecond
			cfg.ElectionTimeoutMin = 300 * time.Millisecond
			cfg.ElectionTimeoutMax = 500 * time.Millisecond
			cfg.LeaderHeartbeat = 100 * time.Millisecond
		}),
	)
}

func lookupLeaderAddr(coordAddr, shardID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	client := coordinatorv1.NewCoordinatorServiceClient(conn)
	resp, err := client.WhereIsLeader(ctx, &coordinatorv1.WhereIsLeaderRequest{ShardId: shardID})
	if err != nil {
		return "", err
	}
	return resp.Address, nil
}

func waitForLeaderAddr(t *testing.T, coordAddr, shardID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		addr, err := lookupLeaderAddr(coordAddr, shardID)
		if err == nil && addr != "" {
			return addr
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no leader found for shard %s within %s", shardID, timeout)
	return ""
}

// --- Tests ---

func TestElection_LeaderDies_FollowerElected(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c", "node-d"}, InitialLeader: "node-a"},
	}
	cluster := startElectionCluster(t, []string{"node-a", "node-b", "node-c", "node-d"}, shardSpec)

	initialLeaderAddr := waitForLeaderAddr(t, cluster.CoordinatorAddr, "shard-0", 3*time.Second)
	require.NotEmpty(t, initialLeaderAddr)

	resp := putKV(t, initialLeaderAddr, "shard-0", "before-election", "yes")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, resp.Result)

	cluster.StopNode(t, "node-a")

	var newLeaderAddr string
	require.Eventually(t, func() bool {
		addr, err := lookupLeaderAddr(cluster.CoordinatorAddr, "shard-0")
		if err != nil || addr == "" || addr == initialLeaderAddr {
			return false
		}
		newLeaderAddr = addr
		return true
	}, 5*time.Second, 100*time.Millisecond)

	t.Logf("new leader elected at %s", newLeaderAddr)
	assert.NotEqual(t, initialLeaderAddr, newLeaderAddr)

	require.Eventually(t, func() bool {
		status := nodeStatus(t, newLeaderAddr)
		for _, shard := range status.Shards {
			if shard.ShardId == "shard-0" {
				return shard.Role == "LEADER"
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
}

func TestElection_NewLeaderServesWrites(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	cluster := startElectionCluster(t, []string{"node-a", "node-b", "node-c"}, shardSpec)

	initialLeaderAddr := waitForLeaderAddr(t, cluster.CoordinatorAddr, "shard-0", 3*time.Second)
	cluster.StopNode(t, "node-a")

	var newLeaderAddr string
	require.Eventually(t, func() bool {
		addr, err := lookupLeaderAddr(cluster.CoordinatorAddr, "shard-0")
		if err != nil || addr == "" || addr == initialLeaderAddr {
			return false
		}
		newLeaderAddr = addr
		return true
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		status := nodeStatus(t, newLeaderAddr)
		for _, shard := range status.Shards {
			if shard.ShardId == "shard-0" {
				return shard.Role == "LEADER"
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		resp := putKV(t, newLeaderAddr, "shard-0", fmt.Sprintf("post-election-%d", time.Now().UnixNano()), "ok")
		return resp.Result == nodev1.PutResponse_RESULT_OK
	}, 3*time.Second, 200*time.Millisecond)
}

func TestElection_CoordinatorDown_ElectionStillWorks(t *testing.T) {
	shardSpec := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	cluster := startElectionCluster(t, []string{"node-a", "node-b", "node-c"}, shardSpec)

	initialLeaderAddr := waitForLeaderAddr(t, cluster.CoordinatorAddr, "shard-0", 3*time.Second)
	require.NotEmpty(t, initialLeaderAddr)

	cluster.StopCoordinator(t)
	cluster.StopNode(t, "node-a")

	survivorAddrs := make([]string, 0, 3)
	for id, addr := range cluster.NodeAddrs {
		if id != "node-a" {
			survivorAddrs = append(survivorAddrs, addr)
		}
	}

	require.Eventually(t, func() bool {
		for _, addr := range survivorAddrs {
			status := nodeStatus(t, addr)
			for _, shard := range status.Shards {
				if shard.ShardId == "shard-0" && shard.Role == "LEADER" {
					return true
				}
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)
}
