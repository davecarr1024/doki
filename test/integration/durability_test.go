//go:build integration

package integration

import (
	"testing"

	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurability_SingleNodeSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	c1 := StartCluster(t, []string{"node-a"}, shards,
		WithDataDirs(map[string]string{"node-a": dataDir}),
		WithSnapshotInterval(100),
	)
	addr1 := c1.NodeAddrs["node-a"]

	putResp := putKV(t, addr1, "shard-0", "persistent", "yes")
	require.Equal(t, nodev1.PutResponse_RESULT_OK, putResp.Result)
	c1.StopAll(t)

	c2 := StartCluster(t, []string{"node-a"}, shards,
		WithDataDirs(map[string]string{"node-a": dataDir}),
		WithSnapshotInterval(100),
	)
	addr2 := c2.NodeAddrs["node-a"]

	getResp := getKV(t, addr2, "shard-0", "persistent")
	require.Equal(t, nodev1.GetResponse_RESULT_OK, getResp.Result)
	assert.Equal(t, "yes", string(getResp.Value))
}

func TestDurability_MultipleWritesSurviveRestart(t *testing.T) {
	dataDir := t.TempDir()
	shards := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a"}, InitialLeader: "node-a"},
	}

	c1 := StartCluster(t, []string{"node-a"}, shards,
		WithDataDirs(map[string]string{"node-a": dataDir}),
		WithSnapshotInterval(100),
	)
	addr1 := c1.NodeAddrs["node-a"]

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, k := range keys {
		r := putKV(t, addr1, "shard-0", k, k+"-val")
		require.Equal(t, nodev1.PutResponse_RESULT_OK, r.Result)
	}
	r := deleteKV(t, addr1, "shard-0", "beta")
	require.Equal(t, nodev1.DeleteResponse_RESULT_OK, r.Result)
	c1.StopAll(t)

	c2 := StartCluster(t, []string{"node-a"}, shards,
		WithDataDirs(map[string]string{"node-a": dataDir}),
		WithSnapshotInterval(100),
	)
	addr2 := c2.NodeAddrs["node-a"]

	for _, k := range []string{"alpha", "gamma", "delta"} {
		resp := getKV(t, addr2, "shard-0", k)
		require.Equal(t, nodev1.GetResponse_RESULT_OK, resp.Result)
	}
	resp := getKV(t, addr2, "shard-0", "beta")
	assert.Equal(t, nodev1.GetResponse_RESULT_NOT_FOUND, resp.Result)
}
