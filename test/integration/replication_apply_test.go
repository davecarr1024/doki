//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/require"
)

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	return resp
}

func TestIntegration_ReplicationAppliesOnFollowers(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	})

	resp := postJSON(t, "http://"+cluster.NodeAddrs["node-a"]+"/kv/shard-0", node.KVRequest{
		Op: "put", Key: "hello", Value: "world",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Followers should apply the write and bump their versions.
	for _, follower := range []string{"node-b", "node-c"} {
		follower := follower
		require.Eventually(t, func() bool {
			var status node.NodeStatusResponse
			if !getJSONDecoded(t, "http://"+cluster.NodeAddrs[follower]+"/status", &status) {
				return false
			}
			if len(status.Shards) != 1 {
				return false
			}
			return status.Shards[0].Version == 1 && status.Shards[0].IsReady
		}, 2*time.Second, 50*time.Millisecond)
	}
}

func TestIntegration_ForceRecoverRestoresFollower(t *testing.T) {
	cluster := startCluster(t, []string{"node-a", "node-b"}, []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b"}, InitialLeader: "node-a"},
	})

	resp := postJSON(t, "http://"+cluster.NodeAddrs["node-a"]+"/kv/shard-0", node.KVRequest{
		Op: "put", Key: "k", Value: "v",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	forceResp, err := http.Post("http://"+cluster.NodeAddrs["node-b"]+"/internal/force_recover/shard-0", "application/json", nil)
	require.NoError(t, err)
	forceResp.Body.Close()
	require.Equal(t, http.StatusOK, forceResp.StatusCode)

	require.Eventually(t, func() bool {
		var status node.NodeStatusResponse
		if !getJSONDecoded(t, "http://"+cluster.NodeAddrs["node-b"]+"/status", &status) {
			return false
		}
		if len(status.Shards) != 1 {
			return false
		}
		return status.Shards[0].IsReady && status.Shards[0].Version == 1
	}, 2*time.Second, 50*time.Millisecond)
}
