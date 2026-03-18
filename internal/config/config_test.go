package config_test

import (
	"testing"
	"time"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var validClusterYAML = []byte(`
coordinator:
  address: "0.0.0.0:7000"
  heartbeat_interval_ms: 500
  failure_timeout_ms: 1500

nodes:
  - id: node-a
    address: "node-a:8000"
  - id: node-b
    address: "node-b:8000"
  - id: node-c
    address: "node-c:8000"

shards:
  - id: shard-0
    replicas: [node-a, node-b, node-c]
    initial_leader: node-a

replication:
  quorum_timeout_ms: 1000
  max_lag_versions: 1000
`)

func TestParseClusterConfig(t *testing.T) {
	cfg, err := config.ParseClusterConfig(validClusterYAML)
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:7000", cfg.Coordinator.Address)
	assert.Equal(t, 500*time.Millisecond, cfg.Coordinator.HeartbeatInterval)
	assert.Equal(t, 1500*time.Millisecond, cfg.Coordinator.FailureTimeout)

	require.Len(t, cfg.Nodes, 3)
	assert.Equal(t, "node-a", cfg.Nodes[0].ID)
	assert.Equal(t, "node-a:8000", cfg.Nodes[0].Address)

	require.Len(t, cfg.Shards, 1)
	assert.Equal(t, "shard-0", cfg.Shards[0].ID)
	assert.Equal(t, []string{"node-a", "node-b", "node-c"}, cfg.Shards[0].Replicas)
	assert.Equal(t, "node-a", cfg.Shards[0].InitialLeader)

	assert.Equal(t, 1000, cfg.Replication.QuorumTimeoutMs)
}

func TestParseClusterConfig_Defaults(t *testing.T) {
	yaml := []byte(`
coordinator:
  address: "0.0.0.0:7000"
nodes:
  - id: node-a
    address: "node-a:8000"
shards:
  - id: shard-0
    replicas: [node-a]
`)
	cfg, err := config.ParseClusterConfig(yaml)
	require.NoError(t, err)

	// Defaults applied
	assert.Equal(t, 500*time.Millisecond, cfg.Coordinator.HeartbeatInterval)
	assert.Equal(t, 1500*time.Millisecond, cfg.Coordinator.FailureTimeout)
	assert.Equal(t, 1000, cfg.Replication.QuorumTimeoutMs)
	assert.Equal(t, 1000, cfg.Replication.MaxLagVersions)
	assert.Equal(t, 100, cfg.Replication.MaxBufferedVersions)
}

func TestParseClusterConfig_MissingAddress(t *testing.T) {
	yaml := []byte(`
coordinator: {}
nodes:
  - id: node-a
    address: "node-a:8000"
shards:
  - id: shard-0
    replicas: [node-a]
`)
	_, err := config.ParseClusterConfig(yaml)
	assert.ErrorContains(t, err, "address")
}

func TestParseClusterConfig_UnknownReplica(t *testing.T) {
	yaml := []byte(`
coordinator:
  address: "0.0.0.0:7000"
nodes:
  - id: node-a
    address: "node-a:8000"
shards:
  - id: shard-0
    replicas: [node-a, node-UNKNOWN]
`)
	_, err := config.ParseClusterConfig(yaml)
	assert.ErrorContains(t, err, "node-UNKNOWN")
}

func TestClusterConfig_NodeByID(t *testing.T) {
	cfg, err := config.ParseClusterConfig(validClusterYAML)
	require.NoError(t, err)

	node, err := cfg.NodeByID("node-b")
	require.NoError(t, err)
	assert.Equal(t, "node-b", node.ID)
	assert.Equal(t, "node-b:8000", node.Address)

	_, err = cfg.NodeByID("node-MISSING")
	assert.Error(t, err)
}

var validNodeYAML = []byte(`
node:
  id: "node-a"
  address: "0.0.0.0:8000"
coordinator_address: "coordinator:7000"
data_dir: "/data"
heartbeat_interval_ms: 500
`)

func TestParseNodeConfig(t *testing.T) {
	cfg, err := config.ParseNodeConfig(validNodeYAML)
	require.NoError(t, err)

	assert.Equal(t, "node-a", cfg.Node.ID)
	assert.Equal(t, "0.0.0.0:8000", cfg.Node.Address)
	assert.Equal(t, "coordinator:7000", cfg.CoordinatorAddress)
	assert.Equal(t, "/data", cfg.DataDir)
	assert.Equal(t, 500*time.Millisecond, cfg.HeartbeatInterval)
}

func TestParseNodeConfig_MissingID(t *testing.T) {
	yaml := []byte(`
node:
  address: "0.0.0.0:8000"
coordinator_address: "coordinator:7000"
`)
	_, err := config.ParseNodeConfig(yaml)
	assert.ErrorContains(t, err, "node.id")
}

func TestParseNodeConfig_MissingCoordinator(t *testing.T) {
	yaml := []byte(`
node:
  id: "node-a"
  address: "0.0.0.0:8000"
`)
	_, err := config.ParseNodeConfig(yaml)
	assert.ErrorContains(t, err, "coordinator_address")
}
