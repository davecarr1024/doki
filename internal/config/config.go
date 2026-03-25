// Package config provides configuration types and loading for Doki.
//
// There are two distinct configs:
//   - ClusterConfig: loaded by the coordinator; describes the full cluster topology
//   - NodeConfig: loaded by each leaf node; describes the node's own identity and
//     how to reach the coordinator
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// --- Coordinator config ---

// CoordinatorConfig holds tuning parameters for the coordinator process.
type CoordinatorConfig struct {
	// Address to listen on, e.g. "0.0.0.0:7000"
	Address string `yaml:"address"`
	// How often nodes are expected to heartbeat (used to set tick rate)
	HeartbeatInterval time.Duration `yaml:"-"`
	// Raw ms value parsed from YAML
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`
	// How long without a heartbeat before a node is considered failed
	FailureTimeout time.Duration `yaml:"-"`
	// Raw ms value parsed from YAML
	FailureTimeoutMs int `yaml:"failure_timeout_ms"`
}

// NodeSpec describes a node in the cluster as seen from the coordinator.
type NodeSpec struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// ShardSpec describes a shard as defined in the static cluster config.
type ShardSpec struct {
	ID            string   `yaml:"id"`
	Replicas      []string `yaml:"replicas"` // node IDs
	InitialLeader string   `yaml:"initial_leader"`
}

// ReplicationConfig holds replication tuning parameters.
type ReplicationConfig struct {
	QuorumTimeoutMs    int `yaml:"quorum_timeout_ms"`
	MaxLagVersions     int `yaml:"max_lag_versions"`
	MaxBufferedVersions int `yaml:"max_buffered_versions"`
}

// ClusterConfig is the coordinator's full configuration.
type ClusterConfig struct {
	Coordinator CoordinatorConfig `yaml:"coordinator"`
	Nodes       []NodeSpec        `yaml:"nodes"`
	Shards      []ShardSpec       `yaml:"shards"`
	Replication ReplicationConfig `yaml:"replication"`
}

// LoadClusterConfig reads and parses a ClusterConfig from a YAML file.
func LoadClusterConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster config %q: %w", path, err)
	}
	return ParseClusterConfig(data)
}

// ParseClusterConfig parses a ClusterConfig from raw YAML bytes.
func ParseClusterConfig(data []byte) (*ClusterConfig, error) {
	var cfg ClusterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cluster config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster config: %w", err)
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func (c *ClusterConfig) applyDefaults() {
	if c.Coordinator.HeartbeatIntervalMs == 0 {
		c.Coordinator.HeartbeatIntervalMs = 500
	}
	if c.Coordinator.FailureTimeoutMs == 0 {
		c.Coordinator.FailureTimeoutMs = 1500
	}
	if c.Replication.QuorumTimeoutMs == 0 {
		c.Replication.QuorumTimeoutMs = 1000
	}
	if c.Replication.MaxLagVersions == 0 {
		c.Replication.MaxLagVersions = 1000
	}
	if c.Replication.MaxBufferedVersions == 0 {
		c.Replication.MaxBufferedVersions = 100
	}
	// Convert ms fields to time.Duration
	c.Coordinator.HeartbeatInterval = time.Duration(c.Coordinator.HeartbeatIntervalMs) * time.Millisecond
	c.Coordinator.FailureTimeout = time.Duration(c.Coordinator.FailureTimeoutMs) * time.Millisecond
}

func (c *ClusterConfig) validate() error {
	if c.Coordinator.Address == "" {
		return fmt.Errorf("coordinator.address is required")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("at least one node must be defined")
	}
	if len(c.Shards) == 0 {
		return fmt.Errorf("at least one shard must be defined")
	}
	nodeIDs := make(map[string]bool, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.ID == "" {
			return fmt.Errorf("node id is required")
		}
		if n.Address == "" {
			return fmt.Errorf("node %q: address is required", n.ID)
		}
		nodeIDs[n.ID] = true
	}
	for _, s := range c.Shards {
		if s.ID == "" {
			return fmt.Errorf("shard id is required")
		}
		if len(s.Replicas) == 0 {
			return fmt.Errorf("shard %q: at least one replica required", s.ID)
		}
		for _, r := range s.Replicas {
			if !nodeIDs[r] {
				return fmt.Errorf("shard %q: replica %q is not a known node", s.ID, r)
			}
		}
		if s.InitialLeader != "" && !nodeIDs[s.InitialLeader] {
			return fmt.Errorf("shard %q: initial_leader %q is not a known node", s.ID, s.InitialLeader)
		}
	}
	return nil
}

// NodeByID returns the NodeSpec with the given ID, or an error if not found.
func (c *ClusterConfig) NodeByID(id string) (NodeSpec, error) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return NodeSpec{}, fmt.Errorf("node %q not found in cluster config", id)
}

// --- Node config ---

// NodeConfig is a leaf node's own configuration.
type NodeConfig struct {
	Node               NodeSpec `yaml:"node"`
	CoordinatorAddress string   `yaml:"coordinator_address"`
	DataDir            string   `yaml:"data_dir"`
	// How often this node sends heartbeats to the coordinator
	HeartbeatInterval   time.Duration `yaml:"-"`
	HeartbeatIntervalMs int           `yaml:"heartbeat_interval_ms"`
	// How long to wait for quorum ACKs during a write
	QuorumTimeout   time.Duration `yaml:"-"`
	QuorumTimeoutMs int           `yaml:"quorum_timeout_ms"`
	// How many writes between WAL snapshots (0 = use default of 100)
	SnapshotInterval int `yaml:"snapshot_interval"`
	// Maximum number of entries retained in the in-memory replication log.
	// Followers that lag by more than this many writes receive a full snapshot.
	// 0 means use the default of 1000.
	ReplicationLogSize int `yaml:"replication_log_size"`
}

// LoadNodeConfig reads and parses a NodeConfig from a YAML file.
func LoadNodeConfig(path string) (*NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read node config %q: %w", path, err)
	}
	return ParseNodeConfig(data)
}

// ParseNodeConfig parses a NodeConfig from raw YAML bytes.
func ParseNodeConfig(data []byte) (*NodeConfig, error) {
	var cfg NodeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse node config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid node config: %w", err)
	}
	cfg.applyDefaults()
	return &cfg, nil
}

func (c *NodeConfig) applyDefaults() {
	if c.HeartbeatIntervalMs == 0 {
		c.HeartbeatIntervalMs = 500
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = time.Duration(c.HeartbeatIntervalMs) * time.Millisecond
	}
	if c.QuorumTimeoutMs == 0 {
		c.QuorumTimeoutMs = 500
	}
	if c.QuorumTimeout == 0 {
		c.QuorumTimeout = time.Duration(c.QuorumTimeoutMs) * time.Millisecond
	}
	if c.ReplicationLogSize == 0 {
		c.ReplicationLogSize = 1000
	}
}

// EnsureDefaults fills in zero-value fields with sensible defaults.
// It is called automatically by ParseNodeConfig. Call it explicitly when
// constructing a NodeConfig in tests without going through the YAML parser.
func (c *NodeConfig) EnsureDefaults() {
	c.applyDefaults()
}

func (c *NodeConfig) validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id is required")
	}
	if c.Node.Address == "" {
		return fmt.Errorf("node.address is required")
	}
	if c.CoordinatorAddress == "" {
		return fmt.Errorf("coordinator_address is required")
	}
	return nil
}
