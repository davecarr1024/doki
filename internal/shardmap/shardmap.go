// Package shardmap defines the runtime shard map and its operations.
//
// The ShardMap is the coordinator's authoritative view of the cluster:
// which shards exist, which nodes replicate them, and who the current leader is.
// It is versioned; each mutation increments the version so consumers can detect staleness.
package shardmap

import (
	"fmt"
	"sync"
)

// ShardInfo is the runtime state of a single shard.
type ShardInfo struct {
	ID       string   // shard identifier, e.g. "shard-0"
	Replicas []string // node IDs that host this shard
	Leader   string   // current leader node ID; empty if no leader assigned
}

// Quorum returns the number of replicas required to form a majority.
// For n replicas: quorum = floor(n/2) + 1
func (s ShardInfo) Quorum() int {
	return len(s.Replicas)/2 + 1
}

// HasReplica reports whether nodeID is a replica of this shard.
func (s ShardInfo) HasReplica(nodeID string) bool {
	for _, r := range s.Replicas {
		if r == nodeID {
			return true
		}
	}
	return false
}

// ShardMap is the versioned map of all shards in the cluster.
// It is safe for concurrent reads; mutations require holding the lock.
type ShardMap struct {
	mu      sync.RWMutex
	version uint64
	shards  map[string]*ShardInfo // keyed by shard ID
}

// New creates an empty ShardMap.
func New() *ShardMap {
	return &ShardMap{shards: make(map[string]*ShardInfo)}
}

// Version returns the current shard map version.
// The version increments on every mutation.
func (m *ShardMap) Version() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

// Get returns the ShardInfo for the given shard ID.
// Returns an error if the shard is not found.
func (m *ShardMap) Get(shardID string) (ShardInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.shards[shardID]
	if !ok {
		return ShardInfo{}, fmt.Errorf("shard %q not found", shardID)
	}
	return *s, nil
}

// All returns a snapshot of all shards.
func (m *ShardMap) All() []ShardInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]ShardInfo, 0, len(m.shards))
	for _, s := range m.shards {
		result = append(result, *s)
	}
	return result
}

// AddShard adds a new shard to the map.
// Returns an error if a shard with the same ID already exists.
func (m *ShardMap) AddShard(info ShardInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.shards[info.ID]; exists {
		return fmt.Errorf("shard %q already exists", info.ID)
	}
	cp := info
	m.shards[info.ID] = &cp
	m.version++
	return nil
}

// SetLeader updates the leader for a shard.
// Returns an error if the shard does not exist or the nodeID is not a replica.
func (m *ShardMap) SetLeader(shardID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %q not found", shardID)
	}
	// Validate the new leader is a known replica
	found := false
	for _, r := range s.Replicas {
		if r == nodeID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("node %q is not a replica of shard %q", nodeID, shardID)
	}
	s.Leader = nodeID
	m.version++
	return nil
}

// ShardsForNode returns all shards that have nodeID as a replica.
func (m *ShardMap) ShardsForNode(nodeID string) []ShardInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []ShardInfo
	for _, s := range m.shards {
		if s.HasReplica(nodeID) {
			result = append(result, *s)
		}
	}
	return result
}

// LeaderFor returns the current leader for a shard, or an error if not found.
func (m *ShardMap) LeaderFor(shardID string) (string, error) {
	s, err := m.Get(shardID)
	if err != nil {
		return "", err
	}
	if s.Leader == "" {
		return "", fmt.Errorf("shard %q has no leader", shardID)
	}
	return s.Leader, nil
}

// Snapshot returns the current version and a copy of all shard infos.
// Used for serialization (e.g., serving the /shardmap endpoint).
func (m *ShardMap) Snapshot() (uint64, []ShardInfo) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shards := make([]ShardInfo, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, *s)
	}
	return m.version, shards
}
