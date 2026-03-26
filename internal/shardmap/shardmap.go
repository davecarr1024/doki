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
	ID       string   `json:"id"`       // shard identifier, e.g. "shard-0"
	Replicas []string `json:"replicas"` // node IDs that host this shard
	Leader   string   `json:"leader"`   // current leader node ID; empty if no leader assigned

	// BootstrapSourceShardID is set during a shard split. When non-empty, nodes
	// that are initialising this shard should fetch their initial snapshot from
	// the named shard's leader instead of this shard's (not-yet-elected) leader.
	// Cleared by the coordinator once all replicas report ready.
	BootstrapSourceShardID string `json:"bootstrap_source_shard_id,omitempty"`

	// IncomingReplicas tracks the target replica set during a shard migration.
	// While this slice is non-empty the coordinator considers the shard to be
	// mid-migration.  Once every incoming replica is ready the coordinator
	// sets Replicas = IncomingReplicas and clears this field.
	IncomingReplicas []string `json:"incoming_replicas,omitempty"`
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

// SetReplicas atomically replaces the replica set for a shard.
// Returns an error if the shard does not exist.
// The leader is cleared if it is no longer in the new replica set.
func (m *ShardMap) SetReplicas(shardID string, replicas []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %q not found", shardID)
	}
	s.Replicas = append([]string(nil), replicas...)
	// Clear leader if it's no longer in the new replica set.
	if s.Leader != "" {
		found := false
		for _, r := range s.Replicas {
			if r == s.Leader {
				found = true
				break
			}
		}
		if !found {
			s.Leader = ""
		}
	}
	m.version++
	return nil
}

// SetIncomingReplicas marks a shard as mid-migration with the given target replica set.
// The full replica set (old ∪ new) should already be set via SetReplicas before calling this.
func (m *ShardMap) SetIncomingReplicas(shardID string, incoming []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %q not found", shardID)
	}
	s.IncomingReplicas = append([]string(nil), incoming...)
	m.version++
	return nil
}

// ClearBootstrapSource removes the BootstrapSourceShardID from a shard after
// all replicas have finished bootstrapping.
func (m *ShardMap) ClearBootstrapSource(shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %q not found", shardID)
	}
	if s.BootstrapSourceShardID == "" {
		return nil // nothing to do
	}
	s.BootstrapSourceShardID = ""
	m.version++
	return nil
}

// RemoveShard removes a shard from the map.
// Returns an error if the shard does not exist.
func (m *ShardMap) RemoveShard(shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.shards[shardID]; !ok {
		return fmt.Errorf("shard %q not found", shardID)
	}
	delete(m.shards, shardID)
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
