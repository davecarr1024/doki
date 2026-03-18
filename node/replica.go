package node

import (
	"sync"

	"github.com/davecarr1024/doki/internal/storage"
	"github.com/davecarr1024/doki/internal/storage/memory"
)

// Role is the role a node plays for a particular shard.
type Role string

const (
	RoleLeader   Role = "LEADER"
	RoleFollower Role = "FOLLOWER"
	RoleUnknown  Role = "UNKNOWN"
)

// ReplicaState is the complete runtime state of one shard replica hosted by this node.
//
// All fields are protected by mu. The caller (server.go) is responsible for acquiring
// the lock before accessing fields directly; methods on ReplicaState are not
// individually lock-safe unless documented otherwise.
type ReplicaState struct {
	mu sync.RWMutex

	// ShardID is the shard this replica is serving.
	ShardID string

	// NodeID is this node's own identity.
	NodeID string

	// Role is whether this replica is currently a leader or follower.
	Role Role

	// LeaderID is the current known leader for this shard (may be this node).
	LeaderID string

	// Term is the leader epoch. Incremented each time a new leader is assigned.
	// Used to detect and reject stale replication messages.
	Term uint64

	// Version is a monotonically increasing commit counter.
	// Incremented on each committed write. Used to compare replica freshness.
	Version uint64

	// IsReady is false while the replica is recovering (pulling a snapshot).
	// Not-ready replicas do not count toward quorum.
	IsReady bool

	// Peers is the list of other node IDs that host replicas of this shard.
	Peers []string

	// KV is the storage engine for this replica.
	KV storage.Storage
}

// NewReplicaState creates a new replica in FOLLOWER state, not ready.
func NewReplicaState(shardID, nodeID string, peers []string) *ReplicaState {
	return &ReplicaState{
		ShardID: shardID,
		NodeID:  nodeID,
		Role:    RoleFollower,
		IsReady: false, // becomes ready after initial recovery
		Peers:   peers,
		KV:      memory.New(),
	}
}

// StatusSnapshot returns a read-safe copy of the replica's current status.
// Safe to call without holding the lock.
func (r *ReplicaState) StatusSnapshot() ReplicaStatusSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return ReplicaStatusSnapshot{
		ShardID:  r.ShardID,
		Role:     r.Role,
		LeaderID: r.LeaderID,
		Term:     r.Term,
		Version:  r.Version,
		IsReady:  r.IsReady,
		Peers:    append([]string(nil), r.Peers...),
	}
}

// ReplicaStatusSnapshot is a point-in-time copy of a ReplicaState's observable fields.
// Returned by StatusSnapshot() for use in HTTP responses.
type ReplicaStatusSnapshot struct {
	ShardID  string   `json:"shard_id"`
	Role     Role     `json:"role"`
	LeaderID string   `json:"leader_id"`
	Term     uint64   `json:"term"`
	Version  uint64   `json:"version"`
	IsReady  bool     `json:"is_ready"`
	Peers    []string `json:"peers"`
}
