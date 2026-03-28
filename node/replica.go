package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecarr1024/doki/internal/replicationlog"
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
//
// writeMu serializes concurrent write requests; it is separate from mu so that
// long-running replication fan-out does not block status reads.
type ReplicaState struct {
	mu      sync.RWMutex
	writeMu sync.Mutex

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

	// RepLog is the bounded in-memory replication log.
	// On each committed write, the entry is appended here so that lagging
	// followers can catch up via log replay instead of a full snapshot.
	RepLog *replicationlog.Log

	// Phase 4: election state (all protected by mu).

	// LastLeaderContact is the last time a valid leader message was received
	// (replication or leader heartbeat). Used by the election timer.
	LastLeaderContact time.Time

	// VotedFor is the candidate this replica voted for in VotedForTerm.
	VotedFor string

	// VotedForTerm is the term of the last vote granted.
	VotedForTerm uint64

	// ElectionTimeout is the randomized timeout for this replica.
	// A new election is triggered when (now - LastLeaderContact) > ElectionTimeout.
	ElectionTimeout time.Duration

	// PeerLastContact tracks the most recent successful contact time for each peer.
	// Used by the leader to fast-fail writes when quorum is clearly unavailable.
	PeerLastContact map[string]time.Time

	// Phase 5: bootstrap state for shard splits.
	// When BootstrapShardID is non-empty, the recovery loop fetches data from
	// BootstrapShardID (a different shard) rather than this replica's own shard.
	// BootstrapLeaderAddr overrides the leader address for the initial recovery.
	// Both fields are cleared once the first recovery completes successfully.
	BootstrapShardID    string
	BootstrapLeaderAddr string

	// Counters accessed atomically (no lock needed).
	ElectionCount atomic.Int64
	RecoveryCount atomic.Int64
	WriteOpsTotal atomic.Int64
	WriteErrTotal atomic.Int64
}

// NewReplicaState creates a new replica in FOLLOWER state, not ready.
// logSize is the maximum number of entries retained in the replication log;
// pass 0 to use the default of 1000.
// electionTimeout is the randomized election timeout for Phase 4; pass 0 to
// disable election timer behaviour (useful for Phase 0–3 unit tests).
func NewReplicaState(shardID, nodeID string, peers []string, logSize int, electionTimeout time.Duration) *ReplicaState {
	return &ReplicaState{
		ShardID:         shardID,
		NodeID:          nodeID,
		Role:            RoleFollower,
		IsReady:         false, // becomes ready after initial recovery
		Peers:           peers,
		KV:              memory.New(),
		RepLog:          replicationlog.New(logSize),
		ElectionTimeout: electionTimeout,
		PeerLastContact: make(map[string]time.Time, len(peers)),
	}
}

// StatusSnapshot returns a read-safe copy of the replica's current status.
// Safe to call without holding the lock.
func (r *ReplicaState) StatusSnapshot() ReplicaStatusSnapshot {
	r.mu.RLock()
	lastContact := r.LastLeaderContact
	snap := ReplicaStatusSnapshot{
		ShardID:       r.ShardID,
		Role:          r.Role,
		LeaderID:      r.LeaderID,
		Term:          r.Term,
		Version:       r.Version,
		IsReady:       r.IsReady,
		Peers:         append([]string(nil), r.Peers...),
		ElectionCount: r.ElectionCount.Load(),
		RecoveryCount: r.RecoveryCount.Load(),
		WriteOpsTotal: r.WriteOpsTotal.Load(),
		WriteErrTotal: r.WriteErrTotal.Load(),
	}
	r.mu.RUnlock()
	if !lastContact.IsZero() {
		snap.LastLeaderContactMs = time.Since(lastContact).Milliseconds()
	}
	return snap
}

// ReplicaStatusSnapshot is a point-in-time copy of a ReplicaState's observable fields.
// Returned by StatusSnapshot() for use in HTTP responses.
type ReplicaStatusSnapshot struct {
	ShardID             string   `json:"shard_id"`
	Role                Role     `json:"role"`
	LeaderID            string   `json:"leader_id"`
	Term                uint64   `json:"term"`
	Version             uint64   `json:"version"`
	IsReady             bool     `json:"is_ready"`
	Peers               []string `json:"peers"`
	LastLeaderContactMs int64    `json:"last_leader_contact_ms"`
	ElectionCount       int64    `json:"election_count"`
	RecoveryCount       int64    `json:"recovery_count"`
	WriteOpsTotal       int64    `json:"write_ops_total"`
	WriteErrTotal       int64    `json:"write_err_total"`
}
