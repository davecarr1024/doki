package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
)

// NodeStatus is the coordinator's view of a single node's liveness.
type NodeStatus struct {
	ID              string
	Address         string
	IsAlive         bool
	LastHeartbeatAt time.Time
	// ShardVersions tracks the last reported version for each shard hosted by this node.
	// Populated from heartbeat payloads; used to select the best candidate when
	// reassigning a leader (prefer highest version).
	ShardVersions map[string]uint64
}

// ShardStatus is reported by nodes in their heartbeat.
type ShardStatus struct {
	ShardID string `json:"shard_id"`
	Role    string `json:"role"` // "LEADER" or "FOLLOWER"
	Version uint64 `json:"version"`
	IsReady bool   `json:"is_ready"`
	Term    uint64 `json:"term"`
}

// HeartbeatRequest is the body of a POST /heartbeat request from a node.
type HeartbeatRequest struct {
	NodeID string        `json:"node_id"`
	Shards []ShardStatus `json:"shards"`
}

// HeartbeatResponse is returned to nodes after a heartbeat.
// If ShardMapVersion is higher than the node's cached version, the node
// should refetch the shard map and update its role assignments.
type HeartbeatResponse struct {
	ShardMapVersion uint64 `json:"shard_map_version"`
}

// Membership tracks the liveness of all nodes in the cluster.
//
// Nodes heartbeat periodically; the coordinator marks them alive or dead
// based on how recently a heartbeat was received.
type Membership struct {
	mu      sync.RWMutex
	nodes   map[string]*NodeStatus
	timeout time.Duration
	clock   clock.Clock
}

// NewMembership creates a Membership registry pre-populated from the cluster config.
func NewMembership(nodes []config.NodeSpec, failureTimeout time.Duration, clk clock.Clock) *Membership {
	m := &Membership{
		nodes:   make(map[string]*NodeStatus, len(nodes)),
		timeout: failureTimeout,
		clock:   clk,
	}
	for _, n := range nodes {
		m.nodes[n.ID] = &NodeStatus{
			ID:            n.ID,
			Address:       n.Address,
			IsAlive:       false,
			ShardVersions: make(map[string]uint64),
		}
	}
	return m
}

// RecordHeartbeat records a heartbeat from nodeID including the node's shard statuses.
// Returns an error if the nodeID is not a known cluster member.
func (m *Membership) RecordHeartbeat(nodeID string, shards []ShardStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	ns.LastHeartbeatAt = m.clock.Now()
	for _, s := range shards {
		ns.ShardVersions[s.ShardID] = s.Version
	}
	return nil
}

// RefreshLiveness re-evaluates which nodes are alive based on the failure timeout.
// Returns the list of nodes whose status changed.
func (m *Membership) RefreshLiveness() []NodeStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	var changed []NodeStatus
	for _, ns := range m.nodes {
		wasAlive := ns.IsAlive
		if ns.LastHeartbeatAt.IsZero() {
			ns.IsAlive = false
		} else {
			ns.IsAlive = now.Sub(ns.LastHeartbeatAt) < m.timeout
		}
		if wasAlive != ns.IsAlive {
			changed = append(changed, *ns)
		}
	}
	return changed
}

// All returns a snapshot of all node statuses.
func (m *Membership) All() []NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]NodeStatus, 0, len(m.nodes))
	for _, ns := range m.nodes {
		result = append(result, *ns)
	}
	return result
}

// Get returns the status of a single node.
func (m *Membership) Get(nodeID string) (NodeStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ns, ok := m.nodes[nodeID]
	if !ok {
		return NodeStatus{}, fmt.Errorf("unknown node %q", nodeID)
	}
	return *ns, nil
}

// AliveNodes returns the IDs of all currently alive nodes.
func (m *Membership) AliveNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []string
	for id, ns := range m.nodes {
		if ns.IsAlive {
			alive = append(alive, id)
		}
	}
	return alive
}

// AddNode registers a new node in the membership map.
// If the node already exists, its address is updated but live state is preserved.
func (m *Membership) AddNode(spec config.NodeSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[spec.ID]; ok {
		m.nodes[spec.ID].Address = spec.Address
		return
	}
	m.nodes[spec.ID] = &NodeStatus{
		ID:            spec.ID,
		Address:       spec.Address,
		IsAlive:       false,
		ShardVersions: make(map[string]uint64),
	}
}

// VersionForShard returns the last reported version of a shard on a given node.
// Returns 0 if unknown.
func (m *Membership) VersionForShard(nodeID, shardID string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ns, ok := m.nodes[nodeID]
	if !ok {
		return 0
	}
	return ns.ShardVersions[shardID]
}
