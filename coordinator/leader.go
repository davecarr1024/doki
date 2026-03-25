package coordinator

import (
	"fmt"
	"log"
	"sync"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
)

// LeaderManager assigns and reassigns shard leaders.
//
// In Phase 1, the coordinator is the sole authority for leader assignment.
// It tracks a term per shard; the term increments on every new leader election.
// When picking a replacement leader, it prefers the alive replica with the
// highest reported version for that shard (most up-to-date data).
type LeaderManager struct {
	mu         sync.RWMutex
	shards     *shardmap.ShardMap
	membership *Membership
	terms      map[string]uint64 // shard_id → current term
}

// NewLeaderManager creates a LeaderManager backed by the given shard map and membership.
func NewLeaderManager(shards *shardmap.ShardMap, membership *Membership) *LeaderManager {
	return &LeaderManager{
		shards:     shards,
		membership: membership,
		terms:      make(map[string]uint64),
	}
}

// InitFromConfig populates the shard map from the cluster config and assigns initial leaders.
func (lm *LeaderManager) InitFromConfig(cfg *config.ClusterConfig) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, spec := range cfg.Shards {
		info := shardmap.ShardInfo{
			ID:       spec.ID,
			Replicas: spec.Replicas,
			Leader:   spec.InitialLeader,
		}
		if err := lm.shards.AddShard(info); err != nil {
			return fmt.Errorf("init shard %q: %w", spec.ID, err)
		}
		lm.terms[spec.ID] = 1 // term starts at 1
		log.Printf("shard initialized shard_id=%s leader=%s replicas=%v term=1",
			spec.ID, spec.InitialLeader, spec.Replicas)
	}
	return nil
}

// TermForShard returns the current term for a shard.
func (lm *LeaderManager) TermForShard(shardID string) uint64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.terms[shardID]
}

// CheckAndReassign inspects all shards and reassigns leadership where the current
// leader is dead. Prefers the alive replica with the highest version.
// Returns the list of shards whose leader changed.
func (lm *LeaderManager) CheckAndReassign() []string {
	var changed []string
	for _, shard := range lm.shards.All() {
		if shard.Leader == "" {
			if candidate, term := lm.pickCandidate(shard); candidate != "" {
				if err := lm.shards.SetLeader(shard.ID, candidate); err == nil {
					lm.mu.Lock()
					lm.terms[shard.ID] = term
					lm.mu.Unlock()
					log.Printf("leader assigned shard_id=%s new_leader=%s term=%d", shard.ID, candidate, term)
					changed = append(changed, shard.ID)
				}
			}
			continue
		}
		ns, err := lm.membership.Get(shard.Leader)
		if err != nil || !ns.IsAlive {
			candidate, term := lm.pickCandidate(shard)
			if candidate == "" {
				log.Printf("no alive replica for shard shard_id=%s", shard.ID)
				continue
			}
			if err := lm.shards.SetLeader(shard.ID, candidate); err != nil {
				log.Printf("failed to reassign leader shard_id=%s err=%v", shard.ID, err)
				continue
			}
			lm.mu.Lock()
			lm.terms[shard.ID] = term
			lm.mu.Unlock()
			log.Printf("leader reassigned shard_id=%s old=%s new=%s term=%d",
				shard.ID, shard.Leader, candidate, term)
			changed = append(changed, shard.ID)
		}
	}
	return changed
}

// NotifyLeader accepts a distributed election result from a node.
// It updates the shard map if the reported term is strictly greater than the
// coordinator's current term for that shard. Returns true if the update was
// accepted, false if the term was stale or the shard was unknown.
func (lm *LeaderManager) NotifyLeader(shardID, leaderID string, term uint64) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	currentTerm, ok := lm.terms[shardID]
	if !ok {
		return false // unknown shard
	}
	if term <= currentTerm {
		log.Printf("notify_leader rejected shard_id=%s stale_term=%d current_term=%d", shardID, term, currentTerm)
		return false
	}

	if err := lm.shards.SetLeader(shardID, leaderID); err != nil {
		log.Printf("notify_leader set leader failed shard_id=%s err=%v", shardID, err)
		return false
	}
	lm.terms[shardID] = term
	log.Printf("notify_leader accepted shard_id=%s leader=%s term=%d", shardID, leaderID, term)
	return true
}

// pickCandidate selects the best alive non-current-leader replica.
// Prefers the replica with the highest version for the shard.
// Returns the chosen node ID and the new term, or ("", 0) if none available.
func (lm *LeaderManager) pickCandidate(shard shardmap.ShardInfo) (string, uint64) {
	lm.mu.RLock()
	currentTerm := lm.terms[shard.ID]
	lm.mu.RUnlock()

	var bestID string
	var bestVersion uint64
	for _, replicaID := range shard.Replicas {
		if replicaID == shard.Leader {
			continue
		}
		ns, err := lm.membership.Get(replicaID)
		if err != nil || !ns.IsAlive {
			continue
		}
		v := lm.membership.VersionForShard(replicaID, shard.ID)
		if bestID == "" || v > bestVersion {
			bestID = replicaID
			bestVersion = v
		}
	}
	if bestID == "" {
		return "", 0
	}
	return bestID, currentTerm + 1
}
