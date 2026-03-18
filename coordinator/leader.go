package coordinator

import (
	"fmt"
	"log"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
)

// LeaderManager assigns and reassigns shard leaders.
//
// In v1, the coordinator is the sole authority for leader assignment.
// Leadership changes happen when:
//  1. The cluster starts (initial leaders from config)
//  2. The current leader is detected as dead by the health monitor
type LeaderManager struct {
	shards     *shardmap.ShardMap
	membership *Membership
}

// NewLeaderManager creates a LeaderManager backed by the given shard map and membership.
func NewLeaderManager(shards *shardmap.ShardMap, membership *Membership) *LeaderManager {
	return &LeaderManager{shards: shards, membership: membership}
}

// InitFromConfig populates the shard map from the cluster config and assigns initial leaders.
func (lm *LeaderManager) InitFromConfig(cfg *config.ClusterConfig) error {
	for _, spec := range cfg.Shards {
		info := shardmap.ShardInfo{
			ID:       spec.ID,
			Replicas: spec.Replicas,
			Leader:   spec.InitialLeader,
		}
		if err := lm.shards.AddShard(info); err != nil {
			return fmt.Errorf("init shard %q: %w", spec.ID, err)
		}
		log.Printf("shard initialized shard_id=%s leader=%s replicas=%v",
			spec.ID, spec.InitialLeader, spec.Replicas)
	}
	return nil
}

// CheckAndReassign inspects all shards and reassigns leadership where the current
// leader is dead. Prefers the alive replica with the information known to the
// coordinator (in v1 this is just any alive replica; version-aware election
// is introduced in Phase 4).
//
// Returns the list of shards whose leader changed.
func (lm *LeaderManager) CheckAndReassign() []string {
	var changed []string
	for _, shard := range lm.shards.All() {
		if shard.Leader == "" {
			// No leader at all — try to assign one
			if candidate := lm.pickCandidate(shard); candidate != "" {
				if err := lm.shards.SetLeader(shard.ID, candidate); err == nil {
					log.Printf("leader assigned shard_id=%s new_leader=%s", shard.ID, candidate)
					changed = append(changed, shard.ID)
				}
			}
			continue
		}
		ns, err := lm.membership.Get(shard.Leader)
		if err != nil || !ns.IsAlive {
			// Current leader is dead; pick a new one
			candidate := lm.pickCandidate(shard)
			if candidate == "" {
				log.Printf("no alive replica for shard shard_id=%s", shard.ID)
				continue
			}
			if err := lm.shards.SetLeader(shard.ID, candidate); err != nil {
				log.Printf("failed to reassign leader shard_id=%s err=%v", shard.ID, err)
				continue
			}
			log.Printf("leader reassigned shard_id=%s old_leader=%s new_leader=%s",
				shard.ID, shard.Leader, candidate)
			changed = append(changed, shard.ID)
		}
	}
	return changed
}

// pickCandidate returns the first alive replica for the shard, excluding the
// current (dead) leader. Returns "" if no alive candidate exists.
func (lm *LeaderManager) pickCandidate(shard shardmap.ShardInfo) string {
	for _, replicaID := range shard.Replicas {
		if replicaID == shard.Leader {
			continue // skip the dead leader
		}
		ns, err := lm.membership.Get(replicaID)
		if err == nil && ns.IsAlive {
			return replicaID
		}
	}
	// If all non-leader replicas are dead but the leader itself is the only one,
	// we can't elect a new one. Also check the leader itself as last resort
	// in case our view is stale.
	return ""
}
