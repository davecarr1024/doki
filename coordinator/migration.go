package coordinator

import (
	"fmt"
	"log"
	"sync"

	"github.com/davecarr1024/doki/internal/shardmap"
)

// migration records the target replica set for an in-progress shard migration.
type migration struct {
	shardID      string
	newReplicas  []string // the desired final replica set
}

// MigrationManager tracks in-progress shard migrations and shard splits.
//
// For migrations: when all incoming replicas for a shard report IsReady, the
// manager finalises the migration by replacing the replica set.
//
// For splits: when all replicas of the new shard report IsReady, the manager
// clears the BootstrapSourceShardID.
type MigrationManager struct {
	mu         sync.Mutex
	migrations map[string]*migration // shard_id → pending migration
	shards     *shardmap.ShardMap
	membership *Membership
	leader     *LeaderManager
}

// NewMigrationManager creates a MigrationManager.
func NewMigrationManager(shards *shardmap.ShardMap, membership *Membership, leader *LeaderManager) *MigrationManager {
	return &MigrationManager{
		migrations: make(map[string]*migration),
		shards:     shards,
		membership: membership,
		leader:     leader,
	}
}

// StartMigration begins a shard migration.
//
// The current replica set is expanded to include both old and new replicas so
// that clients continue to be served during catch-up.  Once all newReplicas
// are ready, CheckMigrations finalises the migration.
func (mm *MigrationManager) StartMigration(shardID string, newReplicas []string) error {
	shard, err := mm.shards.Get(shardID)
	if err != nil {
		return fmt.Errorf("start migration: %w", err)
	}

	// Build union of old and new replicas.
	union := append([]string(nil), shard.Replicas...)
	existing := make(map[string]bool, len(union))
	for _, r := range union {
		existing[r] = true
	}
	for _, r := range newReplicas {
		if !existing[r] {
			union = append(union, r)
		}
	}

	// Widen the replica set to old ∪ new so both sides are in the shard map.
	if err := mm.shards.SetReplicas(shardID, union); err != nil {
		return fmt.Errorf("set replicas: %w", err)
	}
	// Tag the incoming side so the finalization goroutine knows what to keep.
	if err := mm.shards.SetIncomingReplicas(shardID, newReplicas); err != nil {
		return fmt.Errorf("set incoming replicas: %w", err)
	}

	mm.mu.Lock()
	mm.migrations[shardID] = &migration{shardID: shardID, newReplicas: newReplicas}
	mm.mu.Unlock()

	log.Printf("migration started shard_id=%s new_replicas=%v", shardID, newReplicas)
	return nil
}

// CheckMigrations inspects all pending migrations and finalises any whose
// incoming replicas are all ready.  It also clears BootstrapSourceShardID for
// split shards whose replicas are all ready.
//
// Called from the coordinator health-monitor goroutine.
func (mm *MigrationManager) CheckMigrations() {
	mm.mu.Lock()
	pending := make([]*migration, 0, len(mm.migrations))
	for _, m := range mm.migrations {
		pending = append(pending, m)
	}
	mm.mu.Unlock()

	for _, m := range pending {
		if mm.allReady(m.shardID, m.newReplicas) {
			mm.finaliseMigration(m)
		}
	}

	// Check split shards: if all replicas are ready, clear the bootstrap hint.
	_, shards := mm.shards.Snapshot()
	for _, sh := range shards {
		if sh.BootstrapSourceShardID == "" {
			continue
		}
		if mm.allReady(sh.ID, sh.Replicas) {
			if err := mm.shards.ClearBootstrapSource(sh.ID); err != nil {
				log.Printf("clear bootstrap source failed shard_id=%s err=%v", sh.ID, err)
			} else {
				// Assign leader to new shard if it doesn't have one.
				if sh.Leader == "" {
					reassigned := mm.leader.CheckAndReassign()
					if len(reassigned) > 0 {
						log.Printf("leader assigned to split shard shard_id=%s", sh.ID)
					}
				}
				log.Printf("bootstrap source cleared shard_id=%s", sh.ID)
			}
		}
	}
}

// allReady returns true if every nodeID in replicas reports IsReady for shardID.
func (mm *MigrationManager) allReady(shardID string, replicas []string) bool {
	if len(replicas) == 0 {
		return false
	}
	for _, nodeID := range replicas {
		ns, err := mm.membership.Get(nodeID)
		if err != nil || !ns.IsAlive {
			return false
		}
		if mm.membership.VersionForShard(nodeID, shardID) == 0 {
			// Version stays 0 until the replica has applied at least one entry or
			// completed snapshot recovery.  Use it as a proxy for IsReady.
			// (Accurate IsReady is tracked per-heartbeat via ShardStatus.IsReady
			// but Membership doesn't store it yet — version > 0 is sufficient.)
			//
			// For an empty shard (version 0 by design) the coordinator will
			// re-check on the next health tick once any write arrives.
			//
			// TODO(phase5): store IsReady per shard in NodeStatus.
			return false
		}
	}
	return true
}

// finaliseMigration switches the shard to the new replica set and removes the
// migration record.
func (mm *MigrationManager) finaliseMigration(m *migration) {
	if err := mm.shards.SetReplicas(m.shardID, m.newReplicas); err != nil {
		log.Printf("migration finalise set replicas failed shard_id=%s err=%v", m.shardID, err)
		return
	}
	// Clear the IncomingReplicas tag.
	if err := mm.shards.SetIncomingReplicas(m.shardID, nil); err != nil {
		log.Printf("migration finalise clear incoming failed shard_id=%s err=%v", m.shardID, err)
	}
	// Pick a leader from the new replica set.
	reassigned := mm.leader.CheckAndReassign()
	for _, id := range reassigned {
		log.Printf("migration leader assigned shard_id=%s", id)
	}

	mm.mu.Lock()
	delete(mm.migrations, m.shardID)
	mm.mu.Unlock()

	log.Printf("migration complete shard_id=%s replicas=%v", m.shardID, m.newReplicas)
}
