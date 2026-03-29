package node

import (
	"context"
	"fmt"
	"log"
	"time"

	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/metrics"
	"github.com/davecarr1024/doki/internal/replicationlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SyncResponse is returned by GET /internal/sync/{shard_id}.
// It carries a full KV snapshot so a recovering follower can rebuild state.
type SyncResponse struct {
	Term    uint64            `json:"term"`
	Version uint64            `json:"version"`
	KV      map[string]string `json:"kv"`
}

// doIncrementalRecovery attempts to catch up a lagging follower using the leader's
// in-memory replication log (Phase 3). It sends the follower's current version and
// receives either:
//   - type="entries": the missing log entries to apply sequentially, or
//   - type="snapshot": a full KV snapshot (fallback when the gap is too large).
//
// Returns the recovery type ("incremental" or "snapshot") on success, or an error.
// On success the replica is marked ready.
func doIncrementalRecovery(ctx context.Context, replica *ReplicaState, leaderAddr string, ds *diskState) (string, error) {
	replica.mu.RLock()
	shardID := replica.ShardID
	sinceVersion := replica.Version
	bootstrapShardID := replica.BootstrapShardID
	replica.mu.RUnlock()

	// For shard splits: fetch data from the bootstrap source shard instead.
	recoverShardID := shardID
	if bootstrapShardID != "" {
		recoverShardID = bootstrapShardID
		sinceVersion = 0 // always start fresh from the source
	}

	conn, err := grpc.DialContext(ctx, leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return "", fmt.Errorf("dial leader: %w", err)
	}
	defer func() { _ = conn.Close() }()

	client := nodev1.NewNodeServiceClient(conn)
	resp, err := client.Recover(ctx, &nodev1.RecoverRequest{
		ShardId:      recoverShardID,
		SinceVersion: sinceVersion,
	})
	if err != nil {
		return "", fmt.Errorf("recover request: %w", err)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	switch resp.Type {
	case nodev1.RecoverResponse_TYPE_ENTRIES:
		expected := replica.Version + 1
		for _, e := range resp.Entries {
			if e.Version <= replica.Version {
				continue
			}
			if e.Version != expected {
				return "", fmt.Errorf("recovery gap: expected version %d got %d", expected, e.Version)
			}
			expected++
		}
		for _, e := range resp.Entries {
			// Skip entries we already have (may arrive via replication concurrently).
			if e.Version <= replica.Version {
				continue
			}
			entry := replicationlog.Entry{
				Term:    e.Term,
				Version: e.Version,
				Op:      opTypeFromProto(e.Op),
				Key:     string(e.Op.GetKey()),
				Value:   string(e.Op.GetValue()),
			}
			if err := replica.SM.Apply(entry, ApplyWithWAL); err != nil {
				return "", fmt.Errorf("apply entry: %w", err)
			}
		}
		replica.IsReady = true
		replica.RecoveryState = RecoveryStateHealthy
		replica.RecoverySource = ""
		// Clear bootstrap hint now that first recovery succeeded.
		replica.BootstrapShardID = ""
		replica.BootstrapLeaderAddr = ""
		log.Printf("incremental recovery complete shard_id=%s version=%d entries=%d",
			shardID, replica.Version, len(resp.Entries))
		return "incremental", nil

	case nodev1.RecoverResponse_TYPE_SNAPSHOT:
		kv := kvFromProto(resp.Kv)
		if err := replica.SM.ApplySnapshot(StateSnapshot{
			Term:    resp.Term,
			Version: resp.Version,
			KV:      kv,
		}, SnapshotPersist); err != nil {
			return "", fmt.Errorf("apply snapshot: %w", err)
		}
		replica.IsReady = true
		replica.RecoveryState = RecoveryStateHealthy
		replica.RecoverySource = ""
		// Clear bootstrap hint now that first recovery succeeded.
		replica.BootstrapShardID = ""
		replica.BootstrapLeaderAddr = ""
		log.Printf("snapshot fallback recovery complete shard_id=%s version=%d term=%d",
			shardID, resp.Version, resp.Term)
		return "snapshot", nil

	default:
		return "", fmt.Errorf("unknown recovery response type %v", resp.Type)
	}
}

// runRecoveryLoop periodically attempts recovery for a not-ready follower replica.
// It continues running until the context is cancelled so replicas can be forced
// back into recovery later.
// getLeaderAddr is called each iteration so it always uses the current leader.
// m is used to record recovery type metrics; pass nil to skip metric recording.
//
// Phase 3: recovery attempts incremental log-based catch-up first via
// /internal/recover. The leader falls back to a full snapshot automatically
// when the follower's gap exceeds the replication log size.
func runRecoveryLoop(ctx context.Context, replica *ReplicaState, getLeaderAddr func() string, m *metrics.NodeMetrics, ds *diskState) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			replica.mu.RLock()
			ready := replica.IsReady
			shardID := replica.ShardID
			bootstrapAddr := replica.BootstrapLeaderAddr
			hasBootstrap := replica.BootstrapShardID != ""
			leaderID := replica.LeaderID
			bootstrapShardID := replica.BootstrapShardID
			replica.mu.RUnlock()
			if ready {
				replica.mu.Lock()
				replica.RecoveryState = RecoveryStateHealthy
				replica.RecoverySource = ""
				replica.mu.Unlock()
				continue
			}
			leaderAddr := getLeaderAddr()
			// For shard splits: always use the bootstrap leader address while
			// bootstrapping.  The normal leader might be this node itself,
			// which does not hold the source shard data.
			if hasBootstrap && bootstrapAddr != "" {
				leaderAddr = bootstrapAddr
			} else if leaderAddr == "" {
				leaderAddr = bootstrapAddr
			}
			if leaderAddr == "" {
				replica.mu.Lock()
				replica.RecoveryState = RecoveryStateUnavailable
				replica.RecoverySource = ""
				replica.mu.Unlock()
				continue
			}
			replica.mu.Lock()
			replica.RecoveryState = RecoveryStateRecovering
			if hasBootstrap {
				replica.RecoverySource = recoverySourceBootstrap(bootstrapShardID)
			} else {
				replica.RecoverySource = recoverySourceLeader(leaderID)
			}
			replica.mu.Unlock()
			recoveryType, err := doIncrementalRecovery(ctx, replica, leaderAddr, ds)
			if err != nil {
				log.Printf("recovery attempt failed shard_id=%s err=%v", shardID, err)
				replica.mu.Lock()
				replica.RecoveryState = RecoveryStateLagging
				if hasBootstrap {
					replica.RecoverySource = recoverySourceBootstrap(bootstrapShardID)
				} else {
					replica.RecoverySource = recoverySourceLeader(leaderID)
				}
				replica.mu.Unlock()
			} else {
				replica.RecoveryCount.Add(1)
				if m != nil {
					m.RecoveriesTotal.WithLabelValues(shardID, recoveryType).Inc()
				}
			}
		}
	}
}

func kvFromProto(entries []*commonv1.KVEntry) map[string]string {
	if len(entries) == 0 {
		return map[string]string{}
	}
	kv := make(map[string]string, len(entries))
	for _, e := range entries {
		kv[string(e.Key)] = string(e.Value)
	}
	return kv
}

func opTypeFromProto(op *commonv1.Operation) string {
	if op == nil {
		return ""
	}
	switch op.Type {
	case commonv1.Operation_TYPE_PUT:
		return "put"
	case commonv1.Operation_TYPE_DELETE:
		return "delete"
	default:
		return ""
	}
}
