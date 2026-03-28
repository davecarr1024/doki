package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/davecarr1024/doki/internal/metrics"
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

	url := fmt.Sprintf("http://%s/internal/recover/%s?since_version=%d",
		leaderAddr, recoverShardID, sinceVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build recover request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("recover request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("recover request status %d", resp.StatusCode)
	}
	var recoverResp RecoverResponse
	if err := json.NewDecoder(resp.Body).Decode(&recoverResp); err != nil {
		return "", fmt.Errorf("decode recover response: %w", err)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	switch recoverResp.Type {
	case "entries":
		for _, e := range recoverResp.Entries {
			// Skip entries we already have (may arrive via replication concurrently).
			if e.Version <= replica.Version {
				continue
			}
			if err := applyReplicatedEntryLocked(replica, e, ds); err != nil {
				return "", fmt.Errorf("apply entry: %w", err)
			}
		}
		replica.IsReady = true
		// Clear bootstrap hint now that first recovery succeeded.
		replica.BootstrapShardID = ""
		replica.BootstrapLeaderAddr = ""
		log.Printf("incremental recovery complete shard_id=%s version=%d entries=%d",
			shardID, replica.Version, len(recoverResp.Entries))
		return "incremental", nil

	case "snapshot":
		replica.KV.ApplySnapshot(recoverResp.KV)
		replica.Version = recoverResp.Version
		replica.Term = recoverResp.Term
		replica.IsReady = true
		replica.RepLog.Reset()
		if ds != nil {
			if err := ds.resetSnapshot(recoverResp.Term, recoverResp.Version, recoverResp.KV); err != nil {
				return "", fmt.Errorf("disk snapshot reset: %w", err)
			}
		}
		// Clear bootstrap hint now that first recovery succeeded.
		replica.BootstrapShardID = ""
		replica.BootstrapLeaderAddr = ""
		log.Printf("snapshot fallback recovery complete shard_id=%s version=%d term=%d",
			shardID, recoverResp.Version, recoverResp.Term)
		return "snapshot", nil

	default:
		return "", fmt.Errorf("unknown recovery response type %q", recoverResp.Type)
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
			replica.mu.RUnlock()
			if ready {
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
				continue
			}
			recoveryType, err := doIncrementalRecovery(ctx, replica, leaderAddr, ds)
			if err != nil {
				log.Printf("recovery attempt failed shard_id=%s err=%v", shardID, err)
			} else {
				replica.RecoveryCount.Add(1)
				if m != nil {
					m.RecoveriesTotal.WithLabelValues(shardID, recoveryType).Inc()
				}
			}
		}
	}
}
