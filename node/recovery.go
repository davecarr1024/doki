package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// SyncResponse is returned by GET /internal/sync/{shard_id}.
// It carries a full KV snapshot so a recovering follower can rebuild state.
type SyncResponse struct {
	Term    uint64            `json:"term"`
	Version uint64            `json:"version"`
	KV      map[string]string `json:"kv"`
}

// doRecovery fetches a full snapshot from the leader and applies it to the replica.
// On success the replica is marked ready. On failure the caller may retry.
func doRecovery(ctx context.Context, replica *ReplicaState, leaderAddr string) error {
	replica.mu.RLock()
	shardID := replica.ShardID
	replica.mu.RUnlock()

	url := "http://" + leaderAddr + "/internal/sync/" + shardID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build sync request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("sync request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sync request status %d", resp.StatusCode)
	}
	var syncResp SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return fmt.Errorf("decode sync response: %w", err)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()
	replica.KV.ApplySnapshot(syncResp.KV)
	replica.Version = syncResp.Version
	replica.Term = syncResp.Term
	replica.IsReady = true
	log.Printf("recovery complete shard_id=%s version=%d term=%d", shardID, syncResp.Version, syncResp.Term)
	return nil
}

// runRecoveryLoop periodically attempts recovery for a not-ready follower replica.
// It exits once the replica is marked ready or the context is cancelled.
// getLeaderAddr is called each iteration so it always uses the current leader.
func runRecoveryLoop(ctx context.Context, replica *ReplicaState, getLeaderAddr func() string) {
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
			replica.mu.RUnlock()
			if ready {
				return
			}
			leaderAddr := getLeaderAddr()
			if leaderAddr == "" {
				continue
			}
			if err := doRecovery(ctx, replica, leaderAddr); err != nil {
				log.Printf("recovery attempt failed shard_id=%s err=%v", shardID, err)
			}
		}
	}
}
