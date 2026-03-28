package node

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ReplicateRequest is sent from a leader to each follower to append a pending write.
type ReplicateRequest struct {
	Term    uint64 `json:"term"`
	Version uint64 `json:"version"`
	Op      string `json:"op"` // "put" or "delete"
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// ReplicateResponse is returned by a follower after applying a replicated write.
type ReplicateResponse struct {
	Success bool   `json:"success"`
	Term    uint64 `json:"term"`
	Error   string `json:"error,omitempty"`
}

// fanOutReplicate sends a write to all peerAddresses in parallel and returns the
// list of peer IDs that ACKed before the timeout.
func fanOutReplicate(ctx context.Context, shardID string, req ReplicateRequest, peerAddresses map[string]string, timeout time.Duration) []string {
	if len(peerAddresses) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	acks := make(chan string, len(peerAddresses))
	var wg sync.WaitGroup
	for peerID, addr := range peerAddresses {
		peerID := peerID
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sendReplicateRequest(ctx, addr, shardID, req) {
				acks <- peerID
			}
		}()
	}
	go func() {
		wg.Wait()
		close(acks)
	}()

	var okPeers []string
	for peerID := range acks {
		okPeers = append(okPeers, peerID)
	}
	return okPeers
}

func sendReplicateRequest(ctx context.Context, peerAddr, shardID string, req ReplicateRequest) bool {
	body, err := json.Marshal(req)
	if err != nil {
		return false
	}
	url := "http://" + peerAddr + "/internal/replicate/" + shardID
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var rr ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return false
	}
	return rr.Success
}

// fanOutForceRecover triggers recovery on a list of peers. Best-effort.
func fanOutForceRecover(ctx context.Context, shardID string, peerAddresses map[string]string, peerIDs []string, timeout time.Duration) int {
	if len(peerIDs) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	acks := make(chan bool, len(peerIDs))
	var wg sync.WaitGroup
	for _, peerID := range peerIDs {
		addr, ok := peerAddresses[peerID]
		if !ok {
			continue
		}
		peerAddr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			acks <- sendForceRecoverRequest(ctx, peerAddr, shardID)
		}()
	}
	go func() {
		wg.Wait()
		close(acks)
	}()

	count := 0
	for success := range acks {
		if success {
			count++
		}
	}
	return count
}

func sendForceRecoverRequest(ctx context.Context, peerAddr, shardID string) bool {
	url := "http://" + peerAddr + "/internal/force_recover/" + shardID
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(nil))
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}
