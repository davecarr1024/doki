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
	Op      string `json:"op"`    // "put" or "delete"
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// ReplicateResponse is returned by a follower after staging a replicated write.
type ReplicateResponse struct {
	Success bool   `json:"success"`
	Term    uint64 `json:"term"`
	Error   string `json:"error,omitempty"`
}

// CommitRequest is sent from a leader to each follower after quorum to commit a write.
type CommitRequest struct {
	Term    uint64 `json:"term"`
	Version uint64 `json:"version"`
}

// CommitResponse is returned by a follower after applying a committed write.
type CommitResponse struct {
	Success bool   `json:"success"`
	Term    uint64 `json:"term"`
	Error   string `json:"error,omitempty"`
}

// fanOutReplicate sends a write to all peerAddresses in parallel and returns the
// number of successful ACKs received before the timeout.
func fanOutReplicate(ctx context.Context, shardID string, req ReplicateRequest, peerAddresses []string, timeout time.Duration) int {
	if len(peerAddresses) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	acks := make(chan bool, len(peerAddresses))
	var wg sync.WaitGroup
	for _, addr := range peerAddresses {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			acks <- sendReplicateRequest(ctx, addr, shardID, req)
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

// fanOutCommit sends a commit notification to all peerAddresses in parallel and returns the
// number of successful ACKs received before the timeout.
func fanOutCommit(ctx context.Context, shardID string, req CommitRequest, peerAddresses []string, timeout time.Duration) int {
	if len(peerAddresses) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	acks := make(chan bool, len(peerAddresses))
	var wg sync.WaitGroup
	for _, addr := range peerAddresses {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			acks <- sendCommitRequest(ctx, addr, shardID, req)
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

func sendCommitRequest(ctx context.Context, peerAddr, shardID string, req CommitRequest) bool {
	body, err := json.Marshal(req)
	if err != nil {
		return false
	}
	url := "http://" + peerAddr + "/internal/commit/" + shardID
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
	var rr CommitResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return false
	}
	return rr.Success
}
