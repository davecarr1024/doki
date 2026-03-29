package node

// Phase 4: Distributed Leader Election
//
// Each replica independently runs an election timer. When a follower has not
// heard from the leader (via replication or leader heartbeat) within its
// randomized election timeout, it starts an election:
//
//  1. Increment term, set role = CANDIDATE, vote for self.
//  2. Send RequestVote to all peers.
//  3. If a quorum of replicas grant their vote → become leader.
//  4. Notify the coordinator (best-effort) so the shard map is updated.
//
// The leader separately runs a heartbeat loop (every LeaderHeartbeat) that
// proves it is alive and resets followers' election timers.
//
// Invariants maintained:
//   - A node grants at most one vote per term (VotedFor / VotedForTerm).
//   - A node only votes for a candidate whose version >= own version.
//   - Receiving a message with a higher term immediately demotes the node.

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"

	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Types ---

// VoteRequest is sent by a candidate to peers during an election.
type VoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidate_id"`
	Version     uint64 `json:"version"`
}

// VoteResponse is the reply to a VoteRequest.
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// LeaderHeartbeatRequest is sent by the leader to followers periodically to
// prove liveness and reset their election timers.
type LeaderHeartbeatRequest struct {
	Term     uint64 `json:"term"`
	Version  uint64 `json:"version"`
	LeaderID string `json:"leader_id"`
	ShardID  string `json:"shard_id"`
}

// LeaderHeartbeatResponse is the reply to a LeaderHeartbeatRequest.
type LeaderHeartbeatResponse struct {
	Term uint64 `json:"term"`
}

// --- Server helpers ---

// electionTimeout returns a random duration in [ElectionTimeoutMin, ElectionTimeoutMax).
func (s *Server) electionTimeout() time.Duration {
	min := s.cfg.ElectionTimeoutMin
	max := s.cfg.ElectionTimeoutMax
	if min <= 0 || max <= min {
		return 0
	}
	spread := int64(max - min)
	return min + time.Duration(rand.Int63n(spread)) //nolint:gosec
}

// --- Election timer (follower/candidate side) ---

// runElectionTimer runs continuously for a replica. When the follower has not
// received a valid leader message for longer than its randomised election
// timeout it triggers an election attempt.
//
// The goroutine exits when ctx is cancelled or when the replica's timeout is 0
// (disabled, e.g. in pre-Phase-4 tests).
func (s *Server) runElectionTimer(ctx context.Context, replica *ReplicaState) {
	replica.mu.RLock()
	timeout := replica.ElectionTimeout
	replica.mu.RUnlock()
	if timeout == 0 {
		return // election disabled for this replica
	}

	ticker := time.NewTicker(20 * time.Millisecond) // poll interval
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			replica.mu.RLock()
			role := replica.Role
			last := replica.LastLeaderContact
			elTimeout := replica.ElectionTimeout
			replica.mu.RUnlock()

			if role == RoleLeader {
				// Leaders don't start elections.
				continue
			}
			if last.IsZero() {
				// Not yet initialised; treat as "just now".
				replica.mu.Lock()
				replica.LastLeaderContact = s.clock.Now()
				replica.mu.Unlock()
				continue
			}
			if s.clock.Now().Sub(last) < elTimeout {
				continue
			}
			// Timeout exceeded — start an election.
			s.startElection(ctx, replica)
		}
	}
}

// --- Election state machine ---

// startElection runs one election attempt for the given replica.
// It increments the term, requests votes from peers, and (on win) promotes
// itself to leader and notifies the coordinator.
func (s *Server) startElection(ctx context.Context, replica *ReplicaState) {
	replica.mu.Lock()

	// Double-check role; another goroutine may have resolved the situation.
	if replica.Role == RoleLeader {
		replica.mu.Unlock()
		return
	}

	// Increment term and record our vote for ourselves.
	replica.Term++
	candidateTerm := replica.Term
	candidateVersion := replica.Version
	shardID := replica.ShardID
	nodeID := replica.NodeID
	peers := append([]string(nil), replica.Peers...)
	replica.VotedFor = nodeID
	replica.VotedForTerm = candidateTerm
	// Reset contact timer so we don't immediately retry if we lose.
	replica.LastLeaderContact = s.clock.Now()
	replica.mu.Unlock()

	log.Printf("election started shard_id=%s node_id=%s term=%d version=%d",
		shardID, nodeID, candidateTerm, candidateVersion)

	// Gather peer addresses.
	s.mu.RLock()
	peerAddrs := make(map[string]string, len(peers))
	for _, peerID := range peers {
		if addr, ok := s.nodeAddresses[peerID]; ok {
			peerAddrs[peerID] = addr
		}
	}
	s.mu.RUnlock()

	// Vote for self counts as 1.
	votes := 1
	total := len(peers) + 1
	quorum := total/2 + 1

	voteReq := VoteRequest{
		Term:        candidateTerm,
		CandidateID: nodeID,
		Version:     candidateVersion,
	}

	// Fan-out vote requests (with a per-vote timeout).
	voteTimeout := 200 * time.Millisecond
	type voteResult struct {
		granted bool
		term    uint64
	}
	ch := make(chan voteResult, len(peerAddrs))
	for _, addr := range peerAddrs {
		addr := addr
		go func() {
			granted, peerTerm := sendVoteRequest(ctx, addr, shardID, voteReq, voteTimeout)
			ch <- voteResult{granted, peerTerm}
		}()
	}

	// Collect results. Abort early if a higher term appears.
	for range peerAddrs {
		select {
		case <-ctx.Done():
			return
		case res := <-ch:
			if res.term > candidateTerm {
				// Higher term seen; step down and adopt the newer term.
				replica.mu.Lock()
				if res.term > replica.Term {
					replica.Term = res.term
				}
				if replica.Role != RoleFollower {
					replica.Role = RoleFollower
					log.Printf("higher term seen during election shard_id=%s stepping down term=%d",
						shardID, res.term)
				}
				replica.mu.Unlock()
				return
			}
			if res.granted {
				votes++
			}
		}
	}

	// Final term check: another election may have advanced the term while we
	// were collecting votes.
	replica.mu.Lock()
	if replica.Term != candidateTerm || replica.Role == RoleLeader {
		replica.mu.Unlock()
		return
	}

	if votes < quorum {
		s.m.ElectionsTotal.WithLabelValues(shardID, "lost").Inc()
		replica.ElectionCount.Add(1)
		log.Printf("election lost shard_id=%s term=%d votes=%d/%d",
			shardID, candidateTerm, votes, quorum)
		replica.mu.Unlock()
		return
	}

	// Won the election.
	replica.Role = RoleLeader
	replica.LeaderID = nodeID
	replica.IsReady = true
	replica.LastLeaderContact = s.clock.Now()
	replica.ElectionCount.Add(1)
	replica.mu.Unlock()

	s.m.ElectionsTotal.WithLabelValues(shardID, "won").Inc()
	log.Printf("election won shard_id=%s node_id=%s term=%d votes=%d/%d",
		shardID, nodeID, candidateTerm, votes, quorum)

	// Notify coordinator (best-effort).
	go s.notifyCoordinatorElection(ctx, shardID, nodeID, candidateTerm)
}

// sendVoteRequest sends a single RequestVote RPC and returns (granted, peerTerm).
func sendVoteRequest(ctx context.Context, peerAddr, shardID string, req VoteRequest, timeout time.Duration) (bool, uint64) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(reqCtx, peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return false, 0
	}
	defer func() { _ = conn.Close() }()
	client := nodev1.NewNodeServiceClient(conn)
	resp, err := client.RequestVote(reqCtx, &nodev1.VoteRequest{
		Term:        req.Term,
		CandidateId: req.CandidateID,
		Version:     req.Version,
		ShardId:     shardID,
	})
	if err != nil {
		return false, 0
	}
	return resp.VoteGranted, resp.Term
}

// notifyCoordinatorElection informs the coordinator that this node won an
// election. It retries up to 4 times with backoff. The election result stands
// even if notification fails — nodes refetch the shard map on the next
// coordinator heartbeat cycle. ctx controls retry cancellation.
func (s *Server) notifyCoordinatorElection(ctx context.Context, shardID, leaderID string, term uint64) {
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, client, err := s.dialCoordinator(reqCtx, 2*time.Second)
		if err != nil {
			cancel()
			log.Printf("notify_leader dial failed shard_id=%s attempt=%d err=%v", shardID, attempt, err)
			continue
		}
		resp, err := client.NotifyLeader(reqCtx, &coordinatorv1.NotifyLeaderRequest{
			ShardId:  shardID,
			LeaderId: leaderID,
			Term:     term,
		})
		_ = conn.Close()
		cancel()
		if err != nil {
			log.Printf("notify_leader failed shard_id=%s attempt=%d err=%v", shardID, attempt, err)
			continue
		}
		if resp.Accepted {
			log.Printf("coordinator notified of election shard_id=%s leader=%s term=%d", shardID, leaderID, term)
			return
		}
		log.Printf("notify_leader rejected shard_id=%s current_term=%d", shardID, resp.CurrentTerm)
		return
	}
}

// --- Leader heartbeat (leader side) ---

// runLeaderHeartbeat sends periodic leader heartbeats to all followers while
// this replica is the leader. Exits when ctx is cancelled.
func (s *Server) runLeaderHeartbeat(ctx context.Context, replica *ReplicaState) {
	replica.mu.RLock()
	timeout := replica.ElectionTimeout
	replica.mu.RUnlock()
	if timeout == 0 {
		return // election disabled
	}

	ticker := time.NewTicker(s.cfg.LeaderHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			replica.mu.RLock()
			role := replica.Role
			term := replica.Term
			version := replica.Version
			nodeID := replica.NodeID
			shardID := replica.ShardID
			peers := append([]string(nil), replica.Peers...)
			replica.mu.RUnlock()

			if role != RoleLeader {
				continue
			}

			s.mu.RLock()
			peerAddrs := make(map[string]string, len(peers))
			for _, peerID := range peers {
				if addr, ok := s.nodeAddresses[peerID]; ok {
					peerAddrs[peerID] = addr
				}
			}
			s.mu.RUnlock()

			hbReq := LeaderHeartbeatRequest{
				Term:     term,
				Version:  version,
				LeaderID: nodeID,
				ShardID:  shardID,
			}
			for peerID, addr := range peerAddrs {
				peerID := peerID
				addr := addr
				go func() {
					s.m.LeaderHeartbeatsSentTotal.WithLabelValues(shardID).Inc()
					peerTerm := sendLeaderHeartbeat(ctx, addr, shardID, hbReq, s.cfg.LeaderHeartbeat/2)
					if peerTerm == 0 {
						s.m.LeaderHeartbeatsMissedTotal.WithLabelValues(shardID).Inc()
					} else if peerTerm > term {
						// Step down; a higher term exists.
						s.m.ElectionsTotal.WithLabelValues(shardID, "stepped_down").Inc()
						replica.mu.Lock()
						if peerTerm > replica.Term {
							replica.Term = peerTerm
						}
						if replica.Role == RoleLeader {
							replica.Role = RoleFollower
							log.Printf("leader heartbeat: higher term shard_id=%s stepping down term=%d",
								shardID, peerTerm)
						}
						replica.mu.Unlock()
					} else {
						replica.mu.Lock()
						replica.PeerLastContact[peerID] = s.clock.Now()
						replica.mu.Unlock()
					}
				}()
			}
		}
	}
}

// sendLeaderHeartbeat sends one leader heartbeat to a peer.
// Returns the peer's current term (0 on error).
func sendLeaderHeartbeat(ctx context.Context, peerAddr, shardID string, req LeaderHeartbeatRequest, timeout time.Duration) uint64 {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(reqCtx, peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return 0
	}
	defer func() { _ = conn.Close() }()
	client := nodev1.NewNodeServiceClient(conn)
	resp, err := client.LeaderHeartbeat(reqCtx, &nodev1.LeaderHeartbeatRequest{
		Term:     req.Term,
		Version:  req.Version,
		LeaderId: req.LeaderID,
		ShardId:  shardID,
	})
	if err != nil {
		return 0
	}
	return resp.Term
}

// --- HTTP handlers ---

// handleLeaderHeartbeat handles POST /internal/leader_heartbeat/{shard_id}.
// Followers reset their election timer upon receiving a valid heartbeat.
func (s *Server) handleLeaderHeartbeat(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()
	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	var req LeaderHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	replica.mu.Lock()
	myTerm := replica.Term

	if req.Term < myTerm {
		// Stale leader.
		replica.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LeaderHeartbeatResponse{Term: myTerm})
		return
	}

	if req.Term > myTerm {
		replica.Term = req.Term
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
			log.Printf("leader heartbeat: higher term shard_id=%s stepping down term=%d", shardID, req.Term)
		}
	}

	// Update who we think the leader is and reset our election timer.
	replica.LeaderID = req.LeaderID
	replica.LastLeaderContact = s.clock.Now()
	myTerm = replica.Term
	replica.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(LeaderHeartbeatResponse{Term: myTerm})
}

// handleRequestVote handles POST /internal/request_vote/{shard_id}.
// Grants a vote if:
//   - The candidate's term >= our current term, AND
//   - We haven't voted for a different candidate in this term, AND
//   - The candidate's version >= our version.
func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()
	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	myTerm := replica.Term

	// Reject votes from stale terms.
	if req.Term < myTerm {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: myTerm, VoteGranted: false})
		return
	}

	// Higher term: update our term and reset vote state.
	if req.Term > myTerm {
		replica.Term = req.Term
		replica.VotedFor = ""
		replica.VotedForTerm = 0
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
			log.Printf("request_vote: higher term shard_id=%s stepping down term=%d", shardID, req.Term)
		}
		myTerm = req.Term
	}

	// Already voted for a different candidate this term.
	if replica.VotedForTerm == req.Term && replica.VotedFor != "" && replica.VotedFor != req.CandidateID {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: myTerm, VoteGranted: false})
		return
	}

	// Candidate must be at least as up-to-date as us.
	if req.Version < replica.Version {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VoteResponse{Term: myTerm, VoteGranted: false})
		return
	}

	// Grant the vote.
	replica.VotedFor = req.CandidateID
	replica.VotedForTerm = req.Term
	// Granting a vote counts as "heard from a valid leader-like entity" — reset
	// the election timer so we don't immediately start a competing election.
	replica.LastLeaderContact = s.clock.Now()

	log.Printf("vote granted shard_id=%s voter=%s candidate=%s term=%d",
		shardID, replica.NodeID, req.CandidateID, req.Term)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(VoteResponse{Term: myTerm, VoteGranted: true})
}
