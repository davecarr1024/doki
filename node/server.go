package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/replicationlog"
	"github.com/davecarr1024/doki/internal/wal"
)

// Server is a leaf node's HTTP server.
//
// Phase 0 endpoints:
//   - GET  /status                         — node and shard replica health
//   - GET  /ready                          — readiness check
//
// Phase 1 endpoints:
//   - POST /kv/{shard_id}                  — KV operations (put/get/delete); leader only
//   - POST /internal/replicate/{shard_id}  — replication fan-out from leader
//   - GET  /internal/sync/{shard_id}       — full-state sync for recovering followers
//
// Phase 2 additions:
//   - WAL written before every put/delete (leader and follower)
//   - Periodic snapshot + WAL truncation
//   - InitShards loads from disk if state exists, skipping network recovery
//
// Phase 3 additions:
//   - GET /internal/recover/{shard_id}      — incremental log-based recovery
//   - In-memory replication log per replica (bounded circular buffer)
//   - Recovery tries log-based catch-up first; falls back to full snapshot
type Server struct {
	cfg             *config.NodeConfig
	clock           clock.Clock
	replicas        map[string]*ReplicaState // shard_id → replica
	diskStates      map[string]*diskState    // shard_id → disk state (WAL + snapshot)
	nodeAddresses   map[string]string        // nodeID → HTTP address (from coordinator)
	shardMapVersion uint64
	mu              sync.RWMutex
	startedAt       time.Time
	httpServer      *http.Server
}

// NewServer creates a node Server from the given config.
func NewServer(cfg *config.NodeConfig, clk clock.Clock) *Server {
	cfg.EnsureDefaults()
	if clk == nil {
		clk = clock.Real{}
	}
	return &Server{
		cfg:           cfg,
		clock:         clk,
		replicas:      make(map[string]*ReplicaState),
		diskStates:    make(map[string]*diskState),
		nodeAddresses: make(map[string]string),
		startedAt:     clk.Now(),
	}
}

// Close releases any resources held by the server (e.g. open WAL file handles).
// Call Close after the server has stopped serving.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ds := range s.diskStates {
		if err := ds.close(); err != nil {
			log.Printf("disk state close error: %v", err)
		}
	}
}

// InitShards creates replica state for each shard this node participates in.
// The full ShardMapResponse from the coordinator is used so that peer node
// addresses are available for replication fan-out and recovery.
//
// If cfg.DataDir is set, InitShards opens the per-shard disk state and loads
// any existing WAL/snapshot. Replicas with valid disk state are marked ready
// immediately, skipping network recovery.
func (s *Server) InitShards(resp coordinator.ShardMapResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeAddresses = resp.NodeAddresses
	s.shardMapVersion = resp.Version
	for _, shard := range resp.Shards {
		if !shard.HasReplica(s.cfg.Node.ID) {
			continue
		}
		peers := make([]string, 0, len(shard.Replicas)-1)
		for _, r := range shard.Replicas {
			if r != s.cfg.Node.ID {
				peers = append(peers, r)
			}
		}
		r := NewReplicaState(shard.ID, s.cfg.Node.ID, peers, s.cfg.ReplicationLogSize)
		if shard.Leader == s.cfg.Node.ID {
			r.Role = RoleLeader
			r.IsReady = true // leader starts ready; disk state applied below if available
		}
		r.LeaderID = shard.Leader

		// Try to load from disk if a data directory is configured.
		if s.cfg.DataDir != "" {
			shardDir := shardDataDir(s.cfg.DataDir, shard.ID)
			ds, err := openDiskState(shardDir, s.cfg.SnapshotInterval)
			if err != nil {
				log.Printf("disk state open failed shard_id=%s err=%v — continuing without disk state", shard.ID, err)
			} else {
				s.diskStates[shard.ID] = ds
				loaded, err := ds.load()
				if err != nil {
					log.Printf("disk state load failed shard_id=%s err=%v", shard.ID, err)
				} else if loaded.Valid {
					r.KV.ApplySnapshot(loaded.KV)
					r.Version = loaded.Version
					r.Term = loaded.Term
					r.IsReady = true // disk state means we don't need network recovery
					log.Printf("disk state restored shard_id=%s version=%d term=%d", shard.ID, loaded.Version, loaded.Term)
				}
			}
		}

		s.replicas[shard.ID] = r
		log.Printf("shard initialized shard_id=%s role=%s leader=%s is_ready=%v", shard.ID, r.Role, r.LeaderID, r.IsReady)
	}
}

// shardDataDir returns the directory for a shard's WAL and snapshot.
func shardDataDir(dataDir, shardID string) string {
	return dataDir + "/shards/" + shardID
}

// Start begins serving HTTP and runs background goroutines.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{
		Addr:    s.cfg.Node.Address,
		Handler: mux,
	}
	s.startBackgroundJobs(ctx)
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()
	log.Printf("node listening node_id=%s address=%s", s.cfg.Node.ID, s.cfg.Node.Address)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("node http server: %w", err)
	}
	return nil
}

// StartOnListener starts the server on the provided net.Listener.
// Used in tests to bind to a random port (":0").
func (s *Server) StartOnListener(ctx context.Context, l net.Listener) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{Handler: mux}
	s.startBackgroundJobs(ctx)
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()
	log.Printf("node listening node_id=%s address=%s", s.cfg.Node.ID, l.Addr())
	if err := s.httpServer.Serve(l); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("node http server: %w", err)
	}
	return nil
}

func (s *Server) startBackgroundJobs(ctx context.Context) {
	go s.runHeartbeat(ctx)
	// Start recovery loops for follower replicas that are not yet ready.
	s.mu.RLock()
	for _, r := range s.replicas {
		if r.Role == RoleFollower && !r.IsReady {
			r := r
			go runRecoveryLoop(ctx, r, func() string {
				return s.leaderAddrForReplica(r)
			})
		}
	}
	s.mu.RUnlock()
}

// leaderAddrForReplica returns the HTTP address of the current leader for the given replica.
// Returns "" if the leader is unknown or has no known address.
func (s *Server) leaderAddrForReplica(r *ReplicaState) string {
	r.mu.RLock()
	leaderID := r.LeaderID
	r.mu.RUnlock()
	if leaderID == "" {
		return ""
	}
	s.mu.RLock()
	addr := s.nodeAddresses[leaderID]
	s.mu.RUnlock()
	return addr
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("POST /kv/{shard_id}", s.handleKV)
	mux.HandleFunc("POST /internal/replicate/{shard_id}", s.handleReplicate)
	mux.HandleFunc("GET /internal/sync/{shard_id}", s.handleSync)
	mux.HandleFunc("GET /internal/recover/{shard_id}", s.handleRecover)
}

// Handler builds and returns the HTTP handler for this server.
// Useful for testing individual handlers via httptest without starting a listener.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	return mux
}

// --- Handlers ---

// NodeStatusResponse is the full body of GET /status.
type NodeStatusResponse struct {
	NodeID        string                  `json:"node_id"`
	UptimeSeconds float64                 `json:"uptime_seconds"`
	Shards        []ReplicaStatusSnapshot `json:"shards"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	shards := make([]ReplicaStatusSnapshot, 0, len(s.replicas))
	for _, rep := range s.replicas {
		shards = append(shards, rep.StatusSnapshot())
	}
	s.mu.RUnlock()

	resp := NodeStatusResponse{
		NodeID:        s.cfg.Node.ID,
		UptimeSeconds: s.clock.Now().Sub(s.startedAt).Seconds(),
		Shards:        shards,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	allReady := true
	for _, rep := range s.replicas {
		if !rep.StatusSnapshot().IsReady {
			allReady = false
			break
		}
	}
	s.mu.RUnlock()

	if !allReady {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

// KVRequest is the body of POST /kv/{shard_id}.
type KVRequest struct {
	Op    string `json:"op"`              // "put", "get", or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // only for "put"
}

// KVResponse is returned by POST /kv/{shard_id}.
type KVResponse struct {
	OK           bool   `json:"ok"`
	Value        string `json:"value,omitempty"`
	Error        string `json:"error,omitempty"`
	LeaderID     string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()

	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	snap := replica.StatusSnapshot()

	// Only leaders serve KV requests.
	if snap.Role != RoleLeader {
		leaderAddr := ""
		s.mu.RLock()
		leaderAddr = s.nodeAddresses[snap.LeaderID]
		s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(KVResponse{
			OK:            false,
			Error:         "NOT_LEADER",
			LeaderID:      snap.LeaderID,
			LeaderAddress: leaderAddr,
		})
		return
	}

	if !snap.IsReady {
		http.Error(w, "replica not ready", http.StatusServiceUnavailable)
		return
	}

	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch req.Op {
	case "get":
		val, ok := replica.KV.Get(req.Key)
		if !ok {
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: "not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true, Value: val})

	case "put":
		if err := s.leaderWrite(r.Context(), replica, req); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true})

	case "delete":
		if err := s.leaderWrite(r.Context(), replica, req); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true})

	default:
		http.Error(w, "unknown op: "+req.Op, http.StatusBadRequest)
	}
}

// leaderWrite applies a write locally, fans out to followers, and checks quorum.
func (s *Server) leaderWrite(ctx context.Context, replica *ReplicaState, req KVRequest) error {
	replica.writeMu.Lock()
	defer replica.writeMu.Unlock()

	replica.mu.Lock()
	term := replica.Term
	version := replica.Version + 1

	// Write-Ahead Log: persist to disk before applying in memory.
	s.mu.RLock()
	ds := s.diskStates[replica.ShardID]
	s.mu.RUnlock()
	if ds != nil {
		walEntry := walEntryFrom(req.Op, req.Key, req.Value, term, version)
		if err := ds.appendWAL(walEntry); err != nil {
			replica.mu.Unlock()
			return fmt.Errorf("wal append: %w", err)
		}
	}

	// Apply locally.
	switch req.Op {
	case "put":
		replica.KV.Put(req.Key, req.Value)
	case "delete":
		replica.KV.Delete(req.Key)
	}
	replica.Version = version
	// Append to replication log while still holding the lock so entries
	// are always recorded in version order.
	replica.RepLog.Append(replicationlog.Entry{
		Term:    term,
		Version: version,
		Op:      req.Op,
		Key:     req.Key,
		Value:   req.Value,
	})
	// Collect peer addresses while holding the lock.
	peers := append([]string(nil), replica.Peers...)
	replica.mu.Unlock()

	// Possibly take a snapshot now that the write is applied.
	if ds != nil {
		kv := replica.KV.Snapshot()
		if err := ds.maybeSnapshot(term, version, kv); err != nil {
			log.Printf("snapshot failed shard_id=%s err=%v", replica.ShardID, err)
		}
	}

	// Determine quorum requirement.
	// We count the leader as 1; need (quorum-1) follower ACKs.
	s.mu.RLock()
	peerAddrs := make([]string, 0, len(peers))
	for _, peerID := range peers {
		if addr, ok := s.nodeAddresses[peerID]; ok {
			peerAddrs = append(peerAddrs, addr)
		}
	}
	s.mu.RUnlock()

	// Total replicas = followers + 1 (leader). Quorum = floor(total/2)+1.
	total := len(peers) + 1
	quorum := total/2 + 1
	needed := quorum - 1 // leader already counts as 1

	if needed == 0 {
		// Single-replica shard; no followers needed.
		return nil
	}

	replReq := ReplicateRequest{
		Term:    term,
		Version: version,
		Op:      req.Op,
		Key:     req.Key,
		Value:   req.Value,
	}
	acks := fanOutReplicate(ctx, replica.ShardID, replReq, peerAddrs, s.cfg.QuorumTimeout)
	if acks < needed {
		return fmt.Errorf("quorum unavailable: got %d/%d follower ACKs", acks, needed)
	}
	return nil
}

// handleReplicate handles POST /internal/replicate/{shard_id} from the leader.
func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()

	if replica == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Error: "shard not found"})
		return
	}

	var req ReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Error: "bad request"})
		return
	}

	replica.mu.Lock()
	// Reject stale leader.
	if req.Term < replica.Term {
		term := replica.Term
		replica.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Term: term, Error: "stale term"})
		return
	}

	// Write-Ahead Log: persist before applying.
	s.mu.RLock()
	ds := s.diskStates[shardID]
	s.mu.RUnlock()
	if ds != nil {
		walEntry := walEntryFrom(req.Op, req.Key, req.Value, req.Term, req.Version)
		if err := ds.appendWAL(walEntry); err != nil {
			replica.mu.Unlock()
			log.Printf("follower wal append failed shard_id=%s err=%v", shardID, err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Error: "wal error"})
			return
		}
	}

	// Ignore duplicates: if we already have this version (e.g. received via
	// incremental recovery and replication concurrently), skip silently.
	if req.Version <= replica.Version {
		term := replica.Term
		replica.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: true, Term: term})
		return
	}

	// Apply the write.
	switch req.Op {
	case "put":
		replica.KV.Put(req.Key, req.Value)
	case "delete":
		replica.KV.Delete(req.Key)
	}
	replica.Version = req.Version
	if req.Term > replica.Term {
		replica.Term = req.Term
	}
	replica.IsReady = true // receiving replication means we're in sync
	// Append to replication log so that if this follower is later promoted to
	// leader it can serve incremental recovery without having an empty log.
	replica.RepLog.Append(replicationlog.Entry{
		Term:    req.Term,
		Version: req.Version,
		Op:      req.Op,
		Key:     req.Key,
		Value:   req.Value,
	})
	replica.mu.Unlock()

	// Possibly snapshot after applying.
	if ds != nil {
		kv := replica.KV.Snapshot()
		if err := ds.maybeSnapshot(req.Term, req.Version, kv); err != nil {
			log.Printf("follower snapshot failed shard_id=%s err=%v", shardID, err)
		}
	}

	log.Printf("replicated shard_id=%s op=%s key=%s version=%d", shardID, req.Op, req.Key, req.Version)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: true, Term: req.Term})
}

// handleSync handles GET /internal/sync/{shard_id} — full-state snapshot for recovery.
func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()

	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	replica.mu.RLock()
	if replica.Role != RoleLeader {
		replica.mu.RUnlock()
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	kv := replica.KV.Snapshot()
	version := replica.Version
	term := replica.Term
	replica.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(SyncResponse{
		Term:    term,
		Version: version,
		KV:      kv,
	})
}

// RecoverResponse is returned by GET /internal/recover/{shard_id}.
// Type is "entries" when the log covers the requested gap, or "snapshot"
// when the follower is too far behind and must rebuild from a full snapshot.
type RecoverResponse struct {
	Type    string                 `json:"type"`              // "entries" or "snapshot"
	Version uint64                 `json:"version"`           // leader version at response time
	Entries []replicationlog.Entry `json:"entries,omitempty"` // for type="entries"
	Term    uint64                 `json:"term,omitempty"`    // for type="snapshot"
	KV      map[string]string      `json:"kv,omitempty"`      // for type="snapshot"
}

// handleRecover handles GET /internal/recover/{shard_id}?since_version=N.
//
// Phase 3 incremental recovery: if the log covers the follower's gap the leader
// sends only the missing entries. Otherwise it falls back to a full snapshot.
// Only the leader serves this endpoint.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")
	sinceVersion, _ := strconv.ParseUint(r.URL.Query().Get("since_version"), 10, 64)

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()

	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	replica.mu.RLock()
	if replica.Role != RoleLeader {
		replica.mu.RUnlock()
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	leaderVersion := replica.Version
	term := replica.Term

	var resp RecoverResponse
	resp.Version = leaderVersion

	if leaderVersion == sinceVersion {
		// Follower is already up to date.
		replica.mu.RUnlock()
		resp.Type = "entries"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	entries, ok := replica.RepLog.Since(sinceVersion)
	if ok && (len(entries) > 0 || leaderVersion == sinceVersion) {
		// Log covers the gap: send entries.
		replica.mu.RUnlock()
		resp.Type = "entries"
		resp.Entries = entries
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Gap too large or log empty with leader ahead: send full snapshot.
	kv := replica.KV.Snapshot()
	replica.mu.RUnlock()
	resp.Type = "snapshot"
	resp.Term = term
	resp.KV = kv
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// --- Heartbeat ---

func (s *Server) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	// Send one immediately on startup.
	s.sendHeartbeat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendHeartbeat()
		}
	}
}

func (s *Server) sendHeartbeat() {
	s.mu.RLock()
	shards := make([]coordinator.ShardStatus, 0, len(s.replicas))
	for _, rep := range s.replicas {
		snap := rep.StatusSnapshot()
		shards = append(shards, coordinator.ShardStatus{
			ShardID: snap.ShardID,
			Role:    string(snap.Role),
			Version: snap.Version,
			IsReady: snap.IsReady,
			Term:    snap.Term,
		})
	}
	s.mu.RUnlock()

	req := coordinator.HeartbeatRequest{
		NodeID: s.cfg.Node.ID,
		Shards: shards,
	}
	body, err := json.Marshal(req)
	if err != nil {
		log.Printf("heartbeat marshal error node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}

	url := "http://" + s.cfg.CoordinatorAddress + "/heartbeat"
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		log.Printf("heartbeat failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		log.Printf("heartbeat rejected node_id=%s status=%d", s.cfg.Node.ID, resp.StatusCode)
		return
	}

	// Check if coordinator's shard map version is newer; if so, refetch.
	var hbResp coordinator.HeartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
		return
	}
	s.mu.RLock()
	currentVersion := s.shardMapVersion
	s.mu.RUnlock()
	if hbResp.ShardMapVersion > currentVersion {
		s.refetchShardMap()
	}
}

// refetchShardMap pulls an updated shard map from the coordinator and applies
// any leader or role changes to existing replicas.
func (s *Server) refetchShardMap() {
	url := "http://" + s.cfg.CoordinatorAddress + "/shardmap"
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		log.Printf("shardmap refetch failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	var smResp coordinator.ShardMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&smResp); err != nil {
		log.Printf("shardmap decode failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}

	s.mu.Lock()
	s.nodeAddresses = smResp.NodeAddresses
	s.shardMapVersion = smResp.Version
	for _, shard := range smResp.Shards {
		r := s.replicas[shard.ID]
		if r == nil {
			continue
		}
		r.mu.Lock()
		oldLeader := r.LeaderID
		r.LeaderID = shard.Leader
		if shard.Leader == s.cfg.Node.ID && r.Role != RoleLeader {
			r.Role = RoleLeader
			r.IsReady = true
			log.Printf("promoted to leader shard_id=%s", shard.ID)
		} else if shard.Leader != s.cfg.Node.ID && r.Role == RoleLeader {
			r.Role = RoleFollower
			log.Printf("demoted to follower shard_id=%s", shard.ID)
		}
		if r.LeaderID != oldLeader {
			log.Printf("leader changed shard_id=%s old=%s new=%s", shard.ID, oldLeader, r.LeaderID)
		}
		r.mu.Unlock()
	}
	s.mu.Unlock()
}


// walEntryFrom builds a wal.Entry from a KV operation's fields.
func walEntryFrom(op, key, value string, term, version uint64) wal.Entry {
	return wal.Entry{
		Term:    term,
		Version: version,
		Op:      op,
		Key:     key,
		Value:   value,
	}
}
