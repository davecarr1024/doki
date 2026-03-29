package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/metrics"
	"github.com/davecarr1024/doki/internal/replicationlog"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/davecarr1024/doki/internal/wal"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server is a leaf node's HTTP server.
//
// Phase 0 endpoints:
//   - GET  /status                         — node and shard replica health
//   - GET  /ready                          — readiness check
//
// Phase 1 endpoints:
//   - POST /kv/{shard_id}                  — KV operations (put/get/delete); leader only
//   - POST /internal/replicate/{shard_id}  — apply replicated entry
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
//
// Phase 4 additions:
//   - POST /internal/request_vote/{shard_id}     — election vote request
//   - POST /internal/leader_heartbeat/{shard_id} — leader liveness heartbeat
//   - Distributed leader election with randomized timeouts
//   - POST /notify_leader on coordinator — elected leader notifies coordinator
//
// Recovery control:
//   - POST /internal/force_recover/{shard_id} — mark replica not-ready to trigger recovery
//
// Phase 5 additions:
//   - Dynamic shard initialisation: new shards detected via shard map updates
//   - Dynamic shard removal: shards removed from this node's assignments are dropped
//   - Shard splits: bootstrap recovery from a source shard's leader
type Server struct {
	cfg           *config.NodeConfig
	clock         clock.Clock
	m             *metrics.NodeMetrics
	replicas      map[string]*ReplicaState // shard_id → replica
	diskStates    map[string]*diskState    // shard_id → disk state (WAL + snapshot)
	nodeAddresses map[string]string        // nodeID → HTTP address (from coordinator)
	// shardLeaders tracks the current leader for each shard (updated from shard map).
	// Used to resolve bootstrap leader addresses for split shards.
	shardLeaders    map[string]string // shard_id → leader node_id
	shardMapVersion uint64
	mu              sync.RWMutex
	startedAt       time.Time
	httpServer      *http.Server
	grpcServer      *grpc.Server
	// Phase 5: per-shard contexts allow individual shards to be stopped cleanly.
	serverCtx   context.Context
	cancelFuncs map[string]context.CancelFunc // shard_id → cancel
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
		m:             metrics.NewNodeMetrics(),
		replicas:      make(map[string]*ReplicaState),
		diskStates:    make(map[string]*diskState),
		nodeAddresses: make(map[string]string),
		shardLeaders:  make(map[string]string),
		cancelFuncs:   make(map[string]context.CancelFunc),
		startedAt:     clk.Now(),
	}
}

// Metrics returns the node's Prometheus registry for use in tests.
func (s *Server) Metrics() *metrics.NodeMetrics { return s.m }

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
		s.initShardLocked(shard, resp)
	}
}

// initShardLocked initialises a single shard replica.
// Must be called with s.mu held for writing.
// resp is the full shard map response (used to resolve bootstrap source leader).
func (s *Server) initShardLocked(shard shardmap.ShardInfo, resp coordinator.ShardMapResponse) {
	if _, exists := s.replicas[shard.ID]; exists {
		return // already initialised
	}

	peers := make([]string, 0, len(shard.Replicas)-1)
	for _, r := range shard.Replicas {
		if r != s.cfg.Node.ID {
			peers = append(peers, r)
		}
	}
	r := NewReplicaState(shard.ID, s.cfg.Node.ID, peers, s.cfg.ReplicationLogSize, s.electionTimeout())
	now := s.clock.Now()
	for _, peerID := range peers {
		r.PeerLastContact[peerID] = now
	}
	if shard.Leader == s.cfg.Node.ID {
		r.Role = RoleLeader
		// Phase 5: don't mark ready yet for bootstrap shards — recovery loop
		// must apply the source shard's data first.
		if shard.BootstrapSourceShardID == "" {
			r.IsReady = true
		}
	}
	r.LeaderID = shard.Leader

	// Phase 5: for shard splits, record where to fetch the initial snapshot.
	if shard.BootstrapSourceShardID != "" {
		r.BootstrapShardID = shard.BootstrapSourceShardID
		// Find the source shard's leader address.
		for _, sh := range resp.Shards {
			if sh.ID == shard.BootstrapSourceShardID && sh.Leader != "" {
				r.BootstrapLeaderAddr = resp.NodeAddresses[sh.Leader]
				break
			}
		}
		log.Printf("shard split bootstrap shard_id=%s source=%s bootstrap_addr=%s",
			shard.ID, shard.BootstrapSourceShardID, r.BootstrapLeaderAddr)
	}

	// Try to load from disk if a data directory is configured.
	if s.cfg.DataDir != "" {
		shardDir := shardDataDir(s.cfg.DataDir, shard.ID)
		ds, err := openDiskState(shardDir, s.cfg.SnapshotInterval)
		if err != nil {
			log.Printf("disk state open failed shard_id=%s err=%v — continuing without disk state", shard.ID, err)
		} else {
			s.diskStates[shard.ID] = ds
			if r.SM != nil {
				r.SM.SetDiskState(ds)
			}
			loaded, err := ds.load()
			if err != nil {
				log.Printf("disk state load failed shard_id=%s err=%v", shard.ID, err)
			} else if loaded.Valid {
				if err := r.SM.ApplySnapshot(StateSnapshot{
					Term:    loaded.Term,
					Version: loaded.Version,
					KV:      loaded.KV,
				}, SnapshotNoPersist); err != nil {
					log.Printf("disk state apply failed shard_id=%s err=%v", shard.ID, err)
				} else {
					r.IsReady = true // disk state means we don't need network recovery
				}
				r.BootstrapShardID = ""
				r.BootstrapLeaderAddr = ""
				log.Printf("disk state restored shard_id=%s version=%d term=%d", shard.ID, loaded.Version, loaded.Term)
			}
		}
	}

	s.replicas[shard.ID] = r
	log.Printf("shard initialized shard_id=%s role=%s leader=%s is_ready=%v", shard.ID, r.Role, r.LeaderID, r.IsReady)
}

// startShardGoroutines launches the per-replica background goroutines for shard
// shardID.  serverCtx must already be stored on s.  Must NOT hold s.mu.
func (s *Server) startShardGoroutines(shardID string) {
	s.mu.Lock()
	r, ok := s.replicas[shardID]
	ctx := s.serverCtx
	s.mu.Unlock()
	if !ok || ctx == nil {
		return
	}

	r.mu.Lock()
	r.LastLeaderContact = s.clock.Now()
	r.mu.Unlock()

	replicaCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancelFuncs[shardID] = cancel
	s.mu.Unlock()

	// Recovery loop stays active so replicas can be forced to recover later.
	go runRecoveryLoop(replicaCtx, r, func() string {
		return s.leaderAddrForReplica(r)
	}, s.m, s.diskStates[r.ShardID])
	go s.runElectionTimer(replicaCtx, r)
	go s.runLeaderHeartbeat(replicaCtx, r)
}

// dropShard stops all goroutines for a shard and removes it from this node.
// It is safe to call even if the shard is not present.
// Must NOT hold s.mu.
func (s *Server) dropShard(shardID string) {
	s.mu.Lock()
	cancel, hasCancel := s.cancelFuncs[shardID]
	delete(s.cancelFuncs, shardID)
	ds := s.diskStates[shardID]
	delete(s.diskStates, shardID)
	delete(s.replicas, shardID)
	delete(s.shardLeaders, shardID)
	s.mu.Unlock()

	if hasCancel {
		cancel()
	}
	if ds != nil {
		if err := ds.close(); err != nil {
			log.Printf("disk state close error shard_id=%s err=%v", shardID, err)
		}
	}
	log.Printf("shard dropped shard_id=%s node_id=%s", shardID, s.cfg.Node.ID)
}

// shardDataDir returns the directory for a shard's WAL and snapshot.
func shardDataDir(dataDir, shardID string) string {
	return dataDir + "/shards/" + shardID
}

// Start begins serving HTTP and runs background goroutines.
func (s *Server) Start(ctx context.Context) error {
	l, err := net.Listen("tcp", s.cfg.Node.Address)
	if err != nil {
		return fmt.Errorf("node listen: %w", err)
	}
	return s.StartOnListener(ctx, l)
}

// StartOnListener starts the server on the provided net.Listener.
// Used in tests to bind to a random port (":0").
func (s *Server) StartOnListener(ctx context.Context, l net.Listener) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{Handler: mux}
	s.grpcServer = grpc.NewServer()
	nodev1.RegisterNodeServiceServer(s.grpcServer, &grpcServer{s: s})
	reflection.Register(s.grpcServer)

	m := cmux.New(l)
	grpcL := m.Match(cmux.HTTP2())
	httpL := m.Match(cmux.Any())

	s.startBackgroundJobs(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(grpcL)
	}()
	go func() {
		if err := s.httpServer.Serve(httpL); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("node http server: %w", err)
		}
	}()
	go func() {
		if err := m.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			errCh <- fmt.Errorf("node cmux: %w", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.grpcServer.GracefulStop()
		_ = s.httpServer.Shutdown(shutCtx)
		_ = l.Close()
	}()

	log.Printf("node listening node_id=%s address=%s", s.cfg.Node.ID, l.Addr())
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (s *Server) startBackgroundJobs(ctx context.Context) {
	s.mu.Lock()
	s.serverCtx = ctx
	s.mu.Unlock()

	go s.runHeartbeat(ctx)
	// Start per-replica background goroutines.
	s.mu.Lock()
	for _, r := range s.replicas {
		r := r
		// Seed the election timer so the replica doesn't immediately call an
		// election before the initial shard map is fully distributed.
		r.mu.Lock()
		r.LastLeaderContact = s.clock.Now()
		r.mu.Unlock()
		replicaCtx, cancel := context.WithCancel(ctx)
		s.cancelFuncs[r.ShardID] = cancel
		ds := s.diskStates[r.ShardID]
		// Phase 3+: recovery loop stays active so replicas can be forced to recover later.
		go runRecoveryLoop(replicaCtx, r, func() string {
			return s.leaderAddrForReplica(r)
		}, s.m, ds)
		// Phase 4: election timer and leader heartbeat run for every replica.
		go s.runElectionTimer(replicaCtx, r)
		go s.runLeaderHeartbeat(replicaCtx, r)
	}
	s.mu.Unlock()
	// Reliability: update staleness gauges and replica version/term gauges.
	go s.runMetricsUpdater(ctx)
}

// runMetricsUpdater periodically refreshes replica-level Prometheus gauges.
func (s *Server) runMetricsUpdater(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshReplicaMetrics()
		}
	}
}

// refreshReplicaMetrics updates version, term, leader, and staleness gauges.
func (s *Server) refreshReplicaMetrics() {
	now := s.clock.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.replicas {
		r.mu.RLock()
		isLeader := 0.0
		if r.Role == RoleLeader {
			isLeader = 1.0
		}
		version := r.Version
		term := r.Term
		lastContact := r.LastLeaderContact
		shardID := r.ShardID
		r.mu.RUnlock()
		nodeID := s.cfg.Node.ID
		s.m.ReplicaVersion.WithLabelValues(shardID, nodeID).Set(float64(version))
		s.m.ReplicaTerm.WithLabelValues(shardID, nodeID).Set(float64(term))
		s.m.ReplicaIsLeader.WithLabelValues(shardID, nodeID).Set(isLeader)
		if !lastContact.IsZero() {
			s.m.LastLeaderContactSeconds.WithLabelValues(shardID, nodeID).Set(now.Sub(lastContact).Seconds())
		}
	}
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
	mux.HandleFunc("GET /ready", s.handleReady)
	// Reliability: Prometheus metrics.
	mux.Handle("GET /metrics", s.m.Handler())
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
	Op    string `json:"op"` // "put", "get", or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // only for "put"
}

// KVResponse is returned by POST /kv/{shard_id}.
type KVResponse struct {
	OK             bool   `json:"ok"`
	Value          string `json:"value,omitempty"`
	Error          string `json:"error,omitempty"`
	LeaderID       string `json:"leader_id,omitempty"`
	LeaderAddress  string `json:"leader_address,omitempty"`
	AppliedVersion uint64 `json:"applied_version,omitempty"`
	Quorum         int    `json:"quorum,omitempty"`
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

	snap, err := ensureLeaderReady(replica)
	if err != nil {
		if errors.Is(err, errNotLeader) {
			leaderAddr := ""
			s.mu.RLock()
			leaderAddr = s.nodeAddresses[snap.LeaderID]
			s.mu.RUnlock()
			s.m.WritesTotal.WithLabelValues(shardID, "not_leader").Inc()
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
		val, ok := replica.SM.Get(req.Key)
		if !ok {
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: "not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true, Value: val})

	case "put":
		result, err := s.leaderWrite(r.Context(), replica, req)
		if err != nil {
			s.m.WritesTotal.WithLabelValues(shardID, "quorum_unavailable").Inc()
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: err.Error()})
			return
		}
		s.m.WritesTotal.WithLabelValues(shardID, "ok").Inc()
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true, AppliedVersion: result.AppliedVersion, Quorum: result.Quorum})

	case "delete":
		result, err := s.leaderWrite(r.Context(), replica, req)
		if err != nil {
			s.m.WritesTotal.WithLabelValues(shardID, "quorum_unavailable").Inc()
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(KVResponse{OK: false, Error: err.Error()})
			return
		}
		s.m.WritesTotal.WithLabelValues(shardID, "ok").Inc()
		_ = json.NewEncoder(w).Encode(KVResponse{OK: true, AppliedVersion: result.AppliedVersion, Quorum: result.Quorum})

	default:
		http.Error(w, "unknown op: "+req.Op, http.StatusBadRequest)
	}
}

// leaderWrite applies a write locally, fans out to followers, and checks quorum.
func (s *Server) leaderWrite(ctx context.Context, replica *ReplicaState, req KVRequest) (WriteResult, error) {
	start := s.clock.Now()
	defer func() {
		s.m.WriteDuration.WithLabelValues(replica.ShardID).Observe(s.clock.Now().Sub(start).Seconds())
	}()
	replica.writeMu.Lock()
	defer replica.writeMu.Unlock()

	now := s.clock.Now()
	if _, err := ensureLeaderReady(replica); err != nil {
		return WriteResult{}, err
	}
	replica.mu.RLock()
	term := replica.Term
	version := replica.Version + 1
	entry := replicationlog.Entry{
		Term:    term,
		Version: version,
		Op:      req.Op,
		Key:     req.Key,
		Value:   req.Value,
	}
	peers := append([]string(nil), replica.Peers...)
	peerLast := make(map[string]time.Time, len(replica.PeerLastContact))
	for id, ts := range replica.PeerLastContact {
		peerLast[id] = ts
	}
	replica.mu.RUnlock()

	// Gather peer addresses and disk state.
	s.mu.RLock()
	peerAddrs := make(map[string]string, len(peers))
	for _, peerID := range peers {
		if addr, ok := s.nodeAddresses[peerID]; ok {
			peerAddrs[peerID] = addr
		}
	}
	ds := s.diskStates[replica.ShardID]
	s.mu.RUnlock()

	// Determine quorum requirement based on live peers.
	quorumPlan := planQuorum(peerAddrs, peerLast, now, s.cfg.LeaderHeartbeat*2)
	needed := quorumPlan.NeededFollowers

	if needed == 0 {
		// Single-replica shard; commit locally immediately.
		replica.mu.Lock()
		if err := replica.SM.Apply(entry, ApplyWithWAL); err != nil {
			replica.mu.Unlock()
			return WriteResult{}, fmt.Errorf("apply: %w", err)
		}
		replica.IsReady = true
		replica.mu.Unlock()
		if ds != nil {
			snap := replica.SM.Snapshot()
			if err := ds.maybeSnapshot(term, version, snap.KV); err != nil {
				log.Printf("snapshot failed shard_id=%s err=%v", replica.ShardID, err)
			}
		}
		replica.WriteOpsTotal.Add(1)
		return quorumPlan.result(entry.Version, 0), nil
	}

	if !quorumPlan.hasLiveQuorum() {
		replica.WriteErrTotal.Add(1)
		return WriteResult{}, fmt.Errorf("quorum unavailable: need %d live followers, have %d", needed, len(quorumPlan.LivePeers))
	}

	replReq := ReplicateRequest{
		Term:    term,
		Version: entry.Version,
		Op:      entry.Op,
		Key:     entry.Key,
		Value:   entry.Value,
	}
	acked := fanOutReplicate(ctx, replica.ShardID, replReq, quorumPlan.LivePeers, s.cfg.QuorumTimeout)
	if len(acked) < needed {
		_ = fanOutForceRecover(ctx, replica.ShardID, quorumPlan.LivePeers, acked, s.cfg.QuorumTimeout)
		replica.WriteErrTotal.Add(1)
		return WriteResult{}, fmt.Errorf("quorum unavailable: got %d/%d follower ACKs", len(acked), needed)
	}

	replica.mu.Lock()
	for _, peerID := range acked {
		replica.PeerLastContact[peerID] = now
	}
	if err := replica.SM.Apply(entry, ApplyWithWAL); err != nil {
		replica.mu.Unlock()
		return WriteResult{}, fmt.Errorf("apply: %w", err)
	}
	replica.IsReady = true
	replica.mu.Unlock()

	if ds != nil {
		snap := replica.SM.Snapshot()
		if err := ds.maybeSnapshot(term, version, snap.KV); err != nil {
			log.Printf("snapshot failed shard_id=%s err=%v", replica.ShardID, err)
		}
	}

	replica.WriteOpsTotal.Add(1)
	return quorumPlan.result(entry.Version, len(acked)), nil
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
		s.m.ReplicationsTotal.WithLabelValues(shardID, "stale_term").Inc()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Term: term, Error: "stale term"})
		return
	}

	if req.Term > replica.Term {
		replica.Term = req.Term
		if replica.Role != RoleFollower {
			replica.Role = RoleFollower
		}
	}

	// Ignore duplicates: if we already have this version committed, skip silently.
	if req.Version <= replica.Version {
		term := replica.Term
		replica.mu.Unlock()
		s.m.ReplicationsTotal.WithLabelValues(shardID, "duplicate").Inc()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: true, Term: term})
		return
	}

	if req.Version != replica.Version+1 {
		term := replica.Term
		replica.IsReady = false
		replica.LastLeaderContact = s.clock.Now()
		replica.mu.Unlock()
		s.m.ReplicationsTotal.WithLabelValues(shardID, "gap").Inc()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Term: term, Error: "version gap"})
		return
	}

	entry := replicationlog.Entry{
		Term:    req.Term,
		Version: req.Version,
		Op:      req.Op,
		Key:     req.Key,
		Value:   req.Value,
	}
	if err := replica.SM.Apply(entry, ApplyWithWAL); err != nil {
		replica.mu.Unlock()
		s.m.ReplicationsTotal.WithLabelValues(shardID, "wal_error").Inc()
		log.Printf("replicate wal append failed shard_id=%s err=%v", shardID, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ReplicateResponse{Success: false, Error: "wal error"})
		return
	}
	replica.IsReady = true
	// Phase 4: valid replication from leader proves leader is alive; reset election timer.
	replica.LastLeaderContact = s.clock.Now()
	replica.mu.Unlock()

	s.m.ReplicationsTotal.WithLabelValues(shardID, "apply_ok").Inc()
	log.Printf("replicated shard_id=%s op=%s key=%s version=%d applied=true", shardID, req.Op, req.Key, req.Version)
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
	snap := replica.SM.Snapshot()
	replica.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(SyncResponse{
		Term:    snap.Term,
		Version: snap.Version,
		KV:      snap.KV,
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
	snap := replica.SM.Snapshot()
	leaderVersion := snap.Version
	term := snap.Term

	var resp RecoverResponse
	resp.Version = leaderVersion

	if sinceVersion > leaderVersion {
		// Follower is ahead (likely applied uncommitted entries). Force snapshot rollback.
		replica.mu.RUnlock()
		resp.Type = "snapshot"
		resp.Term = term
		resp.KV = snap.KV
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if leaderVersion == sinceVersion {
		// Follower is already up to date.
		replica.mu.RUnlock()
		resp.Type = "entries"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	entries, ok := replica.SM.EntriesSince(sinceVersion)
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
	replica.mu.RUnlock()
	resp.Type = "snapshot"
	resp.Term = term
	resp.KV = snap.KV
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleForceRecover marks a replica not-ready so it re-enters recovery.
// Used by leaders to roll back followers after a failed quorum attempt.
func (s *Server) handleForceRecover(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")

	s.mu.RLock()
	replica := s.replicas[shardID]
	s.mu.RUnlock()
	if replica == nil {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	replica.mu.Lock()
	replica.IsReady = false
	replica.SM.ResetLog()
	replica.LastLeaderContact = s.clock.Now()
	replica.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "recovery_triggered"})
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

	conn, client, err := s.dialCoordinator(context.Background(), 2*time.Second)
	if err != nil {
		log.Printf("heartbeat dial failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}
	defer func() { _ = conn.Close() }()

	req := &coordinatorv1.HeartbeatRequest{
		NodeId: s.cfg.Node.ID,
		Shards: shardHeartbeatsFromStatus(shards),
	}
	hbResp, err := client.Heartbeat(context.Background(), req)
	if err != nil {
		log.Printf("heartbeat failed node_id=%s err=%v", s.cfg.Node.ID, err)
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
// any leader or role changes to existing replicas. Phase 5: also detects
// newly-assigned shards (calls initShardLocked) and removed shards (dropShard).
func (s *Server) refetchShardMap() {
	conn, client, err := s.dialCoordinator(context.Background(), 2*time.Second)
	if err != nil {
		log.Printf("shardmap dial failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}
	defer func() { _ = conn.Close() }()

	resp, err := client.GetShardMap(context.Background(), &coordinatorv1.GetShardMapRequest{})
	if err != nil {
		log.Printf("shardmap refetch failed node_id=%s err=%v", s.cfg.Node.ID, err)
		return
	}
	smResp := shardMapResponseFromProto(resp)

	// Collect shard IDs this node should now host.
	shouldHost := make(map[string]shardmap.ShardInfo, len(smResp.Shards))
	for _, shard := range smResp.Shards {
		if shard.HasReplica(s.cfg.Node.ID) {
			shouldHost[shard.ID] = shard
		}
	}

	s.mu.Lock()
	s.nodeAddresses = smResp.NodeAddresses
	s.shardMapVersion = smResp.Version
	// Update shard leader index.
	for _, shard := range smResp.Shards {
		s.shardLeaders[shard.ID] = shard.Leader
	}

	// Collect shards currently hosted that are no longer in shouldHost.
	var toRemove []string
	for shardID := range s.replicas {
		if _, ok := shouldHost[shardID]; !ok {
			toRemove = append(toRemove, shardID)
		}
	}

	// Collect newly-assigned shards.
	var newShardIDs []string
	for shardID, shard := range shouldHost {
		if _, exists := s.replicas[shardID]; exists {
			// Existing replica: update leader/role/peers.
			r := s.replicas[shardID]
			r.mu.Lock()
			oldLeader := r.LeaderID
			r.LeaderID = shard.Leader
			// Update peer list in case replicas changed (migration window).
			peers := make([]string, 0, len(shard.Replicas)-1)
			for _, rid := range shard.Replicas {
				if rid != s.cfg.Node.ID {
					peers = append(peers, rid)
				}
			}
			r.Peers = peers
			// Refresh peer liveness tracking.
			now := s.clock.Now()
			nextPeerLast := make(map[string]time.Time, len(peers))
			for _, pid := range peers {
				if last, ok := r.PeerLastContact[pid]; ok {
					nextPeerLast[pid] = last
				} else {
					nextPeerLast[pid] = now
				}
			}
			r.PeerLastContact = nextPeerLast
			if shard.Leader == s.cfg.Node.ID && r.Role != RoleLeader {
				r.Role = RoleLeader
				// Phase 5: don't mark ready yet if bootstrap recovery is still
				// pending — the recovery loop will set IsReady once it completes.
				if r.BootstrapShardID == "" {
					r.IsReady = true
				}
				log.Printf("promoted to leader shard_id=%s", shard.ID)
			} else if shard.Leader != s.cfg.Node.ID && r.Role == RoleLeader {
				r.Role = RoleFollower
				log.Printf("demoted to follower shard_id=%s", shard.ID)
			}
			if r.LeaderID != oldLeader {
				log.Printf("leader changed shard_id=%s old=%s new=%s", shard.ID, oldLeader, r.LeaderID)
				r.LastLeaderContact = s.clock.Now()
			}
			r.mu.Unlock()
		} else {
			// Brand-new shard for this node.
			s.initShardLocked(shard, smResp)
			newShardIDs = append(newShardIDs, shardID)
		}
	}
	s.mu.Unlock()

	// Start goroutines for new shards (must not hold lock).
	for _, shardID := range newShardIDs {
		s.startShardGoroutines(shardID)
	}
	// Drop removed shards (must not hold lock).
	for _, shardID := range toRemove {
		s.dropShard(shardID)
	}
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
