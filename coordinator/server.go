package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/metrics"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server is the coordinator's HTTP server.
//
// Phase 1 endpoints:
//   - POST /heartbeat      — nodes report liveness and shard versions
//   - GET  /shardmap       — returns shard map + node addresses
//   - GET  /status         — cluster health overview
//   - GET  /ready          — readiness probe
//   - GET  /leader/{shard} — direct leader lookup for a shard
//
// Phase 4 additions:
//   - POST /notify_leader  — distributed election notification
//
// Phase 5 additions:
//   - POST /admin/add_node       — register a new node in the cluster
//   - POST /admin/migrate_shard  — begin migrating a shard to a new replica set
//   - POST /admin/split_shard    — create a new shard bootstrapped from an existing one
//
// Reliability additions:
//   - GET /metrics         — Prometheus metrics
type Server struct {
	cfg        *config.ClusterConfig
	membership *Membership
	shards     *shardmap.ShardMap
	leader     *LeaderManager
	migration  *MigrationManager
	clock      clock.Clock
	m          *metrics.CoordinatorMetrics
	httpServer *http.Server
	grpcServer *grpc.Server
	ready      bool
}

// NewServer creates a coordinator Server from the given cluster config.
func NewServer(cfg *config.ClusterConfig, clk clock.Clock) *Server {
	membership := NewMembership(cfg.Nodes, cfg.Coordinator.FailureTimeout, clk)
	shards := shardmap.New()
	leader := NewLeaderManager(shards, membership)
	migration := NewMigrationManager(shards, membership, leader)
	return &Server{
		cfg:        cfg,
		membership: membership,
		shards:     shards,
		leader:     leader,
		migration:  migration,
		clock:      clk,
		m:          metrics.NewCoordinatorMetrics(),
	}
}

// Metrics returns the coordinator's Prometheus registry for use in tests.
func (s *Server) Metrics() *metrics.CoordinatorMetrics { return s.m }

// Init populates the shard map from config and marks the server as ready.
func (s *Server) Init() error {
	if err := s.leader.InitFromConfig(s.cfg); err != nil {
		return fmt.Errorf("coordinator init: %w", err)
	}
	s.ready = true
	return nil
}

// Start begins serving HTTP on the configured address.
func (s *Server) Start(ctx context.Context) error {
	l, err := net.Listen("tcp", s.cfg.Coordinator.Address)
	if err != nil {
		return fmt.Errorf("coordinator listen: %w", err)
	}
	return s.StartOnListener(ctx, l)
}

// StartOnListener starts the server on the provided net.Listener (used in tests).
func (s *Server) StartOnListener(ctx context.Context, l net.Listener) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{Handler: mux}
	s.grpcServer = grpc.NewServer()
	coordinatorv1.RegisterCoordinatorServiceServer(s.grpcServer, &grpcServer{s: s})
	reflection.Register(s.grpcServer)

	m := cmux.New(l)
	grpcL := m.Match(cmux.HTTP2())
	httpL := m.Match(cmux.Any())

	go s.runHealthMonitor(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(grpcL)
	}()
	go func() {
		if err := s.httpServer.Serve(httpL); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("coordinator http server: %w", err)
		}
	}()
	go func() {
		if err := m.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			errCh <- fmt.Errorf("coordinator cmux: %w", err)
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

	log.Printf("coordinator listening address=%s", l.Addr())
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /ready", s.handleReady)
	// Phase 5: dynamic cluster management.
	mux.HandleFunc("POST /admin/add_node", s.handleAdminAddNode)
	mux.HandleFunc("POST /admin/migrate_shard", s.handleAdminMigrateShard)
	mux.HandleFunc("POST /admin/split_shard", s.handleAdminSplitShard)
	// Reliability: Prometheus metrics.
	mux.Handle("GET /metrics", s.m.Handler())
}

// --- Handlers ---

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.membership.RecordHeartbeat(req.NodeID, req.Shards); err != nil {
		log.Printf("heartbeat rejected node_id=%s err=%v", req.NodeID, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.m.HeartbeatReceivedTotal.WithLabelValues(req.NodeID).Inc()
	smVersion, _ := s.shards.Snapshot()
	resp := HeartbeatResponse{ShardMapVersion: smVersion}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ShardMapResponse is the JSON body returned by GET /shardmap.
// NodeAddresses maps node IDs to their HTTP addresses so nodes can
// contact each other for replication and recovery.
type ShardMapResponse struct {
	Version       uint64               `json:"version"`
	Shards        []shardmap.ShardInfo `json:"shards"`
	NodeAddresses map[string]string    `json:"node_addresses"`
}

func (s *Server) handleShardMap(w http.ResponseWriter, r *http.Request) {
	resp := s.buildShardMapResponse()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) buildShardMapResponse() ShardMapResponse {
	version, shards := s.shards.Snapshot()
	// Use membership as the authoritative source of node addresses so that
	// dynamically-added nodes (Phase 5) are included.
	allNodes := s.membership.All()
	nodeAddrs := make(map[string]string, len(allNodes))
	for _, ns := range allNodes {
		nodeAddrs[ns.ID] = ns.Address
	}
	return ShardMapResponse{
		Version:       version,
		Shards:        shards,
		NodeAddresses: nodeAddrs,
	}
}

// LeaderQueryResponse is returned by GET /leader/{shard_id}.
type LeaderQueryResponse struct {
	ShardID  string `json:"shard_id"`
	LeaderID string `json:"leader_id"`
	Address  string `json:"address"`
	Term     uint64 `json:"term"`
}

func (s *Server) handleLeaderQuery(w http.ResponseWriter, r *http.Request) {
	shardID := r.PathValue("shard_id")
	leader, err := s.shards.LeaderFor(shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	ns, err := s.membership.Get(leader)
	if err != nil {
		http.Error(w, "leader node not found", http.StatusInternalServerError)
		return
	}
	resp := LeaderQueryResponse{
		ShardID:  shardID,
		LeaderID: leader,
		Address:  ns.Address,
		Term:     s.leader.TermForShard(shardID),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// NodeStatusResponse is one node's entry in the coordinator status response.
type NodeStatusResponse struct {
	NodeID          string `json:"node_id"`
	Address         string `json:"address"`
	IsAlive         bool   `json:"is_alive"`
	LastHeartbeatMs int64  `json:"last_heartbeat_ms"`
}

// ShardStatusResponse is the extended per-shard view in coordinator status.
type ShardStatusResponse struct {
	shardmap.ShardInfo
	Term              uint64            `json:"term"`
	QuorumAlive       bool              `json:"quorum_alive"`
	ReplicaVersions   map[string]uint64 `json:"replica_versions"`
	MaxReplicationLag uint64            `json:"max_replication_lag"`
}

// CoordinatorStatusResponse is the full body of GET /status.
type CoordinatorStatusResponse struct {
	ShardMapVersion uint64                `json:"shard_map_version"`
	Nodes           []NodeStatusResponse  `json:"nodes"`
	Shards          []ShardStatusResponse `json:"shards"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	now := s.clock.Now()
	nodeStatuses := s.membership.All()
	nodeResps := make([]NodeStatusResponse, len(nodeStatuses))
	for i, ns := range nodeStatuses {
		msSince := int64(-1)
		if !ns.LastHeartbeatAt.IsZero() {
			msSince = now.Sub(ns.LastHeartbeatAt).Milliseconds()
		}
		nodeResps[i] = NodeStatusResponse{
			NodeID:          ns.ID,
			Address:         ns.Address,
			IsAlive:         ns.IsAlive,
			LastHeartbeatMs: msSince,
		}
	}
	version, shards := s.shards.Snapshot()

	// Build alive-node set for quorum calculation.
	aliveNodes := make(map[string]bool, len(nodeStatuses))
	for _, ns := range nodeStatuses {
		if ns.IsAlive {
			aliveNodes[ns.ID] = true
		}
	}

	shardResps := make([]ShardStatusResponse, len(shards))
	for i, sh := range shards {
		replicaVersions := make(map[string]uint64, len(sh.Replicas))
		var leaderVersion uint64
		for _, rep := range sh.Replicas {
			v := s.membership.VersionForShard(rep, sh.ID)
			replicaVersions[rep] = v
			if rep == sh.Leader {
				leaderVersion = v
			}
		}
		var maxLag uint64
		aliveCount := 0
		for _, rep := range sh.Replicas {
			if aliveNodes[rep] {
				aliveCount++
			}
			v := replicaVersions[rep]
			if leaderVersion > v && leaderVersion-v > maxLag {
				maxLag = leaderVersion - v
			}
		}
		shardResps[i] = ShardStatusResponse{
			ShardInfo:         sh,
			Term:              s.leader.TermForShard(sh.ID),
			QuorumAlive:       aliveCount >= sh.Quorum(),
			ReplicaVersions:   replicaVersions,
			MaxReplicationLag: maxLag,
		}
	}

	resp := CoordinatorStatusResponse{
		ShardMapVersion: version,
		Nodes:           nodeResps,
		Shards:          shardResps,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

// NotifyLeaderRequest is the body of POST /notify_leader.
type NotifyLeaderRequest struct {
	ShardID  string `json:"shard_id"`
	LeaderID string `json:"leader_id"`
	Term     uint64 `json:"term"`
}

// handleNotifyLeader accepts a distributed election result from a node.
// Returns 200 OK if accepted, 409 Conflict if the term is stale.
func (s *Server) handleNotifyLeader(w http.ResponseWriter, r *http.Request) {
	var req NotifyLeaderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.ShardID == "" || req.LeaderID == "" || req.Term == 0 {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	if !s.leader.NotifyLeader(req.ShardID, req.LeaderID, req.Term) {
		s.m.NotifyLeaderTotal.WithLabelValues("rejected").Inc()
		http.Error(w, "stale term or unknown shard", http.StatusConflict)
		return
	}
	s.m.NotifyLeaderTotal.WithLabelValues("accepted").Inc()
	// SetLeader inside NotifyLeader already incremented the shard map version,
	// so nodes will refetch on their next heartbeat cycle.
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// --- Health monitor ---

func (s *Server) runHealthMonitor(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Coordinator.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed := s.membership.RefreshLiveness()
			if len(changed) > 0 {
				for _, ns := range changed {
					if ns.IsAlive {
						log.Printf("node came alive node_id=%s", ns.ID)
					} else {
						log.Printf("node failure detected node_id=%s", ns.ID)
					}
				}
				reassigned := s.leader.CheckAndReassign()
				for _, shardID := range reassigned {
					s.m.LeaderReassignmentsTotal.WithLabelValues(shardID).Inc()
				}
			}
			// Phase 5: advance any in-progress shard migrations / splits.
			s.migration.CheckMigrations()
			s.refreshMetrics()
		}
	}
}

// refreshMetrics updates coordinator gauges from current cluster state.
// Called on every health monitor tick so metrics stay current.
func (s *Server) refreshMetrics() {
	_, shards := s.shards.Snapshot()
	nodes := s.membership.All()

	var withLeader, quorumAvail float64
	for _, sh := range shards {
		if sh.Leader != "" {
			withLeader++
		}
		alive := 0
		for _, repID := range sh.Replicas {
			for _, ns := range nodes {
				if ns.ID == repID && ns.IsAlive {
					alive++
					break
				}
			}
		}
		if alive >= sh.Quorum() {
			quorumAvail++
		}
		s.m.ShardTerm.WithLabelValues(sh.ID).Set(float64(s.leader.TermForShard(sh.ID)))
	}
	s.m.ShardsTotal.Set(float64(len(shards)))
	s.m.ShardsWithLeader.Set(withLeader)
	s.m.ShardsQuorumAvailable.Set(quorumAvail)

	for _, ns := range nodes {
		v := 0.0
		if ns.IsAlive {
			v = 1.0
		}
		s.m.NodeAlive.WithLabelValues(ns.ID).Set(v)
	}
}

// --- Phase 5: admin handlers ---

// AddNodeRequest is the body of POST /admin/add_node.
type AddNodeRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// handleAdminAddNode registers a new node so it can heartbeat and receive shards.
func (s *Server) handleAdminAddNode(w http.ResponseWriter, r *http.Request) {
	var req AddNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.Address == "" {
		http.Error(w, "node_id and address required", http.StatusBadRequest)
		return
	}
	s.membership.AddNode(config.NodeSpec{ID: req.NodeID, Address: req.Address})
	log.Printf("node registered node_id=%s address=%s", req.NodeID, req.Address)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// MigrateShardRequest is the body of POST /admin/migrate_shard.
type MigrateShardRequest struct {
	ShardID     string   `json:"shard_id"`
	NewReplicas []string `json:"new_replicas"`
}

// handleAdminMigrateShard begins a shard migration to a new replica set.
// The migration is finalised automatically once all new replicas catch up.
func (s *Server) handleAdminMigrateShard(w http.ResponseWriter, r *http.Request) {
	var req MigrateShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.ShardID == "" || len(req.NewReplicas) == 0 {
		http.Error(w, "shard_id and new_replicas required", http.StatusBadRequest)
		return
	}
	if err := s.migration.StartMigration(req.ShardID, req.NewReplicas); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "migrating"})
}

// SplitShardRequest is the body of POST /admin/split_shard.
type SplitShardRequest struct {
	SourceShardID string   `json:"source_shard_id"`
	NewShardID    string   `json:"new_shard_id"`
	NewReplicas   []string `json:"new_replicas"`
}

// handleAdminSplitShard creates a new shard bootstrapped from an existing one.
// The new shard's replicas fetch their initial snapshot from the source shard's
// leader. The source shard continues to serve normally throughout.
func (s *Server) handleAdminSplitShard(w http.ResponseWriter, r *http.Request) {
	var req SplitShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.SourceShardID == "" || req.NewShardID == "" || len(req.NewReplicas) == 0 {
		http.Error(w, "source_shard_id, new_shard_id, and new_replicas required", http.StatusBadRequest)
		return
	}
	// Verify source shard exists.
	if _, err := s.shards.Get(req.SourceShardID); err != nil {
		http.Error(w, "source shard not found", http.StatusBadRequest)
		return
	}
	info := shardmap.ShardInfo{
		ID:                     req.NewShardID,
		Replicas:               req.NewReplicas,
		BootstrapSourceShardID: req.SourceShardID,
	}
	if err := s.shards.AddShard(info); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	// Initialise term for the new shard.
	s.leader.InitTerm(req.NewShardID)
	log.Printf("shard split initiated source=%s new=%s replicas=%v",
		req.SourceShardID, req.NewShardID, req.NewReplicas)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "splitting"})
}
