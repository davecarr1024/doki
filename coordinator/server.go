package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
)

// Server is the coordinator's HTTP server.
//
// Phase 1 endpoints:
//   - POST /heartbeat      — nodes report liveness and shard versions
//   - GET  /shardmap       — returns shard map + node addresses
//   - GET  /status         — cluster health overview
//   - GET  /ready          — readiness probe
//   - GET  /leader/{shard} — direct leader lookup for a shard
type Server struct {
	cfg        *config.ClusterConfig
	membership *Membership
	shards     *shardmap.ShardMap
	leader     *LeaderManager
	clock      clock.Clock
	httpServer *http.Server
	ready      bool
}

// NewServer creates a coordinator Server from the given cluster config.
func NewServer(cfg *config.ClusterConfig, clk clock.Clock) *Server {
	membership := NewMembership(cfg.Nodes, cfg.Coordinator.FailureTimeout, clk)
	shards := shardmap.New()
	leader := NewLeaderManager(shards, membership)
	return &Server{
		cfg:        cfg,
		membership: membership,
		shards:     shards,
		leader:     leader,
		clock:      clk,
	}
}

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
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{Addr: s.cfg.Coordinator.Address, Handler: mux}
	go s.runHealthMonitor(ctx)
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()
	log.Printf("coordinator listening address=%s", s.cfg.Coordinator.Address)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("coordinator http server: %w", err)
	}
	return nil
}

// StartOnListener starts the server on the provided net.Listener (used in tests).
func (s *Server) StartOnListener(ctx context.Context, l net.Listener) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{Handler: mux}
	go s.runHealthMonitor(ctx)
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()
	log.Printf("coordinator listening address=%s", l.Addr())
	if err := s.httpServer.Serve(l); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("coordinator http server: %w", err)
	}
	return nil
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /heartbeat", s.handleHeartbeat)
	mux.HandleFunc("GET /shardmap", s.handleShardMap)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /leader/{shard_id}", s.handleLeaderQuery)
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
	smVersion, _ := s.shards.Snapshot()
	resp := HeartbeatResponse{ShardMapVersion: smVersion}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ShardMapResponse is the JSON body returned by GET /shardmap.
// NodeAddresses maps node IDs to their HTTP addresses so nodes can
// contact each other for replication and recovery.
type ShardMapResponse struct {
	Version       uint64            `json:"version"`
	Shards        []shardmap.ShardInfo `json:"shards"`
	NodeAddresses map[string]string `json:"node_addresses"`
}

func (s *Server) handleShardMap(w http.ResponseWriter, r *http.Request) {
	resp := s.buildShardMapResponse()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) buildShardMapResponse() ShardMapResponse {
	version, shards := s.shards.Snapshot()
	nodeAddrs := make(map[string]string, len(s.cfg.Nodes))
	for _, n := range s.cfg.Nodes {
		nodeAddrs[n.ID] = n.Address
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

// CoordinatorStatusResponse is the full body of GET /status.
type CoordinatorStatusResponse struct {
	ShardMapVersion uint64               `json:"shard_map_version"`
	Nodes           []NodeStatusResponse `json:"nodes"`
	Shards          []shardmap.ShardInfo `json:"shards"`
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
	resp := CoordinatorStatusResponse{
		ShardMapVersion: version,
		Nodes:           nodeResps,
		Shards:          shards,
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
				s.leader.CheckAndReassign()
			}
		}
	}
}
