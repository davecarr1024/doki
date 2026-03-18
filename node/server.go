package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
)

// Server is a leaf node's HTTP server.
//
// It exposes:
//   - GET  /status  — node and shard replica health
//   - GET  /ready   — readiness check
//
// In Phase 0, the node only heartbeats to the coordinator and exposes health
// endpoints. KV operations and replication are added in Phase 1.
type Server struct {
	cfg        *config.NodeConfig
	clock      clock.Clock
	replicas   map[string]*ReplicaState // shard_id → replica
	mu         sync.RWMutex
	startedAt  time.Time
	httpServer *http.Server
}

// NewServer creates a node Server from the given config.
func NewServer(cfg *config.NodeConfig, clk clock.Clock) *Server {
	return &Server{
		cfg:       cfg,
		clock:     clk,
		replicas:  make(map[string]*ReplicaState),
		startedAt: clk.Now(),
	}
}

// InitShards creates replica state for each shard this node participates in,
// using the shard map provided by the coordinator.
func (s *Server) InitShards(shards []shardmap.ShardInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, shard := range shards {
		if !shard.HasReplica(s.cfg.Node.ID) {
			continue
		}
		peers := make([]string, 0, len(shard.Replicas)-1)
		for _, r := range shard.Replicas {
			if r != s.cfg.Node.ID {
				peers = append(peers, r)
			}
		}
		r := NewReplicaState(shard.ID, s.cfg.Node.ID, peers)
		// In Phase 0, mark replicas as ready immediately (no real recovery yet)
		r.IsReady = true
		if shard.Leader == s.cfg.Node.ID {
			r.Role = RoleLeader
		}
		r.LeaderID = shard.Leader
		s.replicas[shard.ID] = r
		log.Printf("shard initialized shard_id=%s role=%s", shard.ID, r.Role)
	}
}

// Start begins serving HTTP and runs the heartbeat loop in the background.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.httpServer = &http.Server{
		Addr:    s.cfg.Node.Address,
		Handler: mux,
	}

	go s.runHeartbeat(ctx)

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

	go s.runHeartbeat(ctx)

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

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /ready", s.handleReady)
}

// --- Handlers ---

// NodeStatusResponse is the full body of GET /status.
type NodeStatusResponse struct {
	NodeID        string                   `json:"node_id"`
	UptimeSeconds float64                  `json:"uptime_seconds"`
	Shards        []ReplicaStatusSnapshot  `json:"shards"`
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
		snap := rep.StatusSnapshot()
		if !snap.IsReady {
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

// --- Heartbeat ---

func (s *Server) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	// Send one immediately on startup
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("heartbeat rejected node_id=%s status=%d", s.cfg.Node.ID, resp.StatusCode)
	}
}
