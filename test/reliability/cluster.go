//go:build reliability

// Package reliability provides a fault-injecting cluster harness for chaos,
// load, and stability testing.
//
// Build tag: reliability
// Run with: go test ./test/reliability/... -tags=reliability -v -timeout=120s
package reliability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/require"
)

// faultTransport is an http.RoundTripper that can inject latency or drop
// requests for a configurable set of target addresses.
type faultTransport struct {
	mu       sync.RWMutex
	slow     map[string]time.Duration // addr → extra latency
	dropped  map[string]bool          // addr → drop all requests
	inner    http.RoundTripper
}

func newFaultTransport() *faultTransport {
	return &faultTransport{
		slow:    make(map[string]time.Duration),
		dropped: make(map[string]bool),
		inner:   http.DefaultTransport,
	}
}

func (ft *faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Host

	ft.mu.RLock()
	drop := ft.dropped[host]
	latency := ft.slow[host]
	ft.mu.RUnlock()

	if drop {
		return nil, fmt.Errorf("connection refused (fault-injected partition): %s", host)
	}
	if latency > 0 {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(latency):
		}
	}
	return ft.inner.RoundTrip(req)
}

func (ft *faultTransport) setLatency(addr string, d time.Duration) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if d == 0 {
		delete(ft.slow, addr)
	} else {
		ft.slow[addr] = d
	}
}

func (ft *faultTransport) setDropped(addr string, drop bool) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if !drop {
		delete(ft.dropped, addr)
	} else {
		ft.dropped[addr] = true
	}
}

// reliabilityCluster is a cluster harness for reliability and chaos tests.
// It supports per-node start/stop and fault injection via faultTransport.
type reliabilityCluster struct {
	t               *testing.T
	CoordinatorAddr string
	NodeAddrs       map[string]string // node_id → address
	NodeIDs         []string

	coordCancel context.CancelFunc
	nodeCancels map[string]context.CancelFunc
	nodeServers map[string]*node.Server
	nodeCfgs    map[string]*config.NodeConfig
	clusterCfg  *config.ClusterConfig

	ft *faultTransport

	// httpClient is shared across helpers; uses the fault transport.
	httpClient *http.Client
}

// startReliabilityCluster starts a full cluster with fast election timeouts,
// returning a harness with fault injection support.
func startReliabilityCluster(t *testing.T, nodeIDs []string, shardSpecs []config.ShardSpec) *reliabilityCluster {
	t.Helper()

	ft := newFaultTransport()
	httpClient := &http.Client{Transport: ft, Timeout: 2 * time.Second}

	coordCtx, coordCancel := context.WithCancel(context.Background())
	coordListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	coordAddr := coordListener.Addr().String()

	nodeSpecs := make([]config.NodeSpec, len(nodeIDs))
	nodeListeners := make(map[string]net.Listener, len(nodeIDs))
	nodeAddrs := make(map[string]string, len(nodeIDs))
	for i, id := range nodeIDs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		nodeListeners[id] = l
		nodeAddrs[id] = l.Addr().String()
		nodeSpecs[i] = config.NodeSpec{ID: id, Address: l.Addr().String()}
	}

	clusterCfg := &config.ClusterConfig{
		Coordinator: config.CoordinatorConfig{
			Address:             coordAddr,
			HeartbeatIntervalMs: 100,
			FailureTimeoutMs:    400,
		},
		Nodes:  nodeSpecs,
		Shards: shardSpecs,
		Replication: config.ReplicationConfig{
			QuorumTimeoutMs:     400,
			MaxLagVersions:      1000,
			MaxBufferedVersions: 100,
		},
	}
	clusterCfg.Coordinator.HeartbeatInterval = 100 * time.Millisecond
	clusterCfg.Coordinator.FailureTimeout = 400 * time.Millisecond

	coordServer := coordinator.NewServer(clusterCfg, clock.Real{})
	require.NoError(t, coordServer.Init())
	go func() {
		if err := coordServer.StartOnListener(coordCtx, coordListener); err != nil {
			t.Logf("coordinator stopped: %v", err)
		}
	}()
	waitForReady(t, httpClient, "http://"+coordAddr+"/ready", 5*time.Second)

	nodeCancels := make(map[string]context.CancelFunc, len(nodeIDs))
	nodeServers := make(map[string]*node.Server, len(nodeIDs))
	nodeCfgs := make(map[string]*config.NodeConfig, len(nodeIDs))

	for _, id := range nodeIDs {
		id := id
		l := nodeListeners[id]
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		nodeCancels[id] = nodeCancel

		nodeCfg := &config.NodeConfig{
			Node:                 config.NodeSpec{ID: id, Address: l.Addr().String()},
			CoordinatorAddress:   coordAddr,
			DataDir:              t.TempDir(),
			HeartbeatIntervalMs:  100,
			HeartbeatInterval:    100 * time.Millisecond,
			QuorumTimeoutMs:      400,
			QuorumTimeout:        400 * time.Millisecond,
			ElectionTimeoutMinMs: 300,
			ElectionTimeoutMaxMs: 600,
			LeaderHeartbeatMs:    100,
		}
		nodeCfg.EnsureDefaults()
		nodeCfgs[id] = nodeCfg

		smResp := fetchShardMap(t, httpClient, "http://"+coordAddr+"/shardmap")
		srv := node.NewServer(nodeCfg, clock.Real{})
		srv.InitShards(smResp)
		nodeServers[id] = srv

		go func() {
			if err := srv.StartOnListener(nodeCtx, l); err != nil {
				t.Logf("node %s stopped: %v", id, err)
			}
		}()
	}

	for id, addr := range nodeAddrs {
		waitForReady(t, httpClient, "http://"+addr+"/ready", 5*time.Second)
		t.Logf("node %s ready at %s", id, addr)
	}

	rc := &reliabilityCluster{
		t:               t,
		CoordinatorAddr: coordAddr,
		NodeAddrs:       nodeAddrs,
		NodeIDs:         nodeIDs,
		coordCancel:     coordCancel,
		nodeCancels:     nodeCancels,
		nodeServers:     nodeServers,
		nodeCfgs:        nodeCfgs,
		clusterCfg:      clusterCfg,
		ft:              ft,
		httpClient:      httpClient,
	}
	t.Cleanup(rc.StopAll)
	return rc
}

// StopNode stops a single node.
func (rc *reliabilityCluster) StopNode(id string) {
	if cancel, ok := rc.nodeCancels[id]; ok {
		cancel()
		delete(rc.nodeCancels, id)
	}
}

// StopCoordinator stops the coordinator.
func (rc *reliabilityCluster) StopCoordinator() {
	rc.coordCancel()
}

// StopAll stops everything.
func (rc *reliabilityCluster) StopAll() {
	for _, cancel := range rc.nodeCancels {
		cancel()
	}
	rc.coordCancel()
}

// SlowNode adds artificial latency to requests destined for nodeID.
func (rc *reliabilityCluster) SlowNode(id string, latency time.Duration) {
	addr, ok := rc.NodeAddrs[id]
	if !ok {
		return
	}
	rc.ft.setLatency(addr, latency)
}

// HealNode removes any injected latency or partition for nodeID.
func (rc *reliabilityCluster) HealNode(id string) {
	addr, ok := rc.NodeAddrs[id]
	if !ok {
		return
	}
	rc.ft.setLatency(addr, 0)
	rc.ft.setDropped(addr, false)
}

// PartitionNode causes all outbound HTTP requests to nodeID to fail with a
// connection-refused error (simulates a network partition).
func (rc *reliabilityCluster) PartitionNode(id string) {
	addr, ok := rc.NodeAddrs[id]
	if !ok {
		return
	}
	rc.ft.setDropped(addr, true)
}

// HealPartition removes the partition for nodeID.
func (rc *reliabilityCluster) HealPartition(id string) {
	rc.HealNode(id)
}

// CurrentLeader polls the coordinator for the leader of shardID and returns
// it once found, or fails the test after timeout.
func (rc *reliabilityCluster) CurrentLeader(shardID string, timeout time.Duration) coordinator.LeaderQueryResponse {
	rc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lr coordinator.LeaderQueryResponse
		if getJSONDecoded(rc.t, rc.httpClient, "http://"+rc.CoordinatorAddr+"/leader/"+shardID, &lr) && lr.Address != "" {
			return lr
		}
		time.Sleep(100 * time.Millisecond)
	}
	rc.t.Fatalf("no leader found for shard %s within %s", shardID, timeout)
	return coordinator.LeaderQueryResponse{}
}

// WaitForLeaderChange polls until a different leader than prevAddr is seen,
// or fails after timeout.
func (rc *reliabilityCluster) WaitForLeaderChange(shardID, prevAddr string, timeout time.Duration) coordinator.LeaderQueryResponse {
	rc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lr coordinator.LeaderQueryResponse
		if getJSONDecoded(rc.t, rc.httpClient, "http://"+rc.CoordinatorAddr+"/leader/"+shardID, &lr) &&
			lr.Address != "" && lr.Address != prevAddr {
			return lr
		}
		time.Sleep(100 * time.Millisecond)
	}
	rc.t.Fatalf("leader did not change from %s for shard %s within %s", prevAddr, shardID, timeout)
	return coordinator.LeaderQueryResponse{}
}

// PutKV sends a put to the given node address. Returns true on success.
func (rc *reliabilityCluster) PutKV(nodeAddr, shardID, key, value string) bool {
	req := node.KVRequest{Op: "put", Key: key, Value: value}
	body, err := json.Marshal(req)
	if err != nil {
		return false
	}
	resp, err := rc.httpClient.Post(
		"http://"+nodeAddr+"/kv/"+shardID,
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	var kvr node.KVResponse
	if err := json.NewDecoder(resp.Body).Decode(&kvr); err != nil {
		return false
	}
	return resp.StatusCode == http.StatusOK && kvr.OK
}

// GetKV sends a get to the given node address. Returns (value, found).
func (rc *reliabilityCluster) GetKV(nodeAddr, shardID, key string) (string, bool) {
	req := node.KVRequest{Op: "get", Key: key}
	body, err := json.Marshal(req)
	if err != nil {
		return "", false
	}
	resp, err := rc.httpClient.Post(
		"http://"+nodeAddr+"/kv/"+shardID,
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()
	var kvr node.KVResponse
	if err := json.NewDecoder(resp.Body).Decode(&kvr); err != nil {
		return "", false
	}
	return kvr.Value, resp.StatusCode == http.StatusOK && kvr.OK
}

// NodeShardRole returns the role this node reports for the given shard.
func (rc *reliabilityCluster) NodeShardRole(addr, shardID string) node.Role {
	resp, err := rc.httpClient.Get("http://" + addr + "/status")
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	var status node.NodeStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return ""
	}
	for _, s := range status.Shards {
		if s.ShardID == shardID {
			return s.Role
		}
	}
	return ""
}

// AllNodeAddrs returns all node addresses (not coordinator) that are still
// running.
func (rc *reliabilityCluster) AllNodeAddrs() []string {
	addrs := make([]string, 0, len(rc.NodeAddrs))
	for _, addr := range rc.NodeAddrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

// --- Write Tracker ---

// WriteTracker records put operations for durability verification.
type WriteTracker struct {
	mu      sync.Mutex
	writes  map[string]string // key → value
	success atomic.Int64
	fail    atomic.Int64
}

func newWriteTracker() *WriteTracker {
	return &WriteTracker{writes: make(map[string]string)}
}

// Record stores a successful write.
func (wt *WriteTracker) Record(key, value string) {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	wt.writes[key] = value
}

// Committed returns the map of committed key→value pairs.
func (wt *WriteTracker) Committed() map[string]string {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	out := make(map[string]string, len(wt.writes))
	for k, v := range wt.writes {
		out[k] = v
	}
	return out
}

// Successes returns the number of successful writes.
func (wt *WriteTracker) Successes() int64 { return wt.success.Load() }

// Failures returns the number of failed writes.
func (wt *WriteTracker) Failures() int64 { return wt.fail.Load() }

// --- Helpers ---

func waitForReady(t *testing.T, client *http.Client, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not become ready within %s: %s", timeout, url)
}

func fetchShardMap(t *testing.T, client *http.Client, url string) coordinator.ShardMapResponse {
	t.Helper()
	var sm coordinator.ShardMapResponse
	require.True(t, getJSONDecoded(t, client, url, &sm))
	return sm
}

func getJSONDecoded(t *testing.T, client *http.Client, url string, out any) bool {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return false
	}
	return true
}
