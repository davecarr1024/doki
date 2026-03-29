//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	commonv1 "github.com/davecarr1024/doki/gen/doki/common/v1"
	coordinatorv1 "github.com/davecarr1024/doki/gen/doki/coordinator/v1"
	nodev1 "github.com/davecarr1024/doki/gen/doki/node/v1"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/davecarr1024/doki/node"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClusterControl struct {
	CoordinatorAddr string
	NodeAddrs       map[string]string
	DataDirs        map[string]string

	coordCancel context.CancelFunc
	nodeCancels map[string]context.CancelFunc
	nodeServers map[string]*node.Server
}

type clusterOptions struct {
	dataDirs          map[string]string
	snapshotInterval  int
	nodeConfig        func(*config.NodeConfig)
	coordConfig       func(*config.CoordinatorConfig)
	replicationConfig func(*config.ReplicationConfig)
}

type ClusterOption func(*clusterOptions)

func WithDataDirs(dirs map[string]string) ClusterOption {
	return func(o *clusterOptions) {
		o.dataDirs = dirs
	}
}

func WithSnapshotInterval(interval int) ClusterOption {
	return func(o *clusterOptions) {
		o.snapshotInterval = interval
	}
}

func WithNodeConfig(fn func(*config.NodeConfig)) ClusterOption {
	return func(o *clusterOptions) {
		o.nodeConfig = fn
	}
}

func WithCoordinatorConfig(fn func(*config.CoordinatorConfig)) ClusterOption {
	return func(o *clusterOptions) {
		o.coordConfig = fn
	}
}

func WithReplicationConfig(fn func(*config.ReplicationConfig)) ClusterOption {
	return func(o *clusterOptions) {
		o.replicationConfig = fn
	}
}

func StartCluster(t *testing.T, nodeIDs []string, shardSpecs []config.ShardSpec, opts ...ClusterOption) *ClusterControl {
	t.Helper()
	options := &clusterOptions{}
	for _, opt := range opts {
		opt(options)
	}

	coordCtx, coordCancel := context.WithCancel(context.Background())
	coordListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen for coordinator")
	coordAddr := coordListener.Addr().String()

	nodeSpecs := make([]config.NodeSpec, len(nodeIDs))
	nodeListeners := make(map[string]net.Listener, len(nodeIDs))
	nodeAddrs := make(map[string]string, len(nodeIDs))

	for i, id := range nodeIDs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err, "listen for node %s", id)
		nodeListeners[id] = l
		nodeAddrs[id] = l.Addr().String()
		nodeSpecs[i] = config.NodeSpec{ID: id, Address: l.Addr().String()}
	}

	clusterCfg := &config.ClusterConfig{
		Coordinator: config.CoordinatorConfig{
			Address:             coordAddr,
			HeartbeatIntervalMs: 100,
			FailureTimeoutMs:    300,
		},
		Nodes:  nodeSpecs,
		Shards: shardSpecs,
		Replication: config.ReplicationConfig{
			QuorumTimeoutMs:     500,
			MaxLagVersions:      1000,
			MaxBufferedVersions: 100,
		},
	}
	clusterCfg.Coordinator.HeartbeatInterval = 100 * time.Millisecond
	clusterCfg.Coordinator.FailureTimeout = 300 * time.Millisecond

	if options.coordConfig != nil {
		options.coordConfig(&clusterCfg.Coordinator)
	}
	if options.replicationConfig != nil {
		options.replicationConfig(&clusterCfg.Replication)
	}

	coordServer := coordinator.NewServer(clusterCfg, clock.Real{})
	require.NoError(t, coordServer.Init())
	go func() {
		if err := coordServer.StartOnListener(coordCtx, coordListener); err != nil {
			t.Logf("coordinator stopped: %v", err)
		}
	}()

	waitForHTTP(t, "http://"+coordAddr+"/ready", 3*time.Second)

	resolvedDirs := make(map[string]string, len(nodeIDs))
	for _, id := range nodeIDs {
		if options.dataDirs != nil {
			if d, ok := options.dataDirs[id]; ok && d != "" {
				resolvedDirs[id] = d
				continue
			}
		}
		resolvedDirs[id] = t.TempDir()
	}

	smResp := fetchShardMapRespGRPC(t, coordAddr)

	nodeCancels := make(map[string]context.CancelFunc, len(nodeIDs))
	nodeServers := make(map[string]*node.Server, len(nodeIDs))

	for _, id := range nodeIDs {
		id := id
		l := nodeListeners[id]
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		nodeCancels[id] = nodeCancel

		nodeCfg := &config.NodeConfig{
			Node:               config.NodeSpec{ID: id, Address: l.Addr().String()},
			CoordinatorAddress: coordAddr,
			DataDir:            resolvedDirs[id],
			HeartbeatInterval:  100 * time.Millisecond,
			QuorumTimeout:      500 * time.Millisecond,
			SnapshotInterval:   options.snapshotInterval,
		}
		if options.nodeConfig != nil {
			options.nodeConfig(nodeCfg)
		}
		nodeCfg.EnsureDefaults()

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
		waitForHTTP(t, "http://"+addr+"/ready", 3*time.Second)
		t.Logf("node %s ready at %s", id, addr)
	}

	cc := &ClusterControl{
		CoordinatorAddr: coordAddr,
		NodeAddrs:       nodeAddrs,
		DataDirs:        resolvedDirs,
		coordCancel:     coordCancel,
		nodeCancels:     nodeCancels,
		nodeServers:     nodeServers,
	}
	t.Cleanup(func() { cc.StopAll(t) })
	return cc
}

func (c *ClusterControl) StopNode(t *testing.T, id string) {
	t.Helper()
	if cancel, ok := c.nodeCancels[id]; ok {
		cancel()
		delete(c.nodeCancels, id)
	}
	if srv, ok := c.nodeServers[id]; ok {
		srv.Close()
	}
}

func (c *ClusterControl) StopAll(t *testing.T) {
	t.Helper()
	for _, cancel := range c.nodeCancels {
		cancel()
	}
	c.nodeCancels = map[string]context.CancelFunc{}
	if c.coordCancel != nil {
		c.coordCancel()
	}
	for _, srv := range c.nodeServers {
		srv.Close()
	}
}

func (c *ClusterControl) StopCoordinator(t *testing.T) {
	t.Helper()
	if c.coordCancel != nil {
		c.coordCancel()
		c.coordCancel = nil
	}
}

func dialGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = conn.Close() })
			return conn
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	return nil
}

func coordinatorClient(t *testing.T, addr string) coordinatorv1.CoordinatorServiceClient {
	return coordinatorv1.NewCoordinatorServiceClient(dialGRPC(t, addr))
}

func nodeClient(t *testing.T, addr string) nodev1.NodeServiceClient {
	return nodev1.NewNodeServiceClient(dialGRPC(t, addr))
}

func leaderAddrFor(t *testing.T, coordAddr, shardID string) string {
	t.Helper()
	client := coordinatorClient(t, coordAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.WhereIsLeader(ctx, &coordinatorv1.WhereIsLeaderRequest{ShardId: shardID})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Address, "coordinator should know the leader's address")
	return resp.Address
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func fetchShardMapRespGRPC(t *testing.T, coordAddr string) coordinator.ShardMapResponse {
	t.Helper()
	client := coordinatorClient(t, coordAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetShardMap(ctx, &coordinatorv1.GetShardMapRequest{})
	require.NoError(t, err)
	if resp.ShardMap == nil {
		return coordinator.ShardMapResponse{}
	}
	return coordinator.ShardMapResponse{
		Version:       resp.ShardMap.Version,
		Shards:        shardInfoFromProto(resp.ShardMap.Shards),
		NodeAddresses: resp.NodeAddresses,
	}
}

func shardInfoFromProto(shards []*commonv1.ShardInfo) []shardmap.ShardInfo {
	if len(shards) == 0 {
		return nil
	}
	out := make([]shardmap.ShardInfo, 0, len(shards))
	for _, sh := range shards {
		out = append(out, shardmap.ShardInfo{
			ID:       sh.ShardId,
			Leader:   sh.Leader,
			Replicas: append([]string(nil), sh.Replicas...),
		})
	}
	return out
}

func coordinatorStatus(t *testing.T, addr string) *coordinatorv1.GetStatusResponse {
	t.Helper()
	client := coordinatorClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetStatus(ctx, &coordinatorv1.GetStatusRequest{})
	require.NoError(t, err)
	return resp
}

func nodeStatus(t *testing.T, addr string) *nodev1.GetStatusResponse {
	t.Helper()
	client := nodeClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetStatus(ctx, &nodev1.GetStatusRequest{})
	require.NoError(t, err)
	return resp
}

func shardMapResponse(t *testing.T, addr string) *coordinatorv1.GetShardMapResponse {
	t.Helper()
	client := coordinatorClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetShardMap(ctx, &coordinatorv1.GetShardMapRequest{})
	require.NoError(t, err)
	return resp
}
