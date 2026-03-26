// Package metrics defines all Prometheus metrics for Doki.
//
// Each server (coordinator, node) creates its own Registry so that multiple
// servers can coexist in the same process during tests without label collisions.
// Pass the registry to Server constructors; in tests use
// github.com/prometheus/client_golang/prometheus/testutil.ToFloat64 to read
// metric values without starting an HTTP scraper.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Coordinator metrics ---

// CoordinatorMetrics holds all Prometheus metrics for a coordinator instance.
type CoordinatorMetrics struct {
	Registry *prometheus.Registry

	// ShardsTotal is the total number of shards known to the coordinator.
	ShardsTotal prometheus.Gauge

	// ShardsWithLeader is the number of shards that currently have an elected leader.
	ShardsWithLeader prometheus.Gauge

	// ShardsQuorumAvailable is the number of shards where a majority of replicas are alive.
	ShardsQuorumAvailable prometheus.Gauge

	// ShardTerm is the current leader term per shard.
	ShardTerm *prometheus.GaugeVec

	// NodeAlive is 1 if the node is considered alive by the coordinator, 0 if dead.
	NodeAlive *prometheus.GaugeVec

	// HeartbeatReceivedTotal counts coordinator heartbeats received per node.
	HeartbeatReceivedTotal *prometheus.CounterVec

	// LeaderReassignmentsTotal counts coordinator-initiated leader reassignments per shard.
	LeaderReassignmentsTotal *prometheus.CounterVec

	// NotifyLeaderTotal counts /notify_leader calls by result ("accepted" or "rejected").
	NotifyLeaderTotal *prometheus.CounterVec
}

// NewCoordinatorMetrics creates and registers all coordinator metrics in a fresh registry.
func NewCoordinatorMetrics() *CoordinatorMetrics {
	reg := prometheus.NewRegistry()
	m := &CoordinatorMetrics{
		Registry: reg,
		ShardsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "doki_shards_total",
			Help: "Total number of shards in the cluster.",
		}),
		ShardsWithLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "doki_shards_with_leader",
			Help: "Number of shards that have an elected leader.",
		}),
		ShardsQuorumAvailable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "doki_shards_quorum_available",
			Help: "Number of shards where a majority of replicas are alive.",
		}),
		ShardTerm: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_shard_term",
			Help: "Current leader term for a shard.",
		}, []string{"shard_id"}),
		NodeAlive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_node_alive",
			Help: "1 if the node is alive according to the coordinator, 0 otherwise.",
		}, []string{"node_id"}),
		HeartbeatReceivedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_heartbeat_received_total",
			Help: "Number of coordinator heartbeats received from each node.",
		}, []string{"node_id"}),
		LeaderReassignmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_leader_reassignments_total",
			Help: "Number of coordinator-initiated leader reassignments per shard.",
		}, []string{"shard_id"}),
		NotifyLeaderTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_notify_leader_total",
			Help: "Number of /notify_leader calls by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(
		m.ShardsTotal,
		m.ShardsWithLeader,
		m.ShardsQuorumAvailable,
		m.ShardTerm,
		m.NodeAlive,
		m.HeartbeatReceivedTotal,
		m.LeaderReassignmentsTotal,
		m.NotifyLeaderTotal,
	)
	return m
}

// Handler returns an HTTP handler that serves the Prometheus /metrics page.
func (m *CoordinatorMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// --- Node metrics ---

// NodeMetrics holds all Prometheus metrics for a node instance.
type NodeMetrics struct {
	Registry *prometheus.Registry

	// ReplicaVersion is the current committed version for each replica.
	ReplicaVersion *prometheus.GaugeVec

	// ReplicaTerm is the current term for each replica.
	ReplicaTerm *prometheus.GaugeVec

	// ReplicaIsLeader is 1 if the replica is currently the leader, 0 otherwise.
	ReplicaIsLeader *prometheus.GaugeVec

	// WritesTotal counts KV write operations by result.
	// result labels: "ok", "not_leader", "quorum_unavailable", "error"
	WritesTotal *prometheus.CounterVec

	// WriteDuration measures write latency (leader path, seconds).
	WriteDuration *prometheus.HistogramVec

	// ReplicationsTotal counts incoming replication operations by result.
	// result labels: "ok", "stale_term", "wal_error", "duplicate"
	ReplicationsTotal *prometheus.CounterVec

	// ElectionsTotal counts elections by result.
	// result labels: "won", "lost", "stepped_down"
	ElectionsTotal *prometheus.CounterVec

	// LeaderHeartbeatsSentTotal counts outbound leader heartbeats.
	LeaderHeartbeatsSentTotal *prometheus.CounterVec

	// LeaderHeartbeatsMissedTotal counts heartbeats with no peer response.
	LeaderHeartbeatsMissedTotal *prometheus.CounterVec

	// RecoveriesTotal counts recovery events by type.
	// recovery_type labels: "incremental", "snapshot"
	RecoveriesTotal *prometheus.CounterVec

	// LastLeaderContactSeconds is the age in seconds of the last received
	// leader message for each replica; updated on every heartbeat scrape cycle.
	LastLeaderContactSeconds *prometheus.GaugeVec
}

// NewNodeMetrics creates and registers all node metrics in a fresh registry.
func NewNodeMetrics() *NodeMetrics {
	reg := prometheus.NewRegistry()
	m := &NodeMetrics{
		Registry: reg,
		ReplicaVersion: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_replica_version",
			Help: "Current committed version for a shard replica.",
		}, []string{"shard_id", "node_id"}),
		ReplicaTerm: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_replica_term",
			Help: "Current term for a shard replica.",
		}, []string{"shard_id", "node_id"}),
		ReplicaIsLeader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_replica_is_leader",
			Help: "1 if this replica is the current leader for its shard.",
		}, []string{"shard_id", "node_id"}),
		WritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_writes_total",
			Help: "Total KV write operations by result.",
		}, []string{"shard_id", "result"}),
		WriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "doki_write_duration_seconds",
			Help:    "Latency of leader write operations.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"shard_id"}),
		ReplicationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_replications_total",
			Help: "Total incoming replication operations by result.",
		}, []string{"shard_id", "result"}),
		ElectionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_elections_total",
			Help: "Total elections by result.",
		}, []string{"shard_id", "result"}),
		LeaderHeartbeatsSentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_leader_heartbeats_sent_total",
			Help: "Total leader heartbeats sent to peers.",
		}, []string{"shard_id"}),
		LeaderHeartbeatsMissedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_leader_heartbeats_missed_total",
			Help: "Total leader heartbeats with no peer response.",
		}, []string{"shard_id"}),
		RecoveriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "doki_recoveries_total",
			Help: "Total recovery events by type.",
		}, []string{"shard_id", "recovery_type"}),
		LastLeaderContactSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "doki_last_leader_contact_seconds",
			Help: "Age in seconds of the last received leader message per replica.",
		}, []string{"shard_id", "node_id"}),
	}
	reg.MustRegister(
		m.ReplicaVersion,
		m.ReplicaTerm,
		m.ReplicaIsLeader,
		m.WritesTotal,
		m.WriteDuration,
		m.ReplicationsTotal,
		m.ElectionsTotal,
		m.LeaderHeartbeatsSentTotal,
		m.LeaderHeartbeatsMissedTotal,
		m.RecoveriesTotal,
		m.LastLeaderContactSeconds,
	)
	return m
}

// Handler returns an HTTP handler that serves the Prometheus /metrics page.
func (m *NodeMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
