# Doki — Reliability Engineering

This document defines Doki's reliability strategy: the signals that indicate a
healthy cluster, how to measure them, how to stress-test the system, and how
reliability work integrates into the normal feature development cycle.

The goal is not to achieve five-nines uptime on a learning database. The goal
is to learn how to _reason_ about distributed system reliability — to make
invariants visible, observable, and automatically checked, so that regressions
surface immediately rather than during the next phase of development.

---

## 1. Architectural Health Signals

A "healthy" cluster is one where all five core invariants hold continuously
(see `docs/testing.md` and `CLAUDE.md`). Each invariant maps to one or more
measurable signals:

### Signal Definitions

| Signal | Description | Healthy value |
|--------|-------------|---------------|
| **leader_count** | Number of shards with an elected leader | == total shards |
| **quorum_available** | Number of shards where ≥ majority of replicas are alive and ready | == total shards |
| **replication_lag** | Max version delta between leader and any alive follower, per shard | 0 under steady state; < 100 under load |
| **election_rate** | Elections per minute per shard | 0 under steady state; spikes are expected after failures |
| **write_success_rate** | Fraction of writes that return `OK` | 1.0 under normal conditions; > 0.5 during single-node failure |
| **write_latency_p99** | 99th percentile write latency | < 50ms steady state; < 500ms during recovery |
| **heartbeat_success_rate** | Fraction of coordinator heartbeats that succeed | 1.0 normally |
| **recovery_events_total** | Cumulative count of full-snapshot recoveries (expensive) | Low; incremental should dominate |
| **split_brain_detected** | Any moment where 2+ nodes claim LEADER for same (shard, term) | Always 0 |

**Split-brain detection** is computed by polling all nodes' `/status` endpoints
and checking for duplicate (shard_id, term, role=LEADER) tuples. It must
always be zero.

These signals define the **system health scorecard** used in all reliability
tests and during manual investigation.

---

## 2. Observability

### 2a. Status Endpoint Enhancements

The existing `/status` endpoints expose per-shard role, version, term, and
ready state. The following additions will make them more useful for reliability
work:

**Coordinator `/status` additions:**
```json
{
  "shard_map_version": 5,
  "shards": [{
    "shard_id": "shard-0",
    "leader": "node-a",
    "term": 3,
    "quorum_alive": true,
    "replica_versions": {"node-a": 42, "node-b": 41, "node-c": 42},
    "max_replication_lag": 1
  }],
  "nodes": [...]
}
```

**Node `/status` additions:**
```json
{
  "node_id": "node-a",
  "shards": [{
    "shard_id": "shard-0",
    "role": "LEADER",
    "term": 3,
    "version": 42,
    "is_ready": true,
    "last_leader_contact_ms": 12,
    "election_count": 1,
    "recovery_count": 0,
    "write_ops_total": 150,
    "write_errors_total": 0
  }]
}
```

### 2b. Prometheus Metrics

A `GET /metrics` endpoint (Prometheus text format) is exposed on both
coordinator and node servers. It uses `prometheus/client_golang`.

**Coordinator metrics:**

```
doki_shards_total                    # Gauge: number of shards in cluster
doki_shards_with_leader              # Gauge: shards with an elected leader
doki_shards_quorum_available         # Gauge: shards with quorum alive
doki_shard_term{shard_id}            # Gauge: current term per shard
doki_node_alive{node_id}             # Gauge: 1 if node is alive
doki_heartbeat_received_total{node_id}  # Counter
```

**Node metrics:**

```
doki_replica_version{shard_id,node_id}          # Gauge: current version
doki_replica_term{shard_id,node_id}             # Gauge: current term
doki_replica_is_leader{shard_id,node_id}        # Gauge: leader flag
doki_writes_total{shard_id,result}              # Counter: result=ok|quorum_unavailable|not_leader
doki_write_duration_seconds{shard_id}        # Histogram
doki_replications_total{shard_id,result}     # Counter: follower replication ops
doki_elections_total{shard_id,result}        # Counter: result=won|lost
doki_leader_heartbeats_sent_total{shard_id}  # Counter: outgoing leader heartbeats
doki_leader_heartbeats_missed_total{shard_id} # Counter: heartbeats that timed out
doki_recoveries_total{shard_id,recovery_type}   # Counter: type=incremental|snapshot
doki_last_leader_contact_seconds{shard_id,node_id} # Gauge: age of last leader contact
```

**Implementation note:** Use a global `prometheus.Registry` per process,
injected into `Server` at construction. Tests can assert against metric values
by reading the registry directly — no HTTP scraping required in tests.

### 2c. Metrics in Tests

The test harness can assert health signals directly from the in-memory
Prometheus registry:

```go
// After a scenario: assert no write errors occurred
assert.Equal(t, 0.0, metricValue(nodeReg, "doki_writes_total", Labels{"result":"quorum_unavailable"}))

// Assert split-brain never occurred
assertNoSplitBrain(t, cluster)
```

For interactive investigation, a local Prometheus + Grafana stack can be
started via `make observe` (Docker Compose). This is optional; all reliability
tests work without it.

---

## 3. Chaos Testing

Chaos tests inject failures into a running cluster and verify that health
signals remain within acceptable bounds during and after recovery.

### 3a. Failure Injection Primitives

The in-process test cluster already supports node cancellation (Phase 4 election
tests). The full set of primitives planned:

| Primitive | Mechanism | Simulates |
|-----------|-----------|-----------|
| `StopNode(id)` | Cancel node's context | Clean shutdown / crash |
| `SlowNode(id, latency)` | Fault-injecting HTTP transport | Network degradation |
| `PartitionNode(id)` | Block all HTTP to/from a node | Network partition |
| `HealPartition(id)` | Remove the block | Partition healed |
| `StopCoordinator()` | Cancel coordinator's context | Coordinator failure |
| `DelayHeartbeats(d)` | Sleep in heartbeat sender | Clock skew / heartbeat delay |

`SlowNode` and `PartitionNode` require a **fault-injecting HTTP transport**:
a `http.RoundTripper` that wraps the default transport and intercepts calls
to specific node addresses. Injected into the node's HTTP client at startup.
This is simpler than Toxiproxy and requires no external processes.

### 3b. Chaos Test Scenarios

**Scenario 1: Single leader failure**
1. Write N keys, verify all succeed
2. Kill the leader
3. Wait for re-election (max 3s)
4. Verify all N keys still readable from new leader
5. Assert: no split-brain, write_success_rate recovers to 1.0

**Scenario 2: Minority follower failure**
1. 3-node shard; kill one follower
2. Write 100 keys; expect all OK (quorum = 2, still met by leader + 1)
3. Kill another follower; write one key; expect QUORUM_UNAVAILABLE
4. Restart one follower; wait for recovery; write succeeds again
5. Assert: follower converges to leader state

**Scenario 3: Coordinator failure**
1. Write some keys
2. Kill coordinator
3. Write more keys (should still work via existing shard map)
4. Kill leader; election should proceed without coordinator
5. Assert: new leader elected, writes resume; coordinator absence doesn't cause split-brain

**Scenario 4: Rolling restart**
1. Write continuously in a background goroutine
2. Kill and restart each node in sequence (one at a time)
3. Wait for full recovery after each restart
4. Assert: no committed write was lost; error rate < 5% during restarts

**Scenario 5: Slow follower**
1. Slow one follower's network to 300ms latency
2. Write 50 keys at rate of 10/s
3. Assert: writes succeed (quorum is met by leader + other follower)
4. Remove slowness; assert: slow follower catches up via incremental recovery

**Scenario 6: Simultaneous leader and follower failure** *(stretch goal)*
1. 5-node shard; kill 2 nodes simultaneously (leader + one follower)
2. Quorum is 3; 3 remain → election is possible
3. Assert: new leader elected; no data loss

### 3c. Randomised Chaos (Monkey Mode)

A `ChaosMonkey` runs in the background during a test, randomly picking failure
events from a weighted schedule:

```go
type ChaosMonkey struct {
    Cluster    *ElectionCluster
    Seed       int64          // for reproducibility
    MinPause   time.Duration  // minimum time between events
    MaxPause   time.Duration  // maximum time between events
    Events     []ChaosEvent   // weighted event list
}

type ChaosEvent struct {
    Weight  int
    Apply   func(c *ElectionCluster)
    Recover func(c *ElectionCluster)
}
```

Seed-based randomness makes failures reproducible: a failing seed can be
committed as a regression test with a fixed event sequence.

---

## 4. Load Testing

Load tests verify that the cluster operates correctly under sustained write
throughput, and establish a performance baseline for regression tracking.

### 4a. Load Generator

A simple in-process load generator:

```go
type LoadGen struct {
    NodeAddr string
    ShardID  string
    Writers  int           // concurrent writer goroutines
    Rate     int           // target writes per second (0 = unlimited)
    Duration time.Duration
}

type LoadResult struct {
    TotalOps     int64
    SuccessOps   int64
    ErrorOps     int64
    LatencyP50   time.Duration
    LatencyP95   time.Duration
    LatencyP99   time.Duration
    ErrorsByType map[string]int64 // "not_leader", "quorum_unavail", etc.
}
```

The load generator uses `golang.org/x/time/rate` for rate limiting. It
records per-operation latency in a histogram and reports percentiles at the
end.

### 4b. Load Test Scenarios

**Baseline throughput:**
- 5 writers, unlimited rate, 10 seconds
- Expect: zero errors, p99 < 50ms
- This is the regression baseline; changes that degrade it by > 20% flag.

**Sustained write under single-node failure:**
- 10 writers, 50 writes/s, 30 seconds
- At t=10s, kill leader
- Expect: writes fail during re-election window (<3s), then recover
- Expect: total error rate < 10%; all committed writes readable at end

**High-concurrency write:**
- 50 writers, unlimited rate, 5 seconds
- Expect: quorum satisfied; no split-brain; follower versions within 100 of leader

### 4c. Baseline Tracking

Load test results are written to `test/baselines/load_<git-sha>.json`. A
comparison script flags regressions:

```bash
make test-load          # run load tests, save results
make compare-baseline   # compare to last committed baseline
```

This is implemented as a simple JSON file comparison, not a time-series
database. Sufficient for a learning project.

---

## 5. Integration into the Development Cycle

Reliability is not a phase — it is a layer of the test system that is
maintained continuously alongside feature development.

### 5a. Test Tiers and Make Targets

| Target | Contents | When to run |
|--------|----------|-------------|
| `make test` | Unit tests | Every change (< 5s) |
| `make test-int` | Integration tests | Before every commit (< 30s) |
| `make test-chaos` | Chaos scenarios (deterministic) | Before every PR merge (< 2min) |
| `make test-load` | Load baseline + comparison | Before every PR merge (< 1min) |
| `make test-reliability` | All of the above | Before every phase transition |

`test-chaos` and `test-load` use the `reliability` build tag:

```bash
go test ./test/reliability/... -tags=reliability -timeout 5m
```

### 5b. Per-Feature Reliability Checklist

When implementing a new feature (e.g. a new Phase), the following questions
must be answered before marking the phase complete:

- [ ] **Does the feature change any core invariant?**
      If yes: add an invariant checker that validates it after chaos scenarios.
- [ ] **Does the feature add a new failure mode?**
      If yes: add a targeted chaos scenario that triggers it.
- [ ] **Does the feature affect write latency or throughput?**
      If yes: re-run the load baseline and commit the new result.
- [ ] **Does the feature add observable state?**
      If yes: add metrics and expose them in `/status`.
- [ ] **Are the new metrics asserted in at least one test?**
      Metrics that are never asserted provide false confidence.

This checklist lives in CLAUDE.md and is part of the "Definition of Done" for
each phase.

### 5c. Reliability Regression Procedure

If a reliability test fails:

1. **Identify the failing signal.** Which health signal tripped? (split-brain,
   replication lag, election rate, etc.)
2. **Reproduce with minimum seed.** For chaos tests, record the random seed.
   For deterministic tests, note the failure step.
3. **Add a targeted unit or integration test.** Before fixing the bug, write a
   test that fails due to the bug. This becomes a regression guard.
4. **Fix and verify all layers.** After the fix: unit → integration → chaos →
   load. All must pass before re-marking the phase stable.

### 5d. Phase Stability Gate

Before starting Phase N+1, Phase N must pass the full reliability suite:

```
make test-reliability  # must exit 0
```

This is the only gate. There is no code-coverage number, no line count, no
external review required. If the reliability suite passes, the phase is stable.

---

## 6. Implementation Sequence

The reliability layer is built incrementally. Suggested order:

1. **Prometheus metrics** (coordinator + node) — adds `/metrics` endpoint and
   in-memory counters. Enables metric assertions in existing tests.
2. **Enhanced `/status`** — replica_versions, last_leader_contact_ms,
   election_count, write ops counters.
3. **Invariant checker** — `assertNoSplitBrain`, `assertQuorumAvailable`,
   `assertFollowersConverge` as shared test helpers.
4. **Fault-injecting transport** — enables `SlowNode` and `PartitionNode`.
5. **Deterministic chaos scenarios** (Scenarios 1–5 above).
6. **Load generator** — in-process, records baseline.
7. **Makefile targets** — `test-chaos`, `test-load`, `test-reliability`.
8. **ChaosMonkey** (random, seed-based) — stretch goal after 1–7 are solid.

---

## Open Questions

> These are questions for the developer before implementation begins.
> See also the questions below that require decisions.

*(To be filled in after design review — see questions raised in the doc review.)*
