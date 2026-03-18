# Doki — Testing Strategy

This document describes the testing approach for Doki: philosophy, layers, invariants, test scenarios, and tooling.

---

## Philosophy

Testing is a first-class concern. Doki is a distributed system; the kinds of bugs it produces — data loss, stale reads, split-brain — are subtle and may not appear under normal conditions. Tests must be able to:

- Start and stop real cluster nodes
- Inject failures (node crashes, slow followers, network partitions)
- Assert against distributed invariants (not just individual node state)
- Be deterministic and reproducible

**Rule:** Never rely on wall-clock timing in tests. Use injected clocks and configurable timeouts.

---

## Test Layers

### 1. Unit Tests

Test individual components in isolation:

- Storage engine: correct Get/Put/Delete behavior
- Replication logic: version tracking, quorum counting
- Routing logic: shard map parsing, key-to-shard mapping
- Config parsing: valid and invalid inputs

Unit tests are fast, deterministic, and do not use network or real timers.

```
test/unit/
  storage_test.go
  replicator_test.go
  routing_test.go
  config_test.go
```

### 2. Component Tests

Test a single component with real gRPC but mocked dependencies:

- Node behavior with a mock coordinator
- Coordinator behavior with mock nodes
- Recovery behavior with a mock leader

Component tests use a loopback network. No Docker required.

```
test/component/
  node_test.go
  coordinator_test.go
  recovery_test.go
```

### 3. Integration Tests

Test a full cluster end-to-end:

- Real nodes, real coordinator
- Docker containers via Testcontainers
- Real gRPC over localhost network
- Real failure injection

Integration tests are slower but are the primary correctness validation.

```
test/integration/
  cluster/         # Test cluster builder
  scenarios/       # Test scenarios
  assertions/      # Invariant checkers
```

---

## Integration Test Infrastructure

### Test Cluster Builder

The test cluster builder provides a fluent API for creating clusters:

```go
cluster := testcluster.New().
    WithCoordinator().
    WithNodes(3).
    WithShard("shard-0", replicas: ["node-0", "node-1", "node-2"]).
    Build(t)
defer cluster.Shutdown()

cluster.WaitUntilReady(timeout: 10s)
```

Each node is a Docker container. The cluster builder handles:

- Container lifecycle (start, stop, kill)
- Network management
- Log collection
- Health waiting

### Failure Injection

The test framework supports:

```go
// Kill a node (SIGKILL, immediate)
cluster.KillNode("node-1")

// Stop a node gracefully (SIGTERM)
cluster.StopNode("node-1")

// Restart a stopped node
cluster.RestartNode("node-1")

// Partition a node from the rest of the cluster
cluster.PartitionNode("node-1")

// Heal a partition
cluster.HealPartition("node-1")

// Slow down a node's network responses
cluster.SlowNode("node-1", latency: 500ms)
```

Network partitions and latency injection are implemented using [Toxiproxy](https://github.com/Shopify/toxiproxy), a configurable TCP proxy. Each node's traffic passes through a Toxiproxy instance that tests can control.

### Invariant Checkers

After any scenario, tests run invariant checks:

```go
assertions.AssertSingleLeaderPerShard(cluster)
assertions.AssertCommittedWritesSurviveFailover(cluster, writes)
assertions.AssertNoStalereads(cluster)
assertions.AssertFollowersConverge(cluster, shard_id, timeout)
assertions.AssertRecoveredStateMatchesLeader(cluster, node_id, shard_id)
```

---

## Test Scenarios

### Basic Correctness

| Test | Description |
|------|-------------|
| `TestBasicWrite` | Write a key and read it back |
| `TestOverwrite` | Write a key twice, confirm latest value |
| `TestDelete` | Write a key, delete it, confirm not found |
| `TestNonExistentKey` | Read a key that was never written |
| `TestMultipleShards` | Write to multiple shards, read from each |

### Replication

| Test | Description |
|------|-------------|
| `TestWriteReplicatedToFollowers` | After a write, verify all followers have the value |
| `TestFollowerDoesNotServeReads` | Confirm follower returns NOT_LEADER |
| `TestFollowerRedirectsToLeader` | Confirm follower returns correct leader hint |
| `TestVersionMonotonicity` | Confirm version never decreases |

### Quorum

| Test | Description |
|------|-------------|
| `TestWriteSucceedsWithOneFollowerDown` | 3-node shard, kill 1 follower, writes still succeed |
| `TestWriteFailsWithTwoFollowersDown` | 3-node shard, kill 2 followers, writes return QUORUM_UNAVAILABLE |
| `TestReadSucceedsWithFollowersDown` | Leader-only reads succeed even if followers are dead |

### Failover

| Test | Description |
|------|-------------|
| `TestLeaderFailover` | Kill leader, confirm new leader elected, writes resume |
| `TestLeaderFailoverPreservesCommitted` | Kill leader after N writes, confirm all committed writes survive |
| `TestDoubleFailover` | Kill leader twice in succession |
| `TestFollowerFailover` | Kill a follower and rejoin, confirm state syncs |

### Recovery

| Test | Description |
|------|-------------|
| `TestFollowerRecovery` | Kill a follower, write 100 keys, restart follower, verify sync |
| `TestSlowRecovery` | Simulate a slow follower reconnection under continuous writes |
| `TestRecoveryStateMatches` | Recovered follower's state exactly matches leader state |
| `TestRecoveryAfterLag` | Follower falls far behind, triggers snapshot-based recovery |

### Routing

| Test | Description |
|------|-------------|
| `TestClientRoutingUpdate` | Client caches stale leader; verify it retries correctly |
| `TestClientAfterLeaderChange` | Client continues after failover without restart |

### Coordinator

| Test | Description |
|------|-------------|
| `TestCoordinatorFailure` | Coordinator goes down; cluster continues serving reads/writes |
| `TestCoordinatorRestart` | Coordinator restarts; leadership is reestablished |

---

## Key Invariants

These properties are checked after every scenario:

### I1: Single Leader Per Shard

At any point in time, at most one node believes it is the leader for a given shard at a given term.

```go
// Query all nodes; confirm at most one LEADER per (shard_id, term)
```

This is checked by querying `/status` on all nodes and verifying uniqueness.

### I2: Committed Writes Survive Failover

A write that received `OK` from the leader must be readable after any failover to a new leader.

```go
writes := []Write{{key: "x", value: "1"}, ...}
for _, w := range writes {
    assert.OK(cluster.Put(w.key, w.value))
}
cluster.KillNode(leader)
cluster.WaitForNewLeader(shard)
for _, w := range writes {
    assert.Equal(w.value, cluster.Get(w.key))
}
```

### I3: No Writes Without Quorum

A write must not return `OK` unless quorum was reached. Tested by killing majority of replicas and verifying writes fail.

### I4: Followers Converge

After all writes complete and all nodes are healthy, every follower must have the same state as the leader for the same shard.

### I5: Recovery Yields Identical State

A node that recovers via snapshot must have an identical kv map to the leader at the snapshot version.

---

## Test Determinism

### Injected Clocks

Production code should accept a `Clock` interface:

```go
type Clock interface {
    Now() time.Time
    Sleep(d time.Duration)
    After(d time.Duration) <-chan time.Time
}
```

Tests provide a `FakeClock` that can be advanced manually, eliminating timing-dependent behavior in unit and component tests.

### Configurable Timeouts

All timeouts must be configurable via environment variable or config, not hardcoded. Integration tests use shorter timeouts (100ms heartbeat, 300ms failure timeout) to speed up failure detection.

### Retry Budgets

Tests that wait for convergence should use polling with a maximum retry budget rather than fixed sleeps:

```go
func WaitForCondition(t *testing.T, condition func() bool, timeout time.Duration, msg string) {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if condition() {
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    t.Fatalf("condition not met: %s", msg)
}
```

---

## Running Tests

```bash
# Unit tests (fast, no Docker)
go test ./...

# Component tests
go test ./test/component/...

# Integration tests (requires Docker)
go test ./test/integration/...

# Specific scenario
go test ./test/integration/scenarios/ -run TestLeaderFailover -v

# All tests with verbose output
go test ./... -v -count=1
```

---

## Test Coverage Goals

| Layer | Target Coverage | Notes |
|-------|----------------|-------|
| Storage engine | 95%+ | Pure logic, easy to cover |
| Replication logic | 90%+ | Core protocol |
| Recovery logic | 85%+ | Critical correctness path |
| Coordinator logic | 85%+ | Leader assignment, health monitor |
| Integration scenarios | All key scenarios | Failover, quorum, recovery |

Coverage is a guide, not a target. A badly-written test that hits a line is worse than no test.

---

## Debugging Failed Tests

Integration test failures are diagnosed via:

1. **Container logs** — each test captures logs from all containers, written to `test/logs/<test-name>/`
2. **Status snapshots** — tests periodically call `/status` on all nodes and save responses
3. **Timeline events** — a test event log records writes, kills, and assertions with timestamps

If a test is flaky, the first step is to add more `WaitForCondition` checks rather than increasing sleep durations.
