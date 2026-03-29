# Doki — Testing Strategy

This document describes the testing approach for Doki: philosophy, layers, invariants, test scenarios, and tooling.

---

## Philosophy

Testing is a first-class concern. Doki is a distributed system; the kinds of bugs it produces — data loss, stale reads, split-brain — are subtle and may not appear under normal conditions. Tests must be able to:

- Start and stop real cluster nodes
- Inject failures (node crashes, slow followers)
- Assert against distributed invariants (not just individual node state)
- Be deterministic and reproducible

**Rule:** Never rely on wall-clock timing in tests. Use injected clocks and configurable timeouts where possible; use `require.Eventually` with generous budgets in integration tests.

---

## Test Layers

### 1. Unit Tests

Test individual components in isolation:

- Storage engine: correct Get/Put/Delete behavior
- Replication log: append, replay, bounds checking
- Shard map: parsing, version tracking, mutation methods
- Config parsing: valid and invalid inputs
- Coordinator logic: leader assignment, membership tracking
- Node handlers: routing, term checks, quorum counting

Unit tests are fast, deterministic, and do not use real network or timers.

```bash
make test            # runs all unit tests
go test ./...
```

### 2. Integration Tests

Test a full cluster end-to-end using **real in-process gRPC servers** on random ports (`:0`). No Docker required. HTTP is used only for `/ready` and `/metrics` checks.

```bash
make test-int
go test -tags integration ./test/integration/...
```

Each test:
- Starts real coordinator and node servers as goroutines
- Uses `t.Cleanup()` to shut down servers when the test ends
- Uses `require.Eventually` for convergence assertions
- Runs on random ports to avoid port conflicts

```
test/integration/
  cluster_control.go          # StartCluster + gRPC helpers
  kv_operations_test.go       # read/write/delete scenarios
  replication_consistency_test.go
  recovery_log_test.go
  durability_test.go
  election_failover_test.go
  control_plane_test.go
  sharding_admin_test.go
```

### 3. Reliability Tests (Chaos / Load / Monkey)

Long-running tests that exercise the system under stress:

```bash
make test-reliability  # chaos + load + monkey
make test-chaos        # fault injection only
make test-load         # throughput benchmarks
```

These use the `//go:build reliability` build tag and live in `test/reliability/`. They are not run as part of the normal CI pipeline.

---

## Integration Test Infrastructure

### Test Cluster Builder

`StartCluster` in `cluster_control.go` builds a full cluster:

```go
cluster := startCluster(t, []string{"node-a", "node-b", "node-c"}, []config.ShardSpec{
    {ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
})
// cluster.CoordinatorAddr  — coordinator gRPC address
// cluster.NodeAddrs        — map[nodeID]address
```

All servers use `t.Cleanup()` to shut down. Random ports prevent interference between parallel tests.

### Key Helpers

```go
// nodeClient returns a gRPC client for a node
client := nodeClient(t, nodeAddr)

// leaderAddrFor returns the current leader's gRPC address for a shard
addr := leaderAddrFor(t, coordAddr, "shard-0")

// waitForHTTP polls until /ready returns 200
waitForHTTP(t, "http://"+addr+"/ready", 5*time.Second)

// fetchShardMapRespGRPC fetches and returns the coordinator's shard map
sm := fetchShardMapRespGRPC(t, coordAddr)
```

### Failure Injection

Integration tests simulate failures by cancelling server contexts:

```go
// Kill a node: cancel its context (stops all goroutines including gRPC server)
cancel()

// Restart-equivalent: start a fresh server on the same or new address
newSrv := node.NewServer(cfg, clock.Real{})
go newSrv.Start(ctx)
```

---

## Test Scenarios

### Basic Correctness

| Test | Description |
|------|-------------|
| `TestBasicPutGet` | Write a key and read it back |
| `TestOverwrite` | Write a key twice, confirm latest value |
| `TestDelete` | Write a key, delete it, confirm not found |
| `TestNonExistentKey` | Read a key that was never written |
| `TestMultipleShards` | Write to multiple shards, read from each |

### Replication

| Test | Description |
|------|-------------|
| `TestWriteReplicatedToFollowers` | After a write, verify all followers converge |
| `TestFollowerDoesNotServeReads` | Follower returns NOT_LEADER |
| `TestVersionMonotonicity` | Version never decreases |

### Quorum

| Test | Description |
|------|-------------|
| `TestWriteSucceedsWithOneFollowerDown` | 3-node shard, kill 1 follower, writes succeed |
| `TestWriteFailsWithTwoFollowersDown` | 3-node shard, kill 2 followers, QUORUM_UNAVAILABLE |
| `TestReadSucceedsWithFollowersDown` | Leader-only reads succeed even if followers are dead |

### Failover

| Test | Description |
|------|-------------|
| `TestLeaderFailover` | Kill leader, confirm new leader elected, writes resume |
| `TestLeaderFailoverPreservesCommitted` | Kill leader after N writes, confirm all survive |
| `TestDoubleFailover` | Kill leader twice in succession |
| `TestFollowerFailover` | Kill a follower, rejoin, confirm state syncs |

### Recovery

| Test | Description |
|------|-------------|
| `TestFollowerRecovery` | Kill a follower, write keys, restart follower, verify sync |
| `TestIncrementalRecovery` | Follower catches up via log entries (not full snapshot) |
| `TestSnapshotFallback` | Follower falls far behind; snapshot recovery kicks in |
| `TestRecoveryStateMatchesLeader` | Recovered state exactly matches leader at same version |

### Leader Election (Phase 4)

| Test | Description |
|------|-------------|
| `TestSelfElection` | Node detects missed heartbeats and triggers election |
| `TestElectionWithHigherVersionWins` | Candidate with latest data wins election |
| `TestNoDoubleVote` | Node does not vote twice in same term |
| `TestLeaderHeartbeatPreventsElection` | Active leader resets election timer via replication |

### Dynamic Sharding (Phase 5)

| Test | Description |
|------|-------------|
| `TestDynamic_AddNode` | New node registered, appears alive in coordinator status |
| `TestDynamic_MigrateShard` | Pre-migration writes survive; new nodes serve data after migration |
| `TestDynamic_SplitShard` | New shard bootstraps from source; both shards independently writable |
| `TestDynamic_ClientsContinueDuringMigration` | Old leader serves writes during migration window |

---

## Key Invariants

These properties are checked in integration tests after every significant scenario:

### I1: Single Leader Per Shard

At most one node has `role=LEADER` for a given `(shard_id, term)` at any point.

```go
// AssertNoSplitBrain queries GetStatus on all nodes and confirms at most
// one LEADER per (shard_id, term).
assertNoSplitBrain(t, cluster)
```

### I2: Committed Writes Survive Failover

A write that received `OK` from the leader must be readable from the new leader after failover.

```go
// Write N keys → kill leader → wait for new leader → read all N keys
assertAllCommittedWritesSurvive(t, cluster, writes)
```

### I3: No Writes Without Quorum

A write must not return `OK` unless quorum was reached.

### I4: Followers Converge

After all writes complete and all nodes are healthy, every follower must have the same KV state as the leader for the same shard.

### I5: Recovery Yields Identical State

A node that recovers via snapshot (or incremental catch-up) must have the exact same KV contents as the leader at the recovered version.

---

## Test Determinism

### Injected Clocks

The `Clock` interface (`internal/clock`) is injected throughout the codebase:

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}
```

Unit tests use `FakeClock`, which can be advanced manually:

```go
clk := clock.NewFake(time.Now())
clk.Advance(2 * time.Second)
```

Integration tests use `clock.Real{}` with real timers.

### Configurable Timeouts

All timeouts are configurable via `NodeConfig` / `CoordinatorConfig`. Integration tests use aggressive timeouts:

```go
nodeCfg := &config.NodeConfig{
    HeartbeatIntervalMs: 100,
    HeartbeatInterval:   100 * time.Millisecond,
}
```

This speeds up failure detection from 1.5s to ~300ms.

### Convergence Polling

Integration tests use `require.Eventually` rather than fixed sleeps:

```go
require.Eventually(t, func() bool {
    return isLeaderReady(coordAddr, "shard-0")
}, 5*time.Second, 100*time.Millisecond, "shard-0 leader not ready")
```

---

## Running Tests

```bash
# Unit tests only (fast, no network)
make test
go test ./...

# Integration tests (starts real gRPC servers)
make test-int
go test -tags integration ./test/integration/... -v

# Single integration test
go test -tags integration ./test/integration/ -run TestDynamic_MigrateShard -v

# Reliability suite (chaos + load, long-running)
make test-reliability

# Chaos tests only
make test-chaos

# Load tests only
make test-load
```

---

## Test Coverage Goals

| Layer | Target | Notes |
|-------|--------|-------|
| Storage engine | 95%+ | Pure logic |
| Replication log | 90%+ | Core protocol component |
| Recovery logic | 85%+ | Critical correctness path |
| Coordinator logic | 85%+ | Leader assignment, membership |
| Integration scenarios | All key scenarios | Failover, quorum, recovery, dynamic sharding |
| Reliability | Chaos + load | Non-deterministic; run separately |

Coverage is a guide, not a target. A badly-written test that hits a line is worse than no test.

---

## Debugging Failed Tests

Integration test failures are diagnosed via:

1. **Test logs** — use `-v` flag; each server logs to the test logger via `t.Logf`
2. **Status snapshots** — tests call `GetStatus` on coordinator and nodes during assertions
3. **`require.Eventually` messages** — failure messages include a description of what was expected

If a test is flaky:
- Check for missing `require.Eventually` (replace `time.Sleep` + immediate assertion)
- Check for goroutine leaks (use `t.Cleanup` to cancel contexts, not `defer` in loops)
- Check if a shard still shows `is_ready=false` when traffic is being sent to it
