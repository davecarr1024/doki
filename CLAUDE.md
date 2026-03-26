# Doki — Claude Development Guide

Doki is a learning-focused distributed database. This guide covers everything needed to work on the codebase.

---

## Quick Commands

```bash
make build            # compile all binaries
make test             # run all unit tests
make test-int         # run integration tests (starts real servers)
make test-chaos       # run chaos scenarios (fault injection)
make test-load        # run load / throughput tests
make test-reliability # run full reliability suite (chaos + load + monkey)
make proto            # regenerate protobuf code (requires buf)
make lint             # run golangci-lint
make docker-build     # build Docker images
make up               # docker compose up
make down             # docker compose down
make clean            # remove build artifacts
```

---

## Project Structure

```
doki/
├── CLAUDE.md
├── Makefile
├── go.mod / go.sum
├── buf.yaml / buf.gen.yaml         # protobuf generation config
├── proto/                          # .proto source files
│   ├── common.proto
│   ├── coordinator.proto
│   └── node.proto
├── internal/                       # shared packages (not importable externally)
│   ├── clock/                      # Clock interface for testable time
│   ├── config/                     # Config types and YAML loading
│   ├── shardmap/                   # ShardMap types and operations
│   └── storage/                    # Storage interface + implementations
│       └── memory/                 # In-memory storage engine
├── coordinator/                    # Coordinator process
│   ├── main.go
│   ├── membership.go               # Node heartbeat tracking
│   ├── leader.go                   # Leader assignment
│   └── server.go                   # HTTP server + handlers
├── node/                           # Leaf node process
│   ├── main.go
│   ├── replica.go                  # Per-shard replica state
│   └── server.go                   # HTTP server + handlers
├── test/
│   ├── integration/                # Integration tests (real servers, no Docker)
│   ├── reliability/                # Chaos, load, and monkey tests (build tag: reliability)
│   └── baselines/                  # Persisted load test results for regression tracking
├── config/                         # Example YAML configs
├── docker/                         # Dockerfiles
└── docs/                           # Design documentation
```

---

## Development Phases

The project is built incrementally. Know which phase you are in before adding complexity.

| Phase | Status | What it adds |
|-------|--------|-------------|
| 0 | Complete | Infrastructure, health endpoints, heartbeat, scaffolding |
| 1 | Complete | In-memory KV, replication, quorum writes, snapshot recovery |
| 2 | Complete | WAL + durability, disk snapshots, restart recovery |
| 3 | Complete | Incremental replication log |
| 4 | Complete | Distributed leader election |
| 5 | **Current** | Dynamic sharding |
| 6 | Planned | SQL layer |

**Rule:** Do not implement Phase N+1 concepts while working in Phase N.

---

## Key Invariants

These must hold at all times (from Phase 1 onwards):

1. **One leader per shard** — at most one node has `role=LEADER` for a given `(shard_id, term)`
2. **Quorum before commit** — no write returns `OK` without majority acknowledgment
3. **Monotonic versions** — the version counter for a shard never decreases
4. **Committed writes survive failover** — a write that returned `OK` is readable after leader change
5. **Followers converge** — after quorum is reached, all healthy replicas eventually have identical state

If you change the replication or recovery code, verify all five invariants still hold.

---

## Code Conventions

### Error Handling

- Return errors up the stack; don't swallow them
- Use `fmt.Errorf("context: %w", err)` for wrapping
- Distinguish user-facing errors (e.g., `NOT_LEADER`) from internal errors

### Concurrency

- Each shard has its own `sync.RWMutex`; never hold two shard locks simultaneously
- The coordinator uses a single `sync.RWMutex` for its state (acceptable in v1)
- Background goroutines receive a `context.Context`; exit when it is cancelled

### Interfaces for Testability

- `Clock` — always use the injected clock, never `time.Now()` or `time.Sleep()` directly
- `Storage` — always use the interface, never the concrete type in application code
- HTTP clients in node/coordinator — inject as interface so tests can mock

### Tests

- Unit tests: no network, no goroutines that outlive the test, use `FakeClock`
- Integration tests: use real servers on random ports (`:0`), real goroutines, real timers
- Reliability tests: use `//go:build reliability` tag; run with `make test-reliability`
- Always use `t.Cleanup()` to stop servers, not `defer` in loops

### Definition of Done (per feature)

Before a feature is considered complete, verify the following checklist:

- [ ] Unit tests cover the happy path and key error paths
- [ ] Integration tests cover cross-process interactions
- [ ] `AssertNoSplitBrain` passes under the feature's failure modes
- [ ] `AssertAllCommittedWritesSurvive` passes after leader failover
- [ ] New Prometheus metrics added for observability (counters, gauges, histograms)
- [ ] `/status` endpoints updated if new state is relevant to operators
- [ ] Relevant chaos scenario passes (`make test-chaos`)
- [ ] Load test shows no regression vs. recorded baseline (`make test-load`)
- [ ] `docs/` updated if the protocol or architecture changed

### Logging

Use structured logging (key-value pairs), not format strings:

```go
// Good
log.Printf("write committed shard=%s version=%d", shardID, version)

// Avoid
log.Printf("committed write %v to %v", version, shardID)
```

Full structured logging (slog or zap) will be added in a later phase.

---

## Adding a New RPC / Endpoint

Phase 0 uses HTTP/JSON. Phase 1 will migrate to gRPC. To add a new endpoint in Phase 0:

1. Add handler method to `server.go` (coordinator or node)
2. Register route in `registerRoutes()`
3. Add request/response types as Go structs in the same file or a `types.go`
4. Add unit test for the handler
5. Add integration test if the endpoint crosses process boundaries

When Phase 1 migrates to gRPC:
1. Add RPC to the relevant `.proto` file
2. Run `make proto` to regenerate
3. Implement the generated interface
4. Delete the corresponding HTTP handler

---

## Running the Test Suite

```bash
# Fast — unit tests only, no network, uses fake clock
make test

# Slower — starts real HTTP servers on random ports
make test-int

# Single test
go test ./coordinator/ -run TestMembership -v

# All tests verbose
go test ./... -v -count=1
```

Tests must be deterministic. If a test relies on timing, use `FakeClock` or add a generous `WaitForCondition`.

---

## Protobuf Workflow

The `.proto` files in `proto/` define the eventual gRPC API. Generated code goes in `gen/`.

```bash
# Install buf (once)
brew install bufbuild/buf/buf

# Generate
make proto
```

In Phase 0, proto files are present for documentation and future use. gRPC is wired in Phase 1.

---

## Docker Workflow

```bash
# Build images
make docker-build

# Start full cluster (coordinator + 3 nodes)
make up
docker compose logs -f

# Tear down
make down

# Reset (wipe all state)
docker compose down -v && make up
```

---

## Common Debugging

**Coordinator shows node as dead:**
```bash
curl http://localhost:7000/status | jq '.nodes'
docker compose logs node-a | tail -20
```

**Check replication lag:**
```bash
curl http://localhost:8001/status | jq '.shards[] | {shard_id, version, peer_versions}'
```

**Watch a node recover:**
```bash
watch -n 0.5 'curl -s http://localhost:8001/status | jq ".shards[].is_ready"'
```

**Run cluster smoke test:**
```bash
./scripts/smoke_test.sh
```

---

## Docs

All design decisions live in `docs/`. When you change a protocol or add a mechanism, update the relevant doc.

| Changed area | Update doc |
|---|---|
| Replication protocol | `docs/replication.md` |
| Component structure | `docs/architecture.md` |
| Semantics or data model | `docs/design.md` |
| Test strategy | `docs/testing.md` |
| Running/ops | `docs/operations.md` |
| New phase started | `docs/roadmap.md` |
