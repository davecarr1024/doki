# Doki

> A toy distributed database with strong per-shard consistency, leader-based replication, and a steady heartbeat.

**Doki** (ドキドキ / 同期) evokes both the heartbeat of a live system and the synchronization at the core of replication.

---

## What is Doki?

Doki is a learning-focused distributed key-value database. It is not designed for production use. It is designed to be understood — end to end — by a single engineer over time.

The system implements:

- **Sharding** — keyspace partitioned into named shards
- **Replication** — each shard copied across multiple nodes
- **Strong consistency** — writes require quorum; reads served by leader
- **Leader-based ordering** — one leader per shard defines write order
- **Full-state recovery** — joining nodes pull a complete snapshot from the leader

---

## Why Build This?

Distributed systems are hard to learn abstractly. Reading about Raft or Paxos is useful; building something real is better.

Doki deliberately avoids:

- Over-engineering
- Performance optimization
- Byzantine-fault tolerance
- Dynamic cluster membership (in v1)

Doki deliberately pursues:

- Correctness
- Clarity
- Testability
- Incremental complexity

---

## Architecture at a Glance

```
           ┌──────────────────┐
           │   Coordinator    │  ← cluster membership, shard placement, leader assignment
           └────────┬─────────┘
                    │ metadata / routing
          ┌─────────┼──────────┐
          ▼         ▼          ▼
      ┌───────┐ ┌───────┐ ┌───────┐
      │ Node A│ │ Node B│ │ Node C│  ← leaf nodes: store shards, replicate writes
      └───────┘ └───────┘ └───────┘
          ▲
          │
      ┌───────┐
      │ Client│  ← send reads/writes to any node; redirected to leader if needed
      └───────┘
```

Each **shard** is assigned a **leader** and one or more **followers**. Writes go to the leader; the leader replicates to followers before acknowledging. Reads are always served by the leader.

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/design.md](docs/design.md) | Full design: semantics, protocols, data models, open questions |
| [docs/architecture.md](docs/architecture.md) | Component breakdown, interfaces, deployment topology |
| [docs/replication.md](docs/replication.md) | Replication protocol, write path, recovery, failure handling |
| [docs/testing.md](docs/testing.md) | Testing strategy, integration test patterns, invariants |
| [docs/operations.md](docs/operations.md) | Running the system, observability, health checks |
| [docs/roadmap.md](docs/roadmap.md) | Evolution plan, phase milestones, future directions |

---

## Key Properties

| Property | Doki v1 |
|----------|---------|
| Consistency model | Linearizable per shard |
| Availability | Prefers consistency; fails if quorum unavailable |
| Replication | Synchronous quorum write (majority) |
| Storage | In-memory (`std::map`) |
| Recovery | Full-state snapshot from leader |
| Leader election | Coordinator-driven |
| Sharding | Static, explicit |
| Durability | None in v1 (in-memory only) |
| Protocol | gRPC |
| Deployment | Docker / Docker Compose |

---

## Getting Started

> Implementation is in progress. This section will be updated as code is added.

```bash
# Clone the repo
git clone <repo-url> doki
cd doki

# Start the cluster (Docker Compose)
docker compose up

# Run integration tests
./test/run_tests.sh
```

---

## Development Principles

1. **Correctness first** — slow and correct beats fast and wrong
2. **Simple over clever** — full-state copies, explicit leaders, static config
3. **Build in layers** — each component has clear contracts and is independently testable
4. **Observable by default** — every node exposes health and status APIs
5. **Embrace evolution** — start simple, extend deliberately

---

## Roadmap Summary

1. In-memory KV with replication (v1)
2. Write-ahead log + durability (v2)
3. Incremental replication log (v3)
4. Distributed leader election (v4)
5. Dynamic sharding (v5)
6. SQL layer (v6+)

See [docs/roadmap.md](docs/roadmap.md) for details.

---

## License

MIT
