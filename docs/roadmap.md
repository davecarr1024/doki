# Doki — Roadmap

This document describes the planned evolution of Doki across multiple phases. Each phase builds on the last and introduces a focused set of new capabilities.

The goal is not speed of delivery. The goal is understanding.

---

## Guiding Principle

Each phase should be:

- **Completable** — has a clear definition of done
- **Testable** — correctness can be verified before moving on
- **Educational** — introduces distinct new distributed systems concepts
- **Stable** — does not leave the system in an inconsistent or broken state

Do not start the next phase until the current phase is solid.

---

## Phase 0: Project Setup

**Theme:** Infrastructure before code.

**Goals:**
- Repo structure established
- Build system working (Make / bazel / go build)
- Docker Compose cluster launches successfully
- Testcontainers integration test harness working
- Logging and health endpoint scaffolding in place
- Proto definitions checked in and compiling
- CI pipeline running unit tests

**Definition of Done:**
- `docker compose up` starts coordinator + 3 nodes
- All nodes heartbeat successfully
- Health endpoints return valid JSON
- At least one passing integration test (even if trivial)

---

## Phase 1: In-Memory KV with Replication

**Theme:** The core protocol. Everything else builds on this.

**New Concepts:**
- Leader/follower replication
- Quorum writes
- Term-based stale leader detection
- Full-state snapshot recovery

**What Gets Built:**
- Coordinator: heartbeat tracking, leader assignment, shard map
- Node: per-shard replica state, write handler, replication logic
- Recovery: snapshot send/receive
- Client library: routing, retry on NOT_LEADER

**Key Invariants to Test:**
- Single leader per shard
- Committed writes survive 1-node failure in 3-node shard
- Writes fail when quorum unavailable
- Recovered node's state matches leader

**Definition of Done:**
- All integration test scenarios in `testing.md` pass
- Leader failover works end-to-end
- Follower recovery works after arbitrary crash

---

## Phase 2: Durability (Write-Ahead Log)

**Theme:** Data that survives a crash.

**New Concepts:**
- Write-ahead logging (WAL)
- fsync semantics
- Log replay on restart
- Snapshot + log truncation

**What Gets Built:**
- WAL: append-only log file with checksums
- Startup: replay WAL to reconstruct in-memory state
- Snapshot: periodic disk snapshot + WAL truncation
- Recovery: if WAL exists on restart, replay instead of pulling full snapshot (when version is close enough)

**Key Invariants to Test:**
- Data survives process restart
- WAL replay produces identical state to pre-crash
- Snapshot + partial WAL replay produces correct state
- Corrupted WAL entry triggers safe fallback (recover from leader)

**Definition of Done:**
- A node can be killed and restarted and recover its own data from disk
- Network recovery (snapshot from leader) is still used when WAL diverges significantly

---

## Phase 3: Incremental Replication Log

**Theme:** Efficient catch-up without full snapshots.

**New Concepts:**
- Replication log (operations log, not WAL)
- Log-based follower catch-up
- Log compaction / truncation
- Snapshot + tail-of-log recovery

**What Gets Built:**
- Leaders maintain a bounded replication log (recent N operations)
- Followers that fall slightly behind catch up via log replay, not full snapshot
- Full snapshot still used when follower is too far behind
- Log entries include: `{term, version, op}`

**Key Invariants to Test:**
- Follower that misses N writes (N < log size) recovers via log, not snapshot
- Follower that is far behind still recovers correctly via snapshot
- Leader log does not grow unbounded

**Definition of Done:**
- Recovery for small gaps uses log replay
- Integration test: kill follower, write 50 ops, restart, verify log-based recovery

---

## Phase 4: Distributed Leader Election

**Theme:** Remove the coordinator as a SPOF for leadership.

**New Concepts:**
- Leader election via voting
- Election safety: at most one leader per term
- Vote quorum: majority required to win election
- Candidate selection: prefer highest-version node

**What Gets Built:**
- Nodes detect leader failure (missed heartbeats from leader, not just from coordinator)
- Candidate node initiates an election: sends `RequestVote(term, version)` to peers
- Peers grant vote if: no vote cast this term AND candidate version >= own version
- Candidate wins election if majority votes received
- Elected leader notifies coordinator (coordinator updates shard map)

**Design Note:** This is Raft-like but not full Raft. We skip pre-vote, log matching, and some edge cases. The goal is to learn the core election mechanism, not implement production-grade Raft.

**Key Invariants to Test:**
- Only one winner per election term
- A stale node (lower version) cannot win election if a more up-to-date node is available
- Cluster recovers leadership without coordinator involvement
- No data loss on failover (relies on phase 1+3 correctness)

**Definition of Done:**
- Kill coordinator; cluster continues to elect new leaders on node failure
- Election invariants pass

---

## Phase 5: Dynamic Sharding

**Theme:** Cluster can grow and shards can move.

**New Concepts:**
- Shard migration
- Dynamic cluster membership
- Split/merge shards
- Consistent hash ring (optional)

**What Gets Built:**
- Coordinator API to add/remove nodes
- Shard migration: leader-to-leader snapshot transfer with routing update
- Atomic shard map update: old and new shard location served during migration window
- Shard split: coordinator triggers split, new shard bootstraps from snapshot
- (Optional) Consistent hashing for key routing

**Key Invariants to Test:**
- No writes lost during shard migration
- Clients automatically route to new location after migration
- No double-write window during split

**Definition of Done:**
- A new node can join and receive a migrated shard
- Existing clients continue to work during migration
- Integration test: add node, migrate shard, verify all data accessible

---

## Phase 6: Basic SQL Layer

**Theme:** A real query language on top of the KV engine.

**New Concepts:**
- SQL parsing
- Query planning
- Schema management
- Table-to-shard mapping

**What Gets Built:**
- SQL parser (single-table subset: SELECT, INSERT, UPDATE, DELETE)
- Schema registry (table definitions, primary key mapping)
- Query planner: SQL → KV operation sequence
- Executor: run KV operations
- Primary-key-only access; no secondary indexes

**Supported SQL (v1):**
```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES (1, 'alice', 30);
SELECT * FROM users WHERE id = 1;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

**Not Supported:**
- Range scans
- JOINs
- Multi-row transactions
- Secondary indexes
- Multi-shard queries

**Key Invariants to Test:**
- SQL INSERT/SELECT round-trip
- SQL UPDATE produces correct new state
- SQL DELETE removes the row
- Invalid SQL returns a useful error

**Definition of Done:**
- A SQL client can create a table, insert rows, and query them back

---

## Phase 7: Secondary Indexes

**Theme:** Query by non-primary-key columns.

**New Concepts:**
- Index maintenance (write-time index update)
- Index storage (KV encoding of index entries)
- Index scan in query planner
- Consistency between primary and index data

**What Gets Built:**
- `CREATE INDEX` support
- Write path: index entries written atomically with primary row (within a single shard)
- Read path: index scan → primary key lookup → full row
- Cross-shard consistency is deferred (single-shard only initially)

---

## Phase 8: Multi-Row Transactions

**Theme:** ACID guarantees across multiple operations.

**New Concepts:**
- Two-phase commit (2PC)
- Transaction coordinator
- Optimistic vs pessimistic concurrency control
- Deadlock detection

**What Gets Built:**
- Transaction API: `BEGIN`, `COMMIT`, `ROLLBACK`
- Single-shard transactions: implemented as serialized batch operation
- Multi-shard transactions: two-phase commit with coordinator
- Conflict detection

**Note:** This is the most complex phase. Multi-shard transactions require deep coordination and introduce many failure modes. Full correctness here is ambitious; partial implementation is acceptable for the learning goal.

---

## Summary Table

| Phase | Theme | Key Concept |
|-------|-------|------------|
| 0 | Setup | Infrastructure, CI |
| 1 | In-memory KV | Replication, quorum, recovery |
| 2 | Durability | WAL, fsync, restart recovery |
| 3 | Incremental replication | Log-based catch-up |
| 4 | Distributed election | Raft-like voting |
| 5 | Dynamic sharding | Migration, membership |
| 6 | Basic SQL | Parser, planner, executor |
| 7 | Indexes | Index maintenance |
| 8 | Transactions | 2PC, concurrency control |

---

## What Not To Build (Yet)

These are intentionally deferred:

- **Compression** — not needed until performance matters
- **Authentication / TLS** — out of scope for a learning system
- **Multi-datacenter replication** — too complex without Raft
- **MVCC** — needed for transactions but complex; deferred to phase 8
- **Query optimizer** — not needed with single-table, primary-key-only SQL
- **Horizontal read scaling** — consistent reads from followers require leases; deferred
- **Geo-distribution** — out of scope
