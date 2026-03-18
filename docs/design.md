# Doki — Design Document

This document is the authoritative reference for Doki's design. It covers semantics, data models, protocols, component contracts, and the open questions that will shape future evolution.

---

## Table of Contents

1. [Vision & Philosophy](#1-vision--philosophy)
2. [System Overview](#2-system-overview)
3. [Semantics](#3-semantics)
4. [Data Model](#4-data-model)
5. [Sharding](#5-sharding)
6. [Replica State Model](#6-replica-state-model)
7. [Write Protocol](#7-write-protocol)
8. [Read Protocol](#8-read-protocol)
9. [Replication](#9-replication)
10. [Recovery](#10-recovery)
11. [Leader Assignment](#11-leader-assignment)
12. [Request Routing](#12-request-routing)
13. [Coordinator](#13-coordinator)
14. [Failure Handling](#14-failure-handling)
15. [Storage Engine](#15-storage-engine)
16. [Health & Observability](#16-health--observability)
17. [Wire Protocol](#17-wire-protocol)
18. [Configuration](#18-configuration)
19. [Open Questions](#19-open-questions)

---

## 1. Vision & Philosophy

### Goal

Doki is a learning-focused distributed database. Its purpose is to make distributed systems concepts concrete by building a real, runnable, testable system.

Doki is not a production system. It is optimized for:

- **Clarity** — every mechanism should be understandable without deep prior expertise
- **Correctness** — a wrong answer delivered fast is worse than a slow correct one
- **Testability** — the system should be easy to exercise, observe, and break deliberately
- **Incremental evolution** — new complexity should be addable without rewriting core components

### What Doki Is Not

- A performance benchmark
- A replacement for etcd, CockroachDB, or any production system
- A complete Raft implementation
- A Byzantine-fault-tolerant system

### Guiding Principles

| Principle | Implication |
|-----------|-------------|
| Correctness first | Quorum is required; no degraded writes |
| Simple over clever | Full-state snapshots, not incremental log replay in v1 |
| Build in layers | Each component has a clear interface and can be tested alone |
| Observable by default | Every node exposes structured status |
| Embrace evolution | v1 is not the last word; design for extension |

---

## 2. System Overview

### Components

#### Coordinator

The coordinator is the **control plane**. It is a single process (no HA in v1) responsible for:

- Maintaining the cluster membership list (statically configured)
- Defining the shard map: which nodes host which shards
- Assigning leaders to shards
- Serving routing metadata to clients and nodes
- Detecting node failures via heartbeat

The coordinator does not store user data. It stores only metadata.

#### Leaf Nodes

Each leaf node is a **data plane** process responsible for:

- Hosting one or more shard replicas
- Participating in replication as leader or follower
- Serving client reads (leader only)
- Applying writes from leader (follower)
- Exposing health and status

#### Clients

Clients are the external interface. In v1 they:

- Issue `Put(key, value)`, `Delete(key)`, `Get(key)` requests
- May contact any node
- Are redirected or proxied to the leader if needed
- Query the coordinator for routing hints (shard map)

---

### Topology Example

```
Coordinator
  - Shard 0 → leader: A, followers: B, C
  - Shard 1 → leader: B, followers: C, D

Node A: hosts Shard 0 (leader)
Node B: hosts Shard 0 (follower), Shard 1 (leader)
Node C: hosts Shard 0 (follower), Shard 1 (follower)
Node D: hosts Shard 1 (follower)
```

---

## 3. Semantics

### Consistency Model

Doki provides **linearizability per shard**.

- All operations on a shard appear to execute atomically and in some total order consistent with real time
- The leader defines this order
- A client always sees a result consistent with all prior acknowledged writes

Cross-shard operations do not have atomicity guarantees in v1.

### Write Semantics

A write is **committed** when it has been replicated to a quorum (majority) of replicas.

For a 3-replica shard: quorum = 2 (leader + 1 follower).

A committed write is guaranteed to survive the loss of any minority of replicas.

A write that cannot reach quorum **fails**. The system does not degrade to eventual consistency.

### Read Semantics

All reads are served by the **leader**. This ensures reads always see the latest committed state.

Followers do not serve reads directly. They may proxy to the leader.

> **Decision:** Leader-only reads are simple and correct. Stale reads from followers introduce complexity (read leases, versioned reads) that is not worth it in v1.

### Failure Semantics

Doki uses a **crash-stop** failure model:

- A failed node stops and does not resume with corrupted state
- Nodes do not send incorrect messages (no Byzantine behavior)
- Network partitions may occur but are not fully handled in v1

Under a partition, if a shard's leader cannot reach quorum, writes fail. No split-brain writes are permitted.

### Availability Trade-off

Doki explicitly chooses **consistency over availability** (CP in CAP terms):

- If quorum is unavailable, writes fail
- The system does not attempt to serve stale reads or accept degraded writes
- This is a deliberate learning choice — implementing CA would undermine the purpose

---

## 4. Data Model

### Keyspace

The keyspace is a flat namespace of byte-string keys. Keys are arbitrary byte sequences. Values are arbitrary byte sequences.

```
key:   []byte   (arbitrary, no structure enforced at storage layer)
value: []byte
```

### Operations

| Operation | Description |
|-----------|-------------|
| `Put(key, value)` | Write or overwrite a key |
| `Delete(key)` | Remove a key |
| `Get(key)` | Read a key; returns value or not-found |

There is no scan, range query, or transaction in v1.

### Versioning

Each shard has a monotonically increasing **version counter**. The version increments on every committed write. This is used for:

- Replication ordering
- Recovery state comparison
- Follower lag detection

The version is a 64-bit unsigned integer. It starts at 0 and never resets (unless a node is fully rebuilt from a snapshot).

---

## 5. Sharding

### v1: Static Explicit Shards

Shards are defined in the coordinator's configuration file at startup. There is no dynamic re-sharding in v1.

Each shard has:
- A shard ID (e.g., `shard-0`, `shard-1`)
- A set of replica nodes
- An assigned leader (may change via coordinator)

Example configuration:

```yaml
shards:
  - id: shard-0
    replicas: [node-a, node-b, node-c]
    leader: node-a

  - id: shard-1
    replicas: [node-b, node-c, node-d]
    leader: node-b
```

### Shard Map

The **shard map** is the coordinator's view of the current shard layout. It is versioned (monotonically incrementing integer) so clients can detect staleness.

```
ShardMap {
  version:  uint64
  shards:   map<ShardId, ShardInfo>
}

ShardInfo {
  shard_id:  ShardId
  replicas:  []NodeId
  leader:    NodeId
}
```

### Key Routing

In v1, key-to-shard routing is explicit: the coordinator config specifies which shard owns which key range or key set.

> **Decision for v1:** Rather than hash-based routing, keys are routed to shards by explicit configuration or by a simple modulo hash over the key. This avoids implementing a consistent hash ring in v1 while still making the routing mechanism pluggable.

A simple routing rule:

```
shard_index = hash(key) % num_shards
```

This is a deliberate simplification. In future versions this will be replaced with range-based or consistent-hash routing.

### Shard Map Caching

Clients and nodes cache the shard map. When a request is routed incorrectly (e.g., to a stale leader), the receiving node returns a `NOT_LEADER` response with the current leader hint. The client updates its cache and retries.

---

## 6. Replica State Model

Each replica maintains the following state:

```
ReplicaState {
  shard_id:   ShardId          // which shard this replica serves
  node_id:    NodeId           // identity of this node
  role:       Role             // LEADER or FOLLOWER
  leader_id:  NodeId           // current known leader for this shard
  term:       uint64           // leader epoch; incremented on each new leader
  version:    uint64           // monotonic commit counter; incremented per write
  kv:         map<key, value>  // the actual data
  is_ready:   bool             // false during recovery
  peers:      []NodeId         // other replica nodes for this shard
}
```

### Term vs Version

| Field | Meaning | When it changes |
|-------|---------|-----------------|
| `term` | Leader epoch | Every time a new leader is assigned |
| `version` | Commit counter | Every committed write |

The term is used to detect stale leaders. If a node receives a replication message with a lower term than its own, it rejects it.

The version is used to compare replica freshness during leader election and recovery.

### Readiness

A replica is **not ready** (`is_ready = false`) when it is recovering — i.e., it has requested a snapshot from the leader but has not yet applied it. While not ready, a follower does not serve traffic and does not count toward quorum.

---

## 7. Write Protocol

### Happy Path

```
Client → Leader: Put(shard_id, key, value)

Leader:
  1. Check: am I still leader? (check term)
  2. Increment version: version++
  3. Create replication payload: ReplicateRequest{term, version, op: Put(key, value)}
  4. Send ReplicateRequest to all followers in parallel
  5. Wait for ACK from quorum (majority, including self)
  6. Apply write to local kv store
  7. Return success to client

Follower (on receiving ReplicateRequest):
  1. Check: is term >= my term?
  2. Apply op to local kv store
  3. Update version
  4. Return ACK to leader
```

### Quorum Counting

For a shard with `n` replicas, quorum requires `floor(n/2) + 1` acknowledgments, including the leader itself.

| Replicas | Quorum | Tolerated failures |
|----------|--------|--------------------|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

> **Decision:** n=3 is the standard configuration. This tolerates 1 failure, which is the practical minimum for useful fault tolerance without excessive complexity.

### Simplifications in v1

- No uncommitted/committed phase — followers apply immediately on receipt
- No log — state is the canonical truth
- No rollback — if a follower diverges (e.g., due to a bug), recovery is via full snapshot

These simplifications mean that if a follower applies a write and the leader fails before reaching quorum, the follower may have applied an operation that was never acknowledged to the client. In v1 this is acceptable: clients that received an error should retry.

> **Known Limitation:** Without a proper two-phase commit or log, it is possible for followers to have applied writes the client considers failed. This is addressed in a future evolution phase with a proper replication log.

### Write Timeout and Retry

The leader waits for quorum with a configurable timeout (default: 1 second). If quorum is not reached:

- The write is **not** committed
- The client receives a `QUORUM_UNAVAILABLE` error
- The client may retry

The leader does not retry the write itself. It is the client's responsibility to retry idempotently (e.g., using application-level idempotency keys if needed).

---

## 8. Read Protocol

### Leader-Only Reads

All reads are served by the shard leader. The leader serves the read directly from its local `kv` store.

```
Client → Leader: Get(shard_id, key)
Leader: return kv[key] (or NOT_FOUND)
```

Because all committed writes have been applied to the leader before acknowledgment, the leader's state is always up to date.

### Follower Redirect

If a client sends a read to a follower, the follower:

1. Rejects the request with `NOT_LEADER`
2. Includes `leader_hint: NodeId` in the response

The client updates its routing cache and retries against the leader.

Alternatively, a follower may **proxy** the read to the leader and return the result. This is simpler for the client but adds a network hop.

> **Decision for v1:** Followers return `NOT_LEADER` with a leader hint rather than proxying. Proxying is a future optimization.

---

## 9. Replication

See [replication.md](replication.md) for the full protocol detail.

### Summary

- Replication is **synchronous**: the leader waits for quorum before acknowledging
- Followers apply writes immediately upon receipt (no staging phase)
- Replication is push-based: the leader pushes to followers
- Failed followers are retried in the background
- Severely lagging followers recover via full snapshot

### Replication Message

```
ReplicateRequest {
  shard_id:  ShardId
  term:      uint64        // leader's current term
  version:   uint64        // new version after this write
  op:        Operation     // Put(key, value) or Delete(key)
}

ReplicateResponse {
  success:   bool
  term:      uint64        // follower's current term (for stale leader detection)
}
```

If `response.term > request.term`, the leader knows it is stale and steps down.

---

## 10. Recovery

### Trigger Conditions

A replica enters recovery when:

- It restarts after a crash
- It is added to a shard for the first time
- It detects its version is far behind the leader

### Recovery Protocol

```
Recovering node → Coordinator: WhereIsLeader(shard_id)
Coordinator → Node: leader_id

Node → Leader: SyncRequest{shard_id}
Leader → Node: FullStateSnapshot{term, version, kv_map}

Node:
  1. Replace local kv store with snapshot kv_map
  2. Set version = snapshot.version
  3. Set term = snapshot.term
  4. Set is_ready = true
  5. Begin accepting replication messages
```

### Consistency of Snapshot

The leader creates the snapshot atomically with respect to incoming writes. While the snapshot is being sent, new writes may be committed. The leader tracks the version at snapshot time; the recovering node applies any replication messages with `version > snapshot.version` after applying the snapshot.

> **Implementation note:** The simplest approach is to pause writes on the shard briefly while creating the snapshot. This is acceptable in v1 given no performance requirements. A more sophisticated approach uses copy-on-write semantics.

### Trust Model

Followers trust the leader completely. When a follower receives a snapshot, it **replaces** its entire local state. There is no validation that the snapshot is "correct" beyond checking the term.

---

## 11. Leader Assignment

### v1: Coordinator-Driven

In v1, the coordinator is the sole authority for leader assignment:

1. Initial leaders are defined in the coordinator config
2. On node failure, the coordinator detects the failure (via missed heartbeats)
3. The coordinator selects a new leader from the available replicas
4. Preference: the replica with the highest `version` (most up-to-date)
5. The coordinator writes the new assignment and notifies all nodes

### Heartbeat

Each node sends a heartbeat to the coordinator every `heartbeat_interval` (default: 500ms). If the coordinator does not receive a heartbeat within `failure_timeout` (default: 3x heartbeat = 1500ms), it considers the node failed.

> **Decision:** 3x heartbeat as failure timeout is a conventional default. It avoids false positives under normal jitter while still detecting failures quickly.

### Leader Notification

When the coordinator assigns a new leader:

1. It updates the shard map (increments shard map version)
2. It sends `AssignLeader(shard_id, new_leader_id, new_term)` to the new leader
3. It sends `SetFollower(shard_id, leader_id, new_term)` to all followers
4. It responds to subsequent routing queries with the new leader

### Stale Leader Detection

A node that is no longer the leader may still believe it is (e.g., due to a partition). Detection mechanisms:

1. **Term check:** Every replication message carries the sender's term. If a receiver has a higher term, it rejects the message and notifies the sender.
2. **Coordinator query:** A follower that receives a write from a node that is not the coordinator-assigned leader rejects it.

### Limitations in v1

- No quorum-based election — the coordinator is a single point of failure for leadership decisions
- No split-brain guarantee under network partitions — if the coordinator is partitioned from the cluster, no new leader can be elected, but the old leader may continue serving (reads succeed; writes may fail if followers are unreachable)
- This is acceptable for v1 given the learning focus

---

## 12. Request Routing

### Client Routing Strategy

1. Client queries coordinator for shard map on startup
2. Client caches shard map (invalidated by `NOT_LEADER` responses)
3. Client determines shard for a given key using the routing function
4. Client sends request to known leader for that shard
5. If the response is `NOT_LEADER` (with hint), client updates cache and retries once
6. If the response is `REDIRECT(leader_id)`, client retries against the indicated node

### Node-Level Routing

When a node receives a request for a shard it does not own:

- Return `WRONG_NODE` error with routing hint

When a follower receives a write or read request:

- Return `NOT_LEADER` error with `leader_hint`

---

## 13. Coordinator

### Responsibilities

| Function | Description |
|----------|-------------|
| Cluster registry | Maintains the authoritative list of nodes |
| Shard map | Defines shard→replica mappings |
| Leader assignment | Assigns and reassigns shard leaders |
| Health monitoring | Tracks heartbeats; detects failures |
| Routing service | Answers `WhereIsLeader(shard_id)` queries |

### State

```
CoordinatorState {
  nodes:     map<NodeId, NodeInfo>
  shard_map: ShardMap
  config:    ClusterConfig
}

NodeInfo {
  node_id:       NodeId
  address:       string
  last_heartbeat: timestamp
  is_alive:      bool
}
```

### APIs

```
// Called by nodes
Heartbeat(node_id) → HeartbeatResponse

// Called by clients and nodes
GetShardMap() → ShardMap
WhereIsLeader(shard_id) → NodeId

// Internal (coordinator-initiated)
AssignLeader(shard_id, leader_id, term) → void
SetFollower(shard_id, leader_id, term) → void
```

### Coordinator Failure in v1

The coordinator is a single process with no replication or persistence in v1.

On coordinator restart:
- It reloads configuration from disk
- It waits for nodes to heartbeat in
- It reassigns leaders once it has enough information

During coordinator downtime:
- Existing leaders continue serving reads and writes
- No new leader elections occur
- Routing metadata is stale (clients use cached shard map)

This is a known and accepted limitation of v1. Coordinator HA is a future phase.

---

## 14. Failure Handling

### Node Failure

| Scenario | Behavior |
|----------|----------|
| Follower fails | Leader continues; quorum may still be achievable. Writes succeed if remaining replicas form quorum. |
| Leader fails | Coordinator detects via missed heartbeat. Assigns new leader. Existing in-flight writes are lost (client retries). |
| Coordinator fails | Cluster continues with current leaders. No new leader elections. Clients use cached routing. |

### Network Partition

Doki does not fully handle network partitions in v1. The expected behavior:

- If a leader is partitioned from its followers, writes fail (quorum unavailable)
- The leader does not step down automatically — it waits for quorum
- The coordinator may detect that the old leader is unreachable and assign a new one
- This can result in **two leaders briefly believing they are leader** (split-brain window)
- The new leader will have a higher term; the old leader will be rejected when it tries to replicate
- No data corruption is expected, but in-flight writes to the old leader will be lost

> **Known Gap:** Full split-brain protection requires a distributed consensus protocol (Raft). This is a planned evolution.

### Follower Lag

If a follower is behind by more than `max_lag_versions` (default: 1000), the leader initiates a snapshot push to the lagging follower. The follower does not count toward quorum until it has completed recovery.

---

## 15. Storage Engine

### v1: In-Memory

```cpp
std::map<std::string, std::string> kv;
```

Simple, ordered, no persistence. On process restart, all data is lost. Recovery is always via snapshot from leader.

Operations:
- `Get(key)` → O(log n)
- `Put(key, value)` → O(log n)
- `Delete(key)` → O(log n)
- `Snapshot()` → O(n) full copy

### v2: Write-Ahead Log (Planned)

When durability is added:

1. Writes are appended to a WAL before being applied to the in-memory state
2. On restart, the WAL is replayed to reconstruct state
3. Snapshots are written to disk periodically; the WAL is truncated

### Storage Interface

The storage layer should be abstracted behind an interface to allow swapping:

```cpp
class Storage {
 public:
  virtual Status Get(const Key& key, Value* value) = 0;
  virtual Status Put(const Key& key, const Value& value) = 0;
  virtual Status Delete(const Key& key) = 0;
  virtual Snapshot TakeSnapshot() = 0;
  virtual Status ApplySnapshot(const Snapshot& snap) = 0;
};
```

This interface is established in v1 even though only the in-memory implementation exists.

---

## 16. Health & Observability

### Node Status API

Every node exposes a `/status` endpoint returning:

```json
{
  "node_id": "node-a",
  "shards": [
    {
      "shard_id": "shard-0",
      "role": "LEADER",
      "term": 3,
      "version": 142,
      "is_ready": true,
      "peers": ["node-b", "node-c"],
      "peer_versions": {
        "node-b": 141,
        "node-c": 142
      }
    }
  ],
  "uptime_seconds": 3600
}
```

### Coordinator Status API

The coordinator exposes a `/status` endpoint returning:

```json
{
  "shard_map_version": 7,
  "nodes": [
    {"node_id": "node-a", "is_alive": true, "last_heartbeat_ms": 200},
    {"node_id": "node-b", "is_alive": true, "last_heartbeat_ms": 380},
    {"node_id": "node-c", "is_alive": false, "last_heartbeat_ms": 5200}
  ],
  "shards": [
    {"shard_id": "shard-0", "leader": "node-a", "replicas": ["node-a", "node-b", "node-c"]}
  ]
}
```

### Logging

All components use structured logging (JSON lines). Log levels: DEBUG, INFO, WARN, ERROR.

Key events that must be logged:

| Event | Level |
|-------|-------|
| Write committed | DEBUG |
| Write failed (quorum) | WARN |
| Leader assigned | INFO |
| Leader stepped down | INFO |
| Node failure detected | WARN |
| Recovery started | INFO |
| Recovery completed | INFO |
| Snapshot sent | INFO |
| Term conflict detected | WARN |

---

## 17. Wire Protocol

### gRPC

All communication uses gRPC with Protocol Buffers. This provides:

- Strongly-typed interfaces
- Easy code generation
- Bidirectional streaming (useful for recovery)
- Built-in deadline/timeout support

### Service Definitions (sketch)

```protobuf
service NodeService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);

  rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
  rpc SyncState(SyncRequest) returns (stream SnapshotChunk);

  rpc GetStatus(StatusRequest) returns (NodeStatus);
}

service CoordinatorService {
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc GetShardMap(ShardMapRequest) returns (ShardMap);
  rpc WhereIsLeader(LeaderQuery) returns (LeaderResponse);
  rpc GetStatus(StatusRequest) returns (CoordinatorStatus);
}
```

---

## 18. Configuration

### Coordinator Config (YAML)

```yaml
coordinator:
  address: "0.0.0.0:7000"
  heartbeat_interval_ms: 500
  failure_timeout_ms: 1500

nodes:
  - id: node-a
    address: "node-a:8000"
  - id: node-b
    address: "node-b:8000"
  - id: node-c
    address: "node-c:8000"

shards:
  - id: shard-0
    replicas: [node-a, node-b, node-c]
    initial_leader: node-a
  - id: shard-1
    replicas: [node-b, node-c, node-d]
    initial_leader: node-b

replication:
  quorum_timeout_ms: 1000
  max_lag_versions: 1000
```

### Node Config (YAML)

```yaml
node:
  id: "node-a"
  address: "0.0.0.0:8000"
  coordinator_address: "coordinator:7000"
  data_dir: "/data"
```

---

## 19. Open Questions

These are the known open questions. Each has a recommended answer for v1 to avoid design paralysis, with notes on when to revisit.

### Replication

**Q: Should followers stage (buffer) writes before applying, or apply immediately?**

> **v1 answer:** Apply immediately. This is simpler. The downside (uncommitted state on follower) is accepted in v1. Revisit when implementing a proper replication log in v2.

**Q: How to handle version conflicts if a stale follower receives a write with a skipped version?**

> **v1 answer:** If a follower receives a write with `version > local_version + 1`, it triggers recovery (full snapshot). This is heavy-handed but correct.

**Q: Should replication be pipelined (multiple in-flight writes)?**

> **v1 answer:** No. One write at a time on the leader. Simple. Revisit in v3 for throughput.

### Leader Election

**Q: When to move off coordinator-driven election?**

> **v1 answer:** Not in v1 or v2. Coordinator-driven is fine until we want to handle coordinator failure with HA. Target v4 for Raft-like election.

**Q: How to guarantee safety under partitions?**

> **v1 answer:** We don't, fully, in v1. The coordinator can create a split-brain window. Document this as a known limitation. Add fencing tokens (via term) to reduce the impact. Full solution is v4.

### Recovery

**Q: When to introduce incremental catch-up?**

> **v1 answer:** Not in v1. Full snapshot is correct and simple. Incremental catch-up targets v3.

**Q: Snapshot vs log for recovery?**

> **v1 answer:** Snapshot only. Log-based recovery requires a log (v2). After v2, combine: snapshot + tail of log.

### Sharding

**Q: Hash vs range partitioning?**

> **v1 answer:** Simple modulo hash. No range queries in v1, so range partitioning offers no benefit. Revisit in v5 or when SQL layer needs range scans.

**Q: When to support shard split/merge?**

> **v1 answer:** Not before v5. Requires dynamic cluster membership and significant coordinator complexity.

### Durability

**Q: When to require fsync?**

> **v1 answer:** Not in v1. In-memory only is acceptable. WAL + fsync targets v2. Until then, data does not survive process restart.

**Q: What are the restart guarantees?**

> **v1 answer:** None. A restarted node always recovers from the leader. This is explicit and documented.

### SQL

**Q: When to introduce indexes?**

> **v1 answer:** Not before v6. Primary-key-only access is sufficient for initial SQL layer.

**Q: How to handle multi-shard queries?**

> **v1 answer:** Not supported in v1 SQL. All queries must target a single shard (single primary key). Cross-shard joins and transactions are future work.

### Testing

**Q: How to simulate network partitions in integration tests?**

> **v1 answer:** Use Docker network manipulation (`docker network disconnect`) or a test proxy layer that can drop/delay packets. The test proxy (toxiproxy or a simple custom proxy) is preferred for determinism.

**Q: How deterministic should integration tests be?**

> **v1 answer:** Tests should be deterministic where possible. Use injected clocks and configurable timeouts. Avoid relying on wall-clock timing. The test harness should be able to advance time artificially to trigger timeouts.
