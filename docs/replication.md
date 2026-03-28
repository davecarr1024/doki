# Doki — Replication Protocol

This document describes the replication protocol in detail: the write path, quorum mechanics, failure handling, and recovery.

---

## Overview

Doki uses **synchronous quorum replication**:

- The leader receives a write from a client
- The leader replicates to all followers in parallel
- Followers apply the write immediately and ACK
- The write is committed when a quorum (majority) of replicas acknowledge **apply**
- The leader applies the write locally and returns success to the client

This design is simple, correct, and easy to reason about. It sacrifices throughput for clarity.

---

## Core Invariants

These invariants must hold at all times:

1. **Single leader per shard.** Only one node acts as leader for a shard at any given term.
2. **Quorum before commit.** A write is never returned as success unless a quorum of replicas have applied it.
3. **Monotonic versions.** The version counter for a shard never decreases.
4. **Leader completeness.** A newly elected leader has seen all committed writes (enforced by leader selection policy: prefer highest-version replica).
5. **Followers trust leader.** A follower never applies writes from a node that is not the current leader (checked via term).

---

## Write Protocol: Step by Step

### Happy Path (3-node shard, quorum=2)

```mermaid
sequenceDiagram
    participant CL as Client
    participant A as Node A (Leader)
    participant B as Node B (Follower)
    participant C as Node C (Follower)

    CL->>A: Put(shard_id="shard-0", key="x", value="hello")

    note over A: pre-checks pass<br/>version++ → 143

    par Replicate to all followers
        A->>B: Replicate(term=7, version=143, Put("x","hello"))
        A->>C: Replicate(term=7, version=143, Put("x","hello"))
    end

    B->>B: apply Put("x","hello")
    B-->>A: ACK(success=true, term=7)

    C->>C: apply Put("x","hello")
    C-->>A: ACK(success=true, term=7)

    note over A: quorum reached (A+B or A+C)
    A->>A: apply Put("x","hello"), version=143
    A-->>CL: OK
```

### Follower Failure During Write

```mermaid
sequenceDiagram
    participant CL as Client
    participant A as Node A (Leader)
    participant B as Node B (Follower)
    participant C as Node C (DEAD)

    CL->>A: Put("x", "2")
    note over A: version++ → 144

    par
        A->>B: Replicate(v=144, Put("x","2"))
        A->>C: Replicate(v=144, Put("x","2"))
    end

    B-->>A: ACK

    note over A,C: C timeout — but quorum already reached (A+B)

    A->>A: apply locally
    A-->>CL: OK

    note over C: C recovers via incremental log or full snapshot (Phase 3)
```

### Write Failure (Quorum Unavailable)

```mermaid
sequenceDiagram
    participant CL as Client
    participant A as Node A (Leader)
    participant B as Node B (DEAD)
    participant C as Node C (DEAD)

    CL->>A: Put("x", "3")
    par
        A->>B: Replicate(v=145, ...)
        A->>C: Replicate(v=145, ...)
    end

    note over A: timeout — no ACKs received<br/>quorum not reached (need 2, got 1)

    A-->>CL: QUORUM_UNAVAILABLE
    note over A: force recover followers that applied
```

---

## Leader Pre-Checks

Before replicating, the leader verifies:

1. It is still the current leader (`role == LEADER`)
2. The shard is ready (`is_ready == true`)
3. It has recently heard from enough followers to form a quorum (fast-fail if quorum is clearly unavailable)

If any check fails, the leader returns an error immediately without attempting replication.

---

## Quorum Counting

```
quorum = floor(n/2) + 1
```

| Replicas | Quorum | Tolerated failures |
|----------|--------|--------------------|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

The leader counts itself as an implicit ACK. It only needs `quorum - 1` follower ACKs.
Followers marked `not_ready` are excluded from quorum eligibility.
In practice, the leader treats peers as eligible if they have responded recently to
leader heartbeats or replication requests.

---

## Read Protocol

Reads do not involve replication. The leader reads directly from its local KV store.

```mermaid
sequenceDiagram
    participant CL as Client
    participant L as Leader

    CL->>L: Get(shard_id, key)
    L->>L: read kv[key]
    L-->>CL: value (or NOT_FOUND)
```

All committed writes have been applied to the leader before returning `OK`. Therefore, leader reads are always linearizable.

---

## Heartbeat Between Leader and Followers

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower

    loop every 500ms
        L->>F: Heartbeat(term, shard_id)
        F-->>L: ACK
    end

    note over F: if no heartbeat for 1500ms:<br/>mark leader as potentially dead<br/>notify coordinator
```

The leader uses the liveness set (followers that have responded recently) to fast-fail writes when quorum is clearly unavailable.

---

## Term and Stale Leader Detection

```mermaid
sequenceDiagram
    participant OL as Old Leader (Node A, term=3)
    participant NL as New Leader (Node B, term=4)
    participant F as Follower (Node C)

    note over NL: Coordinator assigned new leader<br/>term incremented to 4

    OL->>F: Replicate(term=3, ...)
    F-->>OL: TERM_MISMATCH(my_term=4)

    note over OL: receives higher term<br/>steps down

    OL->>OL: role = FOLLOWER<br/>term = 4
    OL->>NL: SyncState(shard_id)
    NL-->>OL: FullStateSnapshot
```

Every replication message carries the sender's term. If `response.term > request.term`, the sender is stale and must step down.

---

## Follower Recovery

### Trigger Conditions

```mermaid
flowchart TD
    A{Receive ReplicateRequest} --> B{version == local_version + 1?}
    B -- yes --> C[Apply, ACK]
    B -- no --> D[Mark not-ready]
    D --> E[Request recovery from leader]
    E --> F[Apply snapshot or log entries]
    F --> G[Mark is_ready=true]
```

### Recovery Protocol (Phase 3: Incremental Log)

The default recovery path uses the leader's in-memory replication log to avoid full snapshot transfers for small gaps.

```mermaid
sequenceDiagram
    participant R as Recovering Node
    participant CO as Coordinator
    participant L as Leader

    R->>CO: GET /shardmap
    CO-->>R: {leader: "node-a"}

    note over R: current version = 140
    R->>L: GET /internal/recover/shard-0?since_version=140

    alt log covers the gap (version 141–144 in log)
        L-->>R: {type:"entries", version:144, entries:[v141,v142,v143,v144]}
        note over R: apply entries 141-144<br/>version=144, is_ready=true
    else gap too large (entries evicted)
        L-->>R: {type:"snapshot", version:144, kv:{...}}
        note over R: apply full snapshot<br/>version=144, is_ready=true
    end

    note over L: future writes replicate normally
    L->>R: Replicate(v=145, ...)
    R->>R: apply v=145
    R-->>L: ACK (applied)
```

**Log size default:** 1000 entries (configurable via `replication_log_size` in node config).
**Fallback:** full snapshot when the follower's `since_version` is more than `replication_log_size` writes behind the leader.

### Snapshot Streaming

For large datasets, the snapshot is streamed in chunks:

```
SnapshotChunk {
  shard_id:      ShardId
  term:          uint64
  final_version: uint64   // version at time of snapshot
  chunk_index:   uint32
  is_last:       bool
  entries:       []KVEntry  // batch of key-value pairs
}
```

The follower accumulates chunks until `is_last = true`, then atomically applies the full state.

---

## Divergence Handling

Followers apply entries immediately, but they only keep state that matches the
leader's committed history. If a quorum attempt fails after some followers apply,
the leader forces those followers to recover from its committed snapshot.

---

## Follower State Machine

```mermaid
stateDiagram-v2
    [*] --> NOT_READY

    NOT_READY --> READY : snapshot applied from leader

    READY --> READY : Replicate(v = local_v+1) → apply + ACK

    READY --> NOT_READY : version gap detected\nOR forced recovery

    NOT_READY --> NOT_READY : awaiting snapshot chunks
```

---

## Recovery Reuse for Shard Migration (Phase 5)

The same incremental recovery protocol (`GET /internal/recover/{shard_id}?since_version=N`) is reused for live shard migration and shard splitting:

**Migration:** When a shard is migrated to new nodes, the new nodes start with `version=0` and call `GET /internal/recover/shard-id?since_version=0` against the current leader. This is exactly the same as a fresh follower joining the shard.

**Split Bootstrap:** When a new shard is split from a source shard, the new shard's nodes have `BootstrapShardID=<source>`. Their recovery loop calls `GET /internal/recover/<source-shard-id>?since_version=0` against the **source shard's leader**, copying the source shard's current KV state as the new shard's initial state. Once recovery completes, the `BootstrapShardID` is cleared and the new shard evolves independently.

This means the replication protocol is the only data transfer mechanism in the system — no separate bulk-load path is needed.

---

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `quorum_timeout_ms` | 1000 | Max time leader waits for quorum |
| `replication_timeout_ms` | 500 | Per-follower RPC timeout |
| `heartbeat_interval_ms` | 500 | Leader→follower heartbeat interval |
| `follower_failure_timeout_ms` | 1500 | Time before leader marks follower failed |
| `max_lag_versions` | 1000 | Versions behind before triggering recovery |
| `max_buffered_versions` | 100 | Write buffer size during follower recovery |
| `snapshot_chunk_size` | 1000 | KV entries per snapshot chunk |
| `replication_log_size` | 1000 | Max entries in in-memory replication log |
