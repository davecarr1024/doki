# Doki — Replication Protocol

This document describes the replication protocol in detail: the write path, quorum mechanics, failure handling, and recovery.

---

## Overview

Doki uses **synchronous quorum replication**:

- The leader receives a write from a client
- The leader replicates to all followers in parallel
- The write is committed when a quorum (majority) of replicas acknowledge
- The leader then applies the write locally and returns success to the client

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

### 1. Client Request

```
Client → Leader: PutRequest {
  shard_id: "shard-0"
  key:      "user:42"
  value:    "alice"
}
```

The client sends the request to the node it believes is the leader. If wrong, it gets `NOT_LEADER` and retries.

### 2. Leader Pre-Checks

Before replicating, the leader verifies:

- It is still the current leader (`role == LEADER`)
- It has quorum available (at least `floor(n/2)` followers responding to recent heartbeats)
- The shard is ready

If any check fails, the leader returns an error immediately.

> **Fast failure:** The leader should not spend time waiting for quorum if it already knows quorum is unavailable. Maintain a liveness set: followers that have responded to a recent heartbeat.

### 3. Version Increment

The leader increments the shard version counter atomically:

```
new_version = current_version + 1
```

This happens before sending to followers. The version identifies this specific write.

### 4. Replicate to Followers

The leader sends `ReplicateRequest` to all followers in parallel:

```
ReplicateRequest {
  shard_id:  "shard-0"
  term:      7
  version:   143
  op:        PUT
  key:       "user:42"
  value:     "alice"
}
```

Followers receive this and:

1. Check `term >= local_term`. If not, reject with `TERM_MISMATCH`.
2. Check `version == local_version + 1`. If not, trigger recovery.
3. Apply the operation to local kv store.
4. Increment local version to match.
5. Return `ReplicateResponse { success: true, term: 7 }`.

### 5. Quorum Wait

The leader waits for `ACK` from a quorum of replicas (including itself). The quorum timeout is configurable (default: 1 second).

```
quorum = floor(n/2) + 1

// n=3: quorum=2 (leader + 1 follower)
// n=5: quorum=3 (leader + 2 followers)
```

The leader counts itself as an ACK immediately (it will apply locally on commit).

### 6. Commit and Reply

Once quorum is reached:

1. Leader applies the operation to its local kv store
2. Leader updates local version
3. Leader returns `PutResponse { success: true }` to client

If quorum is not reached within the timeout:

1. Leader does **not** apply the write
2. Leader returns `QUORUM_UNAVAILABLE` to client
3. The partially-replicated write on followers is an inconsistency that recovery will resolve

> **Note:** This is the key simplification in v1. Without a two-phase protocol, followers may have applied writes that the leader considers uncommitted. See "Divergence Handling" below.

---

## Read Protocol

Reads are simple and do not involve replication:

```
Client → Leader: GetRequest { shard_id, key }
Leader: return kv[key] from local state
```

Because all committed writes have been applied to the leader before acknowledgment, the leader's state is always at least as up-to-date as any committed write. This gives linearizable reads.

---

## Heartbeat Between Leader and Followers

In addition to replication, the leader periodically sends heartbeats to followers:

- Interval: 500ms (same as coordinator heartbeat)
- Purpose: maintain follower liveness set; detect when followers are unreachable

The heartbeat also serves as a no-op replication message (same `term`, no-op) that allows followers to detect leader liveness.

If a follower does not receive a heartbeat within `failure_timeout` (1500ms), it marks the leader as potentially dead and notifies the coordinator.

---

## Divergence Handling

### What Can Diverge?

In v1, without a two-phase commit, a follower may apply a write that the leader never commits:

```
Leader: version 142
Leader sends Replicate(version=143) to followers A and B
Follower A applies version 143 → ACK
Leader crashes before Follower B ACKs and before quorum
Leader never commits version 143

New leader is elected (say Follower B, which has version 142)
Follower A now has an "extra" write that is not in the new leader
```

This is a known inconsistency in v1. It is addressed in v2 with a proper replication log.

### v1 Mitigation

When a new leader is elected, it immediately sends a sync (heartbeat with its current `version`) to all followers. Any follower with a higher version than the new leader must roll back. In v1, rollback is implemented as a full snapshot from the new leader — the follower pulls the leader's state and overwrites.

The window for this inconsistency to be visible to clients is small: only writes that received `QUORUM_UNAVAILABLE` or no response can be affected. A client that received `OK` is guaranteed safe.

---

## Follower Recovery

### Trigger

A follower enters recovery when:

1. It restarts after a crash (version = 0, or unknown)
2. It receives a `ReplicateRequest` with `version != local_version + 1` (gap detected)
3. The leader determines the follower is lagging by more than `max_lag_versions`

### Protocol

```
Follower → Leader: SyncRequest { shard_id }

Leader:
  1. Pause incoming writes briefly (or use copy-on-write)
  2. Take snapshot: { term, version, kv_map }
  3. Stream snapshot to follower in chunks

Follower:
  1. Mark shard as NOT_READY
  2. Apply snapshot chunks
  3. Set local term = snapshot.term
  4. Set local version = snapshot.version
  5. Mark shard as READY
  6. Resume accepting ReplicateRequest messages
```

### Snapshot Streaming

For large datasets, the snapshot is streamed in chunks rather than sent as one message:

```
SnapshotChunk {
  shard_id:     ShardId
  term:         uint64
  final_version: uint64   // version at time of snapshot
  chunk_index:  uint32
  is_last:      bool
  entries:      []KVEntry  // batch of key-value pairs
}
```

The follower accumulates chunks until `is_last = true`, then atomically applies.

### Concurrent Writes During Recovery

While a follower is recovering (streaming a snapshot), the leader continues accepting writes. Writes committed after the snapshot's `final_version` are queued and sent to the follower after recovery completes.

In v1, the simplest approach:
- The leader buffers up to `max_buffered_versions` (default: 100) replication messages while a follower recovers
- If more than `max_buffered_versions` writes occur during recovery, the follower restarts recovery from the latest snapshot

---

## Term and Stale Leader Detection

### The Problem

After a network partition or slow node:

- An old leader may still believe it is leader
- A new leader may have been elected
- Both leaders may attempt to replicate to followers

### Detection

Every replication message carries the sender's `term`. The receiver checks:

```
if request.term < local_term:
    reject with TERM_MISMATCH { my_term: local_term }
```

When the old leader receives `TERM_MISMATCH` with a higher term than its own, it:

1. Steps down (changes role to FOLLOWER)
2. Updates its `term` to the received term
3. Queries the coordinator for the new leader
4. Begins recovery

### Leader Fencing

The new leader writes its term to the coordinator before accepting writes. This ensures no write can be committed under the old term.

Followers reject replication from any node with a lower term than the coordinator has told them to expect. This is the primary split-brain defense in v1.

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

---

## Replication State Machine

```
Follower States:

  [NOT_READY] ──────────────────────────────────────────────────┐
       │                                                         │
       │ SyncRequest sent; snapshot received; applied           │
       ▼                                                         │
   [READY] ──── receives Replicate(version = v+1) ──────────── │ applies, ACKs
       │                                                         │
       │ gap detected (version != v+1)                          │
       │ OR lag > max_lag_versions                              │
       └──────────────────────────────────────────────────────── ┘
                     (triggers recovery)


Leader States:

  [LEADER]
     │
     ├── receives write from client
     │   ├── replicate to followers
     │   ├── wait for quorum
     │   └── commit or fail
     │
     ├── receives Replicate from another node with term > my_term
     │   └── step down → FOLLOWER
     │
     └── receives AssignLeader from coordinator with higher term
         └── accept new term, begin serving
```

---

## Example: Normal 3-Node Write

```
Client                 Node A (leader)         Node B (follower)    Node C (follower)
  │                         │                        │                     │
  │ Put("x", "1")           │                        │                     │
  ├────────────────────────▶│                        │                     │
  │                         │ Replicate(v=5,Put x=1) │                     │
  │                         ├───────────────────────▶│                     │
  │                         │ Replicate(v=5,Put x=1) │                     │
  │                         ├────────────────────────────────────────────▶│
  │                         │                        │ ACK                 │
  │                         │◀───────────────────────┤                     │
  │                         │                        │              ACK    │
  │                         │◀────────────────────────────────────────────┤
  │                         │ (quorum reached: A+B)  │                     │
  │                         │ Apply locally          │                     │
  │ OK                      │                        │                     │
  │◀────────────────────────┤                        │                     │
```

## Example: Follower Failure During Write

```
Client                 Node A (leader)         Node B (follower)    Node C (DEAD)
  │                         │                        │
  │ Put("x", "2")           │                        │
  ├────────────────────────▶│                        │
  │                         │ Replicate(v=6,Put x=2) │
  │                         ├───────────────────────▶│
  │                         │ Replicate(v=6,Put x=2)              (timeout)
  │                         │◀ ACK ──────────────────┤
  │                         │ (quorum reached: A+B)  │
  │                         │ Apply locally          │
  │ OK                      │                        │
  │◀────────────────────────┤                        │
  │                         │ (C recovers later via snapshot)
```
