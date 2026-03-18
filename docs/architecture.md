# Doki — Architecture

This document describes the component structure, interfaces, deployment topology, and module boundaries.

---

## Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Doki Cluster                            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      Coordinator                          │  │
│  │                                                          │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │  │
│  │  │  Membership  │  │  Shard Map   │  │   Leader Mgr  │  │  │
│  │  └──────────────┘  └──────────────┘  └───────────────┘  │  │
│  │  ┌──────────────┐  ┌──────────────┐                     │  │
│  │  │  Health Mon  │  │  Routing API │                     │  │
│  │  └──────────────┘  └──────────────┘                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │
│  │    Node A      │  │    Node B      │  │    Node C      │   │
│  │                │  │                │  │                │   │
│  │ Shard 0 LEADER │  │ Shard 0 FLWR   │  │ Shard 0 FLWR   │   │
│  │                │  │ Shard 1 LEADER │  │ Shard 1 FLWR   │   │
│  │ ┌────────────┐ │  │ ┌────────────┐ │  │ ┌────────────┐ │   │
│  │ │  Shard     │ │  │ │  Shard     │ │  │ │  Shard     │ │   │
│  │ │  Manager   │ │  │ │  Manager   │ │  │ │  Manager   │ │   │
│  │ └────────────┘ │  │ └────────────┘ │  │ └────────────┘ │   │
│  │ ┌────────────┐ │  │ ┌────────────┐ │  │ ┌────────────┐ │   │
│  │ │  Storage   │ │  │ │  Storage   │ │  │ │  Storage   │ │   │
│  │ └────────────┘ │  │ └────────────┘ │  │ └────────────┘ │   │
│  └────────────────┘  └────────────────┘  └────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
doki/
├── coordinator/          # Coordinator process
│   ├── main.go
│   ├── server.go         # gRPC server
│   ├── membership.go     # Node registry, heartbeat tracking
│   ├── shard_map.go      # Shard map state and versioning
│   ├── leader_manager.go # Leader assignment logic
│   └── health_monitor.go # Failure detection
│
├── node/                 # Leaf node process
│   ├── main.go
│   ├── server.go         # gRPC server (client-facing + replication)
│   ├── shard_manager.go  # Per-shard replica state management
│   ├── replicator.go     # Leader-side replication logic
│   ├── recovery.go       # Follower recovery / snapshot application
│   └── storage/
│       ├── storage.go    # Storage interface
│       └── memory.go     # In-memory implementation
│
├── proto/                # Protobuf definitions
│   ├── coordinator.proto
│   ├── node.proto
│   └── common.proto
│
├── client/               # Client library
│   ├── client.go         # High-level client API
│   └── routing.go        # Shard map cache + routing
│
├── config/               # Config loading and types
│   ├── coordinator.yaml
│   └── node.yaml
│
├── test/                 # Integration tests
│   ├── cluster/          # Test cluster helpers (Testcontainers)
│   ├── scenarios/        # Test scenarios (failover, recovery, etc.)
│   └── assertions/       # Invariant checks
│
└── docker/
    ├── Dockerfile.coordinator
    ├── Dockerfile.node
    └── docker-compose.yaml
```

> **Language choice:** The module structure above uses Go conventions. Go is a natural fit: strong standard library for networking, gRPC support, simple concurrency model, and easy cross-compilation for Docker. An equivalent structure applies if using C++ or Rust.

---

## Component Interfaces

### Coordinator → Node

The coordinator pushes control messages to nodes when leadership changes.

```
// Coordinator tells a node it is now the leader for a shard
AssignLeader(shard_id, term) → OK | Error

// Coordinator tells a node who the leader is for a shard
SetFollower(shard_id, leader_id, term) → OK | Error
```

### Node → Coordinator

Nodes push heartbeats and pull routing information.

```
// Node signals liveness
Heartbeat(node_id, shard_statuses[]) → OK

// Node queries routing
GetShardMap() → ShardMap
WhereIsLeader(shard_id) → NodeId
```

### Client → Node

Clients issue data operations to nodes (ideally leaders).

```
Put(shard_id, key, value) → OK | NOT_LEADER(hint) | QUORUM_UNAVAILABLE | Error
Get(shard_id, key) → value | NOT_FOUND | NOT_LEADER(hint) | Error
Delete(shard_id, key) → OK | NOT_LEADER(hint) | QUORUM_UNAVAILABLE | Error
```

### Leader → Follower (Replication)

Leaders push replication messages to followers.

```
Replicate(shard_id, term, version, op) → ACK | TERM_MISMATCH(my_term) | Error
SyncState(shard_id) → stream SnapshotChunk
```

### Client → Coordinator

Clients query routing metadata.

```
GetShardMap() → ShardMap
WhereIsLeader(shard_id) → NodeId
```

---

## Data Flow: Write Path

```
Client
  │
  │ Put(key, value)
  ▼
Node (Leader for shard)
  │
  ├──────────────────────────────────────────┐
  │ Replicate(term, version, Put(key,value)) │
  ▼                                          ▼
Follower 1                             Follower 2
  │ ACK                                  │ ACK
  └──────────────────────────────────────┘
                    │
              (quorum reached)
                    │
                    ▼
             Apply to local kv
                    │
                    ▼
              Return OK to Client
```

---

## Data Flow: Read Path

```
Client
  │
  │ Get(key)
  ▼
Node (Leader for shard)
  │
  │ (read from local kv, no replication needed)
  │
  └──▶ Return value to Client
```

---

## Data Flow: Recovery

```
Recovering Node
  │
  │ WhereIsLeader(shard_id)
  ▼
Coordinator
  │ leader_id = "node-a"
  ▼
Recovering Node
  │
  │ SyncState(shard_id)
  ▼
Leader (node-a)
  │
  │ stream SnapshotChunk{term, version, kv_map}
  ▼
Recovering Node
  │ Replace local state with snapshot
  │ Set version, term
  │ Mark is_ready = true
  └──▶ Begin accepting replication messages
```

---

## Deployment Topology

### Docker Compose (Development)

```
┌──────────────────────────────────────────────────┐
│                  docker network: doki             │
│                                                  │
│  coordinator:7000                                │
│                                                  │
│  node-a:8000                                     │
│  node-b:8000                                     │
│  node-c:8000                                     │
│  node-d:8000  (optional)                         │
│                                                  │
└──────────────────────────────────────────────────┘
```

Each component runs in its own container. DNS resolution is by container name. The coordinator starts first; nodes connect to it on startup.

### Startup Order

1. Start coordinator
2. Start nodes (they heartbeat in to coordinator)
3. Coordinator assigns initial leaders once all expected nodes are healthy
4. Cluster is ready

### Integration Test Topology (Testcontainers)

Tests programmatically create and destroy a cluster. Each test starts fresh. Nodes can be killed and restarted. Network partitions are simulated via a proxy layer.

---

## Process Lifecycle

### Node Startup

```
1. Load config (node_id, coordinator address, shard assignments)
2. Initialize storage engine for each shard
3. Start gRPC server
4. Send initial heartbeat to coordinator
5. For each shard:
   a. Mark shard as NOT_READY
   b. Request recovery from leader (SyncState)
   c. Apply snapshot
   d. Mark shard as READY
6. Begin serving traffic
```

### Coordinator Startup

```
1. Load config (nodes, shards, initial leaders)
2. Start gRPC server
3. Start heartbeat monitor
4. Wait for nodes to check in (or proceed with config defaults)
5. Assign initial leaders
6. Serve routing queries
```

### Graceful Shutdown

A graceful shutdown:
1. Stops accepting new client requests
2. Completes in-flight writes
3. Notifies coordinator (optional in v1)
4. Exits

Ungraceful shutdown (crash): the coordinator detects via missed heartbeat.

---

## Error Codes

| Code | Meaning |
|------|---------|
| `OK` | Success |
| `NOT_LEADER` | Request went to a follower; includes leader hint |
| `WRONG_NODE` | Request went to a node that doesn't host this shard |
| `NOT_FOUND` | Key does not exist |
| `QUORUM_UNAVAILABLE` | Leader could not reach quorum in time |
| `TERM_MISMATCH` | Sender's term is lower than receiver's term |
| `NOT_READY` | Replica is still recovering |
| `INTERNAL_ERROR` | Unexpected error; see logs |

---

## Concurrency Model

### Per-Node

Each node runs:
- One gRPC server (handles all inbound RPC calls, dispatches to handlers)
- One goroutine (or thread) per shard for replication
- One heartbeat goroutine

All access to a shard's `ReplicaState` is serialized through a per-shard mutex. There is no shared mutable state between shards.

### Per-Coordinator

The coordinator runs:
- One gRPC server
- One heartbeat check goroutine (periodic, default 500ms tick)
- All state access is serialized through a single coordinator mutex (simple, acceptable for v1)

---

## Security (v1: None)

v1 has no authentication, authorization, or encryption. All traffic is plaintext gRPC. This is acceptable for a local development and learning environment.

Future: mTLS for node-to-node communication; token-based auth for clients.
