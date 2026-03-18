# Doki — Architecture

This document describes the component structure, interfaces, deployment topology, and module boundaries.

---

## Component Overview

```mermaid
graph TB
    subgraph Cluster
        subgraph CP["Control Plane"]
            CO[Coordinator<br/>membership · shard map · leader assignment]
        end

        subgraph DP["Data Plane"]
            NA[Node A<br/>Shard 0: LEADER<br/>Shard 1: FOLLOWER]
            NB[Node B<br/>Shard 0: FOLLOWER<br/>Shard 1: LEADER]
            NC[Node C<br/>Shard 0: FOLLOWER<br/>Shard 1: FOLLOWER]
        end
    end

    CL[Client]

    CL -- "routing queries" --> CO
    CL -- "reads / writes" --> NA
    NA -- "heartbeat" --> CO
    NB -- "heartbeat" --> CO
    NC -- "heartbeat" --> CO
    CO -- "assign leader" --> NA
    CO -- "set follower" --> NB
    CO -- "set follower" --> NC
    NA -- "replicate" --> NB
    NA -- "replicate" --> NC
```

---

## Module Structure

```
doki/
├── cmd/
│   ├── coordinator/          # Entry point: coordinator binary
│   └── node/                 # Entry point: node binary
├── coordinator/              # Coordinator library
│   ├── membership.go         # Node registry, heartbeat tracking
│   ├── leader.go             # Leader assignment logic
│   └── server.go             # HTTP server + handlers
├── node/                     # Node library
│   ├── replica.go            # Per-shard ReplicaState
│   └── server.go             # HTTP server + handlers, heartbeat loop
├── internal/
│   ├── clock/                # Clock interface (real + fake)
│   ├── config/               # Config types and YAML loading
│   ├── shardmap/             # ShardMap types and operations
│   └── storage/              # Storage interface + implementations
│       └── memory/           # In-memory storage engine (v1)
├── test/
│   └── integration/          # End-to-end tests (real servers, no Docker)
├── proto/                    # Protobuf definitions (gRPC, Phase 1+)
│   ├── common.proto
│   ├── coordinator.proto
│   └── node.proto
├── config/                   # Example YAML configs
└── docker/                   # Dockerfiles + docker-compose
```

---

## Component Interfaces

### Coordinator → Node (Phase 1+, gRPC)

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant N as Node

    C->>N: AssignLeader(shard_id, term)
    N-->>C: OK

    C->>N: SetFollower(shard_id, leader_id, term)
    N-->>C: OK
```

### Node → Coordinator (HTTP, Phase 0; gRPC, Phase 1+)

```mermaid
sequenceDiagram
    participant N as Node
    participant C as Coordinator

    loop every heartbeat_interval
        N->>C: POST /heartbeat {node_id, shards[]}
        C-->>N: 200 OK
    end

    N->>C: GET /shardmap
    C-->>N: ShardMap{version, shards[]}
```

---

## Data Flow: Write Path

```mermaid
sequenceDiagram
    participant CL as Client
    participant L as Leader (Node A)
    participant F1 as Follower (Node B)
    participant F2 as Follower (Node C)

    CL->>L: Put(shard_id, key, value)
    L->>L: version++
    par Replicate in parallel
        L->>F1: Replicate(term, version, Put(key,value))
        L->>F2: Replicate(term, version, Put(key,value))
    end
    F1-->>L: ACK
    F2-->>L: ACK
    note over L: quorum reached (2/3)
    L->>L: apply to local KV
    L-->>CL: OK
```

---

## Data Flow: Read Path

```mermaid
sequenceDiagram
    participant CL as Client
    participant L as Leader (Node A)

    CL->>L: Get(shard_id, key)
    L->>L: read from local KV
    L-->>CL: value
```

If the client reaches a follower by mistake:

```mermaid
sequenceDiagram
    participant CL as Client
    participant F as Follower (Node B)
    participant L as Leader (Node A)

    CL->>F: Get(shard_id, key)
    F-->>CL: NOT_LEADER {leader_hint: "node-a"}
    CL->>L: Get(shard_id, key)
    L-->>CL: value
```

---

## Data Flow: Recovery

```mermaid
sequenceDiagram
    participant R as Recovering Node
    participant CO as Coordinator
    participant L as Leader

    R->>CO: GET /shardmap
    CO-->>R: ShardMap (leader = "node-a")

    R->>L: SyncState(shard_id)
    loop stream chunks
        L-->>R: SnapshotChunk{term, version, entries[]}
    end
    note over R: apply snapshot atomically
    R->>R: is_ready = true
    loop apply buffered writes
        L->>R: Replicate(version > snapshot.version)
    end
```

---

## Replica State Machine

```mermaid
stateDiagram-v2
    [*] --> NOT_READY : node starts / restarts

    NOT_READY --> READY : snapshot received and applied

    READY --> READY : Replicate(version = v+1) received; apply + ACK

    READY --> NOT_READY : gap detected (version != v+1)\nOR lag > max_lag_versions\n→ trigger recovery

    READY --> LEADER : AssignLeader received from coordinator

    LEADER --> FOLLOWER : TERM_MISMATCH received\n(stale leader detected)

    LEADER --> LEADER : write committed; Replicate sent to followers

    FOLLOWER --> LEADER : AssignLeader received (new election)
```

---

## Deployment Topology

### Docker Compose (Development)

```mermaid
graph LR
    subgraph docker-network["Docker Network: doki-net"]
        CO["coordinator<br/>:7000"]
        NA["node-a<br/>:8001"]
        NB["node-b<br/>:8002"]
        NC["node-c<br/>:8003"]
    end

    EX["External / tests"] -- "7000" --> CO
    EX -- "8001" --> NA
    EX -- "8002" --> NB
    EX -- "8003" --> NC

    NA -- heartbeat --> CO
    NB -- heartbeat --> CO
    NC -- heartbeat --> CO
```

### Startup Order

```mermaid
sequenceDiagram
    participant D as docker compose
    participant CO as Coordinator
    participant N as Nodes

    D->>CO: start
    CO->>CO: load config, init shard map
    CO->>CO: start health monitor
    note over CO: ready

    D->>N: start (depends_on: coordinator healthy)
    N->>CO: GET /shardmap
    CO-->>N: ShardMap
    N->>N: init replica state for each shard
    N->>N: mark shards ready (Phase 0)
    note over N: ready
    loop every 500ms
        N->>CO: POST /heartbeat
    end
```

---

## Process Lifecycle

### Node Startup

```mermaid
flowchart TD
    A[Load config] --> B[Fetch shard map from coordinator]
    B --> C[InitShards: create ReplicaState per shard]
    C --> D{Phase 0?}
    D -- yes --> E[Mark all shards is_ready=true]
    D -- no --> F[Request snapshot from leader\nMark is_ready=false]
    F --> G[Apply snapshot\nMark is_ready=true]
    E --> H[Start HTTP server]
    G --> H
    H --> I[Start heartbeat goroutine]
    I --> J[Serve traffic]
```

### Coordinator Startup

```mermaid
flowchart TD
    A[Load config] --> B[Build shard map from ShardSpec list]
    B --> C[Assign initial leaders from config]
    C --> D[Start HTTP server]
    D --> E[Start health monitor goroutine]
    E --> F{node heartbeat received?}
    F -- yes --> G[Record timestamp]
    G --> F
    F -- timeout --> H[Mark node dead\nReassign leader if needed]
    H --> F
```

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

```mermaid
graph LR
    GS[gRPC/HTTP Server goroutine] --> |dispatch| SH[Shard handlers]
    SH --> |per-shard lock| RS[ReplicaState]
    HR[Heartbeat goroutine] --> CO[Coordinator]
    RP[Replication goroutines\none per follower] --> FN[Follower nodes]
    RS --> KV[Storage engine]
```

Each shard's `ReplicaState` is protected by its own `sync.RWMutex`. No shard lock is ever held while acquiring another shard's lock.

The coordinator uses a single `sync.RWMutex` for its state — acceptable in v1.

---

## Security (v1: None)

v1 has no authentication, authorization, or encryption. All traffic is plaintext HTTP/JSON (Phase 0) or plaintext gRPC (Phase 1+). This is acceptable for a local development and learning environment.

Future: mTLS for node-to-node communication; token-based auth for clients.
