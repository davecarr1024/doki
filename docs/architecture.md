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
│   ├── membership.go         # Node registry, heartbeat tracking, dynamic join
│   ├── leader.go             # Leader assignment logic
│   ├── migration.go          # MigrationManager: live shard migration + split (Phase 5)
│   └── server.go             # HTTP server + handlers
├── node/                     # Node library
│   ├── replica.go            # Per-shard ReplicaState
│   ├── election.go           # Distributed leader election (Phase 4)
│   ├── recovery.go           # Incremental + bootstrap recovery (Phase 3 / 5)
│   ├── replication.go        # Quorum replication fan-out
│   ├── diskstate.go          # WAL + snapshot per shard (Phase 2)
│   └── server.go             # HTTP server + handlers, heartbeat loop
├── internal/
│   ├── clock/                # Clock interface (real + fake)
│   ├── config/               # Config types and YAML loading
│   ├── metrics/              # Prometheus metrics definitions
│   ├── replicationlog/       # Bounded circular replication log (Phase 3)
│   ├── shardmap/             # ShardMap types and operations
│   ├── snapshot/             # Atomic disk snapshot (Phase 2)
│   ├── storage/              # Storage interface + implementations
│   │   └── memory/           # In-memory storage engine
│   └── wal/                  # Write-ahead log (Phase 2)
├── internal/
│   ├── sql/                  # SQL compiler pipeline (Phase 6-9)
│   │   ├── token.go          # Token types
│   │   ├── lexer.go          # Tokenizer
│   │   ├── ast.go            # AST node types
│   │   ├── parser.go         # Recursive descent parser
│   │   ├── catalog.go        # Schema catalog + KV row encoding
│   │   ├── analyzer.go       # Semantic analysis + type checking
│   │   ├── planner.go        # Physical plan generation + optimizer
│   │   └── executor.go       # Plan execution against KV interface
├── test/
│   ├── integration/          # End-to-end tests (real servers, no Docker)
│   └── reliability/          # Chaos, load, and monkey tests (build tag: reliability)
├── proto/                    # Protobuf definitions (gRPC, Phase 1+)
│   ├── common.proto
│   ├── coordinator.proto
│   └── node.proto
├── config/                   # Example YAML configs
└── docker/                   # Dockerfiles + docker-compose
```

---

## Component Interfaces

### Coordinator → Node (Phase 1+, HTTP/JSON)

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant N as Node

    C->>N: AssignLeader(shard_id, term)
    N-->>C: OK

    C->>N: SetFollower(shard_id, leader_id, term)
    N-->>C: OK
```

### Node → Coordinator (HTTP/JSON)

```mermaid
sequenceDiagram
    participant N as Node
    participant C as Coordinator

    loop every heartbeat_interval
        N->>C: POST /heartbeat {node_id, shards[]}
        C-->>N: 200 OK {shard_map_version}
    end

    N->>C: GET /shardmap
    C-->>N: ShardMap{version, shards[], node_addresses}
```

### Operator → Coordinator: Dynamic Cluster Management (Phase 5)

```mermaid
sequenceDiagram
    participant OP as Operator
    participant C as Coordinator

    OP->>C: POST /admin/add_node {node_id, address}
    C-->>OP: 200 OK

    OP->>C: POST /admin/migrate_shard {shard_id, new_replicas}
    C-->>OP: 200 {status: migrating}
    note over C: background: shard_map version bumps,\nnew nodes recover, migration finalises

    OP->>C: POST /admin/split_shard {source_shard_id, new_shard_id, new_replicas}
    C-->>OP: 200 {status: splitting}
    note over C: new shard bootstraps from source;\nbootstrap hint cleared when all replicas ready
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

    R->>L: GET /internal/recover/{shard_id}?since_version=N
    alt incremental (gap is small)
        L-->>R: {type:"entries", entries:[...]}
        note over R: apply log entries in order
    else snapshot fallback (gap too large)
        L-->>R: {type:"snapshot", kv:{...}, version:V}
        note over R: apply snapshot atomically
    end
    R->>R: is_ready = true
    note over R: replication fan-out resumes normally
```

---

## Data Flow: Shard Migration (Phase 5)

```mermaid
sequenceDiagram
    participant OP as Operator
    participant CO as Coordinator
    participant OLD as Old Replicas
    participant NEW as New Replicas

    OP->>CO: POST /admin/migrate_shard {shard_id, new_replicas}
    CO->>CO: Replicas = old ∪ new\nIncomingReplicas = new
    CO-->>OP: 200 {status: migrating}

    note over NEW: refetchShardMap detects new shards
    NEW->>CO: GET /shardmap
    NEW->>OLD: GET /internal/recover/{shard_id}
    OLD-->>NEW: snapshot / entries

    loop health monitor tick
        CO->>CO: allReady? check IncomingReplicas\nVersionForShard > 0 for each
    end

    note over CO: all incoming replicas ready
    CO->>CO: Replicas = new only\nIncomingReplicas = []\nReassign leader from new set
    OLD->>OLD: dropShard (refetch removes old shard)
```

---

## Data Flow: Shard Split (Phase 5)

```mermaid
sequenceDiagram
    participant OP as Operator
    participant CO as Coordinator
    participant SRC as Source Shard Leader
    participant NEW as New Shard Nodes

    OP->>CO: POST /admin/split_shard {source, new_shard, new_replicas}
    CO->>CO: Create new shard with BootstrapSourceShardID=source
    CO-->>OP: 200 {status: splitting}

    NEW->>CO: GET /shardmap
    note over NEW: BootstrapSourceShardID set; is_ready=false
    NEW->>SRC: GET /internal/recover/{source_shard_id}?since_version=0
    SRC-->>NEW: snapshot of source shard

    note over NEW: apply snapshot; clear BootstrapSourceShardID; is_ready=true
    loop health monitor tick
        CO->>CO: allReady? IncomingReplicas VersionForShard > 0
    end
    CO->>CO: ClearBootstrapSource; IncomingReplicas=[]\nAssign leader for new shard
```

---

## Replica State Machine

```mermaid
stateDiagram-v2
    [*] --> NOT_READY : node starts / restarts

    NOT_READY --> NOT_READY : Bootstrap recovery in progress\n(BootstrapShardID set; fetching source shard)

    NOT_READY --> READY : recovery complete\n(snapshot/entries applied; is_ready=true)

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
    C --> D{BootstrapSourceShardID set?}
    D -- yes --> E[Mark is_ready=false\nSet bootstrap leader addr]
    D -- no --> F{Has prior WAL/snapshot?}
    F -- yes --> G[Replay WAL\nMark is_ready=true]
    F -- no --> H[Mark is_ready=false]
    E --> I[Start HTTP server]
    G --> I
    H --> I
    I --> J[Start heartbeat goroutine]
    J --> K[Start per-shard recovery loops\nfor not-ready replicas]
    K --> L[Serve traffic]
    L --> M{shardmap changed?}
    M -- new shard --> N[initShardLocked\nstartShardGoroutines]
    M -- removed shard --> O[dropShard\ncancel context]
    N --> L
    O --> L
```

### Coordinator Startup

```mermaid
flowchart TD
    A[Load config] --> B[Build shard map from ShardSpec list]
    B --> C[Assign initial leaders from config]
    C --> D[Init MigrationManager]
    D --> E[Start HTTP server]
    E --> F[Start health monitor goroutine]
    F --> G{node heartbeat received?}
    G -- yes --> H[Record timestamp + shard versions]
    H --> G
    G -- timeout --> I[Mark node dead\nReassign leader if needed]
    I --> J[CheckMigrations\nfinalise if all incoming ready]
    J --> G
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
    GS[HTTP Server goroutine] --> |dispatch| SH[Shard handlers]
    SH --> |per-shard lock| RS[ReplicaState]
    HR[Heartbeat goroutine] --> CO[Coordinator]
    RP[Replication goroutines\none per follower] --> FN[Follower nodes]
    RC[Recovery goroutine\nper not-ready shard] --> LN[Leader node]
    EL[Election timer goroutine\nper shard] --> RS
    RS --> KV[Storage engine]
    RS --> WL[WAL + Snapshot\nDiskState]
```

Each shard's `ReplicaState` is protected by its own `sync.RWMutex`. No shard lock is ever held while acquiring another shard's lock.

Each shard also has its own `context.CancelFunc` stored in the node's `cancelFuncs` map. When a shard is removed (e.g. after migration away), `dropShard` cancels the context, stopping all background goroutines for that shard cleanly.

The coordinator uses a single `sync.RWMutex` for its state — acceptable in v1.

---

## SQL Compiler Pipeline (Phase 6-9)

The SQL layer sits above the KV engine. It compiles SQL text into KV operations through a standard compiler pipeline:

```mermaid
flowchart LR
    SQL["SQL text"] --> LEX["Lexer\n(token.go)"]
    LEX --> PAR["Parser\n(parser.go)"]
    PAR --> ANA["Analyzer\n(analyzer.go)"]
    ANA --> PLN["Planner\n(planner.go)"]
    PLN --> EXE["Executor\n(executor.go)"]
    EXE --> KV["KV Interface"]
    CAT["Catalog\n(catalog.go)"] --> ANA
    CAT --> EXE
```

### Stages

| Stage | Input | Output | Responsibility |
|-------|-------|--------|---------------|
| Lexer | SQL string | `[]Token` | Tokenise; skip whitespace and `--` comments |
| Parser | `[]Token` | `Statement` (AST) | Recursive descent; grammar for CREATE/INSERT/SELECT/UPDATE/DELETE |
| Analyzer | `Statement` + `Catalog` | `ResolvedStatement` | Name resolution, type checking, PK WHERE enforcement |
| Planner | `ResolvedStatement` | `PhysicalPlan` | Map to KV operations; optimize PK WHERE → PointGet |
| Executor | `PhysicalPlan` + `KV` + `Catalog` | `*ResultSet` | Execute KV ops; encode/decode JSON rows; project columns |

### Physical Plan Types

| Plan | KV Operation | When used |
|------|-------------|-----------|
| `PointGet` | `kv.Get(table/pk)` | SELECT WHERE pk = ? |
| `TableScan` | `kv.Scan(table/)` | SELECT with no WHERE |
| `InsertPlan` | `kv.Put(table/pk, row)` | INSERT |
| `PointUpdate` | `kv.Get` + `kv.Put` | UPDATE WHERE pk = ? |
| `PointDelete` | `kv.Delete(table/pk)` | DELETE WHERE pk = ? |
| `CreateTablePlan` | `catalog.CreateTable` | CREATE TABLE |

### KV Encoding

Rows are stored as JSON at keys of the form `table/pk_value`:
```
key:   "users/42"
value: {"id":42,"name":"alice","active":true}
```

### Supported SQL (v1)

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL);
INSERT INTO users VALUES (1, 'alice', true);
INSERT INTO users (id, name) VALUES (2, 'bob');
SELECT * FROM users;
SELECT id, name FROM users WHERE id = 1;
UPDATE users SET name = 'alice2' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

---

## Security (v1: None)

v1 has no authentication, authorization, or encryption. All traffic is plaintext HTTP/JSON (Phase 0) or plaintext gRPC (Phase 1+). This is acceptable for a local development and learning environment.

Future: mTLS for node-to-node communication; token-based auth for clients.
