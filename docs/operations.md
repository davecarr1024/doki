# Doki — Operations Guide

This document covers running Doki: startup, configuration, observability, health checks, and common operational tasks.

---

## Running a Cluster

### Prerequisites

- Docker and Docker Compose
- (Optional) Go toolchain for building from source

### Quick Start

```bash
# Clone and start
git clone <repo-url> doki
cd doki

# Start coordinator + 3 nodes
docker compose up

# Check cluster health (gRPC)
grpcurl -plaintext \
  -import-path proto \
  -proto proto/doki/coordinator/v1/coordinator.proto \
  -d '{}' \
  localhost:7000 \
  doki.coordinator.v1.CoordinatorService/GetStatus | jq .

# Run a quick smoke test
./scripts/smoke_test.sh
```

### Docker Compose Layout

```
docker compose up
  → coordinator (port 7000)
  → node-a (port 8001)
  → node-b (port 8002)
  → node-c (port 8003)
```

The compose file sets up:
- A shared Docker network (`doki-net`)
- Persistent volumes for each node's data directory (v2+)
- Environment variables for coordinator address and node IDs

---

## Configuration

### Coordinator Config

Location: `config/coordinator.yaml` (or override via `DOKI_COORDINATOR_CONFIG` env var)

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

replication:
  quorum_timeout_ms: 1000
  max_lag_versions: 1000
```

### Node Config

Location: `config/node.yaml` (or override via `DOKI_NODE_CONFIG` env var)

```yaml
node:
  id: "node-a"
  address: "0.0.0.0:8000"
  coordinator_address: "coordinator:7000"
  data_dir: "/data"   # WAL + snapshot directory (Phase 2+)
```

### Environment Variable Overrides

All config values can be overridden via environment variables:

| Env Var | Config Key | Example |
|---------|-----------|---------|
| `DOKI_NODE_ID` | `node.id` | `node-a` |
| `DOKI_NODE_ADDRESS` | `node.address` | `0.0.0.0:8000` |
| `DOKI_COORDINATOR_ADDRESS` | `node.coordinator_address` | `coordinator:7000` |
| `DOKI_HEARTBEAT_INTERVAL_MS` | `coordinator.heartbeat_interval_ms` | `500` |
| `DOKI_LOG_LEVEL` | — | `DEBUG`, `INFO`, `WARN`, `ERROR` |

---

## Health Checks

### Coordinator Health

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/doki/coordinator/v1/coordinator.proto \
  -d '{}' \
  coordinator:7000 \
  doki.coordinator.v1.CoordinatorService/GetStatus
```

Response:

```json
{
  "shard_map_version": 7,
  "nodes": [
    {"node_id": "node-a", "is_alive": true, "last_heartbeat_ms": 210},
    {"node_id": "node-b", "is_alive": true, "last_heartbeat_ms": 380},
    {"node_id": "node-c", "is_alive": false, "last_heartbeat_ms": 5200}
  ],
  "shards": [
    {
      "shard_id": "shard-0",
      "leader": "node-a",
      "replicas": ["node-a", "node-b", "node-c"]
    }
  ]
}
```

Key fields:
- `is_alive: false` → coordinator has not received a heartbeat within `failure_timeout`
- `last_heartbeat_ms` → milliseconds since last heartbeat received

### Node Health

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/doki/node/v1/node.proto \
  -d '{}' \
  node-a:8000 \
  doki.node.v1.NodeService/GetStatus
```

Response:

```json
{
  "node_id": "node-a",
  "uptime_seconds": 3600,
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
  ]
}
```

Key fields:
- `role`: `LEADER` or `FOLLOWER`
- `is_ready`: `false` if recovering
- `version`: current commit counter
- `peer_versions`: follower versions as seen from the leader (only on leader)
- Replication lag per follower: `version - peer_versions[peer]`

### Readiness Check (for Docker/Kubernetes)

```bash
# Returns 200 if node is ready to serve traffic, 503 if recovering
curl -f http://node-a:8000/ready
```

A node is "ready" if all shards it hosts have `is_ready: true`.

---

## Observability

### Logs

All components emit structured JSON logs to stdout.

```json
{"time":"2025-01-15T12:34:56Z","level":"INFO","msg":"leader assigned","shard_id":"shard-0","leader":"node-a","term":3}
{"time":"2025-01-15T12:34:57Z","level":"WARN","msg":"follower unreachable","shard_id":"shard-0","peer":"node-c","retries":2}
{"time":"2025-01-15T12:34:58Z","level":"INFO","msg":"recovery started","shard_id":"shard-0","node":"node-c"}
```

Log levels: `DEBUG`, `INFO`, `WARN`, `ERROR`

Set with `DOKI_LOG_LEVEL=DEBUG` for verbose output.

### Key Log Events

| Event | Level | Meaning |
|-------|-------|---------|
| `write committed` | DEBUG | Normal write success |
| `write failed quorum` | WARN | Leader could not reach quorum |
| `leader assigned` | INFO | New leader elected |
| `leader stepped down` | INFO | Node received higher-term message |
| `node failure detected` | WARN | Coordinator lost heartbeat |
| `recovery started` | INFO | Node beginning snapshot pull |
| `recovery completed` | INFO | Snapshot applied; node is ready |
| `term conflict` | WARN | Stale leader detected |
| `replication lag high` | WARN | Follower lagging by >100 versions |

### Metrics (Prometheus)

All nodes expose Prometheus metrics at `GET /metrics`.

Key metrics:

| Metric | Labels | Description |
|--------|--------|-------------|
| `doki_replica_version` | `shard_id`, `node_id` | Current committed version per replica |
| `doki_replica_term` | `shard_id`, `node_id` | Current term per replica |
| `doki_replica_is_leader` | `shard_id`, `node_id` | 1 if the replica is leader |
| `doki_writes_total` | `shard_id`, `result` | Total write operations by result |
| `doki_write_duration_seconds` | `shard_id` | Leader write latency |
| `doki_replications_total` | `shard_id`, `result` | Incoming replication results |
| `doki_elections_total` | `shard_id`, `result` | Election results |
| `doki_leader_heartbeats_sent_total` | `shard_id` | Outbound leader heartbeats |
| `doki_leader_heartbeats_missed_total` | `shard_id` | Heartbeats with no peer response |
| `doki_recoveries_total` | `shard_id`, `recovery_type` | Recovery events (incremental/snapshot) |
| `doki_last_leader_contact_seconds` | `shard_id`, `node_id` | Age of last leader contact |

Scrape all nodes in your Prometheus config:

```yaml
scrape_configs:
  - job_name: doki
    static_configs:
      - targets: ['node-a:8001', 'node-b:8002', 'node-c:8003']
```

---

## Admin API (Phase 5: Dynamic Cluster Management)

The coordinator exposes admin endpoints for live cluster changes. These do not require a cluster restart.

### Add a New Node

Register a new node so it can receive heartbeats and be assigned shard replicas:

```bash
curl -s -X POST http://coordinator:7000/admin/add_node \
  -H 'Content-Type: application/json' \
  -d '{"node_id": "node-d", "address": "node-d:8004"}'
# → 200 OK
```

After this call, start the new node binary. It will heartbeat in and appear in `GetStatus`.

### Migrate a Shard to New Nodes

Move a shard's replica set to a different set of nodes without downtime:

```bash
curl -s -X POST http://coordinator:7000/admin/migrate_shard \
  -H 'Content-Type: application/json' \
  -d '{"shard_id": "shard-0", "new_replicas": ["node-d", "node-e", "node-f"]}'
# → 200 {"status":"migrating"}
```

The coordinator:
1. Sets `Replicas = old ∪ new` so both old and new nodes serve the shard simultaneously
2. New nodes detect the shard in their next `GetShardMap` refresh and start recovery
3. Once all new nodes have recovered (`incoming_replicas` all have `version > 0`), the coordinator swaps `Replicas = new` and assigns a leader from the new set
4. Old nodes detect they are no longer in the shard map and drop the shard

Monitor progress:
```bash
watch -n 1 'grpcurl -plaintext -import-path proto -proto proto/doki/coordinator/v1/coordinator.proto -d "{}" coordinator:7000 doki.coordinator.v1.CoordinatorService/GetShardMap | jq ".shardMap.shards[] | select(.id==\"shard-0\") | {leader, replicas, incomingReplicas}"'
```

Migration is complete when `incoming_replicas` is empty and `leader` is one of the new nodes.

### Split a Shard

Create a new shard bootstrapped from an existing shard's data:

```bash
curl -s -X POST http://coordinator:7000/admin/split_shard \
  -H 'Content-Type: application/json' \
  -d '{"source_shard_id": "shard-0", "new_shard_id": "shard-1", "new_replicas": ["node-d", "node-e", "node-f"]}'
# → 200 {"status":"splitting"}
```

The new shard nodes bootstrap their initial state from the source shard leader's current snapshot. Once all replicas of the new shard are ready, they operate independently from the source shard.

Monitor progress:
```bash
watch -n 1 'grpcurl -plaintext -import-path proto -proto proto/doki/coordinator/v1/coordinator.proto -d "{}" coordinator:7000 doki.coordinator.v1.CoordinatorService/GetShardMap | jq ".shardMap.shards[] | select(.id==\"shard-1\")"'
```

Split is complete when `shard-1` appears in the shard map with a `leader` and `incoming_replicas` is empty.

---

## Common Operational Tasks

### Starting the Cluster

```bash
docker compose up -d
docker compose logs -f
```

Wait for all nodes to log `recovery completed` and the coordinator to log `initial leaders assigned`.

### Stopping the Cluster

```bash
docker compose down
```

All in-memory data is lost (v1). This is expected.

### Killing a Node

```bash
docker compose kill node-c
```

The coordinator detects the failure within `failure_timeout_ms` (default: 1.5s). If the killed node was a leader, a new leader is elected.

### Restarting a Node

```bash
docker compose start node-c
```

The node restarts, connects to the coordinator, and begins recovery. It pulls a snapshot from the shard leader and becomes ready.

Monitor recovery:

```bash
# Watch until is_ready: true
watch -n 0.5 'grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" localhost:8003 doki.node.v1.NodeService/GetStatus | jq ".shards[].isReady"'
```

### Forcing a Leader Change

Not directly supported via API in v1. To change a leader:

1. Kill the current leader node
2. The coordinator will elect a new one
3. Restart the old leader; it rejoins as a follower

### Checking Replication Lag

On the leader:

```bash
grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" node-a:8001 doki.node.v1.NodeService/GetStatus | jq '.shards[] | {shardId, version, peerVersions}'
```

Example output:

```json
{
  "shard_id": "shard-0",
  "version": 142,
  "peer_versions": {
    "node-b": 141,
    "node-c": 142
  }
}
```

`node-b` is 1 version behind. This is normal if a write was just committed.

---

## Troubleshooting

### Writes Return QUORUM_UNAVAILABLE

**Cause:** The leader cannot reach quorum (majority of replicas are down or unreachable).

**Check:**
1. How many replicas are alive? (`CoordinatorService/GetStatus`)
2. Is the quorum size correct? (3-node shard needs 2 alive)
3. Are followers reachable from the leader? (check network, check follower logs)

**Resolution:**
- Restart failed nodes
- They will recover automatically via snapshot

---

### Node Stuck in Not Ready

**Cause:** Recovery is taking too long or snapshot transfer failed.

**Check:**
```bash
grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" node-c:8003 doki.node.v1.NodeService/GetStatus | jq '.shards[] | {shardId, isReady}'
docker compose logs node-c | grep -i recovery
```

**Resolution:**
- Check leader logs for snapshot errors
- Restart the recovering node
- If persistent, restart the leader (rare)

---

### Coordinator Shows Node as Dead (But It's Running)

**Cause:** Heartbeat packets are being dropped, or the node is overloaded.

**Check:**
```bash
docker compose logs node-a | grep heartbeat
grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" node-a:8001 doki.node.v1.NodeService/GetStatus  # If this responds, the node is alive
```

**Resolution:**
- Check network connectivity between node and coordinator
- Increase `failure_timeout_ms` if you're seeing false positives under load (development only)

---

### Two Nodes Both Report Role: LEADER

**Cause:** Split-brain window during failover. This is a known v1 limitation.

**Expected Behavior:** The stale leader will detect the new leader via `TERM_MISMATCH` on its next replication attempt, step down, and recover.

**Check:**
```bash
for port in 8001 8002 8003; do
  echo "node :$port"
  grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" localhost:$port doki.node.v1.NodeService/GetStatus | jq '.shards[] | {shardId, role, term}'
done
```

**Resolution:**
- This should self-resolve within one heartbeat cycle
- If it persists, check coordinator and look for network partition

---

### Migration Stuck (incoming_replicas Never Clears)

**Cause:** One or more incoming replicas are not reaching `version > 0`. This happens when:
- New nodes cannot reach the current leader for recovery
- New nodes were not started or registered before migration was triggered
- Network issues prevent recovery

**Check:**
```bash
# Is the new node alive?
grpcurl -plaintext -import-path proto -proto proto/doki/coordinator/v1/coordinator.proto -d "{}" coordinator:7000 doki.coordinator.v1.CoordinatorService/GetStatus | jq '.nodes[] | select(.nodeId=="node-d")'

# Does the new node know about the shard?
grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" node-d:8004 doki.node.v1.NodeService/GetStatus | jq '.shards'

# Is recovery progressing?
docker compose logs node-d | grep recovery
```

**Resolution:**
- Ensure the new node is registered via `/admin/add_node` before triggering migration
- Ensure the new node process is running and can reach the coordinator and the current leader
- Check that the node's `data_dir` is writable

---

### Shard Split Bootstrap Fails

**Cause:** New shard nodes cannot recover from the source shard leader. This can happen if:
- The bootstrap leader address was not captured at split time (coordinator restarted between split and recovery)
- The source shard leader changed after the split was initiated

**Check:**
```bash
# Does the new shard appear in the shard map at all?
grpcurl -plaintext -import-path proto -proto proto/doki/coordinator/v1/coordinator.proto -d "{}" coordinator:7000 doki.coordinator.v1.CoordinatorService/GetShardMap | jq '.shardMap.shards[] | select(.id=="shard-1")'

# Is the new node attempting bootstrap recovery?
grpcurl -plaintext -import-path proto -proto proto/doki/node/v1/node.proto -d "{}" node-d:8004 doki.node.v1.NodeService/GetStatus | jq '.shards[] | select(.shardId=="shard-1")'
```

**Resolution:**
- Ensure source shard nodes are running and healthy before initiating a split
- If the bootstrap is permanently stuck, trigger a fresh split with the same target shard ID and new nodes

---

## Cluster Reset

To fully reset the cluster (wipe all state):

```bash
docker compose down -v  # -v removes volumes
docker compose up
```

All data is lost. Nodes start fresh.
