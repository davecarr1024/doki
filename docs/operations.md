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

# Check cluster health
curl http://localhost:7000/status | jq .

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
  data_dir: "/data"   # unused in v1 (in-memory)
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
curl http://coordinator:7000/status
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
curl http://node-a:8000/status
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

### Metrics (Future)

v1 does not expose Prometheus metrics. Future additions:

- `doki_writes_total{shard, result}`
- `doki_reads_total{shard, result}`
- `doki_replication_lag_versions{shard, peer}`
- `doki_leader_changes_total{shard}`
- `doki_recovery_duration_seconds{shard}`

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
watch -n 0.5 'curl -s http://localhost:8003/status | jq ".shards[].is_ready"'
```

### Forcing a Leader Change

Not directly supported via API in v1. To change a leader:

1. Kill the current leader node
2. The coordinator will elect a new one
3. Restart the old leader; it rejoins as a follower

### Checking Replication Lag

On the leader:

```bash
curl -s http://node-a:8001/status | jq '.shards[] | {shard_id, version, peer_versions}'
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
1. How many replicas are alive? (`coordinator /status`)
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
curl http://node-c:8003/status | jq '.shards[] | {shard_id, is_ready}'
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
curl http://node-a:8001/status  # If this responds, the node is alive
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
  curl -s http://localhost:$port/status | jq '.shards[] | {shard_id, role, term}'
done
```

**Resolution:**
- This should self-resolve within one heartbeat cycle
- If it persists, check coordinator and look for network partition

---

## Cluster Reset

To fully reset the cluster (wipe all state):

```bash
docker compose down -v  # -v removes volumes
docker compose up
```

All data is lost. Nodes start fresh.
