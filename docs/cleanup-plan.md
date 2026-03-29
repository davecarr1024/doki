# Doki Cleanup Plan — Replication Simplification + Correctness

## Goals
- Simplify the write/replication protocol to a single apply-and-ACK step.
- Keep correctness explicit: quorum of **applied** replicas before success.
- Make recovery and liveness behavior clear and testable.
- Align documentation with the actual implementation.

## Plan (Applied)
1. **Remove commit phase and apply on replicate**
   - Delete commit endpoint and commit fan-out.
   - Followers apply on replicate and ACK.
   - Leader applies locally after quorum of apply ACKs.

2. **Quorum liveness + fast-fail**
   - Track recent peer contact in leaders.
   - Fail fast if quorum is clearly unavailable.

3. **Recovery correctness**
   - Mark replicas not-ready on version gaps.
   - Keep recovery loop running so forced recovery can be triggered later.
   - Add a force-recover endpoint for rollback after failed quorum.
   - Reset on-disk snapshot/WAL after snapshot recovery.

4. **Documentation alignment**
   - Update replication/design docs to describe apply-and-ACK protocol.
   - Update metrics docs to match current labels.
   - Clarify HTTP/JSON interfaces in architecture doc.

## Applied Changes
- **Code**
  - Removed commit RPC and fan-out; replication is single-phase apply-and-ACK.
  - Added peer liveness tracking to fast-fail when quorum is clearly unavailable.
  - Added `/internal/force_recover/{shard_id}` to trigger recovery after failed quorum.
  - Recovery loop now stays active; gaps mark replicas not-ready.
  - Snapshot recovery resets on-disk snapshot/WAL to avoid replaying stale entries.

- **Docs**
  - `docs/design.md`: write protocol and open-question resolution updated.
  - `docs/replication.md`: diagrams and semantics updated for apply-and-ACK.
  - `docs/architecture.md`: interfaces reflect HTTP/JSON.
  - `docs/operations.md` + `docs/reliability.md`: metrics updated.

## Follow-ups (Optional)
- Add explicit tests for force-recover rollback in quorum-failure scenarios.
- Expose liveness window as a config knob instead of deriving from heartbeat interval.
- Consider per-peer readiness reporting from followers for more precise quorum eligibility.

---

# Doki Cleanup Plan — RPC Migration (gRPC)

## Current State
- **gRPC** is now the primary RPC surface for coordinator and node.
- `proto/doki/*` is extended and generated into `gen/` via Buf.
- Node-to-node replication/recovery and node-to-coordinator heartbeats use gRPC.
- Integration tests exercise gRPC for KV, status, replication, and recovery.
- HTTP remains only for `/metrics`, `/ready`, and admin endpoints.

## Plan (Completed)
1. **Proto + Codegen**
   - Extend node/coordinator protos to cover runtime needs (incremental recovery + leader notification).
   - Generate gRPC code into `gen/` with Buf.
   - Verified `~/.local/go/bin/go test ./...`.

2. **gRPC Servers + Clients**
   - Added gRPC servers for coordinator and node.
   - Replaced node → coordinator calls with gRPC (heartbeat, shard map, leader notify, status, leader lookup).
   - Replaced node → node replication + recovery with gRPC.
   - Kept HTTP **only** for `/metrics`, `/ready`, and admin endpoints.
   - Used cmux to serve gRPC + HTTP on the same address (no config change).
   - Verified `~/.local/go/bin/go test ./...`.

3. **Test Migration**
   - Unit tests now exercise gRPC for replication, recovery, and election paths.
   - Integration tests now use a shared cluster control harness, with gRPC for KV, status, and recovery, and HTTP for `/ready` + admin endpoints.
   - Tests reorganized by functional requirements (control plane, replication, recovery, durability, sharding).
   - Verified `~/.local/go/bin/go test ./...` and `~/.local/go/bin/go test ./test/integration/... -tags=integration`.

## Applied Changes
- Added gRPC servers/clients for node and coordinator, served via cmux.
- Migrated unit tests to gRPC RPCs and added a gRPC test harness for stubs.
- Built a shared integration cluster control helper and reorganized integration tests by functional requirements.

## Next Tasks To Align Implementation With Design
1. **Recovery semantics**
   - Make recovery eligibility explicit with a state enum (`Healthy`, `Lagging`, `Recovering`, `Unavailable`).
   - Ensure recovery cannot advance applied index past missing log entries.
   - Track recovery source selection and expose it in status.

2. **Membership + sharding**
   - Define a single authoritative shard map source (coordinator) and deprecate local overrides.
   - Validate shard ownership transitions (handoff) before accepting writes.
   - Record shard map version in node status and logs for auditability.

3. **Durability + snapshots**
   - Define snapshot cadence and retention policy in config (not implicit).
   - Track snapshot state in metadata and expose it via status.
   - Add corruption detection and guard rails on snapshot load.

4. **Configuration + validation**
   - Centralize config validation with explicit defaults and required fields.
   - Fail fast on invalid configs at startup (before serving).
   - Expose effective config in a read-only admin endpoint.

5. **Observability**
   - Standardize metric names and labels for RPC, replication, and recovery.
   - Add structured logs for write lifecycle and recovery steps.
   - Add trace IDs to cross-node requests for debugging.

6. **Error model**
   - Normalize gRPC errors (status codes + details) for retryable vs fatal failures.
   - Align admin HTTP errors with gRPC status semantics.
   - Add tests for error translations and retry paths.

## Completed Since Last Update
- gRPC migration finished across node/coordinator.
- Unit and integration tests migrated to gRPC.
- Integration tests reorganized by functional requirements with a shared cluster control harness.
- State machine boundary added for apply/replay/snapshot with snapshot format versioning and WAL monotonicity checks.
- Write path invariants centralized (leader guard, quorum planning) with consistent write result metadata.
