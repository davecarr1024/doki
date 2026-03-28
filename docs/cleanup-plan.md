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
- **HTTP/JSON** endpoints are still the primary RPC surface for coordinator and node.
- `proto/doki/*` defines gRPC APIs but no generated code is checked in.
- Node-to-node replication and node-to-coordinator heartbeats use HTTP.
- Integration tests exercise HTTP endpoints.
- HTTP is still used for `/metrics` and `/ready`.

## Plan (To Execute)
1. **Proto + Codegen**
   - Extend node/coordinator protos to cover current runtime needs (incremental recovery + leader notification).
   - Generate gRPC code into `gen/` with Buf.
   - Verify `~/.local/go/bin/go test ./...`.

2. **gRPC Servers + Clients**
   - Add gRPC servers for coordinator and node.
   - Replace node → coordinator calls with gRPC (heartbeat, shard map, leader notify, status, leader lookup).
   - Replace node → node replication + recovery with gRPC.
   - Keep HTTP **only** for `/metrics`, `/ready`, and admin endpoints.
   - Use cmux to serve gRPC + HTTP on the same address (no config change).
   - Verify `~/.local/go/bin/go test ./...`.

3. **Test Migration**
   - Update unit tests to exercise gRPC for replication and status calls where appropriate.
   - Update integration tests to use gRPC for KV operations and status, while preserving `/ready` HTTP.
   - Add integration coverage for gRPC replication/recovery paths.
   - Verify `~/.local/go/bin/go test ./...` and `~/.local/go/bin/go test ./test/integration/... -tags=integration`.
