# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M04 — Durable persistence**  
Overall status: **M04 in progress; M00–M03 complete and hosted-verified**

## Progress rules

- [x] Create this persistent roadmap in the repository.
- [x] Check tasks only after the artifact exists; verification tasks require an actual run.
- [x] Record exact commands and outcomes; never mark an unrun test as passing.
- [x] Keep this file current at every milestone boundary and before ending a work session.
- [x] Prefer small, reviewable commits and explicit limitations.
- [x] Record blockers, assumptions, and manual actions explicitly.

## Milestones

### M00 — Project charter and architecture baseline
- [x] Confirm public repository and write access.
- [x] Define project purpose, non-goals, and initial technology direction.
- [x] Establish persistent roadmap and recovery protocol.
- [x] Add architecture overview.
- [x] Record ADR-0001: Rust-first modular core.
- [x] Add contribution and security guidance.
- [ ] Select and add an explicit open-source license.
- [x] Review scope and first implementation boundary.

### M01 — Rust workspace foundation
- [x] Pin Rust toolchain (1.99.1) and create workspace manifest.
- [x] Establish initial crate boundaries: infrastructure-independent core and server application.
- [x] Add formatting, Clippy, and test conventions.
- [x] Add CI for formatting, Clippy, unit tests, build, and documentation.
- [x] Add minimal executable and bootstrap HTTP health endpoint.
- [x] Verify clean build and all checks from a fresh checkout via hosted CI (run `41353166515`, commit `832e3fc`).

### M02 — Execution domain and state machine
- [x] Define distinct workflow, workflow-run, task, task-run, attempt, and worker identities.
- [x] Specify workflow/task lifecycle states and legal transitions.
- [x] Define the initial bounded retry contract and attempt-count semantics.
- [x] Specify first-committed terminal outcome for timeout/cancellation races and a stable idempotency-key contract.
- [x] Implement deterministic transition tests, including exhaustive workflow/task transition matrices and competing terminal outcomes.
- [ ] Add property-based transition tests (deferred until the dependency and lockfile workflow is established).
- [x] Document failure semantics, versioning invariants, and delivery limitations.

### M03 — Single-node execution runtime
- [x] Implement in-memory repository for deterministic tests and verify it with hosted CI (run `41353963000`).
- [x] Implement bounded async scheduler and worker loop (run `41354805491`).
- [x] Add typed task-handler registration API with duplicate-registration protection.
- [x] Add bounded concurrency, per-attempt timeout, version-guarded cancellation, and retryable/permanent failure policy.
- [x] Add async scheduler/repository tests for success, retry, timeout, cancellation race, and concurrency limits.
- [x] Add validated workflow DAG definitions with deterministic topological ordering, duplicate/missing/self dependency checks, and cycle detection (run `41355173212`).
- [x] Implement workflow-run orchestration and dependency-aware task activation (run `41356396516`).
- [x] Add cooperative handler cancellation and verify direct/workflow cancellation propagation (run `41357065422`).
- [x] Add configurable bounded shutdown: graceful drain, cancellation grace, then force-abort with version-guarded terminal state (run `41357506821`).
- [x] Add 1 MiB bounded TaskInput/TaskOutput, static workflow input delivery, and output retention (run `41358509174`).
- [x] Expose successful direct-dependency outputs in TaskContext; verify dataflow integration (run `41359053561`).

### M04 — Durable persistence
- [x] Design PostgreSQL v1 schema and immutable migration/checksum strategy for workflow/task runs, ordered dependencies, attempts, leases/fencing, and outbox (ADR-0007).
- [x] Select and pin deadpool-postgres 0.14.2, tokio-postgres 0.7.18, serde_json 1.0.154, and sha2 0.10.9 while retaining Rust 1.85 MSRV.
- [x] Add PostgreSQL 18 service to CI and execute DDL against a real server (run `41365026199`).
- [x] Implement advisory-locked transactional migration runner with version/name/SHA-256 checks and idempotent repeat (runs `41362986570` and `41365026199`).
- [x] Add PostgreSQL checks for concurrent first start, checksum drift, untracked history, newer schema, payload/lease/dependency constraints.
- [x] Implement atomic workflow creation with task records and ordered dependency edges (PostgreSQL integration run `41365026199`).
- [x] Implement dependency-aware promotion and atomic `FOR UPDATE SKIP LOCKED` claims with attempt/outbox creation.
- [x] Implement fenced heartbeat and successful completion with output persistence and same-transaction outbox records.
- [x] Implement expired-lease recovery to retry-waiting/failed state; verify stale worker heartbeat/completion is rejected and fencing token increases on reclaim.
- [x] Implement transactional retryable/permanent worker-failure recording with attempt history and outbox events (initial API and integration tests added; hosted verification pending).
- [ ] Implement durable task cancellation and timeout transitions.
- [ ] Aggregate task outcomes into durable workflow-run terminal status.
- [ ] Implement outbox claim/publish/ack/retry and duplicate-delivery consumer contract (claim, reschedule, acknowledgement, and stale publisher token guards added; integration verification pending).
- [ ] Add process-crash/restart and transaction crash-window integration tests; connect the adapter to a durable coordinator/worker loop.

### M05 — Distributed coordination and event delivery
- [ ] Define node membership and worker ownership model.
- [ ] Add transactional outbox/inbox where required.
- [ ] Integrate NATS JetStream after transport-independent contracts stabilize.
- [ ] Test redelivery, lease expiry, partition, and worker loss.
- [ ] Document delivery guarantees and operational limits.

### M06 — WASM extension runtime
- [ ] Define versioned plugin ABI and capability model.
- [ ] Integrate Wasmtime with fuel, memory, and execution limits.
- [ ] Add host-call authorization and deterministic error mapping.
- [ ] Test traps, resource exhaustion, and untrusted modules.
- [ ] Publish a minimal plugin example.

### M07 — Control plane and developer experience
- [ ] Expose versioned HTTP API and OpenAPI contract.
- [ ] Add CLI for inspect, submit, cancel, and worker status.
- [ ] Add Rust SDK; add TypeScript SDK only when API stabilizes.
- [ ] Build dashboard for workflows, runs, tasks, and event history.
- [ ] Add authentication, authorization, and tenant isolation design.

### M08 — Production engineering
- [ ] Add structured logs, metrics, and distributed tracing.
- [ ] Add Docker Compose development environment.
- [ ] Add backup/restore and schema upgrade runbooks.
- [ ] Add reproducible load and benchmark suites.
- [ ] Add chaos/failure-injection scenarios.
- [ ] Add dependency, license, secret, and supply-chain checks.
- [ ] Publish threat model, SLO proposal, and operational guide.

### M09 — Release candidate
- [ ] Complete end-to-end acceptance suite.
- [ ] Verify clean install and documented quickstart.
- [ ] Review API stability and compatibility policy.
- [ ] Publish limitations and known issues.
- [ ] Tag a version only after release criteria pass.

## Current checkpoint

**Completed:** M00 architecture baseline except owner license selection. M01 Rust workspace foundation is hosted-verified (run `41353166515`). M02 includes typed execution identities (including idempotency keys), explicit lifecycle graphs, versioned transitions, capped exponential retry calculation, exhaustive transition matrices, and ADR-0002. M03 single-node runtime is complete and hosted-verified: in-memory repository, bounded Tokio scheduler, deterministic DAG validation, workflow-run coordinator, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and successful direct-dependency output access.

**In progress:** M04 durable persistence. PostgreSQL 18 schema and migration bootstrap are verified: the initial migration applies with a checksum ledger in one transaction; advisory locking serializes concurrent startup; checksum drift, untracked migration history, unsupported newer schema, payload bounds, invalid leases, and duplicate dependency edges are rejected. The async `PgTaskRepository` vertical slice is now hosted-verified. It atomically creates workflow/task/dependency rows, promotes only tasks whose direct dependencies succeeded, claims with `FOR UPDATE SKIP LOCKED`, persists attempts and fencing tokens, extends leases with owner/version/fencing checks, commits successful outputs and outbox events together, and recovers expired leases to retry-waiting or failed state.

**Next action:** Verify the new durable handler failure/retry and outbox claim/retry/ack APIs in PostgreSQL 18 CI. Then implement durable task cancellation/timeout and workflow terminal aggregation. Add crash-window/process-restart tests and connect the async PostgreSQL repository to a durable worker/coordinator loop without reusing process-local monotonic timestamps.

**Verification:** M01 run `41353166515`; M03 repository/server run `41353963000`; async scheduler run `41354805491`; DAG validation run `41355173212`; workflow coordinator run `41356396516`; cooperative cancellation run `41357065422`; bounded shutdown run `41357506821`; payload I/O run `41358509174`; direct-dependency dataflow run `41359053561`; PostgreSQL migration-runner hardening run `41362986570`; latest Rust workspace run `41365026218`; and PostgreSQL 18 durable task repository run `41365026199` all completed successfully. Rust CI passed locked dependency resolution, formatting, Clippy, 53 workspace tests (15 core, 35 runtime, 2 server, 1 storage unit), release build, and docs. The PostgreSQL 18 job executed both storage integration tests: migration bootstrap/fail-closed checks and the durable task flow covering workflow creation, dependency gating, claim, heartbeat, output completion, lease expiry/recovery, retry attempt 2, strictly higher fencing token, stale-worker write rejection, and transactional outbox records. It also passed the six-table/eight-index inventory and SQL constraint fixtures.

**Known limitations:** The PostgreSQL adapter is not yet wired into the existing Tokio scheduler/coordinator. Durable handler-reported retryable/permanent failure, cancellation, timeout, workflow terminal aggregation, and an outbox publisher are not implemented. Crash/restart recovery is currently exercised at the lease/task-row level, not as a full process restart with a reconstructed workflow coordinator. Outbox rows are written transactionally, but delivery/acknowledgement/redelivery is not implemented. The in-memory repository remains process-local. Handler cancellation cannot undo external effects already issued. Task payloads remain opaque bytes capped at 1 MiB; applications own encoding and schema compatibility. The bootstrap HTTP server remains sequential and non-production.

**Blockers:** The remaining M04 work is implementation and verification, not a schema/driver blocker. License selection remains with repository owner.

**Manual action required from owner:** Choose a project license before public release; no legal terms have been assumed.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M00 | Repository verified; charter, architecture, ADR, policies, and persistent roadmap committed |
| 2026-11-02 | M01 | Rust 1.99.1 workspace, core primitive, bootstrap health server, CI, and development guide committed; hosted CI verified successfully in run `41353166515` |
| 2026-11-02 | M02 | Typed IDs and idempotency keys, lifecycle state machines, retry contract, ADR-0002, exhaustive transition matrices, terminal-race and overflow tests committed; hosted CI passed all quality checks on `832e3fc` |
| 2026-11-02 | M03 | Closed and hosted-verified through run `41359053561`: 15 core, 35 runtime, and 2 server tests passed; workflow DAG activation, cooperative cancellation, bounded shutdown, payload I/O, and direct dependency output access verified |

| 2026-11-02 | M04 | PostgreSQL 18 schema and migration runner verified; concurrent startup and fail-closed migration checks passed. Added and verified `PgTaskRepository` for atomic workflow creation, dependency promotion, SKIP LOCKED claim, attempt/outbox insertion, fenced heartbeat/completion, and expired-lease recovery. Rust CI `41365026218` and PostgreSQL 18 CI `41365026199` passed. |

## Resume instructions

1. Open this file on the default branch.
2. Read **Current checkpoint**.
3. Inspect the latest commit and active milestone artifacts.
4. Continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
