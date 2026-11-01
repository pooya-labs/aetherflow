# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M03 — Single-node execution runtime**  
Overall status: **M03 in progress; M01 hosted CI verified**

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
- [x] Implement in-memory repository for deterministic tests and verify it with hosted CI (run `41353963000`, commit `54aee8d`).
- [x] Implement bounded async scheduler and worker loop (hosted CI run `41354805491`).
- [x] Add typed task-handler registration API with duplicate-registration protection; workflow DAG execution API remains open.
- [x] Add bounded concurrency, per-attempt timeout, version-guarded cancellation, and retryable/permanent failure policy.
- [x] Add async scheduler/repository tests for success, retry, timeout, cancellation race, and concurrency limits.
- [x] Add validated workflow DAG definitions with deterministic topological ordering, duplicate/missing/self dependency checks, and cycle detection (formatting repair committed; hosted verification pending).
- [ ] Implement workflow-run orchestration API and dependency-aware task activation.

### M04 — Durable persistence
- [ ] Design PostgreSQL schema and migration strategy.
- [ ] Implement transactional state transitions.
- [ ] Implement claim/lease/heartbeat semantics.
- [ ] Implement recovery after process restart.
- [ ] Add crash-window and duplicate-delivery tests.

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

**Completed:** M00 architecture baseline except owner license selection. M01 is verified by hosted run `41353166515` on commit `832e3fc50c8da7e22d2fedaaec9151b3748168b2`: formatting, Clippy, workspace tests, release build, and documentation build all passed. M02 has distinct execution identities (including idempotency keys), explicit workflow/task lifecycle graphs, versioned transitions, capped exponential retry calculation, exhaustive deterministic transition matrices, terminal-outcome race and overflow tests, and ADR-0002. Tokio `1.54.1` is pinned, its Cargo-generated lockfile is committed, and CI enforces locked dependency resolution.

**In progress:** M03 single-node runtime. The synchronous in-memory repository is verified. The bounded Tokio scheduler and handler registry are now also hosted-verified: it limits in-flight attempts, classifies retryable/permanent failures, applies per-attempt timeouts, settles with expected-version checks, and drains active work after shutdown. Five async scheduler tests cover success, retry, timeout, concurrency limits, and cancellation/completion races. Hosted run `41354805491` passed all checks on commit `ccc549935d96c2f9ef78c2758dd2c30f6694a505`.

**Next action:** Verify workflow-run orchestration in hosted CI. Then add workflow input/output boundaries, cooperative handler cancellation, and a bounded shutdown-drain policy. Keep the in-memory adapter explicitly non-durable; PostgreSQL, leases, and distributed coordination remain M04/M05.

**Verification:** M01 run \`41353166515\`, M03 repository/server run \`41353963000\`, lockfile generation run \`41354159107\`, locked-build run \`41354279238\`, async scheduler run \`41354805491\`, and DAG validation run \`41355173212\` all completed successfully. Workflow coordinator run \`41355491649\` failed at formatting; commit \`dc30e949c1649a5bfb1069cffd11dee485943dae\` fixed the reported diffs. Run \`41356122965\` passed formatting but exposed a deprecated atomic API and an unused shutdown receiver. Commit \`7cd55d94bb22f79a9032ef77a99e9bb5a222ca25\` fixed those, but run \`41356221542\` then reported that \`AtomicU64::try_update\` exceeds the declared Rust 1.85 MSRV (it stabilized in 1.95). This commit replaces it with an MSRV-compatible \`compare_exchange_weak\` loop. Workflow coordinator compile/tests remain pending hosted verification. Local Rust compilation is unavailable because \`rustc\` is not installed in the working environment.

**Known limitations:** Workflow-run coordination is process-local and not persisted; the in-memory task repository loses state on process exit and has no lease expiry, fencing, or crash recovery. Task-run IDs use a process-local sequence generator; a durable backend must replace it with a persistent ID strategy. The scheduler is single-process; timeout drops the handler future, while workflow cancellation changes repository state but does not forcibly stop a running handler future. Worker panics stop the scheduler and can leave a task in Running until process exit. There is no durable event log, PostgreSQL adapter, persisted task input/output, external side-effect idempotency API, or retry jitter. Shutdown drains without a deadline, so a non-terminating handler can delay shutdown indefinitely. The bootstrap HTTP server remains sequential and non-production.

**Blockers:** Hosted verification of workflow-run orchestration pending. Property-based tests remain deferred; Tokio is pinned and its generated lockfile is committed. License selection remains with repository owner.

**Manual action required from owner:** Choose a project license before public release; no legal terms have been assumed.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M00 | Repository verified; charter, architecture, ADR, policies, and persistent roadmap committed |
| 2026-11-02 | M01 | Rust 1.99.1 workspace, core primitive, bootstrap health server, CI, and development guide committed; hosted CI verified successfully in run `41353166515` |
| 2026-11-02 | M02 | Typed IDs and idempotency keys, lifecycle state machines, retry contract, ADR-0002, exhaustive transition matrices, terminal-race and overflow tests committed; hosted CI passed all quality checks on `832e3fc` |
| 2026-11-02 | M03 | Verified in-memory repository/server and bounded Tokio scheduler; run `41354805491` passed all checks. Added deterministic DAG validation/topological ordering with seven tests; run `41355173212` passed all CI checks. Implemented workflow-run orchestration with staged dependency activation, lifecycle aggregation, failure propagation, and external shutdown cancellation; hosted verification pending |

## Resume instructions

1. Open this file on the default branch.
2. Read **Current checkpoint**.
3. Inspect the latest commit and active milestone artifacts.
4. Continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
