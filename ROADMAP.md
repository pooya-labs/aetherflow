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
- [x] Draft PostgreSQL v1 schema and migration strategy for workflow/task runs, ordered dependencies, attempts, fencing leases, and transactional outbox (ADR-0007; real-server validation pending).
- [x] Select and pin deadpool-postgres 0.14.2, tokio-postgres 0.7.18, and sha2 0.10.9; upstream package metadata declares MSRV 1.85.
- [x] Add PostgreSQL 18 integration service and schema constraint fixtures; async migration-runner verification pending.
- [x] Add PostgreSQL tests for checksum drift, untracked schema metadata, unsupported newer schema versions, and concurrent first-start serialization (run `41362986570`).
- [ ] Implement async durable task operations for atomic workflow creation, dependency promotion, SKIP LOCKED claims, fenced heartbeat/completion, and expired-lease recovery (initial vertical slice added; hosted verification pending).
- [x] Implement dependency-aware ready promotion and atomic `FOR UPDATE SKIP LOCKED` claims (initial vertical slice added; hosted verification pending).
- [x] Implement fenced heartbeat/completion and expired-lease recovery with monotonically increasing fencing tokens (initial vertical slice added; hosted verification pending).
- [x] Persist task attempts, dependencies, task inputs/outputs, and successful completion atomically with version transitions (initial vertical slice added; hosted verification pending).
- [x] Append workflow-start, ready, claimed, retry-scheduled, succeeded, and failed outbox events in the same transaction as each transition (initial vertical slice added; hosted verification pending).
- [ ] Add crash-window, stale-worker, lease-expiry, and duplicate-outbox-delivery integration tests.

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

**Completed:** M00 architecture baseline except owner license selection. M01 is hosted-verified by run `41353166515`. M02 includes typed execution identities (including idempotency keys), explicit lifecycle graphs, versioned transitions, capped exponential retry calculation, exhaustive transition matrices, and ADR-0002. M03 single-node runtime is complete: in-memory repository, bounded Tokio scheduler, deterministic DAG validation, workflow-run coordinator, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and successful direct-dependency output access.

**In progress:** M04 durable persistence. The PostgreSQL v1 schema and transaction contract are hosted-verified against PostgreSQL 18, and the migration runner passes concurrent startup/checksum/history negative tests. An initial async durable task repository vertical slice has been added in `crates/aetherflow-runtime/migrations/postgres/0001_initial_schema.sql`, `docs/persistence/postgres-transaction-contract.md`, and ADR-0007. The schema covers workflow runs, task runs, ordered dependency edges, attempt history, lease/fencing state, and an outbox. It has not yet been executed against PostgreSQL; selected pinned async driver stack: deadpool-postgres 0.14.2, tokio-postgres 0.7.18, and sha2 0.10.9, with upstream MSRV 1.85. A new PostgreSQL storage crate now contains a pool boundary, checksum-aware advisory-locked migration runner, and an environment-gated integration test. The new crate passed full Rust CI in run `41362372285`, and PostgreSQL 18 run `41362372347` passed the migration-runner integration test, schema ledger, table/index inventory, and constraint fixtures. The Cargo.lock update is now committed.

**Next action:** Verify the durable task-repository vertical slice in full Rust CI and PostgreSQL 18 CI. Fix any compile/SQL/runtime issues before adding failure/retry persistence, workflow terminal aggregation, and outbox publisher claim/ack/redelivery behavior.

**Verification:** M01 run \`41353166515\`; M03 repository/server run \`41353963000\`; async scheduler run \`41354805491\`; DAG validation run \`41355173212\`; workflow coordinator run \`41356396516\`; cooperative cancellation run \`41357065422\`; bounded shutdown run \`41357506821\`; payload I/O run \`41358509174\`; direct-dependency dataflow run \`41359053561\`; migration-runner baseline run \`41362372347\`; and schema hardening runs \`41362986591\` (Rust) and \`41362986570\` (PostgreSQL) all passed. Commit \`42f9d2f\` introduced the initial durable task repository, but its first Rust run \`41363734905\` stopped at rustfmt before compilation; the PostgreSQL job \`41363734904\` passed only the older migration-only test command. This commit enables main-only automatic rustfmt commits and changes PostgreSQL CI to execute all storage integration tests, including task claiming and lease recovery. Compile/runtime verification remains pending.

**Known limitations:** PostgreSQL schema, driver, migration runner, and an initial async task repository exist; however, the adapter is not yet integrated with the scheduler and has no durable failure/retry API or outbox publisher. Workflow terminal aggregation and process-restart coordinator recovery are not yet implemented. The in-memory repository loses state on process exit. Handler cancellation is cooperative; an aborted future cannot undo external side effects. Task payloads are opaque bytes capped at 1 MiB each; the application owns encoding/schema versions and transforms direct dependency outputs into handler-specific inputs. The bootstrap HTTP server remains sequential and non-production.

**Blockers:** The new durable task repository vertical slice must pass hosted Rust and PostgreSQL verification before its semantics are marked complete. License selection remains with repository owner. License selection remains with repository owner.

**Manual action required from owner:** Choose a project license before public release; no legal terms have been assumed.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M00 | Repository verified; charter, architecture, ADR, policies, and persistent roadmap committed |
| 2026-11-02 | M01 | Rust 1.99.1 workspace, core primitive, bootstrap health server, CI, and development guide committed; hosted CI verified successfully in run `41353166515` |
| 2026-11-02 | M02 | Typed IDs and idempotency keys, lifecycle state machines, retry contract, ADR-0002, exhaustive transition matrices, terminal-race and overflow tests committed; hosted CI passed all quality checks on `832e3fc` |
| 2026-11-02 | M03 | Closed and hosted-verified through run `41359053561`: 15 core, 35 runtime, and 2 server tests passed; workflow DAG activation, cooperative cancellation, bounded shutdown, payload I/O, and direct dependency output access verified |

| 2026-11-02 | M04 | Pinned PostgreSQL driver stack and initial schema/migration runner committed. PostgreSQL 18 CI run `41361580073` passed runner compilation, transactional apply, idempotent re-run, ledger, table/index inventory, and constraint fixtures. Rust workspace CI lockfile refresh detection is being corrected after a shallow-checkout issue. |

## Resume instructions

1. Open this file on the default branch.
2. Read **Current checkpoint**.
3. Inspect the latest commit and active milestone artifacts.
4. Continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
