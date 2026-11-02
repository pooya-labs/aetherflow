# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M05 — Distributed coordination and event delivery**  
Overall status: **M00–M04 complete and hosted-verified; M05 in progress. Owner license selection remains open in M00.**

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
- [x] Implement transactional retryable/permanent worker-failure recording with typed `TaskFailure`, attempt history, and outbox events (PostgreSQL 18 run `41367694624`; Rust CI run `41367694577`).
- [x] Implement version-guarded durable task cancellation and timeout transitions; close active attempts, clear leases, append task outbox events, and reject stale worker writes (PostgreSQL 18 run `41367694624`).
- [x] Aggregate task outcomes into durable workflow-run terminal status; require all tasks to succeed for workflow success, and cancel remaining queued/running tasks plus active attempts on failure, cancellation, or timeout (PostgreSQL 18 run `41367694624`).
- [x] Implement durable outbox claim/retry/ack APIs with leased claims, monotonically increasing claim tokens, and stale-publisher rejection (PostgreSQL 18 runs `41366887867` and `41367694624`).
- [x] Add a transport-neutral PostgreSQL outbox dispatcher: publish outside transactions, ack only after publisher success, retry failures with capped exponential backoff, and preserve claim-token fencing (PostgreSQL 18 run `41370357722`; Rust CI `41370357767`).
- [x] Add durable PostgreSQL consumer inbox keyed by `(consumer_name, event_id)`, with atomic claims, duplicate suppression, release/reclaim, lease expiry, and fencing-token acknowledgement (Rust CI `41376350940`; PostgreSQL 18 `41376350890`).
- [x] Integrate a real NATS JetStream outbox publisher using stable `Nats-Msg-Id`; wait for JetStream storage acknowledgement and verify retry when broker acceptance precedes PostgreSQL outbox acknowledgement (PostgreSQL 18 + NATS run `41378596284`).
- [x] Integrate an inbox-backed NATS JetStream pull consumer; acknowledge only after inbox completion, renew PostgreSQL leases and JetStream progress acknowledgements, suppress logical duplicates, and retry failed handlers (PostgreSQL 18 + NATS run `41378596284`).
- [x] Add a process-restart integration test that drops the original pool/repository and starts a separate OS test process to recover an expired lease, reclaim with a higher fencing token, and finish the task (PostgreSQL 18 run `41368104662`).
- [x] Inject an outbox insert failure in PostgreSQL 18 and prove task state, attempt outcome, and event insertion roll back atomically; after removing the failing constraint, the same valid claim completes successfully (PostgreSQL 18 run `41368359268`; Rust CI run `41368358899`).
- [x] Connect `PgWorkflowCoordinator` and `PostgresTaskWorker` to the shared runtime `TaskRegistry`/`TaskContext`: materialize deterministic task-run IDs from validated DAGs, execute with bounded concurrency, PostgreSQL leases/heartbeats/fencing, durable retries/timeouts, dependency outputs, and automatic workflow aggregation (Rust CI `41369526772`; PostgreSQL 18 `41369526661`).
- [x] Test process termination across database commit and broker publish/ack crash windows: a publisher subprocess exits after JetStream storage acknowledgement but before PostgreSQL outbox acknowledgement; a consumer subprocess exits after inbox commit but before JetStream acknowledgement. Fresh processes recover/redeliver without duplicate logical effects (PostgreSQL 18 + NATS run `41380067612`; Rust CI `41380067843`).

### M05 — Distributed coordination and event delivery
- [x] Define node membership and worker ownership model in ADR-0008: PostgreSQL-backed node incarnations/epochs, worker registrations, draining, fail-closed partitions, and task-lease fencing.
- [x] Add transactional outbox/inbox where required (implemented and PostgreSQL 18 verified in M04).
- [x] Integrate NATS JetStream after transport-independent contracts stabilize (isolated publisher/consumer crate and real PostgreSQL 18 + NATS integration in M04).
- [ ] Extend redelivery and lease-expiry coverage to multi-node database/broker partitions, node-epoch takeover, and worker loss (single-process redelivery, lease recovery, and OS-process death windows are already verified in M04).
- [x] Document current at-least-once delivery guarantees and operational limits in the PostgreSQL transaction contract; extend with membership/partition behavior as M05 implementation lands.

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

**Completed:** M00 architecture baseline (owner license selection remains open); M01 Rust workspace foundation; M02 execution domain/state machines; M03 single-node runtime; and M04 durable persistence are implemented and hosted-verified. M04 includes PostgreSQL v1/v2 migrations, durable task repository/coordinator/worker, transactional outbox dispatcher, PostgreSQL consumer inbox, real NATS JetStream publisher and pull consumer, lease renewal and broker progress acknowledgements, durable retries, dependency output delivery, and workflow aggregation.

**M04 crash consistency verification:** PostgreSQL 18 + real NATS run `41380067612` and Rust CI run `41380067843` passed on code commit `4a5dd80b`. The integration test starts separate OS subprocesses and terminates them with `std::process::exit` at two boundaries. The publisher child commits an outbox claim, waits for JetStream storage acknowledgement, then exits before the PostgreSQL outbox ack; after lease expiry, a fresh dispatcher retries the same event ID and JetStream stores/delivers only one message. The consumer child records a test side effect, commits the inbox row, then exits before broker acknowledgement; JetStream redelivers and a fresh consumer observes `AlreadyProcessed`, acknowledges the message, and does not rerun the handler or duplicate the side effect. The run also verifies migration v1/v2, 7 required tables, 10 indexes, PostgreSQL constraints, outbox redelivery, logical inbox deduplication with a distinct broker message ID, long-handler lease/AckWait heartbeat, and failed-handler release/retry (three NATS integration tests passed).

**In progress:** M05 distributed coordination and event delivery. ADR-0008 defines PostgreSQL-backed node membership and worker ownership: stable node IDs, per-process incarnation IDs, monotonically increasing membership epochs, worker registrations, drain semantics, fail-closed behavior during database partitions, and an additional membership guard around the existing per-task lease/fencing mechanism. PostgreSQL remains the single authority for persisted time and membership; NATS remains transport only.

**Next action:** Implement additive PostgreSQL migration v3 for node membership and worker registrations, then add a `PgMembershipRepository` for join/takeover, heartbeat, worker registration, drain, and stale-epoch rejection. Wire the PostgreSQL worker so task claims and durable task mutations require a live worker registration under the current node epoch. Preserve a consistent lock order and keep all membership/task operations in short transactions. Then test concurrent node join, lease expiry and epoch takeover, stale incarnation rejection, graceful drain, database partition, and worker process loss.

**Verification:** Latest M04 completion runs: PostgreSQL 18 + real NATS `41380067612`; Rust CI `41380067843`; Pages deployment `41380067475`. Both Rust and PostgreSQL workflows passed. PostgreSQL logs show `jetstream_child_process_exits_after_broker_storage_before_outbox_ack` and `jetstream_child_process_exits_after_inbox_commit_before_broker_ack` both passed, followed by `jetstream_publisher_confirms_storage_and_deduplicates_stable_event_ids` and `3 passed; 0 failed`. Rust CI passed format, Clippy, tests, workspace build, documentation build, and lockfile upload. Earlier M04 worker/coordinator, outbox rollback, migration and inbox verification runs remain documented in the M04 checklist and session log.

**Known limitations:** Node membership and worker registration are designed in ADR-0008 but not yet implemented; M05 partition and multi-node failover tests remain open. The durable worker remains an opt-in PostgreSQL worker loop and the process-local `TaskScheduler` remains for local development/tests. Deployment must provision NATS streams/subjects, durable pull consumers, credentials, retention, explicit acknowledgements, and suitable `AckWait`. Delivery remains at-least-once: if an external handler side effect occurs before inbox completion, a process crash can repeat the side effect; use a downstream idempotency key or commit the side effect and inbox completion in the same database transaction. JetStream duplicate suppression is bounded by stream configuration; PostgreSQL inbox state provides durable logical deduplication. Shutdown and lease recovery cannot undo external effects already issued. Task payloads remain opaque bytes capped at 1 MiB; applications own encoding and schema compatibility.

**Blockers:** No M04 implementation or CI blocker remains. M05 implementation begins with the ADR-0008 membership contract. License selection remains with repository owner.

**Manual action required from owner:** Choose a project license before public release; no legal terms have been assumed.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M00 | Repository verified; charter, architecture, ADR, policies, and persistent roadmap committed |
| 2026-11-02 | M01 | Rust 1.99.1 workspace, core primitive, bootstrap health server, CI, and development guide committed; hosted CI verified successfully in run `41353166515` |
| 2026-11-02 | M02 | Typed IDs and idempotency keys, lifecycle state machines, retry contract, ADR-0002, exhaustive transition matrices, terminal-race and overflow tests committed; hosted CI passed all quality checks on `832e3fc` |
| 2026-11-02 | M03 | Closed and hosted-verified through run `41359053561`: 15 core, 35 runtime, and 2 server tests passed; workflow DAG activation, cooperative cancellation, bounded shutdown, payload I/O, and direct dependency output access verified |

| 2026-11-02 | M04 | PostgreSQL 18 schema and migration runner verified; concurrent startup and fail-closed migration checks passed. Added and verified `PgTaskRepository` for atomic workflow creation, dependency promotion, SKIP LOCKED claim, attempt/outbox insertion, fenced heartbeat/completion, and expired-lease recovery. Rust CI `41365026218` and PostgreSQL 18 CI `41365026199` passed. |

| 2026-11-02 | M04 | Restored the PostgreSQL task repository after a broken module refactor; added typed `TaskFailure`, durable task cancellation/timeout, workflow-run aggregation with queued/running sibling cancellation, and outbox claim/retry/ack verification. Rust CI `41367694577` and PostgreSQL 18 CI `41367694624` passed. |

| 2026-11-02 | M04 | Added a true subprocess restart integration test: a fresh process recovers an expired task from PostgreSQL, advances its fencing token, completes it, and aggregates the workflow. PostgreSQL 18 run `41368104662` and Rust CI run `41368104908` passed. |

| 2026-11-02 | M04 | Added PostgreSQL failure-injection coverage proving task/attempt changes roll back when outbox insertion fails; the same fenced claim succeeds after removing the test constraint. PostgreSQL 18 run `41368359268` and Rust CI run `41368358899` passed. |

| 2026-11-02 | M04 | Added `PgWorkflowCoordinator` and `PostgresTaskWorker` over the shared runtime handler contract. PostgreSQL 18 integration executed a two-task dependency DAG through the durable worker, verified dependency output delivery and workflow success; Rust CI `41369526772` and PostgreSQL 18 CI `41369526661` passed. |

| 2026-11-02 | M04 | Added a transport-neutral PostgreSQL outbox dispatcher with bounded batch claims, publish-outside-transaction, ack-after-success, claim fencing, and capped exponential retry. PostgreSQL 18 run `41370357722` and Rust CI run `41370357767` passed; integration verifies failed publish then successful redelivery of the same event ID with attempts/token both equal to 2. |

| 2026-11-02 | M04 | Added immutable PostgreSQL migration v2 and `PgInboxRepository` for durable `(consumer_name, event_id)` deduplication, lease-based claims, retry/reclaim, and fenced/idempotent acknowledgement. Expanded migration and inbox integration tests, fixed the migration SQL runner, and verified PostgreSQL 18 run `41376350890` plus full Rust CI run `41376350940`. A concrete JetStream adapter and crash-window tests remain open. |

| 2026-11-02 | M04 | Added `aetherflow-transport-nats` with a real JetStream outbox publisher and inbox-backed pull consumer. Verified stable message-ID deduplication, broker-accepted/outbox-ack retry, durable consumer dedup, long-handler inbox/JetStream heartbeat, and failed-handler redelivery/retry against PostgreSQL 18 + NATS (run `41378596284`); Rust CI run `41378596179` passed format, Clippy, tests, workspace build, and docs. Only OS-level process-kill tests across commit/publish/ack windows remain. |

| 2026-11-02 | M04 complete | Added OS-process crash-window integration tests for JetStream publish/outbox-ack and inbox-commit/broker-ack. PostgreSQL 18 + NATS run `41380067612` and full Rust CI run `41380067843` passed; both child processes exited at the intended boundary and recovery suppressed duplicate effects. Marked M04 21/21. |

| 2026-11-02 | M05 | Accepted ADR-0008 for PostgreSQL-backed node membership, worker registrations, per-process incarnation epochs, draining, and task-lease fencing. Next: additive v3 migration and membership repository. |

## Resume instructions

1. Open this file on the default branch.
2. Read **Current checkpoint**.
3. Inspect the latest commit and active milestone artifacts.
4. Continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
