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
- [x] Implement additive PostgreSQL v3 `cluster_nodes` and `worker_registrations` schema plus `PgMembershipRepository` for join/takeover, heartbeats, worker registration, drain, active-worker discovery and stale-epoch fencing (PostgreSQL 18 `41382505310`; Rust CI `41382505507`).
- [ ] Gate durable task claims and worker heartbeat/completion/failure/cancellation/timeout/recovery writes on a live worker registration and matching node epoch in the same PostgreSQL transaction.
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

**Completed:** M00 architecture baseline (owner license selection remains open); M01 Rust workspace foundation; M02 execution domain/state machines; M03 single-node runtime; and M04 durable persistence are implemented and hosted-verified. M04 includes PostgreSQL v1/v2 migrations, durable task repository/coordinator/worker, transactional outbox dispatcher, PostgreSQL consumer inbox, real NATS JetStream publisher and pull consumer, durable retries, workflow aggregation, and OS-process crash recovery at both publisher/outbox-ack and consumer/inbox-commit/broker-ack windows.

**M05 membership persistence implemented:** ADR-0008 is backed by additive PostgreSQL migration v3. The `cluster_nodes` table stores stable node IDs, process incarnation IDs, membership epochs, active/draining state, heartbeat and database-owned lease expiry. `worker_registrations` stores worker ID, parent node/epoch, concurrency capacity, lifecycle state and a lease capped by the parent node. `PgMembershipRepository` supports same-incarnation renewal, competing-incarnation rejection, expired-lease takeover with epoch increment, node/worker heartbeat, worker registration, worker/node drain, and active-worker discovery. A node heartbeat that shortens the parent lease atomically caps child worker leases. PostgreSQL time is authoritative; old epoch handles are fenced after takeover.

**Verification:** PostgreSQL 18 run `41382505310` and Rust CI run `41382505507` passed on commit `79c5b690`. The PG18 workflow verified migration v1-v3 checksums, 9 required tables, 13 indexes and membership constraints. Two membership integration tests passed: one covers registration, heartbeat, drain, lease cap, expiry, takeover (epoch 1→2), stale node/worker rejection and active-worker discovery; the second verifies exactly one winner when two different incarnations join the same unowned node concurrently. The same PG18 run also passed existing PostgreSQL task/migration and real NATS JetStream integration tests. Rust CI passed format, Clippy, workspace tests, full build and documentation build. M04 process-crash verification remains PG18 + NATS `41380067612` and Rust CI `41380067843`.

**In progress:** M05 distributed coordination and event delivery. Membership persistence is implemented and tested, but `PostgresTaskWorker` and the task repository's claim/heartbeat/completion/failure/cancellation/timeout/recovery transactions do not yet require a live worker registration and matching node epoch. PostgreSQL is still the membership/time authority and NATS remains transport only.

**Next action:** Add membership-aware task repository operations. Task claim must lock and validate the live active node and worker registration before locking a ready task. Heartbeat and all settlement/recovery paths must validate the same node ID, incarnation, epoch and worker status in the same short transaction. Preserve a single lock order (node → worker → task → attempt/outbox) to avoid deadlocks. Wire `PostgresTaskWorker` to own and renew a `WorkerRegistration`, stop claims when node/worker state is draining or a database lease is lost, and reject stale writes after takeover. Then test database partitions, rejoin, multi-node worker loss and fail-closed behavior.

**Known limitations:** Membership persistence is implemented but is not yet enforced inside task claim or settlement SQL. There is no long-running node supervisor for periodic node/worker renewals or graceful drain orchestration; multi-node partition, rejoin and worker-loss chaos tests remain open. Until task transaction gates land, existing per-task fencing is the only enforced task-write guard. The process-local `TaskScheduler` remains for local development/tests. NATS deployment must provision streams/subjects, durable pull consumers, credentials, retention, explicit acknowledgements and suitable `AckWait`. Delivery remains at-least-once; external side effects require downstream idempotency or a transaction shared with inbox completion. Task payloads remain opaque bytes capped at 1 MiB.

**Blockers:** No migration, repository or CI blocker remains for the membership persistence slice. The next M05 implementation boundary is wiring membership checks into task repository transactions. License selection remains with the repository owner.

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

| 2026-11-02 | M05 | Added PostgreSQL migration v3 and `PgMembershipRepository` for node incarnation/epoch leases, worker registration/capacity, heartbeat, drain, active-worker discovery, expired-lease takeover and stale-epoch fencing. PostgreSQL 18 run `41382505310` and Rust CI `41382505507` passed; migration ledger v1-v3, 9 tables, 13 indexes, and concurrent membership tests were verified. |

## Resume instructions

1. Open this file on the default branch.
2. Read **Current checkpoint**.
3. Inspect the latest commit and active milestone artifacts.
4. Continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
