# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M05 — Distributed coordination and event delivery**  
Overall status: **M00–M04 complete; M05 membership fencing and capability-aware claims implemented and hosted-verified; multi-node partition/worker-loss chaos tests remain. 25 roadmap items remain, including the owner license decision and deferred property tests.**

## Progress rules

- [x] Create this persistent roadmap in the repository.
- [x] Check tasks only after the artifact exists; verification tasks require an actual run.
- [x] Record exact commands and outcomes; never mark an unrun test as passing.
- [x] Keep this file current at every milestone boundary and before ending a work session.
- [x] Prefer small, reviewable commits and explicit limitations.
- [x] Record blockers, assumptions, and manual actions explicitly.


## Session recovery, verification, and rollback protocol

This project is developed directly on `main`. The branch is the canonical source of truth; do not create parallel implementation branches or switch to another repository unless the owner explicitly asks.

### Checkpoint fields that must survive every interruption

Before ending a work session, update **Current checkpoint** with:
- exact `main` HEAD SHA and the last fully verified source SHA (these can differ);
- latest Rust CI, PostgreSQL/NATS CI, and Pages run IDs, URLs, head SHAs, and conclusions;
- completed milestone slice, first unchecked task, next concrete action, and known limitations;
- any failing/cancelled run, root cause, corrective commit, and the run that verified the fix;
- manual decisions or owner-only actions that block release.

A commit is not a verified checkpoint merely because it is on `main`. A cancelled, queued, pending, or in-progress run is never a pass. Only mark a verification task complete when the required workflow has concluded successfully for that exact source tree. A Pages deployment proves publishing succeeded; it does not replace Rust or PostgreSQL integration tests.

### Resume procedure

1. Read this file first, then read the current checkpoint and the latest session/incident log entry.
2. Fetch `refs/heads/main`; compare its SHA with the documented source and verified SHAs. Inspect the intervening commit list and changed files before editing.
3. Inspect the newest Actions runs and their `head_sha`. Ignore stale runs from older commits; do not treat cancelled runs as failures or successes.
4. If a newer commit appeared during recovery, fetch the latest file/blob SHAs and reconcile against it. Never write using a stale file SHA.
5. Continue from the first unchecked task. Do not redo a completed task unless a regression or missing verification is demonstrated.
6. Commit the implementation and its tests in small units; once the final source snapshot is ready, let its required CI matrix finish before adding more commits.
7. Update this roadmap and the incident log before closing the session.

### Safe rollback and database recovery

- Default to **fix forward**. Preserve commit history; never force-push, reset the shared branch, or delete later commits to hide a regression.
- For a bad code commit, identify the exact culprit and dependent commits. Prefer a corrective commit; use `git revert <sha>` only when reverting that change is semantically safe, then run the full required CI matrix again.
- Applied PostgreSQL migrations are immutable. Never edit or delete an applied migration or its checksum-ledger row. Correct schema mistakes with a new additive migration and explicit forward data repair.
- Before any destructive or production data operation, require a verified backup and a restore rehearsal in an isolated database. Do not infer that a backup is usable merely because a backup command returned success.
- Keep test databases isolated per workflow. Never run destructive test cleanup against a non-test database.
- If PostgreSQL or NATS is unavailable, fail closed: do not bypass membership/fencing checks or acknowledge an event without its durable inbox outcome. Recover from persisted leases/outbox/inbox state after connectivity returns.
- Do not commit credentials, tokens, private endpoints, or environment files. Record required variable names and setup instructions, never secret values.

### Incident record format

For every material regression, record: detection time; affected commit and Actions run URL; exact failing job/test and useful log excerpt; user-visible/data-integrity impact; root cause; corrective commit; successful verification run; and any remaining follow-up. Keep the failed run linked rather than rewriting history so a later session can understand why a correction exists.

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
- [x] Gate durable task claims and worker heartbeat/completion/failure/cancellation/timeout/recovery/promotion/aggregation writes on a live worker registration and matching node epoch in the same PostgreSQL transaction; filter claims by the worker's registered `TaskRegistry` capabilities (PostgreSQL 18 + NATS run `41386154586`; Rust CI `41386154607`).
- [x] Add transactional outbox/inbox where required (implemented and PostgreSQL 18 verified in M04).
- [x] Integrate NATS JetStream after transport-independent contracts stabilize (isolated publisher/consumer crate and real PostgreSQL 18 + NATS integration in M04).
- [ ] Extend redelivery and lease-expiry coverage to multi-node database/broker partitions, node-epoch takeover, and worker loss (single-process redelivery, lease recovery, and OS-process death windows are already verified in M04).

**M05 chaos-test exit criteria (all still open):**
1. Run two logical nodes and multiple workers against one PostgreSQL cluster; pause or disconnect node A long enough for its database-owned membership lease to expire.
2. Let node B take over the same logical node ID with a strictly higher membership epoch; prove A cannot heartbeat, claim, complete, fail, cancel, timeout, promote, recover, or aggregate through its stale registration.
3. Expire A's task lease and let B recover/reclaim it; assert the attempt number and task fencing token advance and the task reaches one terminal state.
4. Partition NATS while PostgreSQL remains healthy; prove outbox retry preserves the event ID, inbox dedup suppresses duplicate logical effects, and delivery resumes after the broker returns.
5. Restore connectivity to A and the broker; prove stale work is fenced, no task remains permanently stuck, no event is silently lost, and the workflow state matches its task history.
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

**Canonical branch:** `main`  
**Current main HEAD:** `3090267b4b4e99eebb5dcefa2bde94296909cda6` (CI-generated rustfmt-only commit after capability test commit `c892a8d25330b91b5bfbc6a08289a9502a67e8a2`).  
**Last fully verified implementation snapshot:** `c892a8d25330b91b5bfbc6a08289a9502a67e8a2`; the Rust CI applied rustfmt and committed the formatting-only tree as `3090267`, then completed successfully.  
**Current milestone:** M05 — Distributed coordination and event delivery.  
**Next task:** M05 multi-node partition, epoch-takeover, and worker-loss chaos coverage.

**Completed baseline:** M00 architecture baseline (owner license selection remains open); M01 Rust workspace foundation; M02 execution domain/state machines (property-based transition tests deferred); M03 single-node runtime; and M04 durable persistence are implemented and hosted-verified. M04 includes PostgreSQL v1/v2 migrations, durable task repository/coordinator/worker, transactional outbox dispatcher, PostgreSQL consumer inbox, real NATS JetStream publisher and pull consumer, durable retries, workflow aggregation, and OS-process crash recovery at publisher/outbox-ack and consumer/inbox-commit/broker-ack windows.

**M05 membership and fencing implementation:** ADR-0008 is backed by PostgreSQL migration v3 and `PgMembershipRepository`. Node membership uses a stable logical node ID, process incarnation, database-owned lease, and monotonically increasing membership epoch. Worker registrations are bound to the parent node and epoch; worker leases are capped by the node lease. Same-incarnation renewal, competing-incarnation rejection, expiry takeover, drain, and active-worker discovery are implemented. PostgreSQL time is authoritative.

**M05 task-transaction enforcement is now implemented:** registered task claims validate the live node incarnation/epoch and worker registration inside the same transaction as the claim. Registered heartbeat, completion, failure, cancellation, timeout, expired-lease recovery, due promotion, and workflow aggregation validate membership before task/attempt/outbox mutation. Node and worker rows are locked before task rows. Claims require active membership; a draining worker may finish or cancel work already assigned to it but cannot claim new work. `PostgresTaskWorker` renews node and worker leases while handlers are active, persists draining on shutdown, and uses bounded graceful drain/cancellation. The worker passes the stable, sorted task-ID set from its `TaskRegistry` into the SQL claim filter; an empty registry claims nothing, and a worker cannot lease a task for which it has no handler.

**Verification for the current implementation:** PostgreSQL 18 + NATS run [41386154586](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386154586) passed on `c892a8d`: PostgreSQL storage integration tests (including the explicit empty-capability/matching-capability claim test), schema migration/checksum/table/index validation, real JetStream transport integration, and schema invariants. Rust CI [41386154607](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386154607) passed format, Clippy, workspace tests, release workspace build, documentation build, and lockfile artifact upload; its rustfmt step created the formatting-only `3090267` commit on `main`. Pages deployment [41386206466](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386206466) passed on the current `3090267` HEAD. The live page is https://pooya16pg-spec.github.io/aetherflow/.

**Still open in M05:** Full fault-injection across multiple live nodes is not yet verified. In particular, simulate an old node losing PostgreSQL connectivity until leases expire, a replacement incarnation taking over, stale-node reconnection, task recovery/reclaim under a higher fencing token, and an NATS partition with eventual outbox/inbox recovery. Assert no stale state writes, no permanently stuck tasks, no lost events, and no duplicate logical effects. Do not close M05 or start M06 until these scenarios pass.

**Known limitations:** Delivery is at-least-once; external side effects still require downstream idempotency or a transaction shared with inbox completion. A task remains ready if no live worker advertises a handler for its task ID; capability visibility/alerting is future operational work. There is no full multi-node network-partition/rejoin chaos suite yet. The local in-memory `TaskScheduler` remains for development/tests and is not a distributed ownership mechanism. NATS deployment still requires streams/subjects, durable consumers, credentials, retention, explicit acknowledgements, and suitable `AckWait`. Payloads remain opaque bytes capped at 1 MiB.

**Next concrete actions:**
1. Build deterministic two-node PostgreSQL failover tests using distinct process incarnations and real expiry/takeover; prove every stale registered write is rejected and the replacement can recover, reclaim, and complete the task.
2. Add broker-partition tests with PostgreSQL healthy; verify same event ID on outbox retry, inbox deduplication, and exactly one logical handler effect after NATS returns.
3. Add process/worker-loss and stale-node-rejoin cases; verify no work is stuck and all task/workflow/outbox/inbox state converges.
4. Only after the above are hosted-verified, close M05 and begin M06 (WASM ABI and sandbox).

**Recovery instructions:** Follow the “Session recovery, verification, and rollback protocol” above. If a session stops unexpectedly, fetch `main`, compare its SHA to this checkpoint, inspect newer commits and Actions runs, then continue from the first unchecked milestone task. Never infer success from a pending/cancelled run.

**Owner-only blocker:** Choose an explicit project license before public release. No legal terms have been assumed.

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

| 2026-11-02 | M05 | Enforced membership/epoch guards on registered claims and task state transitions; added registry capability-filtered claims, shutdown-drain persistence, stale-epoch cancellation coverage, and a direct PostgreSQL capability-filter test. PostgreSQL 18 + NATS run [41386154586](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386154586) and Rust CI [41386154607](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386154607) passed; Pages deploy [41386206466](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386206466) passed. Source HEAD after CI rustfmt commit: `3090267b4b4e99eebb5dcefa2bde94296909cda6`. |

## Resume instructions

1. Open this file on `main` and follow **Session recovery, verification, and rollback protocol**.
2. Compare the current `main` SHA with **Current checkpoint**; inspect any newer commits.
3. Match every CI result to its exact `head_sha`; pending/cancelled runs are not verification.
4. Read the current checkpoint and continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes, failure causes, corrective commits, and next actions before stopping.
