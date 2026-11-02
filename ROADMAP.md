# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M06 — WASM extension runtime**  
Overall status: **M00–M05 complete; M06 is in progress. The versioned WIT ABI, Wasmtime 48.0.8 engine, per-invocation execution limits, and capability-gated logging are implemented; hosted verification and adversarial tests remain open. M07–M09 are not started. Owner license selection and deferred property-based transition tests remain open.**

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
- [x] Extend redelivery and lease-expiry coverage to separate OS processes, local TCP fallback, UID-scoped packet-level PostgreSQL egress partition/takeover and stale-process rejoin, active worker-process loss, and NATS three-member JetStream member-loss recovery. Hosted verification: PostgreSQL 18 + NATS `41397940218`, Rust CI `41397940143`; exact boundaries and combined-chaos gap are below.

**M05 chaos-test exit criteria (all component and combined cross-layer faults hosted-verified):**
1. [x] Run an old worker in a child OS process with its own PostgreSQL login and durable task claim. The child connects only through a local TCP relay; the parent closes the relay, which aborts established proxy streams and rejects new connections. The parent and replacement continue using a direct PostgreSQL connection, take over the same logical node ID at a higher epoch, and let the old process confirm a failed heartbeat while the TCP path is cut. This is a real process-separated TCP-path partition; it is not yet a packet-drop firewall/network-namespace test.
2. [x] Restore the TCP relay while the replacement owns attempt 2. The old process reconnects through the relay and its stale heartbeat and completion are rejected with `NodeMembershipLost`; only then does the replacement complete the recovered task.
3. [x] Kill a separate `PostgresTaskWorker` process without graceful shutdown while its handler is running. After membership/task leases expire, the replacement recovers attempt 2, receives a higher fencing token, completes the task, and aggregates the workflow to `Succeeded`.
4. [x] Run three NATS 2.12 JetStream members with two routes each and a stream configured with three replicas. Stop member 1 while PostgreSQL remains healthy; the outbox dispatcher must obtain the publish acknowledgement while that member is still down (first attempt or lease-expiry retry through the surviving quorum). Restart member 1, verify cluster connectivity returns, and assert the consumer receives the stable event ID exactly once with no duplicate message.
5. [x] Add packet-level PostgreSQL network isolation between the old process and PostgreSQL. In CI the child runs under dedicated UID `nobody`; an `iptables OUTPUT` owner rule scoped to that UID and PostgreSQL TCP destination port rejects its egress packets while controller/replacement traffic remains direct. The child fails closed while partitioned; a higher-epoch replacement recovers attempt 2 with a larger fencing token; after removing the rule, stale heartbeat/completion from the rejoined old process are rejected. Full PostgreSQL 18 + NATS workflow `41397940218` and Rust CI `41397940143` passed on implementation source `3c4ecf4`. Boundary: host-firewall/UID-scoped egress isolation, not a separate network namespace or full bidirectional blackhole.
6. [x] Run a two-round combined end-to-end chaos campaign overlapping PostgreSQL packet partition, abrupt active-worker process loss, and one NATS JetStream member outage. Hosted-verified by PostgreSQL 18 + NATS [run 41402000557](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000557) and Rust CI [run 41402000570](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000570). Each round stops one NATS member, applies the UID-scoped PostgreSQL egress rule, then kills the isolated worker process group while its handler is live. The test waits for PostgreSQL's authoritative node-lease expiry, takes over at a higher membership epoch, recovers attempt 2 with a higher fencing token, completes the workflow, verifies every outbox row is published, processes every unique event once, and replays a logical duplicate under a new NATS message ID to prove inbox suppression.
- [x] Document current at-least-once delivery guarantees and operational limits in the PostgreSQL transaction contract; extend with membership/partition behavior as M05 implementation lands.

### M06 — WASM extension runtime
- [x] Define versioned plugin ABI and capability model. ADR-0009, the WIT package `aetherflow:plugin@1.0.0`, and the `aetherflow-wasm` crate establish same-major/minor compatibility, typed task context, default-deny capabilities, and bounded input/output/resource policies. Unit tests cover ABI compatibility, default-deny grants, and hard resource ceilings.
- [ ] Hosted-verify the Wasmtime 48.0.8 LTS runtime implementation. The engine, safe component compilation, per-invocation Store, default-deny `log.write` host import, host-call fuel, log size/rate limits, memory/table/component-handle limits, instruction fuel, and epoch-interrupt deadline are implemented. Workspace MSRV is now 1.95.0. The code source is `8df1f4f`; the latest Rust CI on the final documentation head is queued. Do not mark this item complete until a hosted run passes. Add a valid WIT component fixture and exercise successful execution, denied capabilities, guest errors, fuel exhaustion, deadline interruption, memory/table limits, and unknown/WASI imports.
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

**Canonical branch:** `main`. Fetch the live tip before every edit and compare its parent history; never assume the SHA below is still current after a session interruption.  
**Main HEAD before this checkpoint update:** `8df1f4f6a7ac09e9ca13ea410f9f92b2ce215b1a` (M06 ABI, Wasmtime 48.0.8 dependency, MSRV 1.95, engine configuration, safe component compilation, per-invocation Store, capability-gated logging, resource budgets, and epoch interruption are present; hosted verification is pending).
**Main source tip at verification:** `9ae2d4c8a65a3b033b75deeecdeda232940300ed` (a rustfmt-only commit after the verified implementation source `03e023a330947251a0b5caf8a6523ec1d5a95550`).  
**Last fully hosted-verified implementation source:** `03e023a330947251a0b5caf8a6523ec1d5a95550`.  
**Hosted verification:** PostgreSQL 18 + NATS [run 41402000557](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000557) passed the full workflow on implementation source `03e023a`, including PostgreSQL storage/migrations, packet-level partition, two-round combined chaos, three-member NATS quorum recovery, outbox convergence, inbox deduplication, and schema invariants. Rust CI [run 41402000570](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000570) passed formatting, Clippy, all workspace tests, release build, docs, and lockfile artifact upload; its rustfmt step produced the formatting-only main tip `9ae2d4c`. GitHub Pages [run 41402037576](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402037576) passed on `9ae2d4c`. The source difference between the tested implementation and current main is formatting-only. Live page: https://pooya16pg-spec.github.io/aetherflow/.

**Current milestone:** M06 — WASM extension runtime. The versioned ABI/capability contract and initial Wasmtime execution path are implemented; hosted verification and adversarial component tests are next.  
**Next task:** add a minimal valid component fixture generated from the checked-in WIT, then test end-to-end execution, default-deny and granted logging, output limits, guest errors, fuel exhaustion, epoch timeout, memory/table/handle ceilings, and rejection of undeclared imports/WASI. Only after these tests pass should the runtime be wired into `aetherflow-runtime`.

**Completed baseline:** M00 architecture baseline (owner license selection remains open); M01 Rust workspace foundation; M02 execution domain/state machines (property-based transition tests deferred); M03 single-node runtime; M04 durable persistence; M05 distributed coordination, membership fencing, packet partition/takeover, active-worker loss, NATS quorum loss, and combined chaos recovery; M06 versioned WIT ABI and default-deny capability/resource contract. M04 includes PostgreSQL v1/v2 migrations, durable task repository/coordinator/worker, transactional outbox dispatcher, PostgreSQL consumer inbox, real NATS JetStream publisher and pull consumer, durable retries, workflow aggregation, and OS-process crash recovery at publisher/outbox-ack and consumer/inbox-commit/broker-ack windows.

**M05 membership and fencing implementation:** ADR-0008 is backed by PostgreSQL migration v3 and `PgMembershipRepository`. Node membership uses a stable logical node ID, process incarnation, database-owned lease, and monotonically increasing membership epoch. Worker registrations are bound to the parent node and epoch; worker leases are capped by the node lease. Same-incarnation renewal, competing-incarnation rejection, expiry takeover, drain, and active-worker discovery are implemented. PostgreSQL time is authoritative.

**M05 task-transaction enforcement:** registered claims validate the live node incarnation/epoch and worker registration inside the same transaction as the claim. Registered heartbeat, completion, failure, cancellation, timeout, expired-lease recovery, due promotion, and workflow aggregation validate membership before task/attempt/outbox mutation. Node and worker rows are locked before task rows. Claims require active membership; a draining worker may finish or cancel work already assigned to it but cannot claim new work. `PostgresTaskWorker` renews node and worker leases while handlers are active, persists draining on shutdown, and uses bounded graceful drain/cancellation. The worker passes the stable, sorted task-ID set from its `TaskRegistry` into the SQL claim filter; an empty registry claims nothing.

**Verified packet-level PostgreSQL partition/takeover and stale rejoin:** `postgres_database_partition_then_epoch_takeover_recovers_task` starts `postgres_partition_old_worker_child_process` as a separate OS process. In CI (`AETHERFLOW_TEST_PACKET_PARTITION=1`), the child test executable is copied to a shared temporary directory with executable permissions and launched as the dedicated `nobody` UID with an explicit environment. `IptablesOwnerFilter` applies an `OUTPUT` firewall rule scoped to that UID and the PostgreSQL TCP destination port; it rejects only the old worker's outbound PostgreSQL packets while the parent/controller and replacement retain their direct database path. The child confirms fail-closed heartbeat behavior during partition. After lease expiry, the parent takes over the same logical node ID with a new incarnation and higher epoch, recovers/reclaims attempt 2 with a higher fencing token, removes the firewall rule while replacement work remains active, and lets the old process reconnect. Its stale heartbeat and completion are rejected with `NodeMembershipLost`; only then does the replacement complete the task. Full PostgreSQL 18 + NATS run `41397940218` and full Rust CI run `41397940143` passed against implementation source `3c4ecf4`. **Boundary:** this is a packet-level host firewall test scoped to child UID and PostgreSQL egress port. It does not claim a separate network namespace or a full bidirectional network blackhole. Local runs without firewall privileges retain the `TcpDatabaseGate` fallback.

**Fault-test cleanup guarantees:** `IptablesOwnerFilter` removes its firewall rule on drop, `TcpDatabaseGate` aborts its relay task if dropped during unwinding, and `KillOnDropChild` kills/reaps the OS process if parent assertions fail before stale-epoch assertions finish. The temporary PostgreSQL role is explicitly dropped on the successful path; CI test databases are ephemeral.

**Verified abrupt worker-process loss:** `postgres_worker_process_death_recovers_inflight_task` spawns `postgres_task_worker_crash_child_process`, which runs a real `PostgresTaskWorker` and enters a 60-second handler. The parent verifies the durable task is running and kills the process without requesting shutdown. After membership and task leases expire, a replacement incarnation recovers and reclaims attempt 2 with a higher fencing token, completes the task, and aggregates the workflow to `Succeeded`.

**Verified three-member NATS JetStream member loss:** PostgreSQL/NATS CI starts three NATS 2.12 servers on an isolated Docker network, verifies two cluster routes per server, and exposes three seed endpoints. The integration creates a stream with `num_replicas: 3`. With PostgreSQL kept healthy, it stops member 1 and runs the outbox dispatcher. The test only accepts success if the stable event is acknowledged while that member is still down—either on the initial dispatch or after the outbox lease expires and retry reaches the surviving quorum. It then restarts member 1, waits for cluster connectivity, verifies the outbox is published, reads the stable event ID from the durable consumer, acknowledges it, and asserts no second message is present. This proves one-member loss/quorum delivery and rejoin; it does not prove a specific leader was elected on a particular member, nor packet-level partition between NATS members.

**Other verified delivery guarantees:** publisher-outage retry keeps the same event ID; the broker member-loss path stores exactly one JetStream message; inbox dedup and one-logical-effect behavior across consumer-process restart remain covered by M04 run `41380067612`. Delivery remains at-least-once. External side effects require downstream idempotency or a transaction shared with inbox completion.

**M05 exit status:** no remaining M05 implementation or verification items. The combined campaign ran two iterations and verified workflow/task/attempt convergence, higher fencing-token recovery, complete outbox publication, unique inbox processing, and suppression of an intentional logical duplicate. The separate network-namespace/full bidirectional-blackhole scenario remains outside the claimed packet-partition boundary.

**Known limitations:** A task remains ready if no live worker advertises its task ID; capability visibility/alerting is future operational work. The local in-memory `TaskScheduler` is for development/tests and is not distributed ownership. NATS deployment still requires streams/subjects, durable consumers, credentials, retention, explicit acknowledgements, and suitable `AckWait`. Payloads remain opaque bytes capped at 1 MiB. No production-readiness claim is made.

**Next concrete actions:**
1. M06: define a versioned WASM plugin ABI, capability model, compatibility policy, and explicit host-call surface.
2. Integrate Wasmtime with bounded fuel, memory, and execution time; keep untrusted modules fail-closed.
3. Add authorization, deterministic error mapping, and tests for traps, resource exhaustion, and malicious/untrusted modules.
4. Publish a minimal plugin example and record the ABI decisions in an ADR before expanding the host API.

**Recovery instructions:** Follow the “Session recovery, verification, and rollback protocol” above. If a session stops unexpectedly, fetch `main`, compare live HEAD against this recorded implementation snapshot, inspect intervening commits and exact Actions `head_sha` values, then continue from the first unchecked task. Never infer success from a pending/cancelled run.

**Owner-only blocker:** Choose an explicit open-source license before public release. No legal terms have been assumed.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M05 complete | Hosted-verified the two-round combined PostgreSQL partition, active-worker process-group kill, and NATS member-loss campaign. PostgreSQL 18 + NATS `41402000557`, Rust CI `41402000570`, and Pages `41402037576` passed; M06 is next. |
| 2026-11-02 | M06 ABI foundation | Added `aetherflow-wasm`, WIT ABI `aetherflow:plugin@1.0.0`, default-deny `log.write` capability set, typed execution context, and hard input/output/memory/fuel/host-call/handle/deadline budgets. Accepted ADR-0009; Wasmtime runtime integration remains next. |
| 2026-11-02 | M06 Wasmtime runtime implementation | Added exact Wasmtime `48.0.8` LTS dependency, raised workspace MSRV to `1.95`, configured Component Model + Cranelift + fuel + epoch interruption, added safe component compilation, fresh bounded Store execution, and capability-gated/rate-limited logging. Rust CI `41404117603` was cancelled by subsequent main pushes before execution; latest Rust CI `41404325498` and Pages `41404324928` are queued on the final documentation head. Valid-component and adversarial runtime tests remain open. |
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
| 2026-11-02 | M05 | Added the interruption recovery/rollback protocol to the master roadmap and synchronized the PostgreSQL transaction contract with registered-worker fencing, capability filtering, and draining semantics in one atomic documentation commit `11c4430`. Rust CI `41386598280` and Pages `41386597327` passed. |

| 2026-11-02 | M05 | Added a PostgreSQL access-partition regression: the old worker uses a dedicated role, its login is disabled and sessions terminated, the same logical node is taken over by a replacement incarnation at a higher epoch, and the old worker is rejected after DB access is restored. The replacement recovers/reclaims attempt 2 with a higher fencing token and completes. PostgreSQL 18 + NATS run [41387614000](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387614000) and Rust CI [41387613957](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387613957) passed. This is DB-access fault injection, not a separate-host packet-level network partition. |
| 2026-11-02 | M05 | Added injected broker-unavailable retry plus a CI-only real NATS container stop/start with JetStream state on persistent Docker volume. PostgreSQL stayed online; the interrupted outbox claim expired, retried with the same event ID, and the durable consumer received one message. PostgreSQL 18 + NATS [41387614000](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387614000), Rust CI [41387613957](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387613957), and Pages [41387655365](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387655365) passed. The Pages run targets formatting-only main commit `030a5ba`. |
| 2026-11-02 | M05 | Added CI packet-level PostgreSQL egress isolation for the old worker process, scoped by dedicated UID and PostgreSQL TCP port. Fixed the initial sudo launch issue by copying the test executable to a shared temporary directory, setting executable permissions and passing child variables explicitly through `/usr/bin/env`; the child now claims, partitions, rejoins, and is fenced after higher-epoch takeover. PostgreSQL 18 + NATS [run 41397940218](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397940218) and Rust CI [run 41397940143](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397940143) passed on implementation source `3c4ecf4`; Pages [run 41397988550](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397988550) passed on formatting-only tip `c72f69a`. The combined cross-layer chaos campaign is the first unchecked M05 criterion. |

## Incident log

| Date | Failed run | Detection | Root cause | Corrective commit | Verification |
|---|---|---|---|---|---|
| 2026-11-02 | [41401550331](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41401550331) | Combined-chaos recovery reported `NodeLeaseBusy`; the worker child later attempted to write into a removed marker directory. | The test guard terminated the `sudo` supervisor but did not reliably terminate the supervised worker process. The orphan could continue renewing its node lease and outlive test cleanup. | `03e023a` gives the sudo launch an isolated process group, kills the complete group, and waits for PostgreSQL's authoritative lease-expiry condition before takeover. | PostgreSQL 18 + NATS [41402000557](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000557), Rust CI [41402000570](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000570), and Pages [41402037576](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402037576) passed. |
| 2026-11-02 | [41400827449](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41400827449) | The first combined-chaos run stopped making handler progress before worker kill; the publisher retry test also observed an unexpected extra outbox attempt. | The test waited after applying the DB partition before killing the active worker, and parallel tests could contend for the shared outbox/NATS fixture. | `c009352` moved the kill to the overlapping fault boundary; `c5d2c8f` serialized the shared NATS integration tests. A later serialized run exposed the separate sudo process-tree cleanup issue recorded above. | The intermediate PostgreSQL + NATS run [41401545387](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41401545387) passed; final verification is [41402000557](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000557) plus Rust CI [41402000570](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000570). |
| 2026-11-02 | [41397690910](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397690910) | Packet-partition integration test timed out before the old-worker readiness marker appeared. | `sudo -E` was ignored by the runner's sudo build and the test binary remained under `/home/runner/work/...`, which the `nobody` UID could not execute/traverse; the child never claimed the task. | [3c4ecf4](https://github.com/pooya16pg-spec/aetherflow/commit/3c4ecf4acc25fda7e9f833fe5a4da04197c30b0d) copies the executable to a shared temp directory, sets mode `0755`, and passes child environment explicitly via `/usr/bin/env` after `sudo -u nobody`. | PostgreSQL 18 + NATS [41397940218](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397940218) and Rust CI [41397940143](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41397940143) passed on the corrected implementation source. |
| 2026-11-02 | 41396388194 / 41396388224 | Cleanup guard patch failed compilation with E0509. | A type implementing `Drop` cannot move its control-sender field directly in an async shutdown method. | `abf8bb9` stores the sender in `Option`, closes it with `take()`, and aborts any relay task left during unwind. | PostgreSQL 18 + NATS [41396515766](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515766), Rust CI [41396515757](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515757), and Pages [41396515007](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515007) passed. |
| 2026-11-02 | 41389441731 / 41389441852 | NATS three-node test did not compile. | `async_nats::jetstream::stream::Config` has no `replicas` field; the async-nats 0.46 API names it `num_replicas`. | `138c7e5` changed the config to `num_replicas: 3`. | Later PostgreSQL + NATS run [41393631402](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631402) passed the transport integration. |
| 2026-11-02 | 41393400599 / 41393400813 | Cluster integration connection failed with `invalid port number`; a follow-up Rust run caught a readiness-helper type mismatch. | A comma-separated seed list was passed as one URL, and the first helper shape did not preserve transient connection errors as a retryable result. | `bd14ecf` parses the seed list into `Vec<ServerAddr>` and advertises host-mapped client ports; `a9305ed` corrected the readiness result match; `750b0ae` returns `Result` so recovery retries transient failures. | PostgreSQL + NATS [41393631402](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631402) and Rust CI [41393631501](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631501) passed. |
| 2026-11-02 | [41386911298](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41386911298) | New DB-partition test failed before takeover assertions. | PostgreSQL utility statement `CREATE ROLE` does not accept a bind placeholder for the password option. | `b143db6` uses a generated restricted-character SQL literal for the ephemeral test role. | [41387614000](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387614000) passed full PostgreSQL/NATS workflow. |
| 2026-11-02 | [41387001815](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387001815) | Stale-epoch test and DB-partition test interfered under concurrent integration tests. | An unrestricted test worker could claim another test's ready task. | `3a19a2d` confines the stale-epoch test's claims to its own task capability. | [41387056602](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387056602) passed; later partition and NATS restart verification is [41387614000](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41387614000). |
| 2026-11-02 | M05 | Added a separate-OS-process PostgreSQL access-partition regression. A child process owns the old node incarnation/worker registration and task claim; its login is disabled and existing sessions are terminated while the child remains alive. The parent takes over the same logical node ID at a higher epoch. The child confirms fail-closed behavior while disconnected, then reconnects while replacement work is active and has stale heartbeat/completion rejected. The replacement recovers attempt 2 with a higher fencing token and completes. PostgreSQL 18 + NATS run [41393631402](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631402) passed. This uses credential/session partitioning, not a packet-level network blackhole. |
| 2026-11-02 | M05 | Added abrupt active-worker process-loss coverage. A separate child runs a real `PostgresTaskWorker` and enters a long-running handler; the parent kills the process without graceful shutdown. After lease expiry, a replacement worker recovers/reclaims attempt 2 with a higher fencing token, completes it, and the workflow aggregates to `Succeeded`. Included in PostgreSQL 18 + NATS run [41393631402](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631402); Rust CI [41393631501](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631501) also passed. |
| 2026-11-02 | M05 | Upgraded the PostgreSQL/NATS CI fixture from one NATS server to a three-member NATS 2.12 JetStream cluster with two routes per member and a three-replica stream. The test stops member 1 while PostgreSQL stays healthy, requires the outbox event to be acknowledged via the surviving quorum before member 1 is restarted, then verifies the stable event ID is delivered once and no duplicate remains. Cluster setup and transport integration passed in PostgreSQL 18 + NATS [41393631402](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631402); Rust CI [41393631501](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393631501) passed Clippy, tests, workspace build and docs. |
| 2026-11-02 | M05 | Refined the persistent seed-server connection helper so transient NATS reconnect failures return through `Result` and can be retried during recovery. Functional verification passed on `750b0aeb7602867ad66c24b157d3f1d65babe0a4`; formatting-only main follow-up is `6bb8b801b83250b2910763f443dc6c14b6df5732`. GitHub Pages deployment [41393687230](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41393687230) passed on the formatting-only follow-up. |
| 2026-11-02 | M05 | Replaced the PostgreSQL credential-revocation fault in the old-worker test with a dedicated local TCP relay. Only the child process connects through the relay; the parent/replacement retain a direct PostgreSQL path. Closing the gate aborts established proxy streams and rejects new connections; after higher-epoch takeover and attempt-2 recovery, reopening the gate lets the old child reconnect while replacement work is active, where stale heartbeat/completion are fenced. PostgreSQL 18 + NATS [run 41395938794](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41395938794) passed the membership/storage suite, NATS cluster and transport tests; Rust CI [run 41395939011](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41395939011) passed format, Clippy, all tests, workspace build and docs. Pages [run 41395972840](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41395972840) passed on formatting-only HEAD `32d59fc`. The remaining network gap is packet-level drop/firewall/network-namespace isolation, not TCP connection severance. |
| 2026-11-02 | M05 | Hardened chaos-test cleanup paths: `TcpDatabaseGate` aborts its relay task if dropped during unwinding, while graceful shutdown closes its control channel through `Option::take()`; `KillOnDropChild` kills and reaps the old OS child if any parent assertion fails before the child exits. PostgreSQL 18 + NATS [run 41396515766](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515766), Rust CI [run 41396515757](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515757), and Pages [run 41396515007](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41396515007) all passed on `abf8bb9`. |

## Resume instructions

1. Open this file on `main` and follow **Session recovery, verification, and rollback protocol**.
2. Compare the current `main` SHA with **Current checkpoint**; inspect any newer commits.
3. Match every CI result to its exact `head_sha`; pending/cancelled runs are not verification.
4. Read the current checkpoint and continue from the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes, failure causes, corrective commits, and next actions before stopping.
