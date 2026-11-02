# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M05 distributed coordination is hosted-verified and complete. CI covers separate-process PostgreSQL packet-level egress partition/takeover and stale rejoin through a UID-scoped host-firewall rule, abrupt worker-process loss during an active handler, one-member loss in a three-member NATS JetStream cluster, and a two-round combined chaos campaign. The combined test verifies lease expiry/takeover, higher fencing-token recovery, workflow/task/attempt convergence, complete outbox publication, and inbox suppression of a replayed logical duplicate (PostgreSQL 18 + NATS run [41402000557](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000557); Rust CI [41402000570](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41402000570)). The packet test does not claim a separate network namespace or a full bidirectional network blackhole. M06 WASM sandboxing is next. APIs remain experimental and the complete distributed workflow engine is not production-ready.
>
> **Live overview:** [Open the AetherFlow GitHub Pages site](https://pooya16pg-spec.github.io/aetherflow/).

## Why AetherFlow?

Distributed work is easy to start and hard to make reliable. Processes crash, networks partition, workers disappear, messages are delivered more than once, and external side effects cannot always be rolled back. AetherFlow aims to make these realities explicit rather than hiding them behind a simplistic task queue.

## Design goals

- Durable workflow and task state with auditable transitions.
- At-least-once delivery paired with idempotent execution primitives.
- Recoverable workers using leases, heartbeats, and bounded retries.
- Explicit cancellation, timeout, concurrency, and backpressure semantics.
- A modular Rust core with async execution.
- A sandboxed WebAssembly extension boundary.
- Operational visibility through structured events, metrics, and traces.
- Reproducible tests for failure and recovery behavior.

AetherFlow will **not** claim universal exactly-once execution. External side effects require application-level idempotency or an explicit transactional integration.

## Planned technology

| Area | Direction |
|---|---|
| Core | Rust, Tokio |
| HTTP control API | Axum |
| RPC | Tonic / gRPC (when justified) |
| Durable storage | PostgreSQL |
| Event transport | NATS JetStream (after core semantics are proven) |
| Extensions | Wasmtime |
| Observability | OpenTelemetry |
| Dashboard | TypeScript UI (later phase) |
| Deployment | Docker Compose; Kubernetes only after operational needs are demonstrated |

Technology choices are provisional until validated by implementation and recorded in architecture decision records.

## Repository map

The repository is being built incrementally. See [ROADMAP.md](ROADMAP.md) for the authoritative checklist and current checkpoint, [docs/architecture/overview.md](docs/architecture/overview.md) for system boundaries, [docs/decisions/](docs/decisions/) for architecture decisions, and [docs/development.md](docs/development.md) for local commands.

## Development status

The Rust workspace contains an infrastructure-independent core, a single-node runtime with a process-local in-memory repository, a bounded Tokio task scheduler and workflow DAG coordinator, plus a minimal bootstrap health server. The runtime supports typed handlers, bounded concurrency, retry and per-attempt timeout policies, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and successful direct-dependency output access. M03 is hosted-verified. The M04 PostgreSQL adapter includes a checksum/advisory-lock migration runner, durable workflow materializer, and async durable worker over the shared runtime handler contract. `PgWorkflowCoordinator` persists validated DAGs with deterministic task-run IDs and dependency edges. `PostgresTaskWorker` uses the runtime `TaskRegistry` and `TaskContext`, bounded concurrency, PostgreSQL claims/heartbeats/fencing, dependency output loading, durable retry/failure/timeout transitions, lease recovery, and automatic workflow aggregation. PostgreSQL 18 integration CI has executed a two-task DAG through this worker and verified that the child handler receives the root output and the workflow reaches `succeeded`; existing coverage also exercises queued dependency and running sibling cancellation, stale worker rejection, subprocess lease recovery, and transaction rollback when outbox insertion fails. The existing `TaskScheduler` and in-memory repository remain process-local. A transport-neutral PostgreSQL outbox dispatcher now publishes outside transactions, acknowledges only after publisher success, and reschedules failures with capped exponential backoff; PostgreSQL 18 CI verifies a failed publish followed by successful redelivery of the same event ID. The PostgreSQL adapter now includes migration v2 and a durable `PgInboxRepository` keyed by consumer name and stable event ID. It atomically claims new/pending/expired events, reports busy or already-processed deliveries, supports release and reclaim, and fences acknowledgements with monotonically increasing claim tokens. PostgreSQL 18 CI verifies migration checksums, inbox constraints, duplicate suppression, lease recovery, and stale-worker rejection. The isolated `aetherflow-transport-nats` crate now contains a real JetStream outbox publisher and inbox-backed pull consumer. The publisher awaits broker storage acknowledgement and uses the stable event ID as `Nats-Msg-Id`; the consumer commits inbox completion before broker acknowledgement, suppresses logical redelivery, renews PostgreSQL leases and JetStream progress acknowledgements for long handlers, and releases failed claims for retry. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry window, durable inbox deduplication, long-handler heartbeat, and handler failure/retry. OS-level process-kill tests now pass for both critical broker windows: the publisher exits after JetStream storage but before outbox acknowledgement, and the consumer exits after inbox commit but before broker acknowledgement. Fresh processes recover both cases without duplicate logical effects (PostgreSQL 18 + NATS run `41380067612`; Rust CI `41380067843`). M04 is complete. M05 adds additive PostgreSQL v3 node/worker membership storage and a `PgMembershipRepository` with incarnation epochs, expired takeover, heartbeats, draining, worker capacity, and stale-epoch fencing. All durable task claim and settlement transactions validate the live worker registration and node epoch in the same PostgreSQL transaction. The two-round combined packet-partition, active-worker process-kill, and NATS member-loss campaign passed PostgreSQL 18 + NATS run `41402000557`; Rust CI `41402000570` passed the full workspace checks. M05 is complete; see [ROADMAP.md](ROADMAP.md) for the authoritative checkpoint.

M06 has started with the versioned plugin contract: [ADR-0009](docs/decisions/0009-versioned-wasm-plugin-abi.md), the [WIT v1 interface](crates/aetherflow-wasm/wit/plugin.wit), and the `aetherflow-wasm` ABI crate. ABI v1 is default-deny, exposes only bounded `log.write`, and does not import WASI. Wasmtime 48.0.8 LTS is now integrated in `aetherflow-wasm`: the runtime configures fuel, epoch interruption, fresh per-invocation Stores, memory/table/component-handle ceilings, and capability-gated/rate-limited logging. Rust CI `41405386605` passed strict Clippy, workspace tests, Release build, and documentation build; Pages deployment `41405386353` also passed. The current tests validate compilation and host policy types but do not yet execute a valid guest component, so end-to-end execution, resource-exhaustion, trap, and adversarial-import tests remain open. The workspace MSRV is now 1.95.0 (the pinned toolchain is newer).


M05 adds PostgreSQL v3 node/worker membership and membership-gated durable execution. `PostgresTaskWorker` requires a live `WorkerRegistration`, renews membership during active work and graceful drain, and only claims task IDs supported by its `TaskRegistry`. Task claim, heartbeat, completion, failure, registered cancellation, timeout, recovery, promotion, and workflow aggregation validate the current node incarnation/epoch and worker lease inside the same PostgreSQL transaction as the write. Administrative cancellation remains a separate control-plane API. Component-level multi-process partition/rejoin, abrupt worker loss, NATS quorum loss, and the combined two-round chaos campaign are hosted-verified. M05 is complete; M06 introduces the WASM extension runtime.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. The `aetherflow-transport-nats` crate now integrates a real JetStream publisher and inbox-backed pull consumer. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry boundary, stable message-ID deduplication, logical consumer deduplication, lease and AckWait heartbeat for long handlers, and failed-handler redelivery. The durable worker/coordinator remains an opt-in PostgreSQL execution path. OS-level process termination after broker storage and after inbox commit is now integration-tested on PostgreSQL 18 + NATS (run `41380067612`). Delivery remains at-least-once; external side effects require application-level idempotency or a transaction shared with inbox completion. M05 membership-gated task execution is verified on PostgreSQL 18 + NATS (41402000557) with Rust CI (41402000570). Tests cover stale-epoch write rejection, replacement-worker recovery after task-lease expiry, registry capability filtering, lease renewal while gracefully draining a long-running handler, UID-scoped packet-level PostgreSQL partition/rejoin, abrupt worker-process loss, three-member NATS quorum recovery, and the combined two-round chaos campaign. This is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
