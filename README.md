# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M00–M06 are complete. The M06 WASM adapter and scheduler-backed example passed hosted Rust CI [41563269188](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41563269188); Pages [41565088903](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41565088903) passed on the M06 documentation checkpoint. M07 is now underway: ADR-0010 and the [OpenAPI 3.1 v1 contract](docs/api/openapi.yaml) define the control-plane resources and safety boundaries; HTTP routes are not yet implemented. The complete platform remains experimental and is not production-ready.
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

## API contract

The initial versioned control-plane contract is published at [docs/api/openapi.yaml](docs/api/openapi.yaml), with compatibility and security decisions in [ADR-0010](docs/decisions/0010-versioned-control-plane-api.md). The OpenAPI document is a design contract, not a claim that the HTTP routes are already implemented.

## Repository map

The repository is being built incrementally. See [ROADMAP.md](ROADMAP.md) for the authoritative checklist and current checkpoint, [docs/architecture/overview.md](docs/architecture/overview.md) for system boundaries, [docs/decisions/](docs/decisions/) for architecture decisions, and [docs/development.md](docs/development.md) for local commands.

## Development status

The Rust workspace contains an infrastructure-independent core, a single-node runtime with a process-local in-memory repository, a bounded Tokio task scheduler and workflow DAG coordinator, plus a minimal bootstrap health server. The runtime supports typed handlers, bounded concurrency, retry and per-attempt timeout policies, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and successful direct-dependency output access. M03 is hosted-verified. The M04 PostgreSQL adapter includes a checksum/advisory-lock migration runner, durable workflow materializer, and async durable worker over the shared runtime handler contract. `PgWorkflowCoordinator` persists validated DAGs with deterministic task-run IDs and dependency edges. `PostgresTaskWorker` uses the runtime `TaskRegistry` and `TaskContext`, bounded concurrency, PostgreSQL claims/heartbeats/fencing, dependency output loading, durable retry/failure/timeout transitions, lease recovery, and automatic workflow aggregation. PostgreSQL 18 integration CI has executed a two-task DAG through this worker and verified that the child handler receives the root output and the workflow reaches `succeeded`; existing coverage also exercises queued dependency and running sibling cancellation, stale worker rejection, subprocess lease recovery, and transaction rollback when outbox insertion fails. The existing `TaskScheduler` and in-memory repository remain process-local. A transport-neutral PostgreSQL outbox dispatcher now publishes outside transactions, acknowledges only after publisher success, and reschedules failures with capped exponential backoff; PostgreSQL 18 CI verifies a failed publish followed by successful redelivery of the same event ID. The PostgreSQL adapter now includes migration v2 and a durable `PgInboxRepository` keyed by consumer name and stable event ID. It atomically claims new/pending/expired events, reports busy or already-processed deliveries, supports release and reclaim, and fences acknowledgements with monotonically increasing claim tokens. PostgreSQL 18 CI verifies migration checksums, inbox constraints, duplicate suppression, lease recovery, and stale-worker rejection. The isolated `aetherflow-transport-nats` crate now contains a real JetStream outbox publisher and inbox-backed pull consumer. The publisher awaits broker storage acknowledgement and uses the stable event ID as `Nats-Msg-Id`; the consumer commits inbox completion before broker acknowledgement, suppresses logical redelivery, renews PostgreSQL leases and JetStream progress acknowledgements for long handlers, and releases failed claims for retry. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry window, durable inbox deduplication, long-handler heartbeat, and handler failure/retry. OS-level process-kill tests now pass for both critical broker windows: the publisher exits after JetStream storage but before outbox acknowledgement, and the consumer exits after inbox commit but before broker acknowledgement. Fresh processes recover both cases without duplicate logical effects (PostgreSQL 18 + NATS run `41380067612`; Rust CI `41380067843`). M04 is complete. M05 adds additive PostgreSQL v3 node/worker membership storage and a `PgMembershipRepository` with incarnation epochs, expired takeover, heartbeats, draining, worker capacity, and stale-epoch fencing. All durable task claim and settlement transactions validate the live worker registration and node epoch in the same PostgreSQL transaction. The two-round combined packet-partition, active-worker process-kill, and NATS member-loss campaign passed PostgreSQL 18 + NATS run `41402000557`; Rust CI `41402000570` passed the full workspace checks. M05 is complete; see [ROADMAP.md](ROADMAP.md) for the authoritative checkpoint.

M06 has a versioned plugin contract: [ADR-0009](docs/decisions/0009-versioned-wasm-plugin-abi.md), the [WIT v1 interface](crates/aetherflow-wasm/wit/plugin.wit), and the `aetherflow-wasm` ABI/runtime crate. ABI v1 is default-deny, exposes only bounded `log.write`, and imports no WASI. The Wasmtime 48.0.8 runtime applies per-invocation fuel, active epoch deadlines, Store memory/table/core-instance/component-handle ceilings, output limits, and capability-gated/rate-limited host logging. Hosted tests exercise the real WIT-derived Component, typed guest errors, output caps, traps, fuel/deadline interruption, memory/table growth, shared component-handle saturation/reclamation across two resource tables, and rejection of undeclared WASI imports. Rust CI [41465170507](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41465170507) passed all quality, test, Release build, and docs stages on the component-handle implementation; Pages [41465229920](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41465229920) passed on its formatting-only main tip.

The reproducible Rust example in [examples/minimal-plugin](examples/minimal-plugin/) builds a `wasm32-unknown-unknown` guest against the checked-in WIT ABI, componentizes it with the locked WIT tooling, and runs it through `WasmPluginRuntime`. It first verifies default-deny behavior, then registers `WasmTaskHandler` in `TaskRegistry` and executes a workflow through `TaskScheduler`. The integration asserts that the workflow succeeds, the unchanged guest output is retained on the task run, and the explicitly granted `log.write` message reaches the host log sink. Wasmtime executes on Tokio's blocking pool. No WASI adapter is introduced. The example uses the in-memory repository; a dedicated WASM + PostgreSQL end-to-end test remains future coverage. The workspace MSRV is 1.95.0; the repository pins Rust 1.99.1.


M05 adds PostgreSQL v3 node/worker membership and membership-gated durable execution. `PostgresTaskWorker` requires a live `WorkerRegistration`, renews membership during active work and graceful drain, and only claims task IDs supported by its `TaskRegistry`. Task claim, heartbeat, completion, failure, registered cancellation, timeout, recovery, promotion, and workflow aggregation validate the current node incarnation/epoch and worker lease inside the same PostgreSQL transaction as the write. Administrative cancellation remains a separate control-plane API. Component-level multi-process partition/rejoin, abrupt worker loss, NATS quorum loss, and the combined two-round chaos campaign are hosted-verified. M05 is complete. M06 adds the sandboxed WASM task-handler adapter and scheduler-backed example; M07 control-plane API work is next.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. The `aetherflow-transport-nats` crate now integrates a real JetStream publisher and inbox-backed pull consumer. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry boundary, stable message-ID deduplication, logical consumer deduplication, lease and AckWait heartbeat for long handlers, and failed-handler redelivery. The durable worker/coordinator remains an opt-in PostgreSQL execution path. OS-level process termination after broker storage and after inbox commit is now integration-tested on PostgreSQL 18 + NATS (run `41380067612`). Delivery remains at-least-once; external side effects require application-level idempotency or a transaction shared with inbox completion. M05 membership-gated task execution is verified on PostgreSQL 18 + NATS (41402000557) with Rust CI (41402000570). Tests cover stale-epoch write rejection, replacement-worker recovery after task-lease expiry, registry capability filtering, lease renewal while gracefully draining a long-running handler, UID-scoped packet-level PostgreSQL partition/rejoin, abrupt worker-process loss, three-member NATS quorum recovery, and the combined two-round chaos campaign. This is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
