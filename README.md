# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M04 durable persistence is in progress. APIs and implementation details remain experimental; the complete distributed workflow engine is not production-ready.

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

The Rust workspace contains an infrastructure-independent core, a single-node runtime with a process-local in-memory repository, a bounded Tokio task scheduler and workflow DAG coordinator, plus a minimal bootstrap health server. The runtime supports typed handlers, bounded concurrency, retry and per-attempt timeout policies, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and successful direct-dependency output access. M03 is hosted-verified. The M04 PostgreSQL adapter includes a checksum/advisory-lock migration runner, durable workflow materializer, and async durable worker over the shared runtime handler contract. `PgWorkflowCoordinator` persists validated DAGs with deterministic task-run IDs and dependency edges. `PostgresTaskWorker` uses the runtime `TaskRegistry` and `TaskContext`, bounded concurrency, PostgreSQL claims/heartbeats/fencing, dependency output loading, durable retry/failure/timeout transitions, lease recovery, and automatic workflow aggregation. PostgreSQL 18 integration CI has executed a two-task DAG through this worker and verified that the child handler receives the root output and the workflow reaches `succeeded`; existing coverage also exercises queued dependency and running sibling cancellation, stale worker rejection, subprocess lease recovery, and transaction rollback when outbox insertion fails. The existing `TaskScheduler` and in-memory repository remain process-local. A transport-neutral PostgreSQL outbox dispatcher now publishes outside transactions, acknowledges only after publisher success, and reschedules failures with capped exponential backoff; PostgreSQL 18 CI verifies a failed publish followed by successful redelivery of the same event ID. The PostgreSQL adapter now includes migration v2 and a durable `PgInboxRepository` keyed by consumer name and stable event ID. It atomically claims new/pending/expired events, reports busy or already-processed deliveries, supports release and reclaim, and fences acknowledgements with monotonically increasing claim tokens. PostgreSQL 18 CI verifies migration checksums, inbox constraints, duplicate suppression, lease recovery, and stale-worker rejection. A concrete NATS JetStream publisher/consumer and process-termination tests around commit and publish/ack remain outstanding. See [ROADMAP.md](ROADMAP.md) for the authoritative checkpoint and verification runs.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. Remaining M04 work is to add a real broker publisher and idempotent consumer/inbox, and verify process termination around database commit and broker publish/ack boundaries. The durable worker/coordinator is an opt-in PostgreSQL execution path; the outbox dispatcher is transport-neutral and has only been exercised with a test publisher, not an external broker. Until those pieces are complete, this is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
