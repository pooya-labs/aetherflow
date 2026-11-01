# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** Architecture and foundation phase. APIs and implementation details are not yet stable.

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

The Rust workspace contains an infrastructure-independent core, a single-node runtime with a process-local in-memory repository, bounded Tokio task scheduler, and workflow DAG coordinator, plus a minimal bootstrap health server. The runtime supports typed handlers, bounded concurrency, retry and per-attempt timeout policies, cooperative cancellation, bounded two-stage shutdown, 1 MiB input/output payloads, and access to successful direct-dependency outputs. M03 is hosted-verified. The M04 PostgreSQL storage crate now includes a pool boundary and checksum/advisory-lock migration runner; initial schema application and idempotent re-run passed PostgreSQL 18 CI. Durable task repository operations, leases, and crash recovery remain under development; the in-memory repository remains non-durable. See the roadmap for exact verification status.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema are under active M04 verification. The schema has passed PostgreSQL 18 DDL and constraint checks; the async migration runner and durable repository operations are not yet declared production-ready. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
