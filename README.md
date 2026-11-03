# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M00–M06 are complete. M07 now includes an Axum v1 control plane backed by PostgreSQL: workflow submission and inspection, task inspection and version-guarded cancellation, and live worker discovery. The HTTP routes and API integration suite have passed hosted verification; follow-up tests extend coverage for missing records, live worker results, and safe storage-error mapping. The listener rejects non-loopback binds until authentication and tenant isolation exist. AetherFlow remains experimental and is not production-ready.
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
| HTTP control API | Axum (v1 control-plane routes implemented in M07) |
| RPC | Tonic / gRPC (when justified) |
| Durable storage | PostgreSQL |
| Event transport | NATS JetStream (after core semantics are proven) |
| Extensions | Wasmtime |
| Observability | OpenTelemetry |
| Dashboard | TypeScript UI (later phase) |
| Deployment | Docker Compose; Kubernetes only after operational needs are demonstrated |

Technology choices are provisional until validated by implementation and recorded in architecture decision records.

## API contract

The versioned v1 control-plane API is implemented in apps/aetherflow-server and described by the [OpenAPI 3.1 specification](docs/api/openapi.yaml). Its routes are:

| Method | Route | Purpose |
|---|---|---|
| GET | /health | Process liveness; does not check PostgreSQL |
| POST | /api/v1/workflow-runs | Validate and durably submit a workflow-run snapshot |
| GET | /api/v1/workflow-runs/{workflow_run_id} | Read a workflow run and its task summaries |
| GET | /api/v1/task-runs/{task_run_id} | Read task state and payload-size metadata |
| POST | /api/v1/task-runs/{task_run_id}/cancel | Cancel with an expected_version guard |
| GET | /api/v1/workers | List live active worker registrations |

The contract and its compatibility/security decisions are recorded in [ADR-0010](docs/decisions/0010-versioned-control-plane-api.md). Task input is base64 encoded and limited to 1 MiB after decoding; the HTTP request body is capped at 2 MiB. Inspection responses never return task input or output bytes. Reusing a workflow-run ID returns a conflict; the v1 API does not promise idempotent replay.

**Deployment boundary:** the listener defaults to 127.0.0.1:8080 and rejects non-loopback binds. PostgreSQL connections currently use NoTls; there is no HTTP authentication, authorization, or tenant isolation. Use only for local development or an isolated trusted environment, and do not expose it to an untrusted network.

## Repository map

The repository is being built incrementally. See [ROADMAP.md](ROADMAP.md) for the authoritative checklist and current checkpoint, [docs/architecture/overview.md](docs/architecture/overview.md) for system boundaries, [docs/decisions/](docs/decisions/) for architecture decisions, and [docs/development.md](docs/development.md) for local commands.

## Development status

M00–M06 are complete: the Rust execution domain/runtime, durable PostgreSQL repository, worker membership/fencing, NATS JetStream outbox/inbox, combined failure-recovery coverage, and bounded Wasmtime/WIT v1 extension runtime are hosted-verified. See [ROADMAP.md](ROADMAP.md) for the detailed history and exact verification checkpoints.

M07 has an implemented HTTP control-plane slice:
- Axum preserves GET /health and exposes versioned workflow-run, task-run, cancellation, and active-worker routes.
- Workflow submission is validated with the existing DAG/retry-policy model and persisted through PgWorkflowCoordinator.
- Workflow snapshots use a repeatable-read PostgreSQL view; task snapshots omit payload bytes and expose only sizes/presence metadata.
- Cancellation uses the caller's expected task version and aggregates the workflow state after the transition.
- Worker discovery reads live registrations from PgMembershipRepository.
- The listener is loopback-only. Without DATABASE_URL, the process runs in health-only mode and durable routes fail closed with 503 database_unavailable.

The server does not start workers; durable workers run as separate processes using the PostgreSQL worker runtime. The HTTP server currently configures PostgreSQL with NoTls, has no authentication/authorization or tenant isolation, and is not safe to expose to untrusted networks. Workflow definitions are submitted as snapshots rather than stored as reusable versioned definitions. Duplicate workflow-run IDs return 409; v1 does not claim idempotent replay.

The latest verified implementation and remaining M07 work are tracked in [ROADMAP.md](ROADMAP.md). License selection remains an owner-only release decision, and property-based lifecycle tests remain deferred engineering debt.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. The `aetherflow-transport-nats` crate now integrates a real JetStream publisher and inbox-backed pull consumer. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry boundary, stable message-ID deduplication, logical consumer deduplication, lease and AckWait heartbeat for long handlers, and failed-handler redelivery. The durable worker/coordinator remains an opt-in PostgreSQL execution path. OS-level process termination after broker storage and after inbox commit is now integration-tested on PostgreSQL 18 + NATS (run `41380067612`). Delivery remains at-least-once; external side effects require application-level idempotency or a transaction shared with inbox completion. M05 membership-gated task execution is verified on PostgreSQL 18 + NATS (41402000557) with Rust CI (41402000570). Tests cover stale-epoch write rejection, replacement-worker recovery after task-lease expiry, registry capability filtering, lease renewal while gracefully draining a long-running handler, UID-scoped packet-level PostgreSQL partition/rejoin, abrupt worker-process loss, three-member NATS quorum recovery, and the combined two-round chaos campaign. This is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
