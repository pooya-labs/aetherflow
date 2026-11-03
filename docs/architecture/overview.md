# Architecture Overview

**Status:** M00–M06 are implemented and hosted-verified. M07's first Axum/PostgreSQL control-plane slice is implemented; production security and developer tooling remain in progress.

## Problem model
AetherFlow coordinates user-defined work across unreliable processes and machines. Durable intent must be separated from transient execution. A process-local future is not a durable workflow, and a message acknowledgement does not prove an external side effect happened exactly once.

## Core principles
1. Durable state is authoritative.
2. Workers may disappear; ownership must expire and be safely reassigned.
3. Messages may be duplicated or delayed; handlers must be idempotent or guarded.
4. Arbitrary external side effects cannot be made exactly-once by the orchestrator alone.
5. Bound concurrency, queue depth, retries, execution time, and plugin resources.
6. Make attempts, transitions, errors, and recovery decisions inspectable.
7. Keep domain semantics independent from databases, brokers, and UI.

## Logical layers
- **Domain:** identities, state machines, policies, transition validation, execution contracts.
- **Application:** commands and queries coordinating domain behavior and ports.
- **Runtime:** scheduling, worker lifecycle, cancellation, deadlines, task execution.
- **Adapters:** persistence, transport, HTTP/RPC, telemetry, WASM host integration.
- **Developer surfaces:** CLI, SDKs, examples, dashboard.

The repository now includes the Rust core/runtime, durable PostgreSQL and NATS adapters, the WASM host boundary, and an Axum control plane. Keep application semantics explicit and do not mistake this implemented vertical slice for a production-ready service.

## Intended execution flow
1. A client submits a workflow definition snapshot and unique workflow-run ID to the v1 API.
2. The server validates IDs, retry policy, payload bounds, dependencies, and DAG cycles, then materializes the run through PgWorkflowCoordinator.
3. Separate PostgreSQL-backed workers promote eligible tasks, claim them with expiring fenced leases, and record attempts.
4. Workers execute registered handlers and persist success/failure through versioned transactions.
5. PostgreSQL owns retry timing, workflow aggregation, lease recovery, and the transactional outbox.
6. The outbox dispatcher publishes through JetStream; inbox-backed consumers suppress duplicate logical deliveries.
7. The API exposes workflow/task summaries and active worker metadata without exposing payload bytes.

The workflow persistence/worker/event path is implemented and covered by PostgreSQL/NATS integration tests. The HTTP control plane is implemented as a local-only slice; authentication, tenant isolation, and production deployment hardening are not implemented.

## Initial crate boundaries
Candidate crates: `aetherflow-domain`, `aetherflow-application`, `aetherflow-runtime`, `aetherflow-storage-postgres`, `aetherflow-api`, `aetherflow-wasm`, and `aetherflow-cli`. Create crates only when boundaries are useful and testable; avoid empty scaffolding.

## Consistency and delivery
PostgreSQL is the proposed source of truth for durable workflow state. Transitions should be transactional and guarded by expected state/version or equivalent concurrency control. Leases require explicit ownership and expiry. Outbox/inbox patterns may be needed to avoid lost publication windows. NATS JetStream is a transport candidate, not the source of truth.

## Reliability vocabulary
- **At-most-once:** not repeated, but may be lost.
- **At-least-once:** may be repeated until acknowledged or expired.
- **Effectively-once:** duplicates are harmless within a defined idempotency/transaction boundary.
- **Exactly-once:** never claim without naming the precise boundary and assumptions.

## Security boundaries
Workflow definitions and plugin modules are untrusted. Validate payload sizes, constrain resource use, authorize host capabilities, isolate tenants, protect secrets, and avoid logging credentials or sensitive payloads. Design authentication and tenancy before exposing a production control plane.

## WASM plugin boundary (M06 ABI v1)

ADR-0009 accepts the versioned Component Model contract in `crates/aetherflow-wasm/wit/plugin.wit` (`aetherflow:plugin@1.0.0`). The guest exports one task execution function and receives only an opaque payload plus bounded task metadata. The only v1 host import is bounded logging under the explicit `log.write` capability; capability sets default to empty. ABI v1 does not import WASI and has no filesystem, network, environment, secret, process, clock, or database access.

The host owns input/output limits, instruction fuel, host-call transfer fuel, component handle limits, linear memory/table limits, bounded/rate-limited logging, and an actively interruptible wall-clock deadline. Each invocation receives a fresh Store and component instance. Wasmtime 48.0.8 LTS is integrated with default features disabled; the workspace MSRV is 1.95.0. Hosted tests execute a WIT-derived Component, exercise capability-gated logs, typed guest errors, output caps, traps, fuel/deadline interruption, memory/table/handle ceilings, and rejection of undeclared WASI imports. The reproducible guest/host example additionally registers WasmTaskHandler in TaskRegistry and runs it through TaskScheduler, verifying workflow success and persisted task output; Rust CI 41563269188 passed the full example and workspace matrix. Synchronous guest calls run on Tokio's blocking pool. Cancellation/version fencing prevents a late result from settling a superseded task, but dropping the async handler cannot forcibly abort an already-running spawn_blocking call; Wasmtime resource and wall-clock limits remain the bound.

## Control-plane API (M07 v1)

ADR-0010 defines the versioned HTTP contract in docs/api/openapi.yaml. Axum implements GET /health, platform/operator workflow-run submit/read, task-run read/cancel, and active-worker discovery. A separate tenant-authenticated route group exposes workflow submit/read and task read/cancel under `/api/v1/tenant/*`. Tenant middleware resolves an opaque `TenantContext` from a stored credential and enforces explicit Read/Submit/Cancel permissions; client-supplied tenant IDs are never authoritative.

Workflow submission materializes each validated definition snapshot through PgWorkflowCoordinator; reusable workflow-definition versions are not persisted. Workflow snapshots are read with a repeatable-read PostgreSQL transaction. Cancellation requires the caller's expected task version, and the parent workflow is aggregated after the task transition. Worker discovery uses live PgMembershipRepository registrations and remains operator-only.

Task input is base64 encoded at the HTTP boundary and capped at 1 MiB after decoding; the whole HTTP request body is capped at 2 MiB. Workflow/task inspection responses expose payload sizes and output presence but never payload bytes. Reusing a workflow-run ID returns a conflict; no idempotent replay guarantee is advertised. The process does not start workers; durable workers run separately.

PostgreSQL schema v6 enables and forces RLS on all ten tenant-owned tables; v7 adds the worker grant `UPDATE USING` policy required for `SELECT ... FOR SHARE`, with a false write check that denies worker-side grant mutation. Tenant-facing operations use transaction-local tenant context; registered workers use transaction-local worker identity and are limited by active, unexpired tenant grants. Credential authentication uses a transaction-local digest GUC. Migration/DDL and application access use separate pools: `AETHERFLOW_MIGRATION_DATABASE_URL` is the migration owner, while `DATABASE_URL` must be a restricted non-owner runtime role. Server startup rejects superuser/BYPASSRLS/REPLICATION/CREATEROLE/CREATEDB roles, public-schema CREATE, protected-table ownership, any role membership, or missing FORCE RLS.

These M07 tenant/RLS changes are implemented but await hosted verification on the exact current source commit. The listener defaults to loopback and rejects non-loopback addresses. PostgreSQL Rustls TLS protects only the database connection; it does not provide HTTPS for Axum. Do not expose the service to untrusted networks or claim production readiness until HTTPS, negative TLS tests, hosted verification, and threat-model review are complete. RLS GUCs are application-controlled context and do not protect against arbitrary SQL execution or injection; parameterized SQL remains mandatory.
## Initial non-goals
- Universal exactly-once execution across external systems.
- Unrestricted remote code execution.
- Premature Kubernetes/operator complexity.
- A dashboard before execution semantics and APIs stabilize.
- Performance claims without reproducible benchmarks.

## Open questions
Workflow definition/versioning; lease renewal and fencing tokens; event retention; plugin ABI compatibility; tenant isolation and authorization. Resolve these in ADRs before they become hard-to-change dependencies.
