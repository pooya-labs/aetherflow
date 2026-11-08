# Architecture Overview

**Status:** M00–M07 are implemented and hosted-verified. M08 is in progress; observability, recovery drills, local Compose development infrastructure, operations runbooks, benchmark suites, and authenticated-principal rate limiting are implemented and hosted-verified. Production deployment hardening remains incomplete.

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

The workflow persistence/worker/event path is implemented and covered by PostgreSQL/NATS integration tests. The HTTP control plane exposes platform/operator and tenant-authenticated routes with tenant-scoped persistence. Plain HTTP is loopback-only; non-loopback binds require a valid Rustls certificate/key pair. Live PostgreSQL TLS trust/hostname integration passed the PostgreSQL/NATS hosted CI run [41609705469](https://github.com/pooya-labs/aetherflow/actions/runs/41609705469); production deployment hardening remains incomplete.

## Free serverless deployment profiles (separate from the Rust engine)

The project now has a second, intentionally smaller deployment track for environments that cannot reach or operate a separately hosted API Origin. Do not route either free profile through the Axum API, a VPS, a tunnel, PostgreSQL, or NATS.

### Cloudflare Free execution runtime

`cloudflare-free/` is the direct serverless runtime:

- Cloudflare Worker: `aetherflow-free-runtime`, served on the account-owned `workers.dev` hostname.
- D1: `aetherflow-free`, with workspace-scoped workflow, task, session, and event records.
- Cloudflare Workflows: `aetherflow-execution`, which runs accepted workflow tasks as durable steps with platform-managed retries.
- Dashboard: `dashboard/runtime-config.js` selects the Worker endpoint; a public Pages visit obtains a short-lived capability through `POST /api/v1/demo/bootstrap`, then uses the same versioned read/submit routes from the dashboard.

This runtime supports a constrained set of built-in task operations only: `echo`, `collect`, `uppercase`, `sha256`, `add`, and `fail_once`. It validates a bounded DAG before admission, records versioned task/workflow transitions, stores redacted event metadata, and keeps task payload/output values out of dashboard inspection responses. It does not load arbitrary user code, WASM modules, or the Rust plugin ABI. The built-in handler is a deliberate free-tier execution adapter, not feature parity with the Rust runtime.

Current project ceilings are 100 workflow launches/day, 20 tasks/workflow, 12,000 D1 reads/day, 2,500 D1 mutations/day, 60 new sessions/day, 250 reads/session, 60 mutations/session, 16 KiB request bodies, and 8 KiB task inputs. These are project-level safeguards below Cloudflare's published free allowances; they are not guarantees against account-wide usage from other Workers or projects. Check the current provider limits before raising them.

### Google Apps Script profile

`apps-script/` follows the ORBIT portfolio-demo pattern: an owner-deployed V8 Web App wrapper, a `google.script.run` bridge to the Pages dashboard, a private owner-owned Google Spreadsheet, pseudonymous browser workspaces, six-hour session capabilities, and per-session/per-day limits. The GAS adapter exposes bounded workflow submission and inspection, task cancellation and redacted history, but it does not execute the Cloudflare built-in tasks or arbitrary code. It is intended as a low-volume demo/control-plane profile.

The Apps Script source and deployment manifest are ready in the repository; the owner must still create/authorize/deploy the Google Web App from their own Google account. Anonymous Web App availability depends on Google's account/region policies. Do not enable billing or add a server Origin to bypass a Google restriction.

### Architecture boundary

The Rust/PostgreSQL/NATS/WASM engine remains the full distributed runtime and is still experimental pending M08 production acceptance. The Cloudflare profile is the deployed serverless execution target; the Apps Script profile is a small demo/control-plane alternative. Neither changes the Rust API's contracts or claims to provide the same guarantees, queue semantics, plugin isolation, tenant identity, RPO/RTO, or production SLOs.

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

These M07 tenant/RLS changes passed the exact-source Rust CI, PostgreSQL 18 + three-node NATS, and Pages matrix on `74c4e368d6ce43c6b9abd98cf1132120b1bbd199` (runs 41602415273, 41602415207, and 41602415169). The listener defaults to loopback; plaintext is loopback-only, and non-loopback binds require a valid Rustls certificate/key pair. HTTPS listener negotiation and negative-certificate tests passed Rust CI on `54b17742aee822f37a4a3b034968bbcd3e851c13`. Live PostgreSQL TLS trusted/untrusted/hostname-mismatch integration passed run [41609705469](https://github.com/pooya-labs/aetherflow/actions/runs/41609705469). The M08 threat-model review and operational controls remain open. RLS GUCs are application-controlled context and do not protect against arbitrary SQL execution or injection; parameterized SQL remains mandatory.
## Initial non-goals
- Universal exactly-once execution across external systems.
- Unrestricted remote code execution.
- Premature Kubernetes/operator complexity.
- A dashboard before execution semantics and APIs stabilize.
- Performance claims without reproducible benchmarks.

## Open questions
Workflow definition/versioning; lease renewal and fencing tokens; event retention; plugin ABI compatibility; tenant isolation and authorization. Resolve these in ADRs before they become hard-to-change dependencies.
