# ADR-0014: Release candidate contract compatibility, CLI security boundary, migration policy, and at-least-once delivery guarantees

- **Status:** Accepted (maintainer review and decision recorded)
- **Date:** 2026-11-15
- **Scope:** Public HTTP API (`/api/v1`), typed Rust SDK (`aetherflow-sdk`), CLI (`aetherflow-cli`), PostgreSQL schema migrations (v1–v10), and distributed delivery semantics for candidate v1.0.0 release readiness

## Context

Prior to considering any production release candidate for AetherFlow, the maintainers must review the full set of public interfaces, client boundaries, migration behaviors, and distributed delivery contracts to verify cohesion, prevent breaking semantic drift, and ensure users and operators do not harbor incorrect assumptions about system capabilities.

Specifically, Milestone M08 requires a formal review of:
1. Public HTTP API and typed Rust SDK compatibility contracts.
2. PostgreSQL schema migration upgrade, integrity, and downgrade/recovery policies.
3. CLI client security boundaries, credential handling, and HTTP transport constraints.
4. Completeness and alignment of repository documentation and runnable examples.
5. Absolute clarity on the platform's distributed delivery semantics and at-least-once guarantees.

## Review and Decisions

### 1. Public API and Typed Rust SDK Compatibility Boundary

- **Path prefix and major versioning:** All durable control-plane routes live under `/api/v1` for operator access and `/api/v1/tenant/*` for tenant access. `GET /health` remains a non-versioned process-liveness endpoint.
- **Backward compatibility guarantee:**
  - Additive, optional response fields are non-breaking and permissible within v1.
  - Modifying or removing existing JSON fields, changing error code constants, renaming status enum strings, or altering required parameters is strictly breaking and forbidden within v1. Any breaking change requires a new `/api/v2` namespace.
- **Strict wire and memory bounds:**
  - Request bodies are capped at 2 MiB across the HTTP router and CLI (HTTP 413 `payload_too_large`), and at 16 KiB on the Cloudflare Free serverless profile (`request_too_large`).
  - Task inputs are base64-encoded on the wire and bounded to 1 MiB decoded bytes.
  - Resource identifiers (`workflow_id`, `workflow_run_id`, `task_id`, `tenant_id`, `principal_id`) are validated against a 1–64 ASCII alphanumeric, hyphen, or underscore regex (`^[A-Za-z0-9_-]{1,64}$`).
- **Inspection confidentiality:** Inspection endpoints (`GET /api/v1/workflow-runs/{id}`, `GET /api/v1/task-runs/{id}`, and their tenant counterparts) intentionally omit raw task input and output payload bytes from responses. They return payload size in bytes and boolean output presence only, preventing incidental leakage of sensitive payloads into monitoring or log systems.
- **Typed SDK alignment:** The `aetherflow-sdk` crate mirrors all OpenAPI v1 data models (`WorkflowRun`, `TaskRun`, `SubmitWorkflowRequest`, `TaskFailure`, `ActiveWorker`, `MetricsResponse`, `ApiErrorEnvelope`), enforces the 1 MiB input and 2 MiB response ceilings during streaming, and prevents credential leakage by stripping Bearer tokens before error formatting.

### 2. PostgreSQL Schema Upgrade, Integrity, and Downgrade Policy

- **Monotonic additive migrations:** Migrations in `crates/aetherflow-runtime/migrations/postgres/` are numbered sequentially from `0001` through `0010`. Applied migration files are immutable; once merged and released, they must never be edited.
- **Ledger integrity:** The migration runner (`PgStore::migrate()`) operates within a single transaction using a transaction-scoped advisory lock. It validates contiguous version order, asserts that the database schema is not newer than the application binary, and verifies SHA-256 checksums against `aetherflow_schema_migrations`. Any discrepancy causes startup to fail closed.
- **Privilege separation:** Schema migrations require the privileged `AETHERFLOW_MIGRATION_DATABASE_URL` login. The normal runtime role (`DATABASE_URL`) possesses least privilege, cannot perform DDL or alter schema, has no membership in privileged roles, and is subject to `FORCE ROW LEVEL SECURITY` across all ten tenant-owned tables.
- **Downgrade and rollback policy:**
  - Automatic or in-place schema downgrades are unsupported. A previous binary will detect the newer migration version in the database ledger and refuse to start.
  - Backward compatibility is maintained via forward fixes ("fix-forward").
  - In disaster recovery scenarios where rollback to an earlier release is unavoidable, operators must restore a synchronized backup set of both PostgreSQL and NATS JetStream taken at the same coordinate point in time, as documented in `docs/operations/backup-restore.md`.

### 3. CLI Client Security Boundary

- **HTTP-only client:** The `aetherflow` CLI binary (`apps/aetherflow-cli`) is an HTTP/HTTPS client only. It never connects directly to PostgreSQL, never interacts with NATS, and never executes task workers.
- **Credential hygiene:** Operator and tenant Bearer tokens are sourced exclusively from environment variables (`AETHERFLOW_API_TOKEN`, `AETHERFLOW_TENANT_TOKEN`). The CLI explicitly forbids passing credentials as command-line flags or arguments, preventing exposure in `ps` output, shell history, and process table monitoring.
- **Transport security:**
  - Plain HTTP is strictly restricted to numeric loopback addresses (`127.0.0.1`, `::1`).
  - Remote server URLs must use HTTPS with operating system certificate validation; cleartext Bearer token transmission across untrusted networks is rejected at the client boundary.
  - HTTP redirects are suppressed (`redirect::Policy::none()`) to prevent credential leakage to unauthorized origins.
- **Bounded ingestion:** Standard input and file inputs (`--file`) are read through a fixed 2 MiB bounded reader before serialization.

### 4. Documentation and Examples Alignment

- **Operational runbooks:** The documentation suite (`docs/operations/`) provides detailed, verifiable procedures for schema upgrades, backup and recovery, production deployment acceptance, and SLO definitions.
- **Local showcases:** `docs/guides/full-rust-local-showcase.md` and `docs/guides/rust-worker-process.md` document reproducible, zero-cost developer environments using Docker Compose, standalone workers, and multi-tenant task execution.
- **WASM plugin ABI v1:** `examples/minimal-plugin/` provides a working Component Model WebAssembly guest and host example implementing `aetherflow:plugin@1.0.0` with fuel and epoch interruption, default-deny capabilities, and bounded logging.

### 5. Distributed Delivery Guarantees and At-Least-Once Contract

- **Explicit at-least-once delivery:** AetherFlow provides durable **at-least-once delivery**, not magic exactly-once delivery.
  - State mutations and event emissions are coupled atomically in PostgreSQL via the transactional outbox (`outbox_events`).
  - Event publishing to NATS JetStream uses deterministic message IDs to leverage broker-level deduplication within the configured stream deduplication window.
  - Pull consumers utilize a durable database inbox (`consumer_inbox` keyed on `(tenant_id, consumer_name, event_id)`) to record and commit event receipt in a repeatable-read transaction.
- **External side-effect idempotency:**
  - If a task handler or consumer produces external side effects (e.g. third-party HTTP requests, emails, file writes, payment mutations), those external systems must provide idempotency keys or transactional integration.
  - A crash or network timeout during task execution or acknowledgment can result in redelivery or lease recovery. The application must design external handlers to be idempotent across retries.
- **Split-brain mitigation:** Task attempt claims and completions use monotonic fencing tokens (`fencing_token`). Stale workers whose leases have expired or whose membership epoch has been superseded are fenced out and rejected by the database.

## Consequences

- The release candidate contracts for API v1, SDK v1, CLI v1, and PostgreSQL schema v10 are declared cohesive, stable, and locked.
- The v1.0.0 release readiness checklist item for "public API and SDK compatibility, migration upgrade/downgrade policy, CLI behavior, documentation, and examples review" is formally satisfied and closed.
- Production readiness remains gated by the outstanding external infrastructure and deployment requirements: production Cloudflare edge routing and bypass tests, production PostgreSQL PITR and approved RPO/RTO, production NATS cluster security and ACLs, centralized Prometheus metrics collection, and independent human security review.
