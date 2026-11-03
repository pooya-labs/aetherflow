# ADR-0010: Versioned HTTP control-plane API

- **Status:** Accepted
- **Date:** 2026-11-03

## Context

M07 introduces an HTTP control plane for submitting durable workflow runs and inspecting their execution. The existing repository already provides durable workflow materialization, task-run snapshots, version-guarded task cancellation, workflow aggregation, and active worker registrations. It does not persist reusable workflow-definition versions, expose a workflow-run summary query, or implement HTTP authentication and tenant isolation.

The API must describe only capabilities backed by existing domain and storage semantics. It must not expose process-local scheduler snapshots as durable state, return unbounded task payloads by default, or imply exactly-once submission/replay behavior that the database adapter does not implement.

## Decision

1. All control-plane endpoints live below /api/v1. Health remains an independent process-health endpoint at GET /health.
2. The primary resource is a workflow run, not a reusable workflow definition. POST /api/v1/workflow-runs accepts a validated definition snapshot (workflow ID, caller-supplied unique workflow-run ID, tasks, dependencies, and retry policy) and materializes it durably through PgWorkflowCoordinator.
3. Reusing a workflow-run ID is a conflict. The v1 contract does not advertise idempotent replay through an Idempotency-Key header until the repository can return the original result atomically for duplicate submissions.
4. Workflow and task IDs use the existing bounded ASCII identifier contract. Task input is base64 encoded at the wire boundary and decoded before the existing 1 MiB TaskInput validation.
5. Workflow/task inspection responses omit payload bytes by default. They expose payload size and output-presence metadata only. A future explicitly authorized payload retrieval surface may be added separately.
6. Task cancellation requires expected_version and delegates to the durable version-guarded cancellation transition. The handler then invokes workflow aggregation; a stale version or terminal-state race is reported as a conflict, never silently overwritten.
7. Active worker discovery returns only live registrations and exposes operational identity/capacity/lease metadata, never credentials or process environment.
8. All errors use a stable envelope with a machine-readable code and safe message. Internal database diagnostics are logged server-side and are not returned to clients.
9. The service has no authentication, authorization, or tenant isolation in this milestone slice. Until those controls are implemented, the HTTP listener must bind to loopback by default and deployments must not expose it to untrusted networks.

## Compatibility policy

- /api/v1 is a major compatibility boundary. Additive optional response fields are allowed; removing or changing the meaning/type of an existing field requires a new major path.
- Unknown request fields may be ignored by the server, but required fields and enum values are validated strictly.
- Status strings are the stable lowercase names already used by the domain and PostgreSQL model.
- Timestamps are RFC 3339 UTC/offset timestamps.
- Payload sizes are measured in decoded bytes. Base64 input over 1 MiB is rejected even if the encoded request is otherwise valid.

## Consequences

**Benefits:** the contract mirrors existing persistence semantics, protects payload confidentiality by default, makes stale writes explicit, and gives clients a stable version boundary before route implementation.

**Trade-offs:** callers supply workflow-run IDs; duplicate submissions are not transparently replayed; task payload retrieval and reusable workflow definitions are out of scope; API access remains local-only until security is implemented.

## Revisit when

Revisit submission idempotency, reusable definition storage, payload retrieval scopes, and authentication/tenant isolation before the API is exposed beyond a trusted local development process.
