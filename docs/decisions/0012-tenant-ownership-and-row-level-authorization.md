# ADR-0012: Tenant ownership and row-level authorization

- **Status:** Accepted; tenant ownership migration v4 is hosted-verified; tenant inbox migration v5, grant-gated execution/event plane, transaction-local tenant/worker/legacy context, and tenant-authenticated HTTP routes are implemented but await hosted verification; forced RLS and separate runtime DB role remain pending
- **Date:** 2026-11-03
- **Scope:** Tenant identity, API principals and credentials, durable workflow ownership, worker grants, and PostgreSQL row-level isolation

## Implementation status (2026-11-03)

The additive PostgreSQL migration v4 has been implemented and hosted-verified on PostgreSQL 18. It creates the tenant/principal/credential registry and worker-to-tenant grant table, backfills existing workflow, task, dependency, attempt, outbox, and inbox records into `legacy-default`, and adds tenant-consistent composite foreign keys and tenant-leading indexes. True v3-to-v4 backfill, checksum drift, cross-tenant FK rejection, and schema ledger/table/index invariants passed in [PostgreSQL/NATS run 41575288110](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41575288110) on source 3a7d704.

Migration v5 is now committed as `0005_tenant_scoped_inbox_identity.sql` (commit `a975c35`). It replaces the old global inbox primary key with `(tenant_id, consumer_name, event_id)`, removing the v4 transitional unique constraint only after the tenant-leading primary key is ready. Existing inbox data remains under `legacy-default`. This is a schema compatibility boundary: older binaries whose inbox upsert targets `(consumer_name, event_id)` must not run against a v5 database. The schema workflow now verifies the five-entry migration ledger and exact primary-key column order; hosted verification of v5 is pending.

The typed authentication foundation now exists: `PrincipalId`, `TenantRole`, `TenantPermission`, opaque `TenantContext`, and `PgTenantCredentialRepository` were added in commit `2cf8dc8`. Credential lookup validates active credential/principal/tenant state and expiry, stores only a SHA-256 digest, performs a constant-time digest comparison, and updates last-used metadata. Commit `b93abae` adds a transaction-local PostgreSQL context helper using `set_config(..., true)` for tenant, principal, and role, plus a single-connection pool-reuse regression test; hosted verification of that test is pending.

The tenant-aware data-plane implementation now covers the main storage and event paths:

- Tenant workflow/task reads, workflow creation, task cancellation, and workflow aggregation use explicit tenant predicates and transaction-local context; optimistic versions and fencing are retained (implementation commits `ca1e942`, `6c57c6e`, `02d5c57`, and `3af09b3`).
- Registered worker task claim, heartbeat, completion, failure/retry, cancellation/timeout, promotion, lease recovery, and registered aggregation require a current worker registration and active, unexpired tenant grant. The grant is locked in the operation transaction; unregistered compatibility methods are restricted to `legacy-default` (claim commit `27a8fdb`; settlement/promotion/recovery commits `b17984a`, `d15da70`, `3e54d69`, `c200097`, and `14c3b7e`).
- The outbox claim model carries tenant identity. A registered dispatcher can claim, publish, acknowledge, and retry only events covered by its worker grant; the compatibility dispatcher only sees `legacy-default` (storage commits `b6eb266`, `f727b71`). NATS envelopes and headers carry the tenant ID (transport commit `7c94d12`).
- The consumer inbox uses the v5 tenant-leading primary key. Registered inbox claim, renew, acknowledge, and release all revalidate the worker grant; old envelopes without a tenant field default to `legacy-default` (inbox commit `d0e8723`; NATS consumer commit `2446810`).
- PostgreSQL integration tests cover tenant-local inbox identities for the same consumer/event key, cross-tenant outbox claim exclusion, and grant revocation blocking task settlement and event/inbox acknowledgement or retry (test commits `49e5df9` and `6174a96`). Event deserialization has backward-compatibility tests (`71263b3`).

These newer tests have not yet been confirmed by a hosted run. Do not treat the v5 migration, the grant-gated event pipeline, or the latest Rust changes as CI-verified yet.

The tenant data plane and initial HTTP tenant route group are materially scoped, but **row-level tenant isolation is not fully active**. New `/api/v1/tenant/*` submit/read/cancel routes authenticate through `PgTenantCredentialRepository`, derive `TenantContext` only from the stored credential, and enforce `Read`, `Submit`, and `Cancel` roles. Existing `/api/v1/*` and worker inventory routes remain behind the separate global platform-operator credential. The HTTP integration test covers tenant submit/read/cancel, cross-tenant 404 equivalence, invalid/platform credentials, and reader-role denials (implementation commit `29384d7`; HTTP test commits `2d97e53` and `f766f80`; OpenAPI commit `a5556ba`). These changes await hosted verification. Worker task claim/heartbeat/completion/failure/timeout/cancellation, promotion, recovery, registered aggregation, outbox claim/ack/retry, and inbox claim/renew/ack/release now install transaction-local worker and/or tenant GUC state after the relevant registration/grant checks. Batch operations set the worker identity before candidate scans and switch tenant GUC before each tenant-owned mutation. Worker dependency reads now use a registered, grant-checked repository method. Legacy operator reads and compatibility paths are restricted to `legacy-default`. A PostgreSQL single-connection regression test exercises tenant, worker, and legacy GUC cleanup after commit. These changes have not yet been hosted-verified. Worker task claim/heartbeat/completion/failure/timeout/cancellation, promotion, recovery, registered aggregation, outbox claim/ack/retry, and inbox claim/renew/ack/release now install transaction-local worker and/or tenant GUC state after the relevant registration/grant checks. Batch operations set worker identity before candidate scans and switch tenant GUC before each tenant-owned mutation. Worker dependency reads now use a registered, grant-checked repository method. Legacy operator reads and compatibility paths are restricted to `legacy-default`. A single-connection PostgreSQL regression test checks tenant, worker, legacy and credential-digest GUC cleanup after commit. These changes await hosted verification. Forced RLS and a restricted non-owner/non-BYPASSRLS runtime database role are not enabled. The global operator token remains a local/platform bootstrap credential, not a tenant identity. Keep the HTTP listener loopback-only and do not create customer tenant data until transaction-local state, forced RLS, the restricted runtime role, and the remaining deployment boundary are verified.

## Context

ADR-0011 adds one high-entropy operator Bearer token to protect the local v1 control plane. That credential authenticates one trusted operator; it does not establish a tenant identity, grant distinct roles, or isolate durable data.

The current PostgreSQL schema has tenant ownership columns and tenant-consistent constraints on workflow runs, task runs, dependency edges, task attempts, outbox events, and consumer inbox records. Workflow and task run IDs remain globally unique during this compatibility phase; `consumer_inbox` uses the v5 primary key `(tenant_id, consumer_name, event_id)`. Node membership and worker registrations remain cluster-scoped, with customer access mediated by `worker_tenant_grants`. Legacy compatibility repository methods still exist but are restricted to the reserved `legacy-default` tenant. The HTTP control plane still exposes the older global-operator route contract and has not yet adopted tenant credential authentication.

Adding a tenant field only to the HTTP request, or checking it only in Axum middleware, would not provide isolation. A missed predicate in one SQL statement could disclose or mutate another tenant's records. Similarly, allowing a worker to claim every task after adding tenant-aware API credentials would bypass tenant ownership in the execution plane.

## Decisions

### Tenant and principal identity

1. Introduce a typed, immutable `TenantId` and a durable `tenants` registry with active/suspended lifecycle state.
2. API identities are principals owned by exactly one tenant. A principal has a stable ID, a display name, an active/revoked state, and a bounded role set.
3. Credentials belong to principals. Store a one-way digest of a cryptographically random, high-entropy token, a non-secret lookup prefix, creation/expiry/revocation timestamps, and last-used metadata. Never store or log raw tokens.
4. Initial tenant roles are `tenant_admin`, `tenant_operator`, and `tenant_reader`. Permission checks are explicit per route/command; possession of a token alone must not silently grant every operation.
5. The current `AETHERFLOW_API_TOKEN` remains a single-operator local/bootstrap credential only. It is not a tenant API key and must not be used to infer or select a tenant for customer data requests. Platform-level administration is a distinct, auditable capability.
6. The server derives tenant and principal context from the validated credential record. A client-supplied tenant header, path value, workflow field, or query parameter is never authoritative for ownership.

### Tenant ownership in durable records

The next additive PostgreSQL migration must introduce tenant ownership for:
- workflow runs;
- task runs;
- dependency edges;
- task attempts;
- outbox events;
- consumer inbox records.

Each dependent row must carry or be constrained by the same tenant identity as its parent. Add composite unique keys and foreign keys so a task, dependency, attempt, outbox event, inbox completion, or delivery cannot be attached to another tenant's workflow/task by mixing identifiers. Tenant-aware indexes must lead with `tenant_id` for the public list/read paths and tenant-scoped dispatch paths.

The existing global primary keys may be retained during the first migration only if all external identifiers remain globally unique and every access path is still tenant-scoped. If identifiers become tenant-local, convert the relevant keys and foreign keys together and include `tenant_id` in deterministic task-run ID derivation. Do not mix the two key models within one migration.

Existing rows are backfilled to a reserved, explicitly named legacy tenant. The migration must fail closed if any row cannot be assigned to that tenant. Applied migrations v1–v3 are immutable; tenant ownership is an additive migration with a new checksum-ledger entry.

### Database enforcement and repository contracts

1. Every API-facing storage operation must require a typed `TenantContext` containing the authenticated tenant and principal. There must be no unscoped HTTP read, write, cancellation, or listing overload.
2. Every query and mutation must scope by tenant, including not-found detection. A resource owned by another tenant is indistinguishable from a missing resource and returns the same public 404 envelope.
3. Use PostgreSQL row-level security as defense in depth on every tenant-owned table. Enable and force row-level security. Runtime service roles must neither own the protected tables nor have `BYPASSRLS`.
4. Set tenant context with transaction-local state only (`SET LOCAL` / `set_config(..., true)`) inside the same transaction as the operation. Do not use session-level `SET` on pooled connections; a tenant setting must never leak to the next pool borrower.
5. The migration/DDL owner role is separate from the restricted runtime role. Runtime queries execute as a non-owner role so table owners cannot accidentally bypass forced policies.
6. Privileged platform operations use a separate, explicit, auditable path. Do not create a generic repository flag that silently bypasses tenant policy.

### Worker and event ownership

1. Node membership remains a cluster-level liveness record. A worker registration is not automatically authorized for every tenant.
2. Add explicit worker-to-tenant grants. A worker may claim and settle tasks only inside a tenant for which its current registration has a live grant.
3. Worker scheduling becomes tenant-aware: each claim/heartbeat/completion/failure/recovery transaction carries the selected tenant context and enforces both membership fencing and tenant authorization.
4. Outbox events carry tenant identity. Consumer inbox uniqueness and claims are tenant-scoped so event IDs or consumer names cannot collide across tenant ownership boundaries.
5. JetStream subjects/headers and consumer configuration must preserve tenant identity without allowing arbitrary subject construction from unvalidated client input. Delivery remains at-least-once; tenant isolation does not create exactly-once side effects.

### Authorization, HTTPS, and exposure

- Tenant-scoped credentials are authenticated independently of the local platform operator credential.
- Read, submit, cancel, worker visibility, and tenant-administration operations receive explicit role checks.
- Worker infrastructure visibility is platform-admin-only unless a tenant-specific worker inventory is designed.
- The HTTP listener remains loopback-only until tenant scoping, role checks, and an HTTPS deployment boundary have all passed hosted tests and a threat-model review.
- PostgreSQL Rustls TLS protects the database connection only; it does not provide HTTPS for the Axum listener.

## Required implementation and verification sequence

1. Add typed TenantId, TenantContext, tenant/principal/credential data model, and role definitions.
2. Add an additive migration that creates tenants, backfills legacy ownership, and adds tenant-consistent constraints/indexes across workflow/task/dependency/attempt/outbox/inbox rows.
3. Add explicit worker-to-tenant grants and tenant-aware claim/settlement paths while preserving node/worker epoch and task fencing checks.
4. Add scoped repository methods and transaction-local tenant context. Enable forced row-level security and run the application using a restricted non-owner database role.
5. Add tenant-bound credential lookup and route-level authorization. Keep the environment operator token restricted to explicit platform administration.
6. Verify cross-tenant isolation at both repository and HTTP levels: read, list, cancel, submit collision, dependency insertion, worker claim, event dispatch, and inbox deduplication.
7. Verify pooled-connection reuse does not retain a prior tenant setting; verify missing/foreign resources return the same 404 response.
8. Verify migration backfill preserves all legacy workflow/task/attempt/outbox/inbox rows and rejects incomplete ownership.
9. Complete HTTPS deployment verification and a threat-model review before changing the loopback bind guard.

## Consequences

- Tenant ownership is a persistence and execution invariant, not merely an HTTP authorization check.
- Worker scheduling, outbox/inbox delivery, and repository interfaces must become tenant-aware along with the API.
- Existing global identifiers and APIs need a deliberate compatibility migration; no opportunistic tenant field may be added without composite constraints and scoped queries.
- The initial single-operator token remains useful for local development but cannot be presented as a multi-tenant access model.
- Non-loopback exposure remains blocked until every required layer is implemented and verified.

## Revisit when

Revisit identifier locality, tenant quotas, cross-tenant workflow dependencies, shared platform workers, and event-stream partitioning before implementation details become stable. Cross-tenant workflow dependencies remain disallowed by default.
