# ADR-0012: Tenant ownership and row-level authorization

- **Status:** Accepted; tenant ownership migration v4 is hosted-verified; tenant inbox migration v5, forced-RLS migration v6, worker grant-lock migration v7, tenant/worker authorization paths, tenant-authenticated HTTP routes, and migration-owner/runtime-role separation are implemented but await hosted verification
- **Date:** 2026-11-03
- **Scope:** Tenant identity, API principals and credentials, durable workflow ownership, worker grants, and PostgreSQL row-level isolation

## Implementation status (2026-11-03)

The additive PostgreSQL migration v4 is hosted-verified on PostgreSQL 18. It creates tenant/principal/credential registries and worker grants, backfills workflow/task/dependency/attempt/outbox/inbox rows into the reserved `legacy-default` tenant, and adds tenant-consistent composite keys, foreign keys, and tenant-leading indexes. The exact v4 source passed PostgreSQL/NATS run [41575288110](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41575288110) and Rust CI [41575288079](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41575288079) on source `3a7d704`.

Migration v5 replaces the inbox primary key with `(tenant_id, consumer_name, event_id)`. Migration v6, `0006_force_tenant_row_level_security.sql`, enables and forces RLS on ten tenant-owned tables: the six workflow/task/dependency/attempt/outbox/inbox tables, plus `tenants`, `tenant_principals`, `tenant_credentials`, and `worker_tenant_grants`. Node and worker membership tables remain cluster-scoped. Migration v7, `0007_worker_tenant_grant_lock_rls.sql`, adds the PostgreSQL `UPDATE USING` policy needed for a registered worker's `SELECT ... FOR SHARE` grant lock while setting an unconditional false `WITH CHECK` so the worker cannot modify the grant registry. It also keeps the tenant grant-admin policy from matching a worker context. V5–v7 are committed and registered in the immutable migration runner; their hosted PostgreSQL verification is pending.

Tenant credentials are stored as SHA-256 digests. Credential authentication sets only the presented digest in transaction-local `aetherflow.credential_sha256`; the RLS policies expose the matching credential and its tenant/principal join for that transaction, and permit its last-used timestamp update. Tenant contexts install the tenant/principal/role GUCs; registered worker operations install worker identity and switch to a selected tenant only after checking and locking the active grant. All GUCs are installed with transaction-local `set_config(..., true)`, and the one-connection pool test verifies cleanup after commit.

Tenant-facing HTTP endpoints now exist separately under `/api/v1/tenant/*`: workflow submit/read and task read/cancel. They derive `TenantContext` from the stored credential and enforce Read/Submit/Cancel permissions. The older platform/operator endpoints remain a separate local compatibility surface and are restricted to `legacy-default`; worker inventory remains operator-only. The registered worker/task lifecycle, dependency reads, promotion/recovery/aggregation, outbox dispatch, and inbox claim/renew/ack/release enforce live tenant grants. JetStream envelopes and headers carry tenant identity. These implementations and their integration suites are committed, but the source after `3a7d704` is not yet hosted-verified.

The server bootstrap now requires two distinct connections when `DATABASE_URL` is configured:
- `AETHERFLOW_MIGRATION_DATABASE_URL`: migration/DDL owner connection used only to apply migrations.
- `DATABASE_URL`: restricted application runtime connection.

Before creating the API state, startup checks that the runtime role is not a superuser, does not have `BYPASSRLS`, `CREATEROLE`, or `CREATEDB`, has no membership in any other PostgreSQL role, owns none of the protected tables, has no public-schema CREATE privilege or replication capability, and sees ENABLE + FORCE RLS on all ten protected tables. It continues to reject non-loopback HTTP binds; PostgreSQL TLS is not a substitute for HTTPS on the HTTP listener.

The v5–v7 migrations, runtime-role check, and real `SET ROLE` RLS integration test—including worker `FOR SHARE` locking and denied grant mutation—are not yet hosted-verified. The runtime role must be granted only the DML/sequence privileges it needs and must not be a member of any other PostgreSQL role. Transaction-local custom GUCs are application-controlled context, not tamper-proof claims: forced RLS is defense in depth against missing tenant predicates, not a defense against arbitrary SQL execution or SQL injection. Keep parameterized SQL and the loopback boundary.

## Context

ADR-0011 adds one high-entropy operator Bearer token to protect the local v1 control plane. That credential authenticates one trusted operator; it does not establish a tenant identity, grant distinct roles, or isolate durable data.

The current PostgreSQL schema has tenant ownership columns and tenant-consistent constraints on workflow runs, task runs, dependency edges, task attempts, outbox events, and consumer inbox records. Workflow and task run IDs remain globally unique during this compatibility phase; `consumer_inbox` uses the v5 primary key `(tenant_id, consumer_name, event_id)`. Migration v6 enables and forces RLS on those six core tables plus `tenants`, `tenant_principals`, `tenant_credentials`, and `worker_tenant_grants`. Node membership and worker registrations remain cluster-scoped, with worker access mediated by `worker_tenant_grants`. Legacy compatibility methods remain restricted to `legacy-default`. The HTTP control plane now has a separate `/api/v1/tenant/*` route group authenticated by tenant credentials; platform/operator routes and worker inventory remain a separate compatibility surface.

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

Migration v4 introduced tenant ownership for:
- workflow runs;
- task runs;
- dependency edges;
- task attempts;
- outbox events;
- consumer inbox records.

Each dependent row must carry or be constrained by the same tenant identity as its parent. Add composite unique keys and foreign keys so a task, dependency, attempt, outbox event, inbox completion, or delivery cannot be attached to another tenant's workflow/task by mixing identifiers. Tenant-aware indexes must lead with `tenant_id` for the public list/read paths and tenant-scoped dispatch paths.

The existing global primary keys may be retained during the first migration only if all external identifiers remain globally unique and every access path is still tenant-scoped. If identifiers become tenant-local, convert the relevant keys and foreign keys together and include `tenant_id` in deterministic task-run ID derivation. Do not mix the two key models within one migration.

Existing rows were backfilled to the reserved `legacy-default` tenant by migration v4; the migration fails closed when ownership cannot be assigned. Applied migrations v1–v6 are immutable; any later correction must be a new additive migration with a new checksum-ledger entry.

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
