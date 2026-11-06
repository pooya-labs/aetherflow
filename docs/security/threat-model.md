# AetherFlow threat model

**Status:** Engineering threat model, 2026-11-04. This is a design review, not an independent security audit or production approval.

## Scope and security objectives

The scope is the Rust control plane, tenant-aware PostgreSQL storage, worker execution and NATS JetStream delivery, the WebAssembly plugin boundary, the read-only dashboard, telemetry, and the CI/dependency supply chain.

Primary objectives are to preserve workflow/task integrity, prevent cross-tenant data access, protect operator and tenant credentials, constrain untrusted workflow/plugin execution, avoid leaking payloads into telemetry, and make recovery behavior explicit.

## Assets and impact

| Asset | Security impact if compromised |
|---|---|
| Workflow definitions, task inputs/outputs, attempts, leases, and event history | Unauthorized reads, tampering, replay, or denial of execution |
| Tenant, principal, credential, and worker-grant records | Cross-tenant access or privilege escalation |
| Platform/operator and tenant tokens | Control-plane takeover or tenant impersonation |
| PostgreSQL database and migration/runtime credentials | Durable-state compromise or tenant boundary bypass |
| JetStream streams, consumers, and message identities | Message loss, replay, duplicate work, or workflow stalls |
| Plugin artifacts and execution resources | Host resource exhaustion or exploitation of runtime/engine defects |
| TLS private keys and OTLP exporter headers | Impersonation or disclosure of telemetry credentials |
| Source, lockfile, CI workflows, release artifacts | Malicious code or dependency entering a build |

## Trust boundaries

1. **External client → HTTP API.** Platform routes require the operator Bearer credential. Tenant routes authenticate a stored tenant credential and enforce explicit role permissions. Browser CORS is exact-origin allowlisted; CORS is not authentication.
2. **API/worker process → PostgreSQL.** Migrations use `AETHERFLOW_MIGRATION_DATABASE_URL`; normal application queries use the restricted `DATABASE_URL` role. Startup checks reject elevated runtime-role attributes/memberships, protected-table ownership, and missing FORCE RLS.
3. **Tenant context → SQL/RLS.** Ten tenant-owned tables have forced RLS. Tenant and credential context is transaction-local. This guards against accidentally omitted tenant predicates; it does not protect against arbitrary SQL execution under the runtime role or a compromised application process that can set its own custom GUCs.
4. **Worker/outbox → NATS JetStream → inbox consumer.** PostgreSQL remains authoritative for workflow state; outbox/inbox transactions and message identity handle the documented at-least-once delivery boundary. They do not make unrelated external side effects exactly-once.
5. **Host → WebAssembly component.** ABI v1 does not import WASI and exposes only an explicit, bounded logging capability. Each invocation gets a fresh Store and component instance with memory/table/instance/fuel/host-call limits and epoch interruption.
6. **Dashboard → API.** The dashboard is read-only. The operator token is held in page memory rather than persistent browser storage; remote API usage still requires HTTPS and exact CORS configuration.
7. **Repository → CI/dependencies.** Cargo.lock, advisory/source checks, full-history secret scanning, action pinning, and scheduled dependency update PRs are intended to reduce supply-chain exposure. They do not establish that every upstream artifact is trustworthy.

## Threat actors and abuse cases

| Threat | Existing mitigation | Residual risk / required follow-up |
|---|---|---|
| Unauthenticated remote caller | Bearer auth on durable routes; health is liveness-only; non-loopback plaintext HTTP is rejected; authenticated operator/tenant principals have bounded process-local token buckets and receive 429/Retry-After | Invalid/missing credentials are not source-IP throttled; operator credential is shared; quotas are not coordinated across replicas; no external penetration test |
| Tenant attempts to read or mutate another tenant | Credential-derived tenant context, permission checks, composite tenant ownership, forced RLS | RLS is not a defense against arbitrary SQL execution or a compromised runtime process; continue negative cross-tenant tests |
| Stolen tenant/operator token | Only digests are stored; raw tokens are not logged; tenant credentials can be revoked | No managed secret store, no operator identity separation, no non-disruptive operator-token rotation |
| Malicious workflow or plugin | Bounded payloads/retries/concurrency; WASM capability default deny and resource/fuel limits | Engine bugs, host bugs, process-wide resource pressure, and OS isolation remain deployment concerns |
| Network attacker | PostgreSQL certificate verification by default; HTTPS required for non-loopback listener | Certificate issuance/rotation is operator-managed; development Compose intentionally uses local plaintext settings |
| Malicious or compromised dependency/action | Locked Cargo graph, source policy, advisory checks, pinned CI actions, Gitleaks scan | Upstream compromise and maintainer review remain possible; dependency license review and SBOM/signing are not complete |
| Database or broker outage / abrupt process loss | Durable PostgreSQL state, leases/fencing, outbox/inbox, CI failure tests and isolated restore rehearsal | CI restore is not production PITR; production RPO/RTO, monitoring and recovery ownership are not approved |
| Payload or credential leakage via telemetry | Allowlisted request span fields, aggregate metrics, no URI/body/header/token labels, opt-in OTLP | Operators must protect log/OTLP destinations and exporter headers; review new telemetry fields before adding them |

## Implemented boundaries and assumptions

- The operator token is 32–256 URL-safe ASCII characters and must be generated from a cryptographically secure source. It is a single shared operator credential.
- Tenant API credentials are high-entropy opaque values; only SHA-256 digests and non-secret prefixes are stored. Permission checks are role-based and tenant ID is not accepted as an authority from the request.
- The application uses parameterized SQL. RLS is an additional database guardrail, not an SQL injection mitigation.
- PostgreSQL TLS uses the platform native trust roots and validates the server name. `DATABASE_SSL_MODE=disable` is only for the disposable loopback Compose environment. A non-loopback HTTP listener requires a valid Rustls certificate/key pair.
- Compose NATS endpoints are local development services. Do not expose the loopback-only ports or treat the development cluster configuration as a production-authenticated NATS deployment.
- Metrics expose aggregate request counts, a bounded latency histogram, and an authenticated-principal rate-limit counter under operator authentication. The in-memory limiter defaults to burst 30/refill 2 per second, is capped at 4,096 buckets per process, and does not provide a cluster-wide quota. These are request controls, not workflow-level SLO instrumentation.
- At-least-once event delivery means external task handlers must use stable idempotency keys or their own transactional boundary.

## Priority gaps before production

1. Complete independent security review/penetration test and a production deployment architecture review.
2. Add trusted-source throttling for unauthenticated/invalid credentials, cluster-wide rate-limit enforcement or an approved edge-gateway policy for multiple API replicas, per-principal audit identity, managed secret/certificate rotation, and production-safe NATS authentication/TLS configuration.
3. Decide and test production backup/WAL archiving, restore ownership, RPO, and RTO; the current rehearsal is isolated CI only.
4. Add workflow/outbox/inbox SLIs and alerting, then approve SLOs and error budgets.
5. Complete dependency license review, SBOM generation and release provenance/attestation policy.
6. Decide OS/container isolation and operational concurrency limits for untrusted plugins; do not treat Wasmtime limits as a full process sandbox.

## Non-goals

This document does not claim compliance with a security framework, production readiness, immunity to compromised hosts or privileged operators, or exactly-once external side effects. The project remains experimental until the priority gaps and owner decisions are resolved.
