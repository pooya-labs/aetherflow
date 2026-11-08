# Operational guide

**Status:** Operational guidance for controlled evaluation. AetherFlow remains experimental and is not approved as a production-ready service. This guide does not replace deployment-specific security, capacity, or recovery review.

## 1. Deployment boundary

The repository's Compose stack is for local development only:

- PostgreSQL 18 and three NATS 2.12 JetStream nodes.
- Ports published on loopback only.
- Separate migration-owner and restricted runtime database logins.
- Local PostgreSQL plaintext is explicitly enabled only for this disposable Compose environment.

Do not expose Compose ports, reuse `.env.example` values, or copy `DATABASE_SSL_MODE=disable` into a network deployment. The sample NATS cluster is not a production authentication/TLS configuration.

Before an externally reachable deployment exists, choose and validate a production topology for the API, workers, PostgreSQL, NATS, secret delivery, certificates, backups, and alert ownership. The current tests do not prove multi-region behavior or horizontal worker capacity.

## 2. Configuration and secrets

For a PostgreSQL-backed server, supply independent values through a secret manager or protected process environment:

| Variable | Purpose |
|---|---|
| `AETHERFLOW_MIGRATION_DATABASE_URL` | DDL/migration-owner connection; used for startup migrations only |
| `DATABASE_URL` | Restricted runtime database connection |
| `AETHERFLOW_API_TOKEN` | High-entropy platform/operator Bearer credential |
| `DATABASE_SSL_MODE` | Leave at verified TLS mode for remote PostgreSQL; `disable` is only for disposable local Compose |
| `AETHERFLOW_BIND` | Listener address; plaintext is allowed only on loopback |
| `AETHERFLOW_INGRESS_TOKEN` | High-entropy 32–256 character URL-safe edge-to-origin secret; required whenever `AETHERFLOW_BIND` is non-loopback |
| `AETHERFLOW_TLS_CERT_PATH`, `AETHERFLOW_TLS_KEY_PATH` | Required valid Rustls identity for non-loopback HTTP listener binds |
| `AETHERFLOW_CORS_ALLOWED_ORIGINS` | Exact browser origins; CORS is not authentication |
| `AETHERFLOW_API_RATE_LIMIT_BURST` | Positive integer; default 30 authenticated requests per process-local principal bucket |
| `AETHERFLOW_API_RATE_LIMIT_PER_SECOND` | Positive integer; default 2 token refills per second per process-local principal bucket |
| `AETHERFLOW_AUTH_FAILURE_BURST` | Positive integer; default 20 failed authentication attempts per immediate TCP peer |
| `AETHERFLOW_AUTH_FAILURE_PER_SECOND` | Positive integer; default 2 failed-attempt token refills per second per peer |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Optional OTLP HTTP/protobuf destination |
| `OTEL_EXPORTER_OTLP_HEADERS` | Optional exporter credentials; protect and never log these values |
| `AETHERFLOW_NATS_TLS_REQUIRED` | Require TLS for NATS connections; automatically inferred as true when credentials or TLS files are configured |
| `AETHERFLOW_NATS_CREDENTIALS_FILE` | Secret-mounted NATS `.creds` file containing the user JWT and private NKey seed |
| `AETHERFLOW_NATS_CA_FILE` | Optional PEM CA bundle for a private NATS server certificate authority |
| `AETHERFLOW_NATS_CLIENT_CERT_FILE`, `AETHERFLOW_NATS_CLIENT_KEY_FILE` | Optional client certificate/key pair for mutual TLS; both are required together |

Tenant credentials are provisioned and managed as tenant records, not as a substitute for the platform operator token. Generate credentials from a cryptographically secure source, keep the raw values out of source control and logs, and revoke them when no longer needed.

The operator token is shared and rotating it requires process restart. Authenticated operator requests use one process-local token bucket; tenant requests use separate buckets keyed by the credential-derived tenant and principal. The authenticated-principal limiter is capped at 4,096 buckets and emits 429 with `Retry-After`. A separate failed-authentication limiter tracks only missing or invalid operator/tenant credentials by the immediate TCP peer address supplied by the listener; it ignores `X-Forwarded-For` and `Forwarded`, clears the peer's history after successful authentication, and is capped at 4,096 source buckets per process. Both controls are process-local and do not coordinate quotas across replicas. A reverse proxy appears as one peer to the application, so tune this secondary limit accordingly and keep the shared ingress policy below in place. Certificate issuance, renewal, private-key protection, PostgreSQL trust roots, and OTLP collector security remain deployment-operator responsibilities.

### Required ingress policy for external deployments

The repository now includes a Cloudflare Worker implementation under [`cloudflare-gateway/`](../../cloudflare-gateway/README.md). It uses one SQLite-backed Durable Object per SHA-256-hashed client IP for a shared token bucket across Worker replicas and locations, limits API traffic before authentication is forwarded, returns `429` with `Retry-After`, and fails closed if the limiter is unavailable. The Worker exposes only `/api/v1/*` and `/health`, accepts only GET/HEAD/POST/OPTIONS, rejects declared request bodies larger than the API's 2 MiB limit, and refuses upstream redirects. The API's existing Axum body limit remains the authoritative streamed-body ceiling.

The Worker derives source identity from Cloudflare's edge-provided `CF-Connecting-IP`; it hashes the value before using it as a Durable Object name. It strips `X-Forwarded-For`, `Forwarded`, `X-Real-IP`, `True-Client-IP`, caller-supplied `CF-Connecting-IP`, and the caller-supplied ingress-token header. It preserves the API's operator or tenant Bearer token and injects `X-AetherFlow-Ingress-Token` from a Cloudflare Worker Secret. The Rust API stores only the SHA-256 digest of `AETHERFLOW_INGRESS_TOKEN` and compares the supplied header in constant time. The API ignores all forwarded identity headers.

The Rust server now refuses a non-loopback bind unless `AETHERFLOW_INGRESS_TOKEN` is configured, and the API rejects requests missing the matching edge header on every route except public `/health`. This closes the application-level direct-origin bypass when the shared secret remains private. It does not replace network isolation: restrict API origins and alternate replicas to Cloudflare egress or a private Tunnel. The Worker source/configuration is committed but not deployed or routed yet; the Cloudflare account currently has no active Zone or Tunnel, and no AetherFlow API origin has been provisioned. Certificate issuance, network policy, Tunnel setup, API hosting, secret distribution/rotation, concurrency policy and deployment acceptance remain operator-owned gates.

## 3. Database and transport

1. Provision a dedicated database and PostgreSQL roles. Apply schema changes with the migration owner; connect the running service only with a restricted runtime role.
2. Verify the runtime role is not SUPERUSER, BYPASSRLS, REPLICATION, CREATEROLE, CREATEDB, a member of another role, or owner of protected tables. Startup performs these checks and verifies FORCE RLS on ten tenant tables.
3. Use certificate-validated PostgreSQL TLS for remote connections. Install private CA roots into the host's native trust store rather than bypassing certificate verification.
4. Configure a production NATS JetStream cluster with its own credentials, access controls, transport encryption, retention, storage, monitoring, and failure policy. The transport crate now exposes `NatsConnectionConfig` for a secret-mounted `.creds` file, mandatory TLS, a custom CA bundle, and an optional client mTLS certificate/key pair. This secures the client connection only; it does not provision server accounts, subject ACLs, stream retention, or cluster policy. Do not infer that the local Compose cluster provides those controls.
5. Keep API, worker, migration, and broker permissions separated. Never run the application as the database owner.

### NATS client authentication and TLS

The `aetherflow-transport-nats` crate exports `NatsConnectionConfig`. Load it from the process environment and use its `connect` method when creating the JetStream client. The supported settings are listed above and are intentionally independent from the server's NATS account/ACL configuration.

For an externally reachable deployment, mount the `.creds` file and any CA/client identity files from a secret manager rather than copying them into the repository, container image, or environment variable values. A NATS credentials file contains a user JWT and private NKey seed; protect it like a private key. The helper requires TLS whenever credentials, a custom CA, or an mTLS identity is configured. An incomplete client certificate/key pair or an explicit plaintext override is rejected. With no security settings, the default remains plaintext for the loopback-only local Compose cluster.

The credentials file is loaded when a client connection is created; the helper does not watch or hot-reload secret files. Rotate NATS credentials by creating a new client from the updated secret and draining the old client under a controlled rollout. Server-side TLS policy, account provisioning, least-privilege publish/subscribe permissions, stream configuration, and credential revocation remain NATS operator responsibilities.

## 4. Startup and health checks

- `GET /health` is a public process-liveness endpoint only. It does not prove PostgreSQL migrations, NATS connectivity, or end-to-end workflow readiness.
- API control routes require the operator Bearer token; tenant routes require a tenant credential with the appropriate role.
- A non-loopback listener must have a valid certificate and private key before the server starts.
- A non-loopback listener also requires `AETHERFLOW_INGRESS_TOKEN`; all non-health API routes reject requests without the corresponding `X-AetherFlow-Ingress-Token` header.
- Run migrations as part of a controlled deployment window and verify the exact migration ledger after upgrade.
- Use the PostgreSQL schema workflow and Rust CI as pre-deployment evidence, not as a substitute for a production canary.

## 5. Telemetry and monitoring

The process emits structured JSON logs. Keep `RUST_LOG` at an operationally appropriate level; review new log fields before allowing them to include user-controlled strings. The request spans intentionally omit raw URIs, query strings, request/response bodies, Bearer credentials, task payloads, and database URLs. They record the authentication scope and outcome; successful tenant requests also carry the credential-derived tenant and principal IDs as trace/log attributes. Successful operator requests identify the current shared credential as `shared_operator_credential`, not a human operator. These identity fields are not Prometheus labels and must be protected by the same access and retention controls as other security-relevant logs. The shared operator credential still prevents attribution to an individual person.

`GET /api/v1/metrics` exposes aggregate HTTP request counters, in-flight requests, 5xx counts, and bounded latency histograms split across fixed operation classes (health, metrics, operator/tenant reads, submissions, mutations, and other). Response totals are also split into fixed 2xx, 3xx, 4xx, 5xx and other classes per operation. The in-flight gauge is released on every handler-future drop; an aggregate and per-operation abandoned-request counter records futures dropped before a response is observed. This counter does not distinguish client disconnects, upstream deadlines, panics, or process shutdown. It never exports raw paths, tenant IDs, workflow IDs, query strings, or credential values as metric labels. It is operator-authenticated. The OTLP exporter is opt-in and must use an access-controlled collector.

The tenant-only `GET /api/v1/tenant/metrics` endpoint exports a read-only repeatable-read snapshot scoped by the authenticated tenant and FORCE RLS. Alongside current workflow/queue/lease gauges, it reports terminal workflow outcomes and successful completion-latency p50/p95/p99 for the rolling 30-minute window, measured from acceptance (`started_at`) to terminal state (`finished_at`). These are per-tenant observations, not a platform-wide aggregate or a complete workflow SLO: eligible-accepted denominators, deadlines, platform-vs-handler attribution, cumulative publish/consume errors, and production alert ownership remain open. See the [SLO proposal](slo-proposal.md) before designing alerts.

## 6. Routine operations

### Planned deployment

1. Review the change, dependency updates, lockfile diff, and migration behavior.
2. Take a coordinated backup using the [backup and restore runbook](backup-restore.md), or the equivalent managed-service backup policy.
3. Apply the [schema upgrade runbook](schema-upgrades.md) with the migration owner; never edit an applied migration.
4. Deploy the API and workers with the restricted runtime role and production TLS/secrets.
5. Verify process liveness, authenticated API access, database connections, NATS cluster health, worker registrations/leases, and a controlled end-to-end workflow.
6. Retain deployment revision, image digests, schema version, and operational evidence.

### Database errors or failed startup

- Check that the migration URL and runtime URL refer to the intended database and roles.
- Confirm server certificate chain and hostname against native trusted roots.
- Review the migration ledger; do not manually mark a migration as applied or edit an already-applied SQL file.
- If the runtime-role guard fails, repair role ownership/attributes with a separate administrative session; do not relax the application check.

### Stalled workflows or outbox/inbox lag

- Inspect workflow/task status, attempt history, worker registrations, lease expiry, and redacted event history using the dashboard/API.
- Confirm worker processes are alive and have the expected tenant grants.
- Check the PostgreSQL outbox for pending/expired claims and JetStream stream/consumer health using authorized operator tools.
- Avoid deleting inbox/outbox records or force-acking messages to make the dashboard look healthy. Preserve evidence and follow the delivery/idempotency contract.
- External handlers must make side effects idempotent or transactionally coupled to their own state; the platform offers at-least-once delivery, not universal exactly-once effects.

### NATS or PostgreSQL outage

- Stop new submissions if the durable state/transport relationship cannot be trusted.
- Preserve PostgreSQL and JetStream data and logs; do not independently restore one side to an arbitrary point.
- Resume publishers/consumers only after following the coordinated recovery procedure and checking duplicate/replay implications.
- The CI rehearsal restores disposable PostgreSQL/JetStream fixtures, starts the actual control plane with a restricted runtime role, submits a workflow over HTTP, and first forcibly terminates/restarts the API before worker startup to verify accepted state persistence. It then starts the durable worker in a separate process, waits for both the running task attempt and an explicit marker written from inside the handler, kills the API again while the handler is in flight, and verifies the worker still completes the task and event delivery while the control plane is offline. The API is restarted afterward to read the terminal state. This proves only the tested process separation/persistence path, not HA/failover or production point-in-time recovery.

## 7. Backup, restore, and schema changes

Use [backup and restore](backup-restore.md) and [schema upgrades](schema-upgrades.md). They require a coordinated maintenance window and separate validation of PostgreSQL and JetStream state. Select RPO/RTO with the owner and validate them on the actual production services before relying on them.

## 8. Capacity and scaling

The current reference baseline is a single hosted VM session running one worker process and one inbox consumer. It is useful for regression comparison only when the runner/dependency fingerprint and workload are the same. It is not an estimate of production capacity.

Before scaling:
- benchmark multiple worker processes and consumer concurrency;
- include the HTTP/TLS/auth path and tenant-grant checks;
- test representative payload sizes, workflow DAGs, PostgreSQL sizing, NATS storage/replication and failure recovery;
- track queue/lease age and resource saturation;
- define concurrency and backpressure limits for untrusted workloads.

## 9. Production-readiness gate

Do not call the service production-ready until the following are completed and reviewed: independent security assessment; trusted-source throttling for unauthenticated/invalid credentials; cluster-wide rate-limit enforcement or an approved edge-gateway policy for multi-replica deployments; per-principal audit identity; deployment-specific NATS and secret management; approved SLO/RPO/RTO; production backup/PITR and restore rehearsal; horizontal-load and failure-injection evidence; dependency license review and SBOM/provenance policy; and an owner-operated incident/on-call model.
