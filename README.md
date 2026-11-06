# AetherFlow

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M00–M07 are complete; M08 is in progress. The server has structured JSON logs, request-scoped tracing, bounded Prometheus metrics, W3C trace-context propagation, and opt-in OpenTelemetry OTLP export. HTTP metrics include fixed operation classes for health, metrics, operator/tenant reads, submissions and mutations, plus other requests; response totals are split into 2xx, 3xx, 4xx, 5xx and other classes. A cancellation-safe in-flight guard records handler futures dropped before a response. Raw paths and tenant/workflow identifiers are never metric labels. Authenticated request spans record the auth scope/outcome and tenant-principal identity; the shared operator token is identified only as a shared credential, not as an individual human. The local Compose environment provides PostgreSQL 18 and a three-node NATS JetStream cluster with separate migration/runtime database roles. Coordinated PostgreSQL/JetStream backup-restore, schema-upgrade, API process-restart, in-flight-worker-during-API-outage, and full three-member JetStream-cluster-outage recovery drills are hosted-verified. The full-cluster test confirms that PostgreSQL retains an unpublished outbox event while all NATS members are offline and that the event is later published and processed exactly once after quorum returns. Five five-repeat PostgreSQL/NATS benchmark sessions are archived: two matched AMD EPYC 9V45 sessions plus separate EPYC 7763 and EPYC 9V74 observations. Each profile reconciled 896 outbox events with 896 inbox events per workload shape, with zero duplicates or errors. The conservative CI regression gate applies only to the normalized AMD EPYC 9V45 fingerprint; unmatched hardware fingerprints are archived and skipped. The gate is not a production SLO. Dependency advisory/source checks and full-history secret scanning pass hosted CI. The threat model, SLO proposal, and operations guide are published. Further operational failure-injection, matched-runner performance verification, and production-readiness work remain open. The control plane remains experimental and is not production-ready.
> **Live overview:** [Open the AetherFlow GitHub Pages site](https://pooya16pg-spec.github.io/aetherflow/) · [Open the read-only dashboard](https://pooya16pg-spec.github.io/aetherflow/dashboard/).

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
| GET | /api/v1/workflow-runs | List legacy-default workflow runs with keyset pagination (operator) |
| POST | /api/v1/workflow-runs | Validate and durably submit a workflow-run snapshot |
| GET | /api/v1/workflow-runs/{workflow_run_id} | Read a workflow run and its task summaries |
| GET | /api/v1/workflow-runs/{workflow_run_id}/events | Read redacted workflow/task event metadata (operator) |
| GET | /api/v1/task-runs/{task_run_id} | Read task state and payload-size metadata |
| POST | /api/v1/task-runs/{task_run_id}/cancel | Cancel with an expected_version guard |
| GET | /api/v1/workers | List live active worker registrations (platform operator) |
| GET | /api/v1/tenant/workflow-runs | List the authenticated tenant's workflow runs |
| POST | /api/v1/tenant/workflow-runs | Submit a workflow for the authenticated tenant |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id} | Read a tenant-owned workflow |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id}/events | Read redacted events for a tenant-owned workflow |
| GET | /api/v1/tenant/task-runs/{task_run_id} | Read a tenant-owned task |
| POST | /api/v1/tenant/task-runs/{task_run_id}/cancel | Cancel a tenant-owned task with an expected_version guard |

The contract and its compatibility/security decisions are recorded in [ADR-0010](docs/decisions/0010-versioned-control-plane-api.md). Task input is base64 encoded and limited to 1 MiB after decoding; the HTTP request body is capped at 2 MiB. Inspection responses never return task input or output bytes. Reusing a workflow-run ID returns a conflict; the v1 API does not promise idempotent replay.

**Security boundary:** `AETHERFLOW_API_TOKEN` protects platform/operator compatibility routes; tenant routes authenticate separately against stored tenant credentials and derive tenant identity server-side. `AETHERFLOW_MIGRATION_DATABASE_URL` is the DDL/migration owner pool; `DATABASE_URL` must be a restricted non-owner runtime role with no SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, protected-table ownership, public-schema CREATE, REPLICATION, or membership in any other PostgreSQL role. Startup verifies this role and ENABLE + FORCE RLS on all ten protected tables. PostgreSQL uses Rustls with native CA roots and requires TLS by default. The HTTP listener defaults to 127.0.0.1:8080; plaintext HTTP is loopback-only, and non-loopback binds require `AETHERFLOW_TLS_CERT_PATH` and `AETHERFLOW_TLS_KEY_PATH` with valid PEM material. PostgreSQL TLS chain and hostname verification passed hosted integration tests against a live PostgreSQL 18 fixture (run [41609705469](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41609705469)). Transaction-local RLS GUCs are app-controlled context and do not protect against arbitrary SQL execution or SQL injection. Browser access is disabled by default. Set `AETHERFLOW_CORS_ALLOWED_ORIGINS` to a comma-separated list of exact HTTP(S) origins (for example, `https://pooya16pg-spec.github.io`); wildcard origins, paths, cookies, and credentialed CORS are not supported.

## Read-only dashboard

Open the [AetherFlow dashboard](https://pooya16pg-spec.github.io/aetherflow/dashboard/) to browse workflow runs, task snapshots, active workers, and redacted event history. It is a read-only browser client; it does not submit or cancel work. The Bearer token is held in page memory only and is not persisted to local storage, cookies, or the URL. Use HTTPS for remote APIs.

To connect a hosted API, configure `AETHERFLOW_CORS_ALLOWED_ORIGINS=https://pooya16pg-spec.github.io` on the API server and restart it. The origin must match exactly. Operator listing is intentionally limited to the reserved `legacy-default` tenant; tenant credentials list only their own runs. Event responses never include raw outbox payloads.

## Operations runbooks

Operational procedures are documented for a coordinated PostgreSQL/JetStream backup and restore, and for controlled PostgreSQL schema upgrades. The backup procedure is a logical recovery drill, not a production PITR service; the project still needs an explicit owner-approved RPO/RTO and a tested production WAL-archive or managed-database policy.

- [Backup and restore runbook](docs/operations/backup-restore.md)
- [PostgreSQL schema upgrade runbook](docs/operations/schema-upgrades.md)
- [Operational guide](docs/operations/production-guide.md)
- [SLO proposal (draft)](docs/operations/slo-proposal.md)
- [Security threat model](docs/security/threat-model.md)

## Operations telemetry

The server emits newline-delimited JSON logs. Set `RUST_LOG` to adjust the `tracing` filter; the default is `info`. HTTP requests create request-scoped tracing spans with a process-local request ID, method, response status, latency, and a W3C trace ID. Valid `traceparent`/`tracestate` headers are extracted from incoming requests; the server returns its own span context in response trace headers. Browser preflight allows only the explicit `authorization`, `content-type`, `traceparent`, and `tracestate` request headers, and exposes only the two trace response headers. Invalid trace context is ignored. Baggage, arbitrary request headers, URIs, query strings, request/response bodies, Bearer credentials, task payloads, and database URLs are not attached to spans or propagated.

`GET /api/v1/metrics` exposes aggregate Prometheus counters and a bounded request-latency histogram. It is protected by the platform operator Bearer credential and remains unavailable in health-only mode. Metrics do not use paths, tenant/resource identifiers, or other high-cardinality labels.

OpenTelemetry export is opt-in. Configure `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) to enable the default OTLP exporter; the protocol is `http/protobuf` via `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` (or `OTEL_EXPORTER_OTLP_PROTOCOL`). Set `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` to disable export. An explicit `OTEL_TRACES_EXPORTER=otlp` without an endpoint is rejected at startup instead of silently targeting localhost. The OTLP exporter supports the standard `OTEL_EXPORTER_OTLP_HEADERS` environment variable; never put its values in logs or traces. Without an endpoint, the process creates local spans but makes no exporter connection.

## CLI

The CLI is implemented and hosted-verified on exact source [0c19b9b](https://github.com/pooya-labs/aetherflow/commit/0c19b9b7d8124472c81ce53056612344f0ebab57), together with successful Rust CI [41614021774](https://github.com/pooya-labs/aetherflow/actions/runs/41614021774), PostgreSQL/NATS [41614021644](https://github.com/pooya-labs/aetherflow/actions/runs/41614021644), and Pages [41614915557](https://github.com/pooya-labs/aetherflow/actions/runs/41614915557). It exposes health, active worker inventory, workflow submit/inspect, and task inspect/cancel over the v1 API. Operator and tenant credentials are separate and environment-only; remote cleartext HTTP is rejected and workflow input is bounded before reading.

~~~bash
export AETHERFLOW_API_TOKEN="your-operator-token"
cargo run -p aetherflow-cli -- --help
cargo run -p aetherflow-cli -- workers
cargo run -p aetherflow-cli -- workflow inspect run-0001
cargo run -p aetherflow-cli -- workflow submit --file workflow.json
cargo run -p aetherflow-cli -- task inspect task-run-0001
cargo run -p aetherflow-cli -- task cancel task-run-0001 --expected-version 3
~~~

Tenant workflow/task commands use AETHERFLOW_TENANT_TOKEN and the --tenant flag. Worker inventory is operator-only. AETHERFLOW_URL or --url selects a non-default server; the default is http://127.0.0.1:8080. Use --file - to read a submission JSON object from standard input. See [the CLI guide](apps/aetherflow-cli/README.md) and [the API contract](docs/api/openapi.yaml).

## Rust SDK

A typed asynchronous Rust client lives in `crates/aetherflow-sdk`. It models the v1 workflow, task, worker, and error contracts; validates workflow DAGs and payload limits locally; separates anonymous, operator, and tenant scopes; rejects remote plain HTTP; disables redirects; and redacts credentials from Debug output. The SDK is hosted-verified on exact source [047d02f](https://github.com/pooya-labs/aetherflow/commit/047d02f619b6f73720be9a28220d4fa3a1fb1ed3). The client enforces a 1 MiB decoded task-input cap and writes JSON requests into a 2 MiB bounded writer before sending them.

See the [Rust SDK guide](crates/aetherflow-sdk/README.md) for a usage example.

## Reproducible scheduler benchmarks

The repository includes two complementary performance tools: an in-memory scheduler/DAG benchmark and a PostgreSQL 18 + NATS 2.12 end-to-end load profile covering the durable worker, outbox, JetStream, and inbox path. Both report CSV metrics and run in CI. The end-to-end guide records three five-repeat PostgreSQL/NATS sessions and a matched-fingerprint reference; the CI comparator applies conservative limits only to the normalized EPYC 9V45 fingerprint. The baseline is not a production capacity claim. See the [in-memory benchmark guide](docs/performance/benchmarks.md) and the [end-to-end PostgreSQL/JetStream load guide](docs/performance/end-to-end-load.md).

## Repository map

The repository is being built incrementally. See [ROADMAP.md](ROADMAP.md) for the authoritative checklist and current checkpoint, [docs/architecture/overview.md](docs/architecture/overview.md) for system boundaries, [docs/decisions/](docs/decisions/) for architecture decisions, and [docs/development.md](docs/development.md) for local commands.

## Development status

M00–M07 are complete. M08 includes hosted-verified observability, the local PostgreSQL 18 / three-node NATS 2.12 Compose environment, runbook drift checks, isolated PostgreSQL/JetStream restore-to-runtime execution, API restart recovery, in-flight worker completion during API outage, and a full three-member JetStream outage/recovery drill. The complete-cluster drill is covered by `jetstream_full_cluster_outage_retains_and_replays_outbox_event_once`: it stops all three CI NATS members, confirms the outbox event remains unpublished/retryable in PostgreSQL, restores the cluster, then verifies one inbox processing and zero duplicates. The exact-source PostgreSQL/NATS CI [42211375628](https://github.com/pooya-labs/aetherflow/actions/runs/42211375628), Rust CI [42211375893](https://github.com/pooya-labs/aetherflow/actions/runs/42211375893), security [42211375664](https://github.com/pooya-labs/aetherflow/actions/runs/42211375664), and Pages [42211374941](https://github.com/pooya-labs/aetherflow/actions/runs/42211374941) all passed on `c5543d96913fbef356fc7856486feb43ea52c71d`. The same PostgreSQL/NATS run also re-verified the restore-to-runtime and API-outage scenarios: the worker finished its in-flight task while the API was offline; five target outbox events reached the inbox with zero duplicates.

Five five-repeat PostgreSQL/NATS sessions are archived. The two matched AMD EPYC 9V45 sessions recorded median fan-out/chain throughput of 7.319/7.195 and 7.415/7.298 workflows/s respectively, with a one-page visible-memory metadata difference documented in the report. The other three observations remain separate: two EPYC 7763 sessions and one EPYC 9V74 session. The latest EPYC 9V74 session recorded 6.173 fan-out and 5.893 chain workflows/s; its CPU fingerprint did not match the stored reference, so CI correctly skipped threshold application. Every profile reconciled 896 outbox/inbox events per shape with zero duplicates/errors. See the [baseline report](docs/performance/end-to-end-load.md). The CI comparator applies 10% throughput-drop and 15% p95-increase limits only when the complete normalized runner/workload fingerprint matches; unmatched fingerprints are archived and skipped. These hosted measurements are not production capacity or an SLO claim.

The tenant routes derive tenant identity from tenant credentials and enforce Read/Submit/Cancel role permissions. PostgreSQL tenant data has forced RLS on ten tables. Migrations and application queries use separate database roles; startup rejects elevated runtime-role attributes, role memberships, protected-table ownership, or missing FORCE RLS. Plain HTTP is loopback-only; non-loopback binds require a valid Rustls certificate/key pair.

Security and operations documentation describes the implemented boundaries and remaining risks. An authenticated-principal API token bucket is now implemented (burst 30, refill 2 requests/second by default), with bounded process-local state, 429/Retry-After, and an aggregate metric. The limit is separate for the operator and each tenant principal, but it is not global across replicas and does not throttle unauthenticated/invalid credentials by source IP. The production guide now specifies the required shared ingress/API-gateway policy for external deployments (source-based throttling before forwarding, trusted connection identity only, stripped client forwarding headers, and no direct-origin bypass); this policy is documentation only and has not been deployed or verified. No independent production security audit has been completed. Deployment/verification of trusted-source failed-auth throttling, distributed quotas or edge-gateway policy, per-principal audit identity, managed secret rotation, production NATS authentication/TLS, production PITR/RPO/RTO, horizontal scaling evidence, and complete workflow SLO instrumentation remain open.

License selection remains an owner-only release decision; no project license file or third-party license allowlist has been added. M02 property-based lifecycle tests remain deferred, and WASM blocking work is bounded by fuel/epoch deadlines rather than force-aborted. The service remains experimental; see [ROADMAP.md](ROADMAP.md) for the authoritative remaining checklist.

## License

License selection is pending; no license grant is made yet.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. The `aetherflow-transport-nats` crate integrates a real JetStream publisher and inbox-backed pull consumer. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry boundary, stable message-ID deduplication, logical consumer deduplication, lease and AckWait heartbeat for long handlers, failed-handler redelivery, single-member loss, and a full three-member outage with PostgreSQL outbox retention and exactly-one inbox processing after recovery. The durable worker/coordinator remains an opt-in PostgreSQL execution path. OS-level process termination after broker storage and after inbox commit is now integration-tested on PostgreSQL 18 + NATS (run `41380067612`). Delivery remains at-least-once; external side effects require application-level idempotency or a transaction shared with inbox completion. M05 membership-gated task execution is verified on PostgreSQL 18 + NATS (41402000557) with Rust CI (41402000570). Tests cover stale-epoch write rejection, replacement-worker recovery after task-lease expiry, registry capability filtering, lease renewal while gracefully draining a long-running handler, UID-scoped packet-level PostgreSQL partition/rejoin, abrupt worker-process loss, three-member NATS quorum recovery, and the combined two-round chaos campaign. This is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).
