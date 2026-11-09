# AetherFlow

<p align="center">
  <a href="https://pooya-labs.github.io/pooya-labs-platform/" aria-label="Pooya Labs Platform — project owner">
    <img src="assets/brand/pooya-monogram.webp" width="72" height="72" alt="Pooya Labs Platform monogram" />
  </a>
  <br />
  <strong>Project owner · <a href="https://pooya-labs.github.io/pooya-labs-platform/">Pooya Labs Platform</a></strong>
</p>

**A durable, distributed workflow execution platform built with Rust.**

AetherFlow is a public engineering project exploring reliable asynchronous execution across processes and machines. Its design centers on durable state transitions, explicit execution semantics, recoverable workers, and observable operations.

> **Project status:** M00–M07 are complete; M08 production engineering remains in progress. Maintenance PRs [#8](https://github.com/pooya-labs/aetherflow/pull/8), [#9](https://github.com/pooya-labs/aetherflow/pull/9), [#11](https://github.com/pooya-labs/aetherflow/pull/11), [#12](https://github.com/pooya-labs/aetherflow/pull/12), and [#13](https://github.com/pooya-labs/aetherflow/pull/13) are merged, along with benchmark-comparator and later maintenance PRs [#14–#19](https://github.com/pooya-labs/aetherflow/pulls?q=is%3Apr+is%3Amerged). PR #13 is commit [71247b4](https://github.com/pooya-labs/aetherflow/commit/71247b401c3ed9cf3436ed1ff7908c18dc9cce6e); its exact-merge Rust, PostgreSQL/JetStream, security, and Pages checks all passed. PR #14 merged as [5caa66a](https://github.com/pooya-labs/aetherflow/commit/5caa66a045720c01fbb0f7d75d318cac831112ad). Its final PR head [5f18be1](https://github.com/pooya-labs/aetherflow/commit/5f18be1058ee3922151ef1e3e737c7790c901ebd) passed [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/42636921259), [PostgreSQL/JetStream](https://github.com/pooya-labs/aetherflow/actions/runs/42636921218) (31/31 steps), and [security/supply-chain checks](https://github.com/pooya-labs/aetherflow/actions/runs/42636921275). The comparator suite passed **16/16 tests**, covering NaN/infinity/invalid values, duplicate or missing shapes, event reconciliation, exact threshold boundaries, and valid workload-mismatch skips. Fresh exact-merge-SHA main checks on `5caa66a045720c01fbb0f7d75d318cac831112ad` all passed: [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/42639651230), [PostgreSQL/JetStream](https://github.com/pooya-labs/aetherflow/actions/runs/42639651361) (**31/31 steps**), [security](https://github.com/pooya-labs/aetherflow/actions/runs/42639651201), and [Pages deployment](https://github.com/pooya-labs/aetherflow/actions/runs/42639649167). An earlier documentation-only main head was `00dc8706871712209c0edc746fd540cdc6ad9c85`; its [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/42640422805), [supply-chain/secret checks](https://github.com/pooya-labs/aetherflow/actions/runs/42640422824), and [Pages deployment](https://github.com/pooya-labs/aetherflow/actions/runs/42640419606) also passed. The latest Rust run completed formatting, JavaScript and runbook validation, Clippy, workspace tests/build, benchmark smoke, WASM lint/build and ABI example, docs, Compose, and the deployed Cloudflare Free smoke test. This comparator hardening does not close the real matched-runner benchmark gate: the five-repeat hosted sample must still match the stored EPYC 9V45 fingerprint before numeric thresholds can be applied. The overlapping Dependabot PRs [#5](https://github.com/pooya-labs/aetherflow/pull/5), [#6](https://github.com/pooya-labs/aetherflow/pull/6), and [#7](https://github.com/pooya-labs/aetherflow/pull/7) were closed as superseded by #13. CI also rebases generated lockfile/rustfmt commits before pushing ([5c7fec2](https://github.com/pooya-labs/aetherflow/commit/5c7fec23ba8ec0cf62032535c44d870f1e85e032)) and preserves in-flight main verification runs ([5f18821](https://github.com/pooya-labs/aetherflow/commit/5f18821dd1602c2d4e536938949de0ba8c38d6d9), [788c77f](https://github.com/pooya-labs/aetherflow/commit/788c77f2d5fc7d18481120180d73da72b3372a2b)). See [ROADMAP.md](ROADMAP.md) for the exact checkpoint and remaining release gates.
>
> **Latest maintenance checkpoint — 2026-11-09:** PRs [#15](https://github.com/pooya-labs/aetherflow/pull/15), [#16](https://github.com/pooya-labs/aetherflow/pull/16), [#17](https://github.com/pooya-labs/aetherflow/pull/17), [#18](https://github.com/pooya-labs/aetherflow/pull/18), and [#19](https://github.com/pooya-labs/aetherflow/pull/19) are merged. PR #18 adds seeded lifecycle-model walks across 1,024 seeds × 64 transitions for both Workflow and Task; its PR-head Rust/security checks and exact-merge Rust/security/Pages checks passed. PR #19 applies a hard 2 MiB cap to streamed CLI control-plane responses; its exact PR-head Rust CI and security checks passed. Current main is [5cc44c1](https://github.com/pooya-labs/aetherflow/commit/5cc44c1a7c91f0fd330a2055cc2e900b2d58a7c6); post-merge Pages passed, while exact-SHA Rust CI and security checks were still running at this update. M08 remains open, and v1.0.0 is not released.

> The no-Origin Cloudflare Free profile is deployed and executes only its bounded built-in task set. The owner-deployed Apps Script profile persists synthetic control-plane records in a private Spreadsheet but does not execute tasks. Its checked-in source keeps new storage-only workflow and task rows pending; the live Apps Script deployment still needs the owner to copy the checked-in apps-script/Code.gs and publish a new Web App version. Retain the existing Spreadsheet and /exec URL; do not rerun setup.
>
> The documentation checkpoint [5874f6b](https://github.com/pooya-labs/aetherflow/commit/5874f6b99f880296cb23c043e879114b9f114ff8) is historical; the newer exact-main verification on `00dc8706871712209c0edc746fd540cdc6ad9c85` is fully green as detailed above.
>
> Owner-attribution PR #9 has been merged. The official Pooya Labs monogram and owner links are now on main, the product site and dashboard. All 12 Chromium desktop/mobile checks passed on the PR, and the post-merge exact-SHA browser run also passed.
>
> The stable v1.0.0 release has not been published. Production Rust ingress/gateway routing, database recovery objectives/PITR, production NATS server-side policy, approved SLOs, matching-runner benchmark regression verification, independent security review, and release provenance remain open. AetherFlow is an engineering project and is not production-ready.
> **Live overview:** [Open the AetherFlow GitHub Pages site](https://pooya-labs.github.io/aetherflow/) · [Open the dashboard](https://pooya-labs.github.io/aetherflow/dashboard/) · [Open the Apps Script demo](https://script.google.com/macros/s/AKfycby-RppnSarisz26pfexeZLjd-LD_FTmE8q6Ea3JtFLkl4iAfuyCCLX61p92nNZLlWyr/exec).

The public dashboard URL now connects directly to the free Cloudflare runtime (`workers.dev` + D1 + Workflows); it no longer asks visitors to supply an API Origin or Bearer token. Opening the same dashboard through the Apps Script Web App switches to the `google.script.run` bridge and private Spreadsheet profile. The dashboard's **Tenant** scope still includes an operational snapshot for the Rust/PostgreSQL runtime only. It calls `GET /api/v1/tenant/metrics` only with a tenant credential; the card stays hidden in operator scope. Completion latency runs from workflow acceptance to terminal state, including queue and scheduling time. The public console can submit one bounded sample DAG on the Cloudflare profile and inspect the resulting workflow; it does not accept arbitrary user code. Credentials remain in memory, refresh is manual, and the dashboard does not poll in the background. Browser QA [42510150467](https://github.com/pooya-labs/aetherflow/actions/runs/42510150467) passed all eleven desktop/mobile tests, including the Cloudflare sample-workflow interaction; the v10 PostgreSQL/JetStream and restore workflow [42494177393](https://github.com/pooya-labs/aetherflow/actions/runs/42494177393) passed 28/28 steps.

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
| Dashboard | Static read-only browser console (implemented) |
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
| GET | /api/v1/tenant/metrics | Export tenant-scoped workflow and queue health gauges |
| GET | /api/v1/tenant/workflow-runs | List the authenticated tenant's workflow runs |
| POST | /api/v1/tenant/workflow-runs | Submit a workflow for the authenticated tenant |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id} | Read a tenant-owned workflow |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id}/events | Read redacted events for a tenant-owned workflow |
| GET | /api/v1/tenant/task-runs/{task_run_id} | Read a tenant-owned task |
| POST | /api/v1/tenant/task-runs/{task_run_id}/cancel | Cancel a tenant-owned task with an expected_version guard |

The tenant-only `GET /api/v1/tenant/metrics` endpoint returns current workflow/queue/lease gauges plus terminal workflow outcome counts and successful completion-latency p50/p95/p99 over a rolling 30-minute window. Completion latency is measured from workflow acceptance (`started_at`) to terminal state (`finished_at`). The read-only, repeatable-read snapshot derives tenant scope from the validated credential, respects FORCE RLS, and exports only fixed status/state labels; it is not a platform-wide metric or a complete workflow SLO.

The contract and its compatibility/security decisions are recorded in [ADR-0010](docs/decisions/0010-versioned-control-plane-api.md). Task input is base64 encoded and limited to 1 MiB after decoding; the HTTP request body is capped at 2 MiB. Inspection responses never return task input or output bytes. Reusing a workflow-run ID returns a conflict; the v1 API does not promise idempotent replay.

**Security boundary:** `AETHERFLOW_API_TOKEN` protects platform/operator compatibility routes; tenant routes authenticate separately against stored tenant credentials and derive tenant identity server-side. `AETHERFLOW_MIGRATION_DATABASE_URL` is the DDL/migration owner pool; `DATABASE_URL` must be a restricted non-owner runtime role with no SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, protected-table ownership, public-schema CREATE, REPLICATION, or membership in any other PostgreSQL role. Startup verifies this role and ENABLE + FORCE RLS on all ten protected tables. PostgreSQL uses Rustls with native CA roots and requires TLS by default. The HTTP listener defaults to 127.0.0.1:8080; plaintext HTTP is loopback-only, and non-loopback binds require `AETHERFLOW_TLS_CERT_PATH`, `AETHERFLOW_TLS_KEY_PATH`, and a private `AETHERFLOW_INGRESS_TOKEN`. When configured, every non-health route also requires the matching edge-injected `X-AetherFlow-Ingress-Token`; the API refuses a public bind without this secret. PostgreSQL TLS chain and hostname verification passed hosted integration tests against a live PostgreSQL 18 fixture (run [41609705469](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41609705469)). Transaction-local RLS GUCs are app-controlled context and do not protect against arbitrary SQL execution or SQL injection. Browser access is disabled by default. Set `AETHERFLOW_CORS_ALLOWED_ORIGINS` to a comma-separated list of exact HTTP(S) origins (for example, `https://pooya-labs.github.io`); wildcard origins, paths, cookies, and credentialed CORS are not supported.

## Product website and browser console

The public [AetherFlow product site](https://pooya-labs.github.io/aetherflow/) documents the v1 control-plane contract, architecture, recovery evidence, developer setup and release status. The [browser console](https://pooya-labs.github.io/aetherflow/dashboard/) offers distinct profiles rather than one hosted Rust backend: a direct Rust API connection is read-only; the Cloudflare Free profile can submit one bounded sample DAG using its built-in task handlers; and the Apps Script profile can store a synthetic workflow/task record in the owner's private Spreadsheet without executing the task. The selected-run inspection panel remains read-only. No profile accepts arbitrary user code. Bearer credentials are held in page memory only and are not persisted to local storage, cookies or the URL. Use HTTPS for remote APIs.

To connect a hosted API, configure `AETHERFLOW_CORS_ALLOWED_ORIGINS=https://pooya-labs.github.io` on the API server and restart it. The origin must match exactly. Operator listing is intentionally limited to the reserved `legacy-default` tenant; tenant credentials list only their own runs. Event responses never include raw outbox payloads.

## Cloudflare ingress gateway

The repository includes a Cloudflare Worker gateway under [`cloudflare-gateway/`](cloudflare-gateway/README.md). It applies a keyed, per-client token bucket using SQLite-backed Durable Objects, strips caller-supplied forwarding and identity headers, preserves the API's operator/tenant Bearer credential, injects a private edge-to-origin token, and fails closed when its limiter or origin configuration is unavailable. Node policy tests are part of Rust CI.

The Worker is **not deployed or routed yet**. The linked Cloudflare account currently has no active Zone or Tunnel and no AetherFlow API origin. Its configuration disables workers.dev and preview routes. Production ingress remains blocked until a verified HTTPS origin, matching Worker/API secret, custom hostname or tested Tunnel route, origin network restriction, and direct-bypass acceptance test exist.

## Operations runbooks

Operational procedures cover coordinated PostgreSQL/JetStream backup and restore, controlled schema upgrades, and a production deployment acceptance gate. The acceptance template requires explicit gateway/origin, NATS security, recovery and monitoring evidence. The backup procedure is a logical recovery drill, not a production PITR service; an owner-approved RPO/RTO and tested production WAL-archive or managed-database policy are still required.

- [Backup and restore runbook](docs/operations/backup-restore.md)
- [PostgreSQL schema upgrade runbook](docs/operations/schema-upgrades.md)
- [Operational guide](docs/operations/production-guide.md)
- [Production deployment acceptance checklist](docs/operations/production-acceptance.md)
- [SLO proposal (draft)](docs/operations/slo-proposal.md)
- [Version 1.0.0 release readiness checklist](docs/releases/v1-readiness.md)
- [Security threat model](docs/security/threat-model.md)

## Operations telemetry

The server emits newline-delimited JSON logs. Set `RUST_LOG` to adjust the `tracing` filter; the default is `info`. HTTP requests create request-scoped tracing spans with a process-local request ID, method, response status, latency, and a W3C trace ID. Valid `traceparent`/`tracestate` headers are extracted from incoming requests; the server returns its own span context in response trace headers. Browser preflight allows only the explicit `authorization`, `content-type`, `traceparent`, and `tracestate` request headers, and exposes only the two trace response headers. Invalid trace context is ignored. Baggage, arbitrary request headers, URIs, query strings, request/response bodies, Bearer credentials, task payloads, and database URLs are not attached to spans or propagated.

`GET /api/v1/metrics` exposes aggregate HTTP request, in-flight, 5xx, and abandoned-handler-future counters, plus bounded latency histograms partitioned by fixed operation classes (health, metrics, operator/tenant reads, submissions, mutations, and other). Per-operation response totals are split into the fixed 2xx, 3xx, 4xx, 5xx, and other classes. The in-flight gauge is released even when a handler future is dropped. Metrics are protected by the platform operator Bearer credential and remain unavailable in health-only mode; labels never contain raw paths, tenant/workflow/task identifiers, or credential values.

`GET /api/v1/tenant/metrics` exposes a point-in-time, tenant-scoped view of workflow counts, active-workflow age, unpublished/ready outbox events, expired outbox claims, pending/processing inbox events and age, and expired inbox/task leases. It is authenticated with a tenant credential and cannot inspect other tenants. The endpoint intentionally returns aggregates only and sets `Cache-Control: no-store`; it does not provide completion percentiles, outcome attribution, cluster-wide totals, or an approved SLA.

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

## Browser quality checks

The product website and read-only console have a real Chromium acceptance suite. It exercises desktop and mobile layouts, navigation anchors, responsive overflow, theme persistence, clipboard actions, HTTPS-only remote API validation, tenant/operator route separation, workflow and event pagination, stale-response protection when switching workflows, in-flight request cancellation on disconnect, mocked workflow/task/event reads, and the session-only Bearer credential boundary. Tests use a local static server and a mocked control-plane endpoint; they do not require or claim a hosted production API.

Run the suite locally after installing the pinned browser dependency and Chromium:

~~~bash
python3 -m venv .venv
.venv/bin/python -m pip install -r scripts/browser-qa-requirements.txt
.venv/bin/python -m playwright install chromium
.venv/bin/python scripts/browser_qa.py
~~~

GitHub Actions archives desktop/mobile screenshots for review. Passing browser QA validates the Pages interface and mocked API contract only; production authentication, CORS, and a live deployment still require an operator-owned API environment.

Latest exact-source Browser QA: commit `645a747643497263ab031133e44745805d95694c` passed all nine Chromium desktop/mobile tests in 10.154 seconds ([run 42488524145](https://github.com/pooya-labs/aetherflow/actions/runs/42488524145)). GitHub Pages deployment [42488523321](https://github.com/pooya-labs/aetherflow/actions/runs/42488523321) and supply-chain/secret checks [42488524104](https://github.com/pooya-labs/aetherflow/actions/runs/42488524104) also passed on that SHA. Rust CI [42488524137](https://github.com/pooya-labs/aetherflow/actions/runs/42488524137) was still running when this entry was recorded. Browser QA uses mocked API responses; it verifies client behavior, not a hosted production backend.

## Reproducible scheduler benchmarks

The repository includes two complementary performance tools: an in-memory scheduler/DAG benchmark and a PostgreSQL 18 + NATS 2.12 end-to-end load profile covering the durable worker, outbox, JetStream, and inbox path. Both report CSV metrics and run in CI. The end-to-end guide records three five-repeat PostgreSQL/NATS sessions and a matched-fingerprint reference; the CI comparator applies conservative limits only to the normalized EPYC 9V45 fingerprint. The baseline is not a production capacity claim. See the [in-memory benchmark guide](docs/performance/benchmarks.md) and the [end-to-end PostgreSQL/JetStream load guide](docs/performance/end-to-end-load.md).

## Repository map

The repository is being built incrementally. See [ROADMAP.md](ROADMAP.md) for the authoritative checklist and current checkpoint, [docs/architecture/overview.md](docs/architecture/overview.md) for system boundaries, [docs/decisions/](docs/decisions/) for architecture decisions, and [docs/development.md](docs/development.md) for local commands.

## Development status

M00–M07 are complete. M08 includes hosted-verified observability, the local PostgreSQL 18 / three-node NATS 2.12 Compose environment, runbook drift checks, isolated PostgreSQL/JetStream restore-to-runtime execution, API restart recovery, in-flight worker completion during API outage, and a full three-member JetStream outage/recovery drill. The complete-cluster drill is covered by `jetstream_full_cluster_outage_retains_and_replays_outbox_event_once`: it stops all three CI NATS members, confirms the outbox event remains unpublished/retryable in PostgreSQL, restores the cluster, then verifies one inbox processing and zero duplicates. The exact-source PostgreSQL/NATS CI [42211375628](https://github.com/pooya-labs/aetherflow/actions/runs/42211375628), Rust CI [42211375893](https://github.com/pooya-labs/aetherflow/actions/runs/42211375893), security [42211375664](https://github.com/pooya-labs/aetherflow/actions/runs/42211375664), and Pages [42211374941](https://github.com/pooya-labs/aetherflow/actions/runs/42211374941) all passed on `c5543d96913fbef356fc7856486feb43ea52c71d`. The same PostgreSQL/NATS run also re-verified the restore-to-runtime and API-outage scenarios: the worker finished its in-flight task while the API was offline; five target outbox events reached the inbox with zero duplicates.

Five five-repeat PostgreSQL/NATS sessions are archived. The two matched AMD EPYC 9V45 sessions recorded median fan-out/chain throughput of 7.319/7.195 and 7.415/7.298 workflows/s respectively, with a one-page visible-memory metadata difference documented in the report. The other three observations remain separate: two EPYC 7763 sessions and one EPYC 9V74 session. The latest EPYC 9V74 session recorded 6.173 fan-out and 5.893 chain workflows/s; its CPU fingerprint did not match the stored reference, so CI correctly skipped threshold application. Every profile reconciled 896 outbox/inbox events per shape with zero duplicates/errors. See the [baseline report](docs/performance/end-to-end-load.md). The CI comparator applies 10% throughput-drop and 15% p95-increase limits only when the complete normalized runner/workload fingerprint matches; unmatched fingerprints are archived and skipped. These hosted measurements are not production capacity or an SLO claim.

The tenant routes derive tenant identity from tenant credentials and enforce Read/Submit/Cancel role permissions. PostgreSQL tenant data has forced RLS on ten tables. Migrations and application queries use separate database roles; startup rejects elevated runtime-role attributes, role memberships, protected-table ownership, or missing FORCE RLS. Plain HTTP is loopback-only; non-loopback binds require a valid Rustls certificate/key pair.

Security and operations documentation describes the implemented boundaries and remaining risks. Authenticated operator/tenant-principal requests use bounded process-local token buckets (burst 30, refill 2 requests/second by default), with 429/Retry-After and an aggregate metric. A separate bounded failed-authentication limiter covers missing/invalid operator and tenant credentials using the listener's immediate TCP peer, ignores forwarded-IP headers and clears the bucket after successful authentication. This secondary limiter is hosted-verified, but it is process-local and does not create a shared quota across replicas. The required shared ingress/API-gateway policy (actual client-source throttling before forwarding, stripped untrusted forwarding headers and no direct-origin bypass) is documented in the production acceptance checklist but has not been deployed or verified. No independent production security audit has been completed. Distributed quotas/edge policy, per-person operator identity and durable audit retention, managed secret rotation, production NATS authentication/TLS, production PITR/RPO/RTO, horizontal-scaling evidence and complete workflow SLO instrumentation remain open.

The project code is licensed under Apache-2.0; the root LICENSE and Cargo workspace/package SPDX metadata are in place. Third-party dependency and separately supplied asset license review, plus SBOM/provenance generation, remain release-packaging work. M02 property-based lifecycle tests remain deferred, and WASM blocking work is bounded by fuel/epoch deadlines rather than force-aborted. The service remains experimental; see [ROADMAP.md](ROADMAP.md) for the authoritative remaining checklist.

## License

AetherFlow is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](LICENSE) for the complete terms. Every Rust package in this workspace inherits the Apache-2.0 SPDX identifier from the root workspace metadata.

The license does not grant permission to use the AetherFlow or Pooya Labs names, logos, or trademarks. Third-party dependencies and separately supplied assets remain subject to their own license terms.

## Persistence status

The PostgreSQL storage crate and initial schema have passed PostgreSQL 18 CI. Durable handler failure recording, task cancellation/timeout, workflow terminal aggregation, and outbox claim/retry/ack are implemented and integration-tested. Workflow aggregation succeeds only when all tasks succeed; on a terminal failure, cancellation, or timeout it cancels remaining non-terminal tasks and closes active attempts transactionally. The `aetherflow-transport-nats` crate integrates a real JetStream publisher and inbox-backed pull consumer, and now exposes `NatsConnectionConfig` for NATS `.creds` authentication, TLS enforcement, custom CA roots and optional client mTLS. PostgreSQL 18 + NATS CI verifies the broker-accepted/outbox-ack retry boundary, stable message-ID deduplication, logical consumer deduplication, lease and AckWait heartbeat for long handlers, failed-handler redelivery, single-member loss, and a full three-member outage with PostgreSQL outbox retention and exactly-one inbox processing after recovery. The durable worker/coordinator remains an opt-in PostgreSQL execution path. OS-level process termination after broker storage and after inbox commit is now integration-tested on PostgreSQL 18 + NATS (run `41380067612`). Delivery remains at-least-once; external side effects require application-level idempotency or a transaction shared with inbox completion. M05 membership-gated task execution is verified on PostgreSQL 18 + NATS (41402000557) with Rust CI (41402000570). Tests cover stale-epoch write rejection, replacement-worker recovery after task-lease expiry, registry capability filtering, lease renewal while gracefully draining a long-running handler, UID-scoped packet-level PostgreSQL partition/rejoin, abrupt worker-process loss, three-member NATS quorum recovery, and the combined two-round chaos campaign. This is a verified durable execution vertical slice—not a production-ready distributed workflow engine. See [the PostgreSQL transaction contract](docs/persistence/postgres-transaction-contract.md).


---

## Latest project update — 9 November 2026

The free Cloudflare runtime is deployed separately from the Rust/PostgreSQL ingress path: the public dashboard connects to Workers, D1 (WEUR) and Cloudflare Workflows without asking visitors for an API Origin or Bearer token. A live REST smoke test completed the built-in echo workflow and persisted its terminal state and events. The owner-provided [Apps Script Web App](https://script.google.com/macros/s/AKfycby-RppnSarisz26pfexeZLjd-LD_FTmE8q6Ea3JtFLkl4iAfuyCCLX61p92nNZLlWyr/exec) passed both the live read-path check and a real Spreadsheet write/read round-trip: the gas-smoke-61f2c9e5308d4978ae69d39ddf6bf5d6 record remained visible after a full-page reload. The selected-run panel is intentionally read-only, and the Apps Script profile stores synthetic control-plane records without executing tasks. The repository now keeps new Apps Script-only workflow and task rows pending; the owner must still copy the checked-in apps-script/Code.gs into the existing Apps Script project and publish a new Web App version to activate that correction. Retain the existing Spreadsheet and /exec URL; do not rerun setup. Owner-attribution PR #9 has been merged with the official Pooya Labs monogram and owner links. On merge commit c2a385677208845c3aaa81387857adb19a150167, Rust CI, supply-chain/secret checks, Chromium browser QA and Pages deployment all passed. AetherFlow remains experimental and is not production-ready; the Rust ingress gateway, managed recovery objectives, production NATS server-side identity/ACL policy, approved SLOs, matching-runner benchmark comparison and independent security review remain open.

## Project ownership

<p align="center"><a href="https://pooya-labs.github.io/pooya-labs-platform/"><img src="https://raw.githubusercontent.com/pooya-labs/pooya-labs-platform/main/apps/platform/public/brand/pooya-monogram-tile.svg" alt="Pooya Labs Platform monogram" width="96" /></a></p>

**Owned and maintained by [Pooya Labs Platform](https://pooya-labs.github.io/pooya-labs-platform/).** Project direction and engineering: **Pooya Goodarzi**.

## راهنمای فارسی

### معرفی

**AetherFlow** یک پلتفرم متن‌بازِ در حال توسعه برای اجرای گردش‌کارهای توزیع‌شده و بادوام است که هستهٔ آن با Rust ساخته می‌شود. هدف پروژه، مدیریت صریح وضعیت وظایف، بازیابی پس از خرابی، هماهنگی Workerها و مشاهده‌پذیری عملیات است.

### اهداف طراحی

- نگهداری بادوام وضعیت گردش‌کار و وظایف همراه با انتقال وضعیت قابل ممیزی.
- تحویل حداقل یک‌باره (At-least-once) همراه با ابزارهای اجرای idempotent.
- بازیابی Workerها با lease، heartbeat و retry محدود.
- تعریف روشن رفتار لغو، timeout، هم‌زمانی و backpressure.
- هستهٔ ماژولار Rust با اجرای ناهمگام.
- مرز توسعهٔ افزونه‌های WebAssembly.
- ثبت رویدادهای ساخت‌یافته، متریک‌ها و traceها.
- آزمون‌های تکرارپذیر برای سناریوهای خرابی و بازیابی.

این پروژه ادعای اجرای عمومی و تضمین‌شدهٔ «دقیقاً یک‌بار» ندارد. اثرهای جانبی خارجی به idempotency در سطح برنامه یا یکپارچگی تراکنشی صریح نیاز دارند.

### اجزای فنی

هسته با Rust و Tokio توسعه می‌یابد. لایهٔ کنترل HTTP بر پایهٔ Axum، ذخیره‌سازی بادوام با PostgreSQL، انتقال رویداد با NATS JetStream، مرز افزونه‌ها با Wasmtime و مشاهده‌پذیری با OpenTelemetry طراحی شده‌اند. قرارداد API نسخهٔ ۱ و مشخصات OpenAPI 3.1 در مسیر `docs/api/openapi.yaml` قرار دارد.

### وب‌سایت و کنسول مرورگری

وب‌سایت عمومی پروژه مستندات معماری، قرارداد کنترل‌پلین، شواهد بازیابی، راهنمای توسعه و وضعیت انتشار را ارائه می‌کند. کنسول مرورگری سه مسیر جداگانه دارد: اتصال مستقیم به API اصلی Rust برای مشاهدهٔ گردش‌کارها و رویدادها به‌صورت فقط‌خواندنی؛ مسیر رایگان Cloudflare برای ارسال یک گردش‌کار نمونه با مجموعه‌ای محدود از وظایف داخلی؛ و مسیر Apps Script برای ثبت یک نمونهٔ مصنوعی در Spreadsheet خصوصی مالک، بدون اجرای وظیفه. پنل بررسی اجرای انتخاب‌شده فقط‌خواندنی است و هیچ‌یک از این مسیرها کد دلخواه کاربر را اجرا نمی‌کند. اعتبارنامهٔ Bearer فقط در حافظهٔ همان صفحه نگهداری می‌شود و در حافظهٔ پایدار مرورگر، کوکی یا نشانی صفحه ذخیره نمی‌شود.

### وضعیت فعلی توسعه

مراحل M00 تا M07 تکمیل شده‌اند و M08، شامل مهندسی تولید، عملیات و بازیابی، همچنان در حال پیشرفت است. مسیر رایگان Cloudflare در پوشهٔ `cloudflare-free/` مستقر شده و شامل Workers، D1 در WEUR و اتصال Cloudflare Workflows است. داشبورد عمومی این runtime را به‌طور خودکار راه‌اندازی می‌کند و برای اجرای نمونهٔ محدود، نیازی به واردکردن API Origin یا Bearer token ندارد.

این runtime رایگان را نباید با gateway ورودی Rust در پوشهٔ cloudflare-gateway/ یکی دانست؛ gateway مبتنی بر Rust هنوز به یک API Origin واقعی متصل و در مسیر تولید مستقر نشده است. وب‌اپ [Apps Script](https://script.google.com/macros/s/AKfycby-RppnSarisz26pfexeZLjd-LD_FTmE8q6Ea3JtFLkl4iAfuyCCLX61p92nNZLlWyr/exec) آزمون زندهٔ خواندن و همچنین نوشتن و بازیابی واقعی از Spreadsheet خصوصی را گذرانده است؛ رکورد gas-smoke-61f2c9e5308d4978ae69d39ddf6bf5d6 پس از بارگذاری مجدد صفحه باقی مانده است. پنل مشاهده عمداً فقط‌خواندنی است و این پروفایل وظایف را اجرا نمی‌کند. اصلاح منبع برای نگه‌داشتن گردش‌کارها و وظایف جدید در وضعیت pending در مخزن ثبت شده، اما هنوز باید فایل apps-script/Code.gs در پروژهٔ Apps Script موجود جایگزین و نسخهٔ جدید وب‌اپ منتشر شود. Spreadsheet و آدرس فعلی /exec را حفظ کن و راه‌اندازی اولیه را دوباره اجرا نکن. آزمون زندهٔ گردش‌کار echo و ذخیرهٔ رویدادها در D1 نیز موفق بوده است.

با این حال پروژه هنوز آزمایشی است. مدیریت بازیابی تولیدی، امنیت و ACL سمت سرور NATS، SLOهای کامل، استقرار gateway Rust و بازبینی امنیتی مستقل باقی مانده‌اند؛ بنابراین AetherFlow را آمادهٔ استفادهٔ عملیاتی عمومی معرفی نکنید.

### ابزارهای توسعه

مخزن شامل API نسخه‌دار، CLI، SDK ناهمگام Rust، کنسول مرورگری، آزمون‌های Chromium و ابزارهای benchmark است. تحویل پیام حداقل یک‌باره است؛ برای جلوگیری از تکرار اثرهای جانبی خارجی باید idempotency در سطح برنامه رعایت شود.

### اجرای پروژه

برای فرمان‌های دقیق نصب و اجرای سرویس‌ها، راهنمای توسعه در `docs/development.md`، راهنمای CLI در `apps/aetherflow-cli/README.md` و راهنمای SDK در `crates/aetherflow-sdk/README.md` را ببینید. تنظیمات PostgreSQL، NATS، آزمون مرورگر و runbookهای عملیاتی نیز در مستندات مرتبط پوشهٔ `docs/` آمده‌اند.

### مجوز

AetherFlow تحت مجوز Apache License 2.0 منتشر می‌شود. مجوز نرم‌افزار به‌تنهایی اجازهٔ استفاده از نام‌ها، نشان‌ها یا علائم تجاری AetherFlow و Pooya Labs را نمی‌دهد.

### آخرین تغییرات — ۹ نوامبر ۲۰۲۶

محیط رایگان Cloudflare مستقل از مسیر ورودی Rust/PostgreSQL مستقر شده است: داشبورد عمومی مستقیماً به Workers، پایگاه دادهٔ D1 در منطقهٔ WEUR و Cloudflare Workflows متصل می‌شود و برای اجرای نمونهٔ محدود به API Origin یا توکن Bearer نیاز ندارد. آزمون زندهٔ REST یک گردش‌کار echo را اجرا و وضعیت نهایی و رویدادهای آن را در D1 ذخیره کرد. وب‌اپ [Apps Script](https://script.google.com/macros/s/AKfycby-RppnSarisz26pfexeZLjd-LD_FTmE8q6Ea3JtFLkl4iAfuyCCLX61p92nNZLlWyr/exec) نیز آزمون واقعی نوشتن و بازیابی را گذرانده است: رکورد gas-smoke-61f2c9e5308d4978ae69d39ddf6bf5d6 پس از بارگذاری مجدد صفحه همچنان باقی مانده است. آزمون Chromium روی نسخهٔ ادغام‌شده هر ۱۲ سناریوی مرورگر را با موفقیت گذرانده است. اصلاح منبع برای نگه‌داشتن گردش‌کارهای ذخیره‌سازی‌محور در وضعیت pending آماده است، اما فعال‌سازی آن هنوز به جایگزینی Code.gs و انتشار نسخهٔ جدید وب‌اپ توسط مالک نیاز دارد. نشان رسمی و پیوند مالکیت Pooya Labs نیز با PR شمارهٔ ۹ به شاخهٔ اصلی، سایت و داشبورد اضافه شده‌اند.

### مالکیت و نگه‌داری

<p align="center"><a href="https://pooya-labs.github.io/pooya-labs-platform/"><img src="https://raw.githubusercontent.com/pooya-labs/pooya-labs-platform/main/apps/platform/public/brand/pooya-monogram-tile.svg" alt="نشان مونوگرام پویا لبز" width="96" /></a></p>

**مالک و نگه‌دارنده: [پلتفرم پویا لبز](https://pooya-labs.github.io/pooya-labs-platform/)** · طراحی و توسعه: **پویا گودرزی**.
