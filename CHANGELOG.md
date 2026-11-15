# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned for 1.0.0
- Bump publishable workspace crates (`aetherflow-core`, `aetherflow-runtime`, `aetherflow-storage-postgres`, `aetherflow-transport-nats`, `aetherflow-wasm`, `aetherflow-sdk`, `aetherflow-server`, `aetherflow-cli`) from `0.1.0` to `1.0.0`.
- Release provenance, software bill of materials (SBOM), and SHA-256 release artifact checksums.
- Official `v1.0.0` GitHub Release publication following verified operational deployment sign-offs.

## [0.1.0] - 2026-11-15

Initial baseline implementation encompassing Milestones M00 through M08, establishing the core orchestration engine, durable PostgreSQL storage, NATS JetStream transport, WebAssembly plugin runtime, OpenAPI control plane, typed Rust SDK, control-plane CLI, and serverless runtime profiles.

### Added

#### Core & Runtime
- **Execution Lifecycles:** Deterministic state machines for `WorkflowStatus` and `TaskStatus` with combinatorial transition matrices, depth-5 bounded sequence graphs, and 1,024-seed × 64-step model walks ([ADR-0002](docs/decisions/0002-execution-lifecycle.md), [ADR-0013](docs/decisions/0013-lifecycle-model-testing-and-property-generation.md)).
- **DAG Orchestration:** Dependency graph evaluation with cycle detection, concurrency-controlled task scheduling, and atomic sibling task cancellation on failure ([ADR-0005](docs/decisions/0005-workflow-dag-orchestration.md)).
- **Payload Limits:** Decoded 1 MiB task input ceiling, payload presence metadata, and confidential payload redaction from operational inspection surfaces ([ADR-0006](docs/decisions/0006-task-payload-boundaries.md)).

#### Storage & Multi-Tenancy (PostgreSQL 18)
- **Durable Persistence:** Migrations `0001` through `0010` supporting workflows, tasks, attempts, outbox events, consumer inbox, node membership, and tenant credentials ([ADR-0007](docs/decisions/0007-postgresql-durable-state.md)).
- **Multi-Tenant Security:** Forced Row Level Security (`FORCE ROW LEVEL SECURITY`) across all ten tenant-owned tables with least-privilege runtime role enforcement ([ADR-0012](docs/decisions/0012-tenant-ownership-and-row-level-authorization.md)).
- **Lease Fencing & Epochs:** Heartbeat renewal, expired-lease recovery, and monotonic `fencing_token` guards mitigating split-brain executions across worker incarnations ([ADR-0008](docs/decisions/0008-node-membership-and-worker-ownership.md)).
- **Transactional Outbox & Inbox:** Atomic database mutations coupled with `outbox_events` and deduplicated `consumer_inbox` table ensuring at-least-once processing without duplicate logical side effects.

#### Messaging & Transport (NATS 2.12 JetStream)
- **JetStream Outbox Publisher:** Reliable event publishing with broker acknowledgements and backoff retries.
- **Durable Pull Consumer:** Fenced pull consumer integrated with the PostgreSQL transactional inbox.
- **Client Security:** `NatsConnectionConfig` supporting `.creds` authentication (user JWT and NKey seed), TLS enforcement, custom CA bundles, and mutual TLS (mTLS) with hostname verification.
- **Cluster Outage Recovery:** Resilient reconnect and durable event replay across full 3-node NATS cluster outages.

#### WebAssembly Extensibility (Component Model)
- **Plugin ABI:** Pinned `aetherflow:plugin@1.0.0` WIT contract for sandboxed task execution ([ADR-0009](docs/decisions/0009-versioned-wasm-plugin-abi.md)).
- **Sandbox Security:** Wasmtime integration with epoch interruption deadlines, fuel metering, memory/table growth ceilings, and default-deny host capability model (granting only bounded `log.write`).

#### Control Plane, Observability & Clients
- **HTTP Server (`aetherflow-server`):** OpenAPI 3.1 router with Axum, supporting `/api/v1` operator and `/api/v1/tenant/*` tenant routes ([ADR-0010](docs/decisions/0010-versioned-control-plane-api.md)).
- **Authentication & Rate Limiting:** High-entropy Bearer token authentication, process-local token buckets for authenticated principals, and TCP peer failed-auth throttling ([ADR-0011](docs/decisions/0011-control-plane-operator-auth-and-database-tls.md)).
- **Edge Ingress Protection:** 2 MiB request body limits, constant-time `X-AetherFlow-Ingress-Token` header verification for non-loopback binds, and separate `cloudflare-gateway/` reverse proxy with Durable Object rate limiting.
- **Telemetry & Metrics:** Prometheus metrics exposition for HTTP operation classes, NATS connection lifecycles, tenant operational metrics, and W3C distributed trace propagation (`traceparent`/`tracestate`).
- **Standalone Worker Daemon (`aetherflow-worker`):** Independent executable process running task workers, outbox dispatchers, and inbox consumers with opt-in authenticated loopback `/metrics`.
- **Typed Rust SDK (`aetherflow-sdk`):** Client library with request bounds, TLS enforcement, redirect blocking, and secret redaction.
- **Control-Plane CLI (`aetherflow-cli`):** HTTP-only command-line interface with loopback-only plaintext, remote HTTPS enforcement, and environment-based credential loading.

#### Serverless & Demo Profiles
- **Cloudflare Free Runtime (`cloudflare-free`):** Cloudflare Workflows + D1 SQLite database + Pages dashboard providing a standalone, zero-cost demonstration profile with bounded built-in tasks and streamed body rejection.
- **Google Apps Script Bridge (`apps-script`):** Bounded low-volume demo adapter using private Google Spreadsheets.
