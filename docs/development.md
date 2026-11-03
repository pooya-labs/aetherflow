# Development Guide

## Prerequisites

Install Rust with [rustup](https://rustup.rs/). The repository pins Rust 1.99.1 in rust-toolchain.toml; the toolchain includes rustfmt and clippy.

For the HTTP control plane, use a local PostgreSQL instance. The server uses Rustls and the operating system's native CA trust store; TLS is required by default. Install any private database CA in the operating system trust store before launching the server. NATS integration tests additionally require a three-node JetStream fixture and Docker. The full chaos test also requires the CI packet-filter prerequisites documented below.

## Standard Rust checks

Run from the repository root:

~~~bash
cargo fetch --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
cargo doc --workspace --no-deps --locked
~~~

The default workspace test command does not require PostgreSQL or NATS. Database-backed tests skip when their documented environment variables are not set.

## Run the HTTP control plane

Create a high-entropy local operator token and use a disposable local database:

~~~bash
export AETHERFLOW_API_TOKEN="$(openssl rand -hex 32)"
export DATABASE_URL='postgres://aetherflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/aetherflow_dev'
cargo run -p aetherflow-server
~~~

When DATABASE_URL is set, AETHERFLOW_API_TOKEN is mandatory and must be 32–256 ASCII letters, digits, hyphens or underscores. The server stores only its SHA-256 digest in memory. Do not commit the token, paste it into issue reports, or enable shell tracing while exporting it. Rotate it by replacing the environment value and restarting the process.

The server applies pending PostgreSQL migrations before it starts listening. By default it binds to 127.0.0.1:8080 and requires PostgreSQL TLS. Rustls validates the server chain using the platform's native CA store and checks the hostname. Install private database CA certificates in that trust store. For a disposable local PostgreSQL instance without TLS only, explicitly set DATABASE_SSL_MODE=disable; never use that mode over a network. The other accepted values are prefer (may fall back to plaintext) and require (default).

~~~bash
curl --fail http://127.0.0.1:8080/health
curl --fail -H "Authorization: Bearer $AETHERFLOW_API_TOKEN" \
  http://127.0.0.1:8080/api/v1/workers
~~~

GET /health is public process liveness only; it does not prove that PostgreSQL is reachable. When DATABASE_URL is absent, the server deliberately starts in health-only mode and durable API routes return 503 database_unavailable.

AETHERFLOW_BIND remains loopback-only. The single Bearer token is one operator credential, not a multi-user authorization system and not tenant isolation. Do not work around the non-loopback guard or expose the server to an untrusted network. HTTPS for the HTTP listener, per-principal authorization and tenant-scoped storage remain future work.

## HTTP v1 resources

The OpenAPI 3.1 source of truth is docs/api/openapi.yaml.

| Method | Route | Behavior |
|---|---|---|
| GET | /health | Process liveness |
| POST | /api/v1/workflow-runs | Submit and durably materialize a workflow-run snapshot |
| GET | /api/v1/workflow-runs/{workflow_run_id} | Read a workflow-run and task status summary |
| GET | /api/v1/task-runs/{task_run_id} | Read task status and payload metadata |
| POST | /api/v1/task-runs/{task_run_id}/cancel | Version-guarded cancellation and workflow aggregation |
| GET | /api/v1/workers | List live active worker registrations |

The submit request carries a caller-supplied unique workflow_run_id, a workflow ID, task IDs/dependencies, a retry policy, and optional base64 task input. Input is limited to 1 MiB after base64 decoding; the HTTP request body is limited to 2 MiB. A reused workflow-run ID returns 409 Conflict; no idempotent replay contract is implemented. Workflow and task inspection never return input/output bytes.

The HTTP process does not launch a worker. Run durable workers separately through the PostgreSQL runtime. Workflow definitions are currently submitted per run and are not persisted as reusable versioned definitions.

## Workspace layout

- apps/aetherflow-server: Axum control-plane process and loopback guard.
- crates/aetherflow-core: infrastructure-independent identities, states, and retry primitives.
- crates/aetherflow-runtime: scheduler, workflow DAG validation, handlers, and worker execution contracts.
- crates/aetherflow-storage-postgres: migrations, durable task/workflow repositories, membership, outbox, and inbox.
- crates/aetherflow-transport-nats: JetStream publisher and inbox-backed pull consumer.
- crates/aetherflow-wasm: WIT ABI and bounded Wasmtime execution.
- examples/minimal-plugin: reproducible guest/host example using the scheduler-backed WASM handler.

## PostgreSQL-backed tests

The HTTP API integration test uses AETHERFLOW_TEST_DATABASE_URL. It migrates the disposable database and exercises workflow submission, duplicate conflict handling, workflow/task reads, not-found responses, payload redaction, stale cancellation, and active worker discovery.

~~~bash
AETHERFLOW_TEST_DATABASE_URL='postgres://aetherflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/aetherflow_api_test' \
  cargo test -p aetherflow-server --locked -- --test-threads=1
~~~

PostgreSQL storage integration tests use the same variable:

~~~bash
AETHERFLOW_TEST_DATABASE_URL='postgres://aetherflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/aetherflow_storage_test' \
  cargo test -p aetherflow-storage-postgres --tests --locked
~~~

Use a fresh, disposable database for each suite. Do not point integration tests at a development or production database.

## NATS and chaos integration tests

The real JetStream outbox/inbox integration test uses:
- AETHERFLOW_TEST_NATS_DATABASE_URL: a dedicated disposable PostgreSQL database.
- AETHERFLOW_TEST_NATS_URL: comma-separated client seed URLs for the three-node NATS cluster.
- AETHERFLOW_TEST_NATS_CONTAINER: the container name used by the member-loss test.
- AETHERFLOW_TEST_PACKET_PARTITION=1: enables the UID-scoped PostgreSQL egress partition campaign in CI.

AETHERFLOW_TEST_NATS_DATABASE_URL must be isolated from every other database. The transport suite deliberately truncates its test outbox at the beginning of a serialized fixture to prevent earlier test cases from contaminating global outbox claims. Never use a non-test database for this variable.

~~~bash
AETHERFLOW_TEST_NATS_DATABASE_URL='postgres://aetherflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/aetherflow_nats_test' \
AETHERFLOW_TEST_NATS_URL='nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224' \
  cargo test -p aetherflow-transport-nats --test jetstream_outbox --locked -- --test-threads=1
~~~

The combined packet-partition/member-loss test is intended for the hosted CI fixture. It requires a three-node Docker cluster, the configured container name, passwordless sudo, the nobody user, and UID-scoped iptables OUTPUT rules. Do not enable packet-partition mode on a machine unless those prerequisites and cleanup behavior are understood.

## WASM plugin example

The reproducible guest/host example lives in examples/minimal-plugin/. It uses the checked-in WIT ABI, pinned wit-bindgen 0.60.0, and wit-component 0.254.2. The guest targets wasm32-unknown-unknown and imports no ambient WASI interface.

~~~bash
rustup +1.99.1 target add wasm32-unknown-unknown
cargo +1.99.1 build --locked -p aetherflow-plugin-guest --target wasm32-unknown-unknown --release
cargo +1.99.1 run --locked -p aetherflow-plugin-host --release -- target/wasm32-unknown-unknown/release/aetherflow_plugin_guest.wasm
~~~

The host harness verifies default-deny behavior, registers WasmTaskHandler in TaskRegistry, and executes it through TaskScheduler. It checks workflow success, retained task output, and one explicitly granted bounded log.write message. Synchronous Wasmtime work runs on Tokio's blocking pool. A running spawn_blocking guest call is not forcibly aborted when its async future is dropped; fuel and wall-clock limits are the resource boundary.

## Project state

M00–M06 are complete. M07's v1 HTTP routes and PostgreSQL integration path are implemented, but the control plane is not production-ready. Per-principal authorization, tenant isolation, HTTPS for the HTTP listener, CLI/SDK, dashboard, and operational hardening remain future work. The current one-operator Bearer gate and Rustls PostgreSQL client are implemented. The master roadmap (../ROADMAP.md) is the source of truth for exact completion and hosted CI status.
