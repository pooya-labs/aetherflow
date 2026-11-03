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
export AETHERFLOW_MIGRATION_DATABASE_URL='postgres://aetherflow_migrator:YOUR_MIGRATION_PASSWORD@127.0.0.1:5432/aetherflow_dev'
export DATABASE_URL='postgres://aetherflow_runtime:YOUR_RUNTIME_PASSWORD@127.0.0.1:5432/aetherflow_dev'
cargo run -p aetherflow-server
~~~

When DATABASE_URL is set, both database URLs and AETHERFLOW_API_TOKEN are mandatory. AETHERFLOW_API_TOKEN must be 32–256 ASCII letters, digits, hyphens or underscores. The server stores only its SHA-256 digest in memory. Do not commit the token, paste it into issue reports, or enable shell tracing while exporting it. Rotate it by replacing the environment value and restarting the process.

`AETHERFLOW_MIGRATION_DATABASE_URL` is used only by the migration/DDL owner pool. `DATABASE_URL` is used only by the API runtime pool and must authenticate as a restricted non-owner role with no SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, protected-table ownership, or membership in any other PostgreSQL role. Startup checks all ten protected tables for both ENABLE and FORCE ROW LEVEL SECURITY and fails closed if the runtime role is too privileged. Grant the runtime role only USAGE on schema public, DML on the required tables, and USAGE/SELECT on required sequences; do not grant it membership in the migration role. The migration role needs the DDL privileges required to apply the versioned schema.

The server applies pending PostgreSQL migrations before it starts listening. By default it binds to 127.0.0.1:8080 and requires PostgreSQL TLS. Rustls validates the server chain using the platform's native CA store and checks the hostname. Install private database CA certificates in that trust store. For a disposable local PostgreSQL instance without TLS only, explicitly set DATABASE_SSL_MODE=disable; never use that mode over a network. The other accepted values are prefer (may fall back to plaintext) and require (default).

~~~bash
curl --fail http://127.0.0.1:8080/health
curl --fail -H "Authorization: Bearer $AETHERFLOW_API_TOKEN" \
  http://127.0.0.1:8080/api/v1/workers
~~~

GET /health is public process liveness only; it does not prove that PostgreSQL is reachable. When DATABASE_URL is absent, the server deliberately starts in health-only mode and durable API routes return 503 database_unavailable.

AETHERFLOW_BIND defaults to 127.0.0.1:8080. Plain HTTP is accepted only for loopback addresses. A non-loopback bind requires both AETHERFLOW_TLS_CERT_PATH and AETHERFLOW_TLS_KEY_PATH with a valid PEM certificate chain and private key; both paths also enable HTTPS on loopback. A partial or invalid pair fails startup. The global operator token and tenant credentials are separate. Tenant routes are available at `/api/v1/tenant/*`; use a tenant credential for workflow submit/read and task read/cancel. The global worker inventory and compatibility routes remain operator-only. Forced RLS and tenant-scoped storage passed the exact-source Rust/PostgreSQL/NATS/Pages matrix on `74c4e368d6ce43c6b9abd98cf1132120b1bbd199` (run IDs 41602415273, 41602415207, 41602415169). For non-loopback HTTPS, provision a certificate whose subject alternative names match the address clients use, then set AETHERFLOW_BIND, AETHERFLOW_TLS_CERT_PATH, and AETHERFLOW_TLS_KEY_PATH. PostgreSQL TLS trust/hostname verification against a live server is the next CI security gate. This does not yet make AetherFlow production-ready.

## HTTP v1 resources

The OpenAPI 3.1 source of truth is docs/api/openapi.yaml.

| Method | Route | Behavior |
|---|---|---|
| GET | /health | Process liveness |
| POST | /api/v1/workflow-runs | Submit and durably materialize a workflow-run snapshot |
| GET | /api/v1/workflow-runs/{workflow_run_id} | Read a workflow-run and task status summary |
| GET | /api/v1/task-runs/{task_run_id} | Read task status and payload metadata |
| POST | /api/v1/task-runs/{task_run_id}/cancel | Version-guarded cancellation and workflow aggregation |
| GET | /api/v1/workers | List live active worker registrations (operator credential) |
| POST | /api/v1/tenant/workflow-runs | Submit a workflow as the authenticated tenant |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id} | Read the authenticated tenant's workflow |
| GET | /api/v1/tenant/task-runs/{task_run_id} | Read the authenticated tenant's task |
| POST | /api/v1/tenant/task-runs/{task_run_id}/cancel | Cancel the authenticated tenant's task with expected_version |

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

The HTTP API integration test uses AETHERFLOW_TEST_DATABASE_URL. It migrates the disposable database and exercises platform and tenant route authentication, tenant role checks, workflow submission, duplicate conflict handling, own/foreign/missing workflow and task reads, payload redaction, stale cancellation, and active worker discovery.

~~~bash
AETHERFLOW_TEST_DATABASE_URL='postgres://aetherflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/aetherflow_api_test' \
  cargo test -p aetherflow-server --locked -- --test-threads=1
~~~

PostgreSQL storage integration tests use the same variable. The forced-RLS test creates a temporary restricted role with SET ROLE and verifies tenant filtering, worker-grant visibility, cross-tenant write denial, worker `SELECT ... FOR SHARE` grant locking, denied worker grant mutation, and digest-scoped credential lookup. These real-database suites require a disposable PostgreSQL test account with the privileges needed to create and grant a temporary role; the hosted CI service account provides this.

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

M00–M06 are complete. M07's tenant-authenticated routes, transaction-local contexts, PostgreSQL migrations v4–v7, forced RLS, restricted runtime role, tenant storage/API suites, and JetStream integration passed the exact-source hosted matrix at `74c4e368d6ce43c6b9abd98cf1132120b1bbd199` (Rust CI 41602415273, PostgreSQL/NATS 41602415207, Pages 41602415169). The control plane is still not production-ready: live PostgreSQL TLS trust/hostname integration verification, CLI/SDK, dashboard, and operational hardening remain future work. The master roadmap (../ROADMAP.md) is the source of truth for exact completion and hosted CI status.
