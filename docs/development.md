# Development Guide

## Prerequisites

Install Rust with [rustup](https://rustup.rs/). The repository pins Rust 1.99.1 in rust-toolchain.toml; the toolchain includes rustfmt and clippy. Docker Engine with Docker Compose v2 is used for the local PostgreSQL/NATS dependency stack.

For the HTTP control plane, use the included Docker Compose PostgreSQL instance or another local PostgreSQL instance. The server uses Rustls and the operating system's native CA trust store; TLS is required by default. Compose is deliberately local-only and uses `DATABASE_SSL_MODE=disable`; never copy that setting to a network database. NATS integration tests use a three-node JetStream cluster. The full chaos test additionally requires the CI packet-filter prerequisites documented below.

## Docker Compose development environment

The Compose stack runs PostgreSQL 18 and a three-node NATS 2.12 JetStream cluster. All published ports bind to loopback only: PostgreSQL `127.0.0.1:5432`, NATS clients `127.0.0.1:4222-4224`, and monitoring `127.0.0.1:8222-8224`. It does not expose the API server; run the Rust server on the host so its default loopback-only HTTP policy remains intact.

~~~bash
cp .env.example .env
# Generate three local-only values, then paste each into the matching .env entry.
for name in POSTGRES_PASSWORD AETHERFLOW_MIGRATION_PASSWORD AETHERFLOW_RUNTIME_PASSWORD; do
  printf '%s=%s\n' "$name" "$(openssl rand -hex 32)"
done
docker compose up --detach --wait
docker compose ps
~~~

On the first PostgreSQL volume initialization, `docker/postgres/00-bootstrap-roles.sh` creates two independent logins: `aetherflow_migrator` can apply schema DDL, while `aetherflow_runtime` has only DML and sequence privileges and has no role memberships or public-schema CREATE permission. Default privileges ensure tables and sequences created by future migrations are usable by the runtime role. The script runs only when the database volume is empty; changing credentials in `.env` does not rotate roles in an existing volume.

To run the HTTP server from the host, load the local environment and explicitly select plaintext PostgreSQL for this disposable loopback-only database:

~~~bash
set -a
. ./.env
set +a
export DATABASE_SSL_MODE=disable
export AETHERFLOW_MIGRATION_DATABASE_URL="postgres://aetherflow_migrator:${AETHERFLOW_MIGRATION_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
export DATABASE_URL="postgres://aetherflow_runtime:${AETHERFLOW_RUNTIME_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
export AETHERFLOW_API_TOKEN="$(openssl rand -hex 32)"
cargo run -p aetherflow-server
~~~

The server remains on `127.0.0.1:8080`. Compose secrets and sample passwords are for local development only; do not use them outside a disposable workstation. For a clean stop while preserving data, run `docker compose down`. To erase the local database and NATS streams and re-run first-boot role provisioning, use `docker compose down --volumes`; that command is destructive.

## NATS client security configuration

The transport crate exports `NatsConnectionConfig` for building JetStream clients consistently across workers, publishers, consumers and operational tools. It reads these settings from the process environment:

- `AETHERFLOW_NATS_CREDENTIALS_FILE`: path to a secret-mounted NATS `.creds` file (user JWT and private NKey seed).
- `AETHERFLOW_NATS_TLS_REQUIRED`: optional `true`/`false`; TLS is inferred as required whenever credentials, a custom CA, or an mTLS identity is configured.
- `AETHERFLOW_NATS_CA_FILE`: optional PEM CA bundle for a private server certificate authority.
- `AETHERFLOW_NATS_CLIENT_CERT_FILE` and `AETHERFLOW_NATS_CLIENT_KEY_FILE`: optional mTLS identity; the pair must be supplied together.

The default is plaintext to keep the loopback-only Compose cluster usable. That default is not suitable for an externally reachable deployment. Credentials or TLS files combined with an explicit plaintext setting fail validation. The `.creds` file is read when a connection is created; rotate it through a controlled new-client rollout rather than expecting automatic file watching. The CI TLS fixture now requires a CA-signed client certificate and proves that a client without an identity is rejected, while the configured AetherFlow connector can publish to JetStream and still rejects a server-name mismatch. This verifies client-side mTLS behavior; production server-side accounts, subject ACLs, stream retention, and cluster TLS policy still need deployment configuration.

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

## Run the CLI

The CLI calls the versioned HTTP API and never connects directly to PostgreSQL or launches worker processes. Its environment-only credential scopes, loopback-only plaintext policy, redirect blocking, and bounded submission reader are hosted-verified on exact source `0c19b9b7d8124472c81ce53056612344f0ebab57` (Rust CI 41614021774, PostgreSQL/NATS 41614021644, Pages 41614915557).

~~~bash
export AETHERFLOW_URL='http://127.0.0.1:8080'
export AETHERFLOW_API_TOKEN='YOUR_OPERATOR_TOKEN'

cargo run -p aetherflow-cli -- --help
cargo run -p aetherflow-cli -- workers
cargo run -p aetherflow-cli -- workflow inspect run-0001
cargo run -p aetherflow-cli -- workflow submit --file workflow.json
cargo run -p aetherflow-cli -- task inspect task-run-0001
cargo run -p aetherflow-cli -- task cancel task-run-0001 --expected-version 3
~~~

Tenant workflow/task commands use AETHERFLOW_TENANT_TOKEN and the global --tenant flag. Worker inventory is operator-only. AETHERFLOW_URL or --url selects the server; the default is http://127.0.0.1:8080. Tokens are read only from environment variables, never command-line arguments or error output. HTTPS verification stays enabled through Rustls and the platform trust store; the CLI rejects remote plain-HTTP URLs before it can send a bearer token. Plain HTTP is allowed only for loopback development. The --file - form reads a JSON request from standard input through a bounded reader; file and stdin input stop at the API's 2 MiB request limit.

## Use the Rust SDK

The SDK crate is a typed asynchronous HTTP client. It uses separate operator and tenant scopes, performs local identifier/workflow/payload checks, and does not connect to PostgreSQL or launch workers.

~~~rust
use aetherflow_sdk::{
    ControlPlaneClient, RetryPolicy, WorkflowRunSubmission, WorkflowTaskInput,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("AETHERFLOW_API_TOKEN")?;
    let client = ControlPlaneClient::operator("http://127.0.0.1:8080", token)?;
    let submission = WorkflowRunSubmission::new(
        "demo",
        "demo-run-001",
        vec![WorkflowTaskInput::new("first")],
        RetryPolicy::new(2, 100, 1_000)?,
    );
    let receipt = client.submit_workflow_run(&submission).await?;
    println!("created {}", receipt.workflow_run_id);
    Ok(())
}
~~~

For public liveness checks use `ControlPlaneClient::anonymous`; for tenant-owned workflows/tasks use `ControlPlaneClient::tenant` with `AETHERFLOW_TENANT_TOKEN`. Remote URLs must use HTTPS. Plain HTTP is accepted only for loopback development. See the [SDK guide](../crates/aetherflow-sdk/README.md).

## Run the HTTP control plane

Create a high-entropy local operator token and use a disposable local database:

~~~bash
export AETHERFLOW_API_TOKEN="$(openssl rand -hex 32)"
export AETHERFLOW_MIGRATION_DATABASE_URL='postgres://aetherflow_migrator:YOUR_MIGRATION_PASSWORD@127.0.0.1:5432/aetherflow_dev'
export DATABASE_URL='postgres://aetherflow_runtime:YOUR_RUNTIME_PASSWORD@127.0.0.1:5432/aetherflow_dev'
# Browser access is disabled unless an exact allowed origin is configured.
export AETHERFLOW_CORS_ALLOWED_ORIGINS='https://pooya-labs.github.io'
# Optional authenticated-principal limiter settings (defaults shown).
export AETHERFLOW_API_RATE_LIMIT_BURST=30
export AETHERFLOW_API_RATE_LIMIT_PER_SECOND=2
cargo run -p aetherflow-server
~~~

When DATABASE_URL is set, both database URLs and AETHERFLOW_API_TOKEN are mandatory. `AETHERFLOW_CORS_ALLOWED_ORIGINS` is optional; an empty or unset value disables browser cross-origin access. `AETHERFLOW_API_RATE_LIMIT_BURST` and `AETHERFLOW_API_RATE_LIMIT_PER_SECOND` configure authenticated-principal buckets and default to 30 and 2 respectively; both must be positive integers. Operator traffic has one shared bucket, while tenant traffic is keyed by authenticated tenant/principal identity. This limiter is bounded to 4,096 per-process buckets and returns HTTP 429 with `Retry-After` when exhausted. `AETHERFLOW_AUTH_FAILURE_BURST` and `AETHERFLOW_AUTH_FAILURE_PER_SECOND` configure a second limiter for missing or invalid credentials; defaults are 20 failed attempts and 2 token refills per second. It keys only on the TCP peer address supplied by the listener, ignores `X-Forwarded-For` and `Forwarded`, clears a source's failure history after successful authentication, and stores at most 4,096 source buckets per process. Neither limiter coordinates quotas across replicas. For public deployments, retain the shared ingress policy described in the [production guide](operations/production-guide.md); this process-local limit is a secondary control, not a substitute. AETHERFLOW_API_TOKEN must be 32–256 ASCII letters, digits, hyphens or underscores. The server stores only its SHA-256 digest in memory. Do not commit the token, paste it into issue reports, or enable shell tracing while exporting it. Rotate it by replacing the environment value and restarting the process.

`AETHERFLOW_MIGRATION_DATABASE_URL` is used only by the migration/DDL owner pool. `DATABASE_URL` is used only by the API runtime pool and must authenticate as a restricted non-owner role with no SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, protected-table ownership, or membership in any other PostgreSQL role. Startup checks all ten protected tables for both ENABLE and FORCE ROW LEVEL SECURITY and fails closed if the runtime role is too privileged. Grant the runtime role only USAGE on schema public, DML on the required tables, and USAGE/SELECT on required sequences; do not grant it membership in the migration role. The migration role needs the DDL privileges required to apply the versioned schema.

The server applies pending PostgreSQL migrations before it starts listening. By default it binds to 127.0.0.1:8080 and requires PostgreSQL TLS. Rustls validates the server chain using the platform's native CA store and checks the hostname. Install private database CA certificates in that trust store. For a disposable local PostgreSQL instance without TLS only, explicitly set DATABASE_SSL_MODE=disable; never use that mode over a network. The other accepted values are prefer (may fall back to plaintext) and require (default).

~~~bash
curl --fail http://127.0.0.1:8080/health
curl --fail -H "Authorization: Bearer $AETHERFLOW_API_TOKEN" \
  http://127.0.0.1:8080/api/v1/workers
~~~

GET /health is public process liveness only; it does not prove that PostgreSQL is reachable. When DATABASE_URL is absent, the server deliberately starts in health-only mode and durable API routes return 503 database_unavailable.

AETHERFLOW_BIND defaults to 127.0.0.1:8080. Plain HTTP is accepted only for loopback addresses. A non-loopback bind requires both AETHERFLOW_TLS_CERT_PATH and AETHERFLOW_TLS_KEY_PATH with a valid PEM certificate chain and private key; both paths also enable HTTPS on loopback. A partial or invalid pair fails startup. The global operator token and tenant credentials are separate. Tenant routes are available at `/api/v1/tenant/*`; use a tenant credential for workflow submit/read and task read/cancel. The global worker inventory and compatibility routes remain operator-only. Browser preflight accepts only GET/POST and the Authorization, Content-Type, Traceparent, and Tracestate headers from an explicitly listed origin; responses expose only Traceparent and Tracestate. It never enables cookies or wildcard origins. The Pages dashboard is read-only, holds its Bearer token in memory only, and does not persist credentials. Forced RLS and tenant-scoped storage passed the exact-source Rust/PostgreSQL/NATS/Pages matrix on `74c4e368d6ce43c6b9abd98cf1132120b1bbd199` (run IDs 41602415273, 41602415207, 41602415169). For non-loopback HTTPS, provision a certificate whose subject alternative names match the address clients use, then set AETHERFLOW_BIND, AETHERFLOW_TLS_CERT_PATH, and AETHERFLOW_TLS_KEY_PATH. Live PostgreSQL 18 TLS verification passed hosted integration tests: trusted CA + correct host connected, while untrusted CA and hostname mismatch were rejected (run [41609705469](https://github.com/pooya-labs/aetherflow/actions/runs/41609705469)). This does not yet make AetherFlow production-ready.

## HTTP v1 resources

The OpenAPI 3.1 source of truth is docs/api/openapi.yaml.

| Method | Route | Behavior |
|---|---|---|
| GET | /health | Process liveness |
| GET | /api/v1/workflow-runs | List legacy-default runs using a deterministic keyset cursor (operator) |
| POST | /api/v1/workflow-runs | Submit and durably materialize a workflow-run snapshot |
| GET | /api/v1/workflow-runs/{workflow_run_id} | Read a workflow-run and task status summary |
| GET | /api/v1/workflow-runs/{workflow_run_id}/events | Read safe event metadata for a workflow and its tasks (operator) |
| GET | /api/v1/task-runs/{task_run_id} | Read task status and payload metadata |
| POST | /api/v1/task-runs/{task_run_id}/cancel | Version-guarded cancellation and workflow aggregation |
| GET | /api/v1/workers | List live active worker registrations (operator credential) |
| GET | /api/v1/tenant/workflow-runs | List the authenticated tenant's workflow runs |
| POST | /api/v1/tenant/workflow-runs | Submit a workflow as the authenticated tenant |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id} | Read the authenticated tenant's workflow |
| GET | /api/v1/tenant/workflow-runs/{workflow_run_id}/events | Read safe event metadata for the authenticated tenant workflow |
| GET | /api/v1/tenant/task-runs/{task_run_id} | Read the authenticated tenant's task |
| POST | /api/v1/tenant/task-runs/{task_run_id}/cancel | Cancel the authenticated tenant's task with expected_version |

The submit request carries a caller-supplied unique workflow_run_id, a workflow ID, task IDs/dependencies, a retry policy, and optional base64 task input. Input is limited to 1 MiB after base64 decoding; the HTTP request body is limited to 2 MiB. A reused workflow-run ID returns 409 Conflict; no idempotent replay contract is implemented. Workflow and task inspection never return input/output bytes.

The responsive read-only dashboard is published at https://pooya-labs.github.io/aetherflow/dashboard/. Set `AETHERFLOW_CORS_ALLOWED_ORIGINS` to that exact origin before using it against a remote API. Operator dashboard queries intentionally see only `legacy-default`; tenant credentials remain tenant-scoped. The dashboard pages event history with an opaque cursor and never renders raw outbox payloads.

The HTTP process does not launch a worker. Run durable workers separately through the PostgreSQL runtime. Workflow definitions are currently submitted per run and are not persisted as reusable versioned definitions.

## Workspace layout

- apps/aetherflow-server: Axum control-plane process and loopback guard.
- crates/aetherflow-core: infrastructure-independent identities, states, and retry primitives.
- crates/aetherflow-runtime: scheduler, workflow DAG validation, handlers, and worker execution contracts.
- crates/aetherflow-storage-postgres: migrations, durable task/workflow repositories, membership, outbox, and inbox.
- crates/aetherflow-transport-nats: JetStream publisher, inbox-backed pull consumer, and validated NATS client credentials/TLS configuration.
- crates/aetherflow-wasm: WIT ABI and bounded Wasmtime execution.
- crates/aetherflow-sdk: typed async HTTP client for anonymous health, operator, and tenant control-plane scopes.
- examples/minimal-plugin: reproducible guest/host example using the scheduler-backed WASM handler.

## PostgreSQL-backed tests

Create a fresh disposable database inside the Compose PostgreSQL service for each integration suite. The privileged local bootstrap login is available as `POSTGRES_USER`/`POSTGRES_PASSWORD` after loading `.env`:

~~~bash
set -a
. ./.env
set +a
docker compose exec -T postgres createdb -U "$POSTGRES_USER" aetherflow_api_test
docker compose exec -T postgres createdb -U "$POSTGRES_USER" aetherflow_storage_test
docker compose exec -T postgres createdb -U "$POSTGRES_USER" aetherflow_nats_test
~~~

The HTTP API integration test uses AETHERFLOW_TEST_DATABASE_URL. It migrates the disposable database and exercises platform and tenant route authentication, tenant role checks, workflow submission, duplicate conflict handling, own/foreign/missing workflow and task reads, payload redaction, stale cancellation, and active worker discovery.

~~~bash
AETHERFLOW_TEST_DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/aetherflow_api_test" \
  cargo +1.99.1 test -p aetherflow-server --locked -- --test-threads=1
~~~

PostgreSQL storage integration tests use the same variable. The forced-RLS test creates a temporary restricted role with SET ROLE and verifies tenant filtering, worker-grant visibility, cross-tenant write denial, worker `SELECT ... FOR SHARE` grant locking, denied worker grant mutation, and digest-scoped credential lookup. These real-database suites require a disposable PostgreSQL test account with the privileges needed to create and grant a temporary role; the hosted CI service account provides this.

~~~bash
AETHERFLOW_TEST_DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/aetherflow_storage_test" \
  cargo +1.99.1 test -p aetherflow-storage-postgres --tests --locked
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
AETHERFLOW_TEST_NATS_DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/aetherflow_nats_test" \
AETHERFLOW_TEST_NATS_URL='nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224' \
AETHERFLOW_TEST_NATS_CONTAINER='aetherflow-nats-1' \
  cargo +1.99.1 test -p aetherflow-transport-nats --test jetstream_outbox --locked -- --test-threads=1
~~~

The Compose stack provides the three broker members and the configured container name for local JetStream tests. The combined packet-partition campaign remains intended for the hosted CI fixture: it additionally requires passwordless sudo, the nobody user, and UID-scoped iptables OUTPUT rules. Do not set `AETHERFLOW_TEST_PACKET_PARTITION=1` on a workstation unless those prerequisites and cleanup behavior are understood.

## WASM plugin example

The reproducible guest/host example lives in examples/minimal-plugin/. It uses the checked-in WIT ABI, pinned wit-bindgen 0.60.0, and wit-component 0.254.2. The guest targets wasm32-unknown-unknown and imports no ambient WASI interface.

~~~bash
rustup +1.99.1 target add wasm32-unknown-unknown
cargo +1.99.1 build --locked -p aetherflow-plugin-guest --target wasm32-unknown-unknown --release
cargo +1.99.1 run --locked -p aetherflow-plugin-host --release -- target/wasm32-unknown-unknown/release/aetherflow_plugin_guest.wasm
~~~

The host harness verifies default-deny behavior, registers WasmTaskHandler in TaskRegistry, and executes it through TaskScheduler. It checks workflow success, retained task output, and one explicitly granted bounded log.write message. Synchronous Wasmtime work runs on Tokio's blocking pool. A running spawn_blocking guest call is not forcibly aborted when its async future is dropped; fuel and wall-clock limits are the resource boundary.

## Project state

M00–M07 are complete. M08 observability (including W3C trace context, opt-in OTLP, operation-class metrics and response classes), tenant-scoped workflow/outbox/inbox gauges and 30-minute terminal outcomes/successful completion-latency percentiles through `GET /api/v1/tenant/metrics` (schema v10, PostgreSQL/JetStream restore CI verified); the Tenant dashboard renders the snapshot without requesting it in operator scope, authenticated-principal API rate limiting, the loopback-only PostgreSQL 18/three-node NATS Compose stack, runbook drift checks, backup/restore and schema-upgrade procedures, isolated restore-to-runtime, API restart, in-flight-worker/API outage, and full JetStream cluster outage/recovery drills are implemented and hosted-verified. Five five-repeat PostgreSQL/NATS benchmark sessions are archived; the regression comparator applies numeric thresholds only to the normalized AMD EPYC 9V45 reference fingerprint, and a fresh matched-runner hosted verification remains outstanding. Remaining engineering work includes distinct operational failure injection, source-based throttling for unauthenticated or invalid credentials, shared multi-replica ingress/quota enforcement, individual operator audit identity and durable audit retention, managed secret rotation, production NATS security, production PITR/RPO/RTO, horizontal-scale evidence, and workflow SLO instrumentation. Owner license selection and deferred property-based lifecycle tests also remain open. The control plane is experimental and is not production-ready. See [backup and restore](operations/backup-restore.md), [schema upgrades](operations/schema-upgrades.md), [the operational guide](operations/production-guide.md), and the master [roadmap](../ROADMAP.md).

## Reproducible performance benchmarks

Run the in-memory scheduler benchmark suite with:

~~~bash
cargo bench -p aetherflow-runtime --bench scheduler
~~~

Workload parameters, interpretation limits, and a larger comparison profile are documented in the [benchmark guide](performance/benchmarks.md). The benchmark does not include PostgreSQL or NATS.

