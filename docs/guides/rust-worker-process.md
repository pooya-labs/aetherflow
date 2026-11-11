# Run the standalone Rust worker

The `aetherflow-worker` binary connects the existing PostgreSQL-backed scheduler, transactional outbox, NATS JetStream publisher, and durable inbox consumer in one long-lived Rust process. This is the local/private-network execution path for the full Rust engine; it does not use the Cloudflare demo runtime.

## Prerequisites

- Docker Engine with Docker Compose
- Rust toolchain 1.99.1 (the repository CI toolchain)
- A working Cargo registry connection for the first build

Use the repository's loopback-only PostgreSQL and three-node NATS stack. No cloud account, paid host, public domain, or payment card is required.

## Start the infrastructure

From the repository root:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and replace the three local development password values with your own URL-safe strings:

- `POSTGRES_PASSWORD`
- `AETHERFLOW_MIGRATION_PASSWORD`
- `AETHERFLOW_RUNTIME_PASSWORD`

Start the infrastructure:

```powershell
docker compose --env-file .env up -d --wait
```

The PostgreSQL and NATS ports are published on loopback only. Do not expose these local development ports to the public internet.

## Start the API server

Open a second PowerShell window in the repository root. Set the two PostgreSQL connection strings using the same passwords from `.env`. The server runs schema migrations using the separate `aetherflow_migrator` role and serves requests using the restricted `aetherflow_runtime` role.

```powershell
$runtimePassword = "PASTE_AETHERFLOW_RUNTIME_PASSWORD"
$migrationPassword = "PASTE_AETHERFLOW_MIGRATION_PASSWORD"

# This local Compose PostgreSQL instance does not use TLS.
$env:DATABASE_SSL_MODE = "disable"
$env:DATABASE_URL = "postgres://aetherflow_runtime:$runtimePassword@127.0.0.1:5432/aetherflow_dev"
$env:AETHERFLOW_MIGRATION_DATABASE_URL = "postgres://aetherflow_migrator:$migrationPassword@127.0.0.1:5432/aetherflow_dev"

# Generate a 64-character URL-safe operator token; keep it private.
$apiToken = "$([guid]::NewGuid().ToString('N'))$([guid]::NewGuid().ToString('N'))"
$env:AETHERFLOW_API_TOKEN = $apiToken
$apiToken

$env:AETHERFLOW_BIND = "127.0.0.1:8080"
cargo +1.99.1 run --locked -p aetherflow-server
```

Copy the printed API token for the request examples below. It is the operator credential for this local server; do not commit it or share it.

## Start the Rust worker

Open a third PowerShell window in the repository root. Use the restricted runtime database role, not the administrator or migration-owner account:

```powershell
$runtimePassword = "PASTE_THE_SAME_AETHERFLOW_RUNTIME_PASSWORD"
# The local Compose PostgreSQL instance does not use TLS.
$env:DATABASE_SSL_MODE = "disable"
$env:DATABASE_URL = "postgres://aetherflow_runtime:$runtimePassword@127.0.0.1:5432/aetherflow_dev"
$env:AETHERFLOW_NATS_URLS = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"

cargo +1.99.1 run --locked -p aetherflow-transport-nats --bin aetherflow-worker
```

The worker does not run database migrations. It checks that the migration ledger exists, registers separate task and transport workers under a fenced node membership, and provisions the three-replica JetStream stream and durable consumer. It prints its node/worker IDs and process-local publisher/consumer counters every 30 seconds. An authenticated Prometheus endpoint is optional and disabled unless both metrics environment variables are configured.

### Optional authenticated Prometheus endpoint

The worker can expose its NATS publisher and consumer counters at `GET /metrics`. This endpoint is **disabled by default**. It combines the one publisher and one consumer metrics handle owned by this worker process; it does not include the API process or other worker replicas.

Enable it in the same PowerShell window used to start the worker:

```powershell
# Generate a 48-byte URL-safe bearer secret. Do not commit or share it.
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$secretBytes = New-Object byte[] 48
$rng.GetBytes($secretBytes)
$rng.Dispose()
$metricsToken = [Convert]::ToBase64String($secretBytes).TrimEnd('=').Replace('+','-').Replace('/','_')

$env:AETHERFLOW_METRICS_BIND = "127.0.0.1:9464"
$env:AETHERFLOW_METRICS_TOKEN = $metricsToken

cargo +1.99.1 run --locked -p aetherflow-transport-nats --bin aetherflow-worker
```

The listener accepts only numeric loopback socket addresses and requires a 32–256 character URL-safe Bearer token. Only the SHA-256 token digest is retained in memory; comparisons use constant-time equality. The response is Prometheus text exposition with fixed metric names and no tenant, workflow, task, event, subject, credential, or payload labels. Unauthorized requests receive 401, and responses use `Cache-Control: no-store`.

From the same machine, verify access:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:9464/metrics" `
    -Headers @{ Authorization = "Bearer $metricsToken" }
```

The server deliberately rejects non-loopback binds because this endpoint is plain HTTP. For a remote Prometheus collector, keep the worker listener on loopback and place an authenticated TLS-terminating proxy on the same host. Do not expose this listener directly to the internet. If multiple worker processes run, scrape each separately and aggregate in the collector; the endpoint is not a cross-replica aggregate.

### Grant the local demo tenant explicitly

The operator API creates workflows in the reserved `legacy-default` tenant. Registered workers do not receive tenant access implicitly: grant that tenant to both worker IDs printed by the process. This is an intentional authorization boundary, not an optional workaround. Use the PostgreSQL administrator from the local Compose stack for this one-time demo grant; never give the worker the administrator or migration-owner connection.

In a fourth PowerShell window, copy the exact two IDs printed by the worker:

```powershell
$taskWorkerId = "PASTE_task_worker_id_FROM_WORKER_OUTPUT"
$transportWorkerId = "PASTE_transport_worker_id_FROM_WORKER_OUTPUT"

@'
INSERT INTO worker_tenant_grants (tenant_id, worker_id, status)
VALUES
  ('legacy-default', :'task_worker_id', 'active'),
  ('legacy-default', :'transport_worker_id', 'active')
ON CONFLICT (tenant_id, worker_id) DO UPDATE
SET status = 'active',
    granted_at = clock_timestamp(),
    expires_at = NULL,
    revoked_at = NULL;
'@ | docker compose --env-file .env exec -T postgres psql -U aetherflow_admin -d aetherflow_dev `
    --set "task_worker_id=$taskWorkerId" `
    --set "transport_worker_id=$transportWorkerId" `
    --set ON_ERROR_STOP=1
```

After this succeeds, submit the demo workflow. When restarting the worker, new process-specific worker IDs are generated by default, so grant the newly printed IDs again. For a multi-tenant deployment, add only the individual tenant grants approved for each registered worker.

Optional settings:

| Variable | Default | Purpose |
|---|---|---|
| `AETHERFLOW_WORKER_CONCURRENCY` | `4` | Maximum in-flight task attempts (1–256) |
| `AETHERFLOW_NODE_ID` | Process-specific local ID | Stable identity for this worker process; each concurrently active process must use a unique node ID |
| `AETHERFLOW_TASK_WORKER_ID` | Process-specific local ID | Optional explicit identity for the task worker; grant tenant access to this exact ID |
| `AETHERFLOW_TRANSPORT_WORKER_ID` | Process-specific local ID | Optional explicit identity for the outbox/consumer worker; grant tenant access to this exact ID |
| `AETHERFLOW_NATS_STREAM` | `AETHERFLOW_EVENTS` | JetStream stream name |
| `AETHERFLOW_NATS_SUBJECT` | `aetherflow.events` | Concrete subject used for AetherFlow events |
| `AETHERFLOW_NATS_DURABLE` | `aetherflow_worker_events` | Durable pull-consumer name |
| `AETHERFLOW_WASM_PLUGIN_PATH` | unset | Optional compiled AetherFlow ABI v1 component artifact |
| `AETHERFLOW_METRICS_BIND` | unset | Optional numeric loopback socket address for the Prometheus listener; enabling it also requires a bearer token |
| `AETHERFLOW_METRICS_TOKEN` | unset | 32–256 character URL-safe Bearer secret; required when the metrics listener is enabled |

The local worker config currently provisions a three-replica stream to match the repository's three-node Compose cluster. For a remote cluster, configure NATS credentials/TLS using the existing `AETHERFLOW_NATS_*` connection settings and review the stream topology before starting the worker. PostgreSQL connections use the same Rustls/native-CA validation path as the API: `DATABASE_SSL_MODE` accepts `require` (the default), `prefer`, or `disable`. Use `disable` only for the local Compose database shown above; keep TLS verification enabled for remote databases and trust their issuing CA through the host certificate store.

## Submit a workflow

The operator route accepts workflow definitions through the HTTP API. The worker registers these task definition IDs:

| Task ID | Input | Output / behavior |
|---|---|---|
| `echo` | Any payload bytes | Returns the same bytes |
| `uppercase` | UTF-8 text | Returns Unicode uppercase text |
| `json_add` | JSON object with integer fields `a` and `b` | Returns the JSON-encoded integer sum; overflow is rejected |
| `fail_once` | Any payload (may be empty) | First attempt fails retryably; the next attempt returns `retry-succeeded` |
| `wasm` | Plugin-defined bytes | Available only when a valid ABI v1 component is configured with `AETHERFLOW_WASM_PLUGIN_PATH` |

The optional WASM handler starts with an empty capability set (default-deny) and the repository's bounded default execution limits. It does not grant network, filesystem, or other ambient capabilities.

Example: submit an integer addition task from PowerShell. Use the operator token printed by the API server:

```powershell
$apiToken = "PASTE_THE_LOCAL_AETHERFLOW_API_TOKEN"
$inputBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"a":19,"b":23}'))
$runId = "demo-run-$([guid]::NewGuid().ToString('N'))"

$body = @{
    workflow_id = "demo-add"
    workflow_run_id = $runId
    tasks = @(
        @{
            task_id = "json_add"
            depends_on = @()
            input_base64 = $inputBase64
        }
    )
    retry_policy = @{
        max_attempts = 3
        initial_backoff_ms = 100
        max_backoff_ms = 2000
    }
} | ConvertTo-Json -Depth 8

$receipt = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/workflow-runs" `
    -Method Post `
    -Headers @{ Authorization = "Bearer $apiToken" } `
    -ContentType "application/json" `
    -Body $body

$receipt
```

The receipt includes the workflow run ID and deterministic task-run ID. Inspect the durable workflow state and event history:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/workflow-runs/$runId" `
    -Headers @{ Authorization = "Bearer $apiToken" }

Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/workflow-runs/$runId/events" `
    -Headers @{ Authorization = "Bearer $apiToken" }
```

To demonstrate retries, submit a task with `task_id = "fail_once"` and `max_attempts = 3`. The first attempt returns a retryable error; the task is rescheduled by PostgreSQL and the second attempt succeeds. To demonstrate dependency gating, submit multiple task definitions and put the predecessor's task ID in the dependent task's `depends_on` array.

## Shutdown

Press Ctrl+C in the worker terminal. It stops task admission, drains active handlers, then stops the outbox dispatcher and inbox consumer. Pending outbox events remain durable in PostgreSQL and can be picked up by the next worker process.

Stop the local infrastructure:

```powershell
docker compose --env-file .env down
```

To delete all local database and NATS data as well:

```powershell
docker compose --env-file .env down --volumes
```

That last command permanently removes the local demo data.

## Scope

This is an executable local Rust engine path, not a hosted production deployment or a v1.0.0 readiness claim. The API and worker are separate Rust processes. The current operator workflow path uses the reserved `legacy-default` tenant. Tenant-authenticated workflows require explicit worker/tenant grants before they can be executed by a worker. PostgreSQL production TLS/recovery, NATS server-side security/retention, external gateway protection, and the remaining M08 release gates are still separate work.
