# Run the full Rust engine locally

This is the most representative way to evaluate AetherFlow without relying on a hosted runtime, domain, cloud account, or paid service. It runs the actual Rust workflow coordinator, PostgreSQL-backed worker, transactional outbox, NATS JetStream publisher, and PostgreSQL-backed inbox consumer.

## Requirements

- Docker Engine with the Compose plugin
- Rust toolchain 1.99.1 (the repository CI toolchain)
- Git and a working Cargo registry connection for the first build

The demo is local-first and can be run in Iran wherever Docker images and Cargo dependencies are reachable. No Vercel, Cloudflare, custom domain, or cloud VM is involved.

## 1. Prepare the local infrastructure

From the repository root, copy the development environment file and change the three local passwords:

```bash
cp .env.example .env
```

On PowerShell:

```powershell
Copy-Item .env.example .env
```

Start PostgreSQL and the three-node NATS JetStream cluster:

```bash
docker compose --env-file .env up -d --wait
```

The Compose stack binds its published ports to loopback only. It is intended for local development, not public exposure.

## 2. Create two disposable databases

The end-to-end harness runs migrations and leaves its workflow, outbox, and inbox records in place. Use a fresh database for each workload shape so the counts remain independently verifiable.

Using the default values in `.env.example`:

```bash
docker compose --env-file .env exec -T postgres psql -U aetherflow_admin -d postgres -c "CREATE DATABASE aetherflow_demo_fanout;"
docker compose --env-file .env exec -T postgres psql -U aetherflow_admin -d postgres -c "CREATE DATABASE aetherflow_demo_chain;"
```

If you changed `POSTGRES_USER`, substitute that local administrator username. Choose new database names if these already exist. These databases are disposable demo data.

## 3. Run the fan-out workload

Set the database and NATS endpoints, then run the existing Rust end-to-end harness:

```bash
export AETHERFLOW_LOAD_DATABASE_URL="postgres://aetherflow_admin:local-only-admin-password-change-me@127.0.0.1:5432/aetherflow_demo_fanout"
export AETHERFLOW_LOAD_NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
export AETHERFLOW_LOAD_SHAPE=fan_out
cargo +1.99.1 run --locked -p aetherflow-transport-nats --example end_to_end_load
```

PowerShell equivalent:

```powershell
$env:AETHERFLOW_LOAD_DATABASE_URL = "postgres://aetherflow_admin:local-only-admin-password-change-me@127.0.0.1:5432/aetherflow_demo_fanout"
$env:AETHERFLOW_LOAD_NATS_URL = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
$env:AETHERFLOW_LOAD_SHAPE = "fan_out"
cargo +1.99.1 run --locked -p aetherflow-transport-nats --example end_to_end_load
```

Replace the example password with the value in your local `.env`. If it contains URI-reserved characters, percent-encode them in the connection URL.

## 4. Run the dependency-chain workload

Point the harness at the second fresh database and change the shape:

```bash
export AETHERFLOW_LOAD_DATABASE_URL="postgres://aetherflow_admin:local-only-admin-password-change-me@127.0.0.1:5432/aetherflow_demo_chain"
export AETHERFLOW_LOAD_SHAPE=chain
cargo +1.99.1 run --locked -p aetherflow-transport-nats --example end_to_end_load
```

The PowerShell equivalent is the same two environment assignments using `$env:AETHERFLOW_LOAD_DATABASE_URL` and `$env:AETHERFLOW_LOAD_SHAPE`.

## What a successful run proves

The harness submits 16 workflows with four tasks each (64 task runs), using one PostgreSQL-backed worker process with concurrency four. It then waits for all of the following:

1. Every workflow reaches `succeeded` in PostgreSQL.
2. Every transactional outbox event is published and confirmed by JetStream.
3. Every published event is claimed and completed through the PostgreSQL inbox.
4. The number of published events matches the number of processed inbox records for that run.
5. Duplicate and error counters remain visible in the resulting CSV and terminal summary.

The example creates a uniquely named three-replica JetStream stream and durable pull consumer for each run. It intentionally leaves the stream and database rows available for inspection. A successful run is end-to-end evidence for the Rust/PostgreSQL/NATS path; it is not a production deployment, a horizontal-scaling test, or a production capacity/SLO claim.

For workload tuning, output columns, CI artifacts, and historical benchmark interpretation, see [the end-to-end load report](../performance/end-to-end-load.md).

## Stop the local stack

```bash
docker compose --env-file .env down
```

To remove the local PostgreSQL and NATS volumes as well, use `docker compose --env-file .env down --volumes`. This permanently deletes the local demo data.
