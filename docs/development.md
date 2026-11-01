# Development Guide

## Prerequisites

Install Rust through [rustup](https://rustup.rs/). The repository pins Rust 1.99.1 in `rust-toolchain.toml`; rustup selects it automatically in this repository. The pinned toolchain includes `rustfmt` and `clippy`.

## Useful commands

```bash
rustc --version
cargo --version
cargo fetch --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
cargo doc --workspace --no-deps --locked
```

## Start the bootstrap health server

```bash
cargo run -p aetherflow-server
```

Then request `http://127.0.0.1:8080/health`. Change the bind address with `AETHERFLOW_BIND`, for example:

```bash
AETHERFLOW_BIND=0.0.0.0:8080 cargo run -p aetherflow-server
```

Task payloads are transport-neutral byte arrays. Each input/output is limited to 1 MiB. The application owns serialization and schema compatibility. The current server is intentionally a minimal standard-library bootstrap. It handles connections sequentially and is not hardened or intended for production. The runtime scheduler uses Tokio. It supports cooperative handler cancellation and bounded two-stage shutdown (30-second graceful drain plus 5-second cancellation grace by default; both are configurable). The server's network stack remains a sequential standard-library bootstrap and is not production-ready.

## Workspace layout

- `crates/aetherflow-core`: infrastructure-independent domain primitives.
- `crates/aetherflow-runtime`: single-node runtime contracts, in-memory repository, bounded async scheduler, DAG validation, and workflow-run coordinator.
- `apps/aetherflow-server`: minimal process and bootstrap health endpoint.

Task inputs and outputs are opaque byte payloads limited to 1 MiB each; applications own serialization and schema versions. WorkflowTask supports a static input payload, and TaskContext exposes successful outputs from direct dependencies. Handlers own any transformation between dependency outputs and their own input schema. Keep domain rules out of runtime and network adapters. The initial in-memory repository uses one short-held mutex and deterministic caller-supplied timestamps; never hold repository locks across an `.await`. It is a test/runtime adapter, not durable storage. Add dependencies only when a milestone has a concrete need and record material architecture changes in an ADR.

## Verification

The foundation and current single-node runtime milestones are checked by hosted CI. The latest fully green run is `41359053561`; it verified 52 tests (15 core, 35 runtime, and 2 server), formatting, Clippy, release build, and docs. A lockfile is committed; regenerate it with `cargo generate-lockfile` whenever dependencies change, then verify with `cargo fetch --locked`. CI runs every check with locked dependency resolution and publishes the resolved lockfile as a short-lived artifact for review when the dependency graph changes. The latest fully green workflow dataflow Rust run is `41359053561` (52 tests: 15 core, 35 runtime, 2 server). The new PostgreSQL migration-runner integration passed against PostgreSQL 18 in run `41361580073`; full workspace CI with the added crate is pending lockfile refresh correction.

## PostgreSQL persistence work

The initial PostgreSQL schema draft lives in `crates/aetherflow-runtime/migrations/postgres/0001_initial_schema.sql`. Its intended claim, heartbeat, fencing, recovery, and outbox transaction boundaries are described in `docs/persistence/postgres-transaction-contract.md`. The initial migration has been executed successfully against PostgreSQL 18 in run `41361580073`; the async runner applied it transactionally, wrote the migration ledger, and returned `AlreadyCurrent` on a second call. This validates the initial DDL and bootstrap path, not yet durable task operations, lease recovery, or production availability.

## PostgreSQL storage crate

The `aetherflow-storage-postgres` crate owns the asynchronous PostgreSQL pool boundary and initial migration runner. The caller supplies a configured `deadpool-postgres` pool so TLS, authentication, and pool sizing remain deployment choices. `PgStore::migrate()` serializes migration attempts with a transaction-scoped advisory lock, applies the initial DDL and migration-ledger row atomically, and rejects untracked, newer, or checksum-drifted schemas. Its integration test uses `AETHERFLOW_TEST_DATABASE_URL`; without that variable it skips so ordinary workspace tests do not require a running database.

The PostgreSQL migration runner is verified against PostgreSQL 18 in run `41361580073`. The full workspace lockfile is being regenerated automatically on main pushes when manifests change; pull requests continue to require a committed, up-to-date Cargo.lock.
