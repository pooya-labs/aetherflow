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

The foundation and current single-node runtime milestones are checked by hosted CI. The latest fully green Rust workspace run is `41365026218`; it verified 53 tests (15 core, 35 runtime, 2 server, and 1 storage unit test), formatting, Clippy, release build, and docs. A lockfile is committed; regenerate it with `cargo generate-lockfile` whenever dependencies change, then verify with `cargo fetch --locked`. CI runs every check with locked dependency resolution and publishes the resolved lockfile as a short-lived artifact for review when the dependency graph changes. The latest fully green workflow dataflow run is `41359053561`. The current Rust workspace run `41365026218` passed formatting, Clippy, all 53 default workspace tests, release build, and docs. The PostgreSQL 18 integration run `41365026199` executed both migration and durable task-repository tests successfully.

## PostgreSQL persistence work

The initial PostgreSQL schema draft lives in `crates/aetherflow-runtime/migrations/postgres/0001_initial_schema.sql`. Its intended claim, heartbeat, fencing, recovery, and outbox transaction boundaries are described in `docs/persistence/postgres-transaction-contract.md`. The initial migration has been executed successfully against PostgreSQL 18 in runs `41362986570` and `41365026199`; the async runner applies DDL and checksum ledger transactionally, serializes concurrent first startup, rejects checksum/history/version drift, and returns `AlreadyCurrent` on repeat. The latest PostgreSQL run also verified the initial durable task repository flow and lease recovery. This does not yet imply a complete production workflow engine.

## PostgreSQL storage crate

The `aetherflow-storage-postgres` crate owns the asynchronous PostgreSQL pool boundary and initial migration runner. The caller supplies a configured `deadpool-postgres` pool so TLS, authentication, and pool sizing remain deployment choices. `PgStore::migrate()` serializes migration attempts with a transaction-scoped advisory lock, applies the initial DDL and migration-ledger row atomically, and rejects untracked, newer, or checksum-drifted schemas. Its integration test uses `AETHERFLOW_TEST_DATABASE_URL`; without that variable it skips so ordinary workspace tests do not require a running database.

The PostgreSQL migration runner and task repository are verified against PostgreSQL 18 in run `41365026199`; Rust CI passed run `41365026218` (15 core, 35 runtime, 2 server, 1 storage unit test; plus the two PostgreSQL integration tests executed against the live service in the database workflow). Formatting, Clippy, release build, and docs passed. Main-branch CI refreshes and commits Cargo.lock when manifests change; pull requests require an up-to-date committed lockfile.

The PostgreSQL integration suite now checks concurrent first-start serialization, checksum drift rejection, refusal to adopt untracked schemas, and refusal to open a database with a newer schema version. These negative-path checks are pending the next PostgreSQL 18 hosted run.

## Verified PostgreSQL task operations

The current `PgTaskRepository` vertical slice exposes async methods for atomic workflow creation, dependency-aware due-task promotion, `FOR UPDATE SKIP LOCKED` claiming, attempt creation, fenced lease heartbeat, successful completion/output persistence, expired-lease recovery, and task-run snapshots. Timestamps/deadlines use PostgreSQL's clock and absolute `TIMESTAMPTZ`, not process-local monotonic ticks. PostgreSQL 18 integration run `41365026199` verified dependency gating, fencing-token increase on reclaim, stale-worker rejection, and transactional outbox rows. It is not yet wired into the existing Tokio scheduler; durable handler failures, cancellation/timeouts, workflow terminal aggregation, outbox publishing, and full process-restart recovery remain unchecked.
