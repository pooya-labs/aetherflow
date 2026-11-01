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

The current server is intentionally a minimal standard-library bootstrap. It handles connections sequentially and is not hardened or intended for production. The runtime scheduler uses Tokio. It supports cooperative handler cancellation and bounded two-stage shutdown (30-second graceful drain plus 5-second cancellation grace by default; both are configurable). The server's network stack remains a sequential standard-library bootstrap and is not production-ready.

## Workspace layout

- `crates/aetherflow-core`: infrastructure-independent domain primitives.
- `crates/aetherflow-runtime`: single-node runtime contracts, in-memory repository, bounded async scheduler, DAG validation, and workflow-run coordinator.
- `apps/aetherflow-server`: minimal process and bootstrap health endpoint.

Task inputs and outputs are opaque byte payloads limited to 1 MiB each; applications own serialization and schema versions. WorkflowTask supports a static input payload, while automatic predecessor-output mapping is not implemented yet. Keep domain rules out of runtime and network adapters. The initial in-memory repository uses one short-held mutex and deterministic caller-supplied timestamps; never hold repository locks across an `.await`. It is a test/runtime adapter, not durable storage. Add dependencies only when a milestone has a concrete need and record material architecture changes in an ADR.

## Verification honesty

A CI workflow is committed, but its first hosted run must complete successfully before the foundation milestone can be marked verified. A lockfile is committed; regenerate it with `cargo generate-lockfile` whenever dependencies change, then verify with `cargo fetch --locked`. CI runs every check with locked dependency resolution and publishes the resolved lockfile as a short-lived artifact for review when the dependency graph changes. The latest fully green workflow coordinator run is `41356396516` (42 unit tests, release build, and docs).
