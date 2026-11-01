# Development Guide

## Prerequisites

Install Rust through [rustup](https://rustup.rs/). The repository pins Rust 1.99.1 in `rust-toolchain.toml`; rustup selects it automatically in this repository. The pinned toolchain includes `rustfmt` and `clippy`.

## Useful commands

```bash
rustc --version
cargo --version
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo doc --workspace --no-deps
```

## Start the bootstrap health server

```bash
cargo run -p aetherflow-server
```

Then request `http://127.0.0.1:8080/health`. Change the bind address with `AETHERFLOW_BIND`, for example:

```bash
AETHERFLOW_BIND=0.0.0.0:8080 cargo run -p aetherflow-server
```

The current server is intentionally a minimal standard-library bootstrap. It handles connections sequentially and is not hardened or intended for production. The production async HTTP stack will be introduced after execution contracts are defined.

## Workspace layout

- `crates/aetherflow-core`: infrastructure-independent primitives.
- `apps/aetherflow-server`: minimal process and bootstrap health endpoint.

Keep domain rules out of network adapters. Add dependencies only when a milestone has a concrete need and record material architecture changes in an ADR.

## Verification honesty

A CI workflow is committed, but its first hosted run must complete successfully before the foundation milestone can be marked verified. A lockfile is committed; regenerate it with Cargo whenever dependencies change.
