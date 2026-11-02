# Minimal AetherFlow ABI v1 plugin

This example contains a Rust guest and a small host harness. The guest echoes its
opaque input bytes and attempts to emit one bounded diagnostic through the
versioned `aetherflow:plugin@1.0.0` WIT interface.

The guest is compiled for `wasm32-unknown-unknown`, not a WASI target. Its only
import is the explicit ABI v1 `host.log` function. There is no WASI adapter,
filesystem, network, environment, or process capability.

## Prerequisites

Use the pinned toolchain from the repository root and install the WebAssembly
target once:

```sh
rustup toolchain install 1.99.1 --profile minimal
rustup +1.99.1 target add wasm32-unknown-unknown
```

## Build and run

Run these commands from the repository root:

```sh
cargo +1.99.1 build --locked -p aetherflow-plugin-guest \
  --target wasm32-unknown-unknown --release

cargo +1.99.1 run --locked -p aetherflow-plugin-host --release -- \
  target/wasm32-unknown-unknown/release/aetherflow_plugin_guest.wasm
```

The host harness reads the guest core module, validates and componentizes its
embedded WIT metadata with the pinned `wit-component` 0.254.2 toolchain, then
loads it through the real `WasmPluginRuntime`. It invokes the same compiled
plugin twice: once with an empty capability set (the host log is denied and no
log is recorded), and once with `log.write` explicitly granted (one log is
recorded). Both invocations must return the original input bytes unchanged.

Expected output resembles:

```text
ABI v1 plugin passed: output=25 bytes; default-deny logs=0; granted logs=1
```

The exact output length is derived from the input string in the host harness.

## Contract and security notes

- The guest uses the checked-in WIT package directly; it does not duplicate the
  ABI definition.
- `wit-bindgen` is pinned to 0.60.0. The host-side `wit-component` encoder is
  pinned to 0.254.2 to match the repository's locked WIT tooling.
- A component must match the generated `task-plugin` world. Componentization
  validates the WIT metadata before the runtime accepts the artifact.
- `log.write` is default-deny. The plugin's attempt to log is harmless when
  the host grants no capability; the host sees a record only when it opts in.
- Do not switch this guest to `wasm32-wasip1` or `wasm32-wasip2` and add an
  adapter as a shortcut. ABI v1 deliberately has no ambient WASI imports.
