# ADR-0009: Versioned WebAssembly plugin ABI and capability boundary

- **Status:** Accepted
- **Date:** 2026-11-02
- **Decision owners:** AetherFlow maintainers
- **Related milestone:** M06

## Context

AetherFlow needs a stable extension boundary without allowing plugin code to inherit the worker's filesystem, network, environment, credentials, database, or process privileges. The existing runtime accepts opaque byte payloads capped at 1 MiB. The plugin contract must preserve that boundary, remain independent from Wasmtime's Rust API, and make capability grants explicit per invocation.

The workspace currently pins Rust 1.99.1 but declares Rust 1.85 as its package MSRV. Wasmtime 48 is the current LTS line; its LTS policy provides 24 months of support and keeps the branch MSRV fixed. The 48.0.1 manifest declares Rust 1.95.0, so adopting this line requires raising the workspace MSRV to 1.95.0 in the same change that adds the engine dependency.

## Decision

### ABI and compatibility

1. Use the WebAssembly Component Model and WIT as the guest/host contract.
2. Publish the v1 package as `aetherflow:plugin@1.0.0`, with one world named `task-plugin`.
3. The guest exports `guest.execute(context, input) -> result<output, plugin-error>`.
4. The host provides a single `host.log` import. ABI v1 has no other host imports.
5. A host supports a plugin when the ABI major versions match and the host minor version is greater than or equal to the plugin's required minor version. A breaking contract change requires a new major version.
6. The ABI uses WIT primitive/record/list types only. Rust structs, internal domain enums, database rows, and Wasmtime types are never part of the public ABI.
7. Input and output remain opaque byte arrays, each capped at 1 MiB. Guest errors carry a stable code and bounded message; the guest cannot choose whether an error is retryable.

### Capabilities

- Capability identifiers are stable, namespaced strings. ABI v1 defines only `log.write`.
- Capability sets are explicit per invocation and default to empty (default deny).
- A host call must check the invocation's capability set and enforce message-size/rate limits before performing the operation.
- ABI v1 does not import WASI and does not expose filesystem, network, environment variables, secrets, process creation, clocks, database access, or task submission.
- Future capabilities require an explicit host interface, authorization policy, resource budget, tests, and an ADR update. They are not silently granted by adding an import.

### Engine and isolation

1. Use Wasmtime **48.0.8** with default features disabled and only the Component Model, WIT parser/bindgen, runtime, standard-library, and Cranelift features enabled.
2. Use the 48.x LTS line rather than the newest non-LTS release; update to patched 48.x releases deliberately and review advisories before each upgrade.
3. Add the Wasmtime dependency with an explicit exact version. Raise the workspace `rust-version` to **1.95.0** in that same commit; the pinned Rust 1.99.1 toolchain already satisfies it.
4. Do not add `wasmtime-wasi` in ABI v1. Components receive no ambient WASI capabilities.
5. Compile untrusted component bytes through the safe validation/compilation path. Never use unsafe deserialization of precompiled artifacts for untrusted plugins.
6. Create a fresh Store and component instance per invocation. Share the Engine and immutable compiled component artifacts only after cache-key and trust rules are defined.
7. Enforce instruction fuel, host-call fuel, linear-memory/table limits, and a wall-clock deadline that actively interrupts execution. A Tokio timeout alone is insufficient if the guest is running synchronously on a blocking thread.
8. Run synchronous guest execution off Tokio's core executor threads. Cancellation and deadline expiry must map to deterministic host errors and must stop guest execution, not merely stop awaiting its result.

### Initial resource ceilings

| Resource | Default | Hard v1 ceiling |
|---|---:|---:|
| Input payload | 1 MiB | 1 MiB |
| Output payload | 1 MiB | 1 MiB |
| Linear memory | 16 MiB | 64 MiB |
| Core WebAssembly instances per Store | 4 | 16 |
| Table elements | 1,024 | 10,000 |
| Wasmtime instruction fuel | 10,000,000 | 100,000,000 |
| Guest-to-host host-call fuel (per call) | 4 KiB | 16 KiB |
| Component-model handles | 1,024 | 10,000 |
| Wall-clock execution | 2 s | 10 s |
| One log message | 4 KiB | 4 KiB |
| Log calls per invocation | 64 | 256 |

These are host policy limits, not guarantees about total process RSS. Wasmtime's Store resource limiter controls guest-created memories, tables, and core module instances; the embedder must also bound host allocations and concurrency.

## Consequences

- The public ABI is language-neutral and versioned independently from Rust crate versions.
- Plugins are pure task transformers in v1 except for explicitly granted bounded logging.
- The default-deny capability model keeps network, secrets, filesystem, and database integration out of the initial attack surface.
- Wasmtime's 48.x LTS choice increases the workspace MSRV from 1.85 to 1.95 when engine integration lands.
- The component model introduces a WIT toolchain and additional compile cost; features must remain explicitly scoped.
- Fuel, host-call fuel, Store limits, and an active wall-clock interrupt are all required. None substitutes for the others.
- WASM is a strong application-level sandbox boundary, not a substitute for OS-level isolation against engine vulnerabilities or native host bugs.

## Alternatives considered

- **Raw core-Wasm function ABI:** rejected because manually versioning pointer/length conventions and memory ownership would create an avoidable custom ABI.
- **WASI Preview 2/3 in v1:** rejected because it would introduce broad ambient interfaces before AetherFlow has a mature authorization/tenant model.
- **Latest non-LTS Wasmtime major:** rejected for a production-oriented platform because its regular support window is two months rather than the LTS 24-month window.
- **Wasmtime default features:** rejected because the default feature set enables unrelated facilities and increases build surface; select only features required by the component runtime.

## References

- [Wasmtime release and LTS policy](https://docs.wasmtime.dev/stability-release.html)
- [Wasmtime 48.0.8 release notes](https://github.com/bytecodealliance/wasmtime/releases/tag/v48.0.8)
- [Wasmtime Store fuel API](https://docs.wasmtime.dev/api/wasmtime/struct.Store.html)
- [Wasmtime Store resource limits](https://docs.wasmtime.dev/api/wasmtime/struct.StoreLimitsBuilder.html)
- [AetherFlow plugin WIT contract](../../crates/aetherflow-wasm/wit/plugin.wit)
