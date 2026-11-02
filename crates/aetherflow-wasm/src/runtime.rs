//! Wasmtime engine configuration and safe component compilation.
//!
//! Execution and host-call wiring are added in the following M06 slices. This
//! first runtime layer fixes the engine feature set, compiles untrusted
//! components through Wasmtime's validating API, and owns the epoch ticker
//! required for enforceable wall-clock interruption.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use wasmtime::component::Component;
use wasmtime::{Config, Engine};

use crate::MAX_PLUGIN_ARTIFACT_BYTES;

#[allow(dead_code)]
mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "task-plugin",
    });
}

const EPOCH_TICK: Duration = Duration::from_millis(10);

/// A configured Wasmtime engine for AetherFlow plugin components.
pub struct WasmPluginRuntime {
    engine: Engine,
    _epoch_ticker: EpochTicker,
}

impl WasmPluginRuntime {
    /// Creates an engine with the v1 component-model security profile.
    ///
    /// Fuel and epoch interruption are enabled centrally so callers cannot
    /// accidentally instantiate an unbounded Store using this runtime.
    pub fn new() -> Result<Self, PluginRuntimeError> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.consume_fuel(true);
        config.epoch_interruption(true);
        config.wasm_threads(false);
        config.wasm_memory64(false);
        config.wasm_multi_memory(false);

        let engine = Engine::new(&config)
            .map_err(|error| PluginRuntimeError::Engine(error.to_string()))?;
        let epoch_ticker = EpochTicker::new(engine.clone())?;

        Ok(Self {
            engine,
            _epoch_ticker: epoch_ticker,
        })
    }

    /// Validates and compiles an untrusted component from its original bytes.
    ///
    /// This deliberately does not use unsafe deserialization of cached native
    /// artifacts. A component must still match the generated v1 WIT bindings
    /// when it is instantiated by the host linker.
    pub fn compile_component(
        &self,
        artifact: &[u8],
    ) -> Result<CompiledPlugin, PluginRuntimeError> {
        if artifact.len() > MAX_PLUGIN_ARTIFACT_BYTES {
            return Err(PluginRuntimeError::ArtifactTooLarge {
                actual: artifact.len(),
                maximum: MAX_PLUGIN_ARTIFACT_BYTES,
            });
        }

        let component = Component::new(&self.engine, artifact)
            .map_err(|error| PluginRuntimeError::InvalidComponent(error.to_string()))?;

        Ok(CompiledPlugin { component })
    }
}

/// A validated component compiled by one AetherFlow runtime instance.
pub struct CompiledPlugin {
    // Kept private so callers cannot bypass host linker policy or create an
    // unbounded Store. The execution layer consumes this handle internally.
    #[allow(dead_code)]
    component: Component,
}

/// Errors produced while creating the engine or validating a component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginRuntimeError {
    /// Wasmtime engine configuration failed.
    Engine(String),
    /// The epoch-interruption ticker could not be started.
    EpochTicker(String),
    /// The component artifact exceeded the hard byte ceiling.
    ArtifactTooLarge {
        /// Actual artifact size.
        actual: usize,
        /// Maximum permitted artifact size.
        maximum: usize,
    },
    /// Wasmtime rejected the component binary or its validation.
    InvalidComponent(String),
}

impl fmt::Display for PluginRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(message) => write!(formatter, "failed to create Wasmtime engine: {message}"),
            Self::EpochTicker(message) => {
                write!(formatter, "failed to start Wasmtime epoch ticker: {message}")
            }
            Self::ArtifactTooLarge { actual, maximum } => write!(
                formatter,
                "plugin artifact is {actual} bytes; maximum is {maximum} bytes"
            ),
            Self::InvalidComponent(message) => {
                write!(formatter, "plugin component validation failed: {message}")
            }
        }
    }
}

impl Error for PluginRuntimeError {}

struct EpochTicker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl EpochTicker {
    fn new(engine: Engine) -> Result<Self, PluginRuntimeError> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let ticker = thread::Builder::new()
            .name("aetherflow-wasm-epoch".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(EPOCH_TICK);
                    if !thread_stop.load(Ordering::Acquire) {
                        engine.increment_epoch();
                    }
                }
            })
            .map_err(|error| PluginRuntimeError::EpochTicker(error.to_string()))?;

        Ok(Self {
            stop,
            thread: Some(ticker),
        })
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(ticker) = self.thread.take() {
            let _ = ticker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PluginRuntimeError, WasmPluginRuntime};
    use crate::MAX_PLUGIN_ARTIFACT_BYTES;

    #[test]
    fn runtime_rejects_invalid_component_bytes() {
        let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
        assert!(matches!(
            runtime.compile_component(b"not a WebAssembly component"),
            Err(PluginRuntimeError::InvalidComponent(_))
        ));
    }

    #[test]
    fn rejects_oversized_artifacts_before_compilation() {
        let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
        let artifact = vec![0; MAX_PLUGIN_ARTIFACT_BYTES + 1];
        assert!(matches!(
            runtime.compile_component(&artifact),
            Err(PluginRuntimeError::ArtifactTooLarge {
                actual,
                maximum,
            }) if actual == MAX_PLUGIN_ARTIFACT_BYTES + 1
                && maximum == MAX_PLUGIN_ARTIFACT_BYTES
        ));
    }
}
