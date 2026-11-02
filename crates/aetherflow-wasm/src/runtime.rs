//! Wasmtime component runtime with per-invocation resource and capability limits.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, Trap};

use crate::{
    Capability, CapabilitySet, ExecutionLimits, LimitConfigurationError, MAX_PLUGIN_ARTIFACT_BYTES,
    PayloadTooLarge, PluginExecutionContext,
};

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
        config.wasm_memory64(false);
        config.wasm_multi_memory(false);

        let engine =
            Engine::new(&config).map_err(|error| PluginRuntimeError::Engine(error.to_string()))?;
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
    pub fn compile_component(&self, artifact: &[u8]) -> Result<CompiledPlugin, PluginRuntimeError> {
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

    /// Executes one task in a fresh Store and component instance.
    ///
    /// This method is synchronous and must be called from a blocking worker
    /// thread, never directly on a Tokio executor thread. Fuel and epoch
    /// interruption actively stop guest execution when either budget expires.
    pub fn execute(
        &self,
        plugin: &CompiledPlugin,
        context: &PluginExecutionContext,
        input: &[u8],
        capabilities: CapabilitySet,
        limits: ExecutionLimits,
    ) -> Result<PluginExecutionResult, PluginRuntimeError> {
        limits
            .validate()
            .map_err(PluginRuntimeError::InvalidLimits)?;
        limits
            .validate_input(input)
            .map_err(PluginRuntimeError::InputTooLarge)?;

        let host_state = HostState::new(capabilities, limits);
        let mut store = Store::new(&self.engine, host_state);
        store.limiter(|state| &mut state.resource_limits);
        store
            .set_fuel(limits.fuel)
            .map_err(|error| PluginRuntimeError::Wasmtime(error.to_string()))?;
        store.set_hostcall_fuel(limits.hostcall_fuel);
        store.set_max_component_handles(limits.max_component_handles);
        store.set_epoch_deadline(epoch_ticks_for(limits.wall_clock_timeout));

        let mut linker = Linker::new(&self.engine);
        bindings::TaskPlugin::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state)
            .map_err(|error| PluginRuntimeError::Linker(error.to_string()))?;

        let bindings = bindings::TaskPlugin::instantiate(&mut store, &plugin.component, &linker)
            .map_err(|error| match map_wasmtime_error(error) {
                PluginRuntimeError::Wasmtime(message) => PluginRuntimeError::Instantiation(message),
                other => other,
            })?;

        let wit_context = bindings::aetherflow::plugin::types::TaskContext {
            workflow_run_id: context.workflow_run_id.as_str().to_owned(),
            task_run_id: context.task_run_id.as_str().to_owned(),
            attempt: context.attempt,
            deadline_unix_ms: context.deadline_unix_ms,
        };

        let guest_result = bindings
            .aetherflow_plugin_guest()
            .call_execute(&mut store, &wit_context, input)
            .map_err(map_wasmtime_error)?;

        let output = guest_result.map_err(|error| PluginRuntimeError::GuestError {
            code: error.code,
            message: error.message,
        })?;
        limits
            .validate_output(&output)
            .map_err(PluginRuntimeError::OutputTooLarge)?;

        let host_state = store.into_data();
        Ok(PluginExecutionResult {
            output,
            logs: host_state.logs,
        })
    }
}

/// A validated component compiled by one AetherFlow runtime instance.
pub struct CompiledPlugin {
    component: Component,
}

/// The result of a successful plugin invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginExecutionResult {
    /// Opaque guest output payload.
    pub output: Vec<u8>,
    /// Bounded diagnostic messages emitted through the granted host capability.
    pub logs: Vec<PluginLog>,
}

/// One accepted plugin log message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLog {
    /// Guest-selected diagnostic severity.
    pub level: PluginLogLevel,
    /// UTF-8 log message.
    pub message: String,
}

/// Stable host-side representation of plugin log severity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginLogLevel {
    /// Trace diagnostic.
    Trace,
    /// Debug diagnostic.
    Debug,
    /// Informational diagnostic.
    Info,
    /// Warning diagnostic.
    Warn,
    /// Error diagnostic.
    Error,
}

struct HostState {
    capabilities: CapabilitySet,
    limits: ExecutionLimits,
    resource_limits: StoreLimits,
    logs: Vec<PluginLog>,
    log_calls: u32,
}

impl HostState {
    fn new(capabilities: CapabilitySet, limits: ExecutionLimits) -> Self {
        let resource_limits = StoreLimitsBuilder::new()
            .memory_size(limits.max_memory_bytes)
            .table_elements(limits.max_table_elements)
            .instances(limits.max_core_instances)
            .memories(1)
            .tables(1)
            .build();

        Self {
            capabilities,
            limits,
            resource_limits,
            logs: Vec::new(),
            log_calls: 0,
        }
    }
}

// The shared WIT types interface is imported as an empty host interface.
impl bindings::aetherflow::plugin::types::Host for HostState {}

impl bindings::aetherflow::plugin::host::Host for HostState {
    fn log(
        &mut self,
        level: bindings::aetherflow::plugin::types::LogLevel,
        message: String,
    ) -> Result<(), bindings::aetherflow::plugin::types::HostError> {
        use bindings::aetherflow::plugin::types::{HostError, LogLevel};

        if !self.capabilities.allows(Capability::LogWrite) {
            return Err(HostError::CapabilityDenied);
        }
        if message.len() > self.limits.max_log_message_bytes {
            return Err(HostError::MessageTooLarge);
        }
        if self.log_calls >= self.limits.max_log_messages {
            return Err(HostError::RateLimited);
        }

        self.log_calls += 1;
        let level = match level {
            LogLevel::Trace => PluginLogLevel::Trace,
            LogLevel::Debug => PluginLogLevel::Debug,
            LogLevel::Info => PluginLogLevel::Info,
            LogLevel::Warn => PluginLogLevel::Warn,
            LogLevel::Error => PluginLogLevel::Error,
        };
        self.logs.push(PluginLog { level, message });
        Ok(())
    }
}

/// Errors produced while creating the engine or executing a component.
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
        /// Maximum permitted size.
        maximum: usize,
    },
    /// Wasmtime rejected the component binary or its validation.
    InvalidComponent(String),
    /// The component did not match the host's imported/exported WIT world.
    Linker(String),
    /// Component instantiation failed.
    Instantiation(String),
    /// A host-selected resource budget exceeded the v1 hard ceiling.
    InvalidLimits(LimitConfigurationError),
    /// The input payload exceeded its configured limit.
    InputTooLarge(PayloadTooLarge),
    /// The output payload exceeded its configured limit.
    OutputTooLarge(PayloadTooLarge),
    /// Guest code exhausted its deterministic instruction budget.
    FuelExhausted,
    /// Guest code exceeded its wall-clock execution deadline.
    DeadlineExceeded,
    /// Guest allocation exceeded a Wasmtime resource ceiling.
    MemoryLimitExceeded,
    /// The guest returned its own typed error.
    GuestError {
        /// Stable guest error code.
        code: String,
        /// Bounded guest error message.
        message: String,
    },
    /// A Wasmtime execution error that is not one of the classified traps.
    Wasmtime(String),
    /// A guest trap unrelated to a configured resource budget.
    Trap(String),
}

impl fmt::Display for PluginRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(message) => {
                write!(formatter, "failed to create Wasmtime engine: {message}")
            }
            Self::EpochTicker(message) => {
                write!(
                    formatter,
                    "failed to start Wasmtime epoch ticker: {message}"
                )
            }
            Self::ArtifactTooLarge { actual, maximum } => write!(
                formatter,
                "plugin artifact is {actual} bytes; maximum is {maximum} bytes"
            ),
            Self::InvalidComponent(message) => {
                write!(formatter, "plugin component validation failed: {message}")
            }
            Self::Linker(message) => write!(formatter, "plugin ABI linker setup failed: {message}"),
            Self::Instantiation(message) => {
                write!(
                    formatter,
                    "plugin component instantiation failed: {message}"
                )
            }
            Self::InvalidLimits(error) => write!(formatter, "invalid plugin limits: {error}"),
            Self::InputTooLarge(error) => write!(formatter, "invalid plugin input: {error}"),
            Self::OutputTooLarge(error) => write!(formatter, "invalid plugin output: {error}"),
            Self::FuelExhausted => formatter.write_str("plugin instruction fuel exhausted"),
            Self::DeadlineExceeded => formatter.write_str("plugin execution deadline exceeded"),
            Self::MemoryLimitExceeded => formatter.write_str("plugin memory limit exceeded"),
            Self::GuestError { code, message } => {
                write!(formatter, "plugin returned error {code}: {message}")
            }
            Self::Wasmtime(message) => write!(formatter, "Wasmtime execution failed: {message}"),
            Self::Trap(message) => write!(formatter, "plugin trapped: {message}"),
        }
    }
}

impl Error for PluginRuntimeError {}

fn map_wasmtime_error(error: wasmtime::Error) -> PluginRuntimeError {
    if let Some(trap) = error.downcast_ref::<Trap>() {
        return match trap {
            Trap::OutOfFuel => PluginRuntimeError::FuelExhausted,
            Trap::Interrupt => PluginRuntimeError::DeadlineExceeded,
            Trap::AllocationTooLarge => PluginRuntimeError::MemoryLimitExceeded,
            other => PluginRuntimeError::Trap(other.to_string()),
        };
    }

    PluginRuntimeError::Wasmtime(error.to_string())
}

fn epoch_ticks_for(timeout: Duration) -> u64 {
    let tick_ms = EPOCH_TICK.as_millis();
    let timeout_ms = timeout.as_millis();
    let ticks = timeout_ms.div_ceil(tick_ms) + 1;
    u64::try_from(ticks).unwrap_or(u64::MAX)
}

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
    use super::{HostState, PluginRuntimeError, WasmPluginRuntime, bindings};
    use crate::{Capability, CapabilitySet, ExecutionLimits, MAX_PLUGIN_ARTIFACT_BYTES};

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

    #[test]
    fn host_log_is_default_deny_without_an_explicit_capability() {
        use bindings::aetherflow::plugin::host::Host;
        use bindings::aetherflow::plugin::types::{HostError, LogLevel};

        let mut state = HostState::new(CapabilitySet::default(), ExecutionLimits::default());
        let result = state.log(LogLevel::Info, "guest diagnostic".to_owned());

        assert_eq!(result, Err(HostError::CapabilityDenied));
        assert!(state.logs.is_empty());
        assert_eq!(state.log_calls, 0);
    }

    #[test]
    fn host_log_enforces_utf8_byte_limit_and_per_invocation_rate_limit() {
        use bindings::aetherflow::plugin::host::Host;
        use bindings::aetherflow::plugin::types::{HostError, LogLevel};

        let capabilities = CapabilitySet::from_capabilities([Capability::LogWrite]);
        let limits = ExecutionLimits {
            max_log_message_bytes: 3,
            max_log_messages: 1,
            ..ExecutionLimits::default()
        };
        let mut state = HostState::new(capabilities, limits);

        assert_eq!(state.log(LogLevel::Info, "ok".to_owned()), Ok(()));
        assert_eq!(
            state.log(LogLevel::Warn, "éé".to_owned()),
            Err(HostError::MessageTooLarge)
        );
        assert_eq!(
            state.log(LogLevel::Error, "x".to_owned()),
            Err(HostError::RateLimited)
        );
        assert_eq!(state.logs.len(), 1);
        assert_eq!(state.logs[0].message, "ok");
        assert_eq!(state.log_calls, 1);
    }

    #[test]
    fn deadline_tick_budget_rounds_up_and_adds_scheduling_slack() {
        assert_eq!(
            super::epoch_ticks_for(std::time::Duration::from_millis(1)),
            2
        );
        assert_eq!(
            super::epoch_ticks_for(std::time::Duration::from_millis(10)),
            2
        );
        assert_eq!(
            super::epoch_ticks_for(std::time::Duration::from_millis(11)),
            3
        );
    }

    #[test]
    fn store_limiter_enforces_guest_table_growth_ceiling() {
        let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
        let wasm = wat::parse_str(
            r#"(module
                (table 1 2 funcref)
                (func (export "grow") (result i32)
                    ref.null func
                    i32.const 1
                    table.grow 0
                )
            )"#,
        )
        .expect("assemble table-growth probe");
        let module =
            wasmtime::Module::new(&runtime.engine, &wasm).expect("compile table-growth probe");

        let grow_once = |max_table_elements| {
            let limits = ExecutionLimits {
                max_table_elements,
                ..ExecutionLimits::default()
            };
            let state = HostState::new(CapabilitySet::default(), limits);
            let mut store = wasmtime::Store::new(&runtime.engine, state);
            store.limiter(|state| &mut state.resource_limits);
            store
                .set_fuel(limits.fuel)
                .expect("set table-growth probe fuel");
            // The API adds this relative budget to the current epoch; avoid overflow.
            store.set_epoch_deadline(1_000);

            let instance = wasmtime::Instance::new(&mut store, &module, &[])
                .expect("instantiate table-growth probe");
            let grow = instance
                .get_typed_func::<(), i32>(&mut store, "grow")
                .expect("get table-growth export");
            grow.call(&mut store, ())
                .expect("execute table-growth probe")
        };

        assert_eq!(
            grow_once(1),
            -1,
            "table.grow must fail when the configured ceiling equals the initial size"
        );
        assert_eq!(
            grow_once(2),
            1,
            "table.grow must succeed when one additional element is within the ceiling"
        );
    }
}
