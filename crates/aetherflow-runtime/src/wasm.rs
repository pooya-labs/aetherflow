//! Adapter that executes ABI v1 WebAssembly plugins as runtime task handlers.
//!
//! Wasmtime execution is synchronous, so every invocation runs on Tokio's
//! blocking pool rather than occupying an async executor thread. Guest work is
//! bounded by the supplied `ExecutionLimits`; scheduler state/version fencing
//! remains authoritative if a task is cancelled while a blocking invocation is
//! already running.

use std::sync::Arc;

use aetherflow_core::EntityId;
use aetherflow_wasm::{
    CapabilitySet, CompiledPlugin, ExecutionLimits, LimitConfigurationError, PluginExecutionContext,
    PluginLog, PluginRuntimeError, WasmPluginRuntime,
};

use crate::repository::TaskOutput;
use crate::scheduler::{TaskContext, TaskExecutionError, TaskFuture, TaskHandler};

/// Optional host-side receiver for logs explicitly granted to a plugin.
pub type PluginLogSink = Arc<dyn Fn(PluginLog) + Send + Sync + 'static>;

/// Executes one compiled WebAssembly plugin for every attempt of a task.
pub struct WasmTaskHandler {
    runtime: Arc<WasmPluginRuntime>,
    plugin: Arc<CompiledPlugin>,
    capabilities: CapabilitySet,
    limits: ExecutionLimits,
    log_sink: Option<PluginLogSink>,
}

impl WasmTaskHandler {
    /// Creates a handler after validating its per-invocation resource budget.
    ///
    /// Capabilities are explicit and default-deny; pass an empty
    /// `CapabilitySet` when the plugin should receive no host privileges.
    pub fn new(
        runtime: Arc<WasmPluginRuntime>,
        plugin: Arc<CompiledPlugin>,
        capabilities: CapabilitySet,
        limits: ExecutionLimits,
    ) -> Result<Self, LimitConfigurationError> {
        limits.validate()?;
        Ok(Self {
            runtime,
            plugin,
            capabilities,
            limits,
            log_sink: None,
        })
    }

    /// Installs a host callback for logs emitted through the granted
    /// `log.write` capability. Without a sink, accepted guest logs are ignored.
    #[must_use]
    pub fn with_log_sink(
        mut self,
        sink: impl Fn(PluginLog) + Send + Sync + 'static,
    ) -> Self {
        self.log_sink = Some(Arc::new(sink));
        self
    }
}

impl TaskHandler for WasmTaskHandler {
    fn execute(&self, context: TaskContext) -> TaskFuture {
        let runtime = Arc::clone(&self.runtime);
        let plugin = Arc::clone(&self.plugin);
        let capabilities = self.capabilities.clone();
        let limits = self.limits;
        let log_sink = self.log_sink.clone();

        Box::pin(async move {
            let claim = context.claim();
            let task_run = claim.task_run();

            // Domain IDs have already passed the same shared EntityId
            // validation at construction time; parsing here preserves the
            // WASM ABI's infrastructure-independent identifier contract.
            let workflow_run_id = EntityId::parse(task_run.workflow_run_id().as_str())
                .map_err(|error| TaskExecutionError::permanent(error.to_string()))?;
            let task_run_id = EntityId::parse(task_run.task_run_id().as_str())
                .map_err(|error| TaskExecutionError::permanent(error.to_string()))?;
            let plugin_context = PluginExecutionContext::new(
                workflow_run_id,
                task_run_id,
                claim.attempt_number().get(),
                None,
            )
            .map_err(|error| TaskExecutionError::permanent(error.to_string()))?;
            let input = context.input().as_bytes().to_vec();

            let execution = tokio::task::spawn_blocking(move || {
                runtime.execute(&plugin, &plugin_context, &input, capabilities, limits)
            })
            .await
            .map_err(|error| {
                TaskExecutionError::retryable(format!(
                    "WASM blocking worker failed to join: {error}"
                ))
            })?
            .map_err(classify_plugin_error)?;

            if let Some(sink) = log_sink {
                for log in execution.logs {
                    sink(log);
                }
            }

            TaskOutput::new(execution.output).map_err(|error| {
                TaskExecutionError::permanent(format!(
                    "WASM plugin output violates task payload limits: {error}"
                ))
            })
        })
    }
}

fn classify_plugin_error(error: PluginRuntimeError) -> TaskExecutionError {
    match error {
        PluginRuntimeError::Engine(message)
        | PluginRuntimeError::EpochTicker(message)
        | PluginRuntimeError::Wasmtime(message) => TaskExecutionError::retryable(message),
        other => TaskExecutionError::permanent(other.to_string()),
    }
}
