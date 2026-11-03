use std::error::Error;
use std::fs;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aetherflow_core::domain::{
    RetryPolicy, TaskId, WorkflowId, WorkflowRunId, WorkflowStatus, WorkerId,
};
use aetherflow_core::EntityId;
use aetherflow_runtime::clock::MonotonicClock;
use aetherflow_runtime::repository::{InMemoryRepository, TaskInput};
use aetherflow_runtime::scheduler::{SchedulerConfig, TaskRegistry, TaskScheduler};
use aetherflow_runtime::workflow::{
    SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask,
};
use aetherflow_runtime::WasmTaskHandler;
use aetherflow_wasm::{CapabilitySet, ExecutionLimits, PluginExecutionContext, WasmPluginRuntime};
use wit_component::ComponentEncoder;

async fn run() -> Result<(), Box<dyn Error>> {
    let guest_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: aetherflow-plugin-host <guest-core-module.wasm>",
            )
        })?;
    let guest_module = fs::read(&guest_path)?;

    // wit-bindgen embeds WIT component-type metadata in the core module.
    // Validate that metadata and create a component without adding any
    // WASI adapter or ambient interface.
    let component_bytes = ComponentEncoder::default()
        .module(&guest_module)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?
        .validate(true)
        .encode()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

    let runtime = Arc::new(WasmPluginRuntime::new()?);
    let plugin = Arc::new(runtime.compile_component(&component_bytes)?);
    let workflow_context = EntityId::parse("workflow-run-plugin-direct")
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let task_context = EntityId::parse("task-run-plugin-direct")
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let plugin_context = PluginExecutionContext::new(workflow_context, task_context, 1, None)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let input = b"AetherFlow ABI v1 example";

    // Direct Wasmtime calls are synchronous and therefore stay off the Tokio
    // executor. This invocation verifies the default-deny capability boundary.
    let denied_runtime = Arc::clone(&runtime);
    let denied_plugin = Arc::clone(&plugin);
    let denied = tokio::task::spawn_blocking(move || {
        denied_runtime.execute(
            &denied_plugin,
            &plugin_context,
            input,
            CapabilitySet::default(),
            ExecutionLimits::default(),
        )
    })
    .await??;
    ensure(
        denied.output.as_slice() == input,
        "guest output changed when log.write was denied",
    )?;
    ensure(
        denied.logs.is_empty(),
        "default-deny invocation unexpectedly emitted a host log",
    )?;

    // Exercise the real integration seam: a TaskRegistry handler is claimed
    // by TaskScheduler, dispatched through WasmTaskHandler, and its output is
    // committed to the in-memory task repository.
    let task_id = TaskId::parse("plugin-echo").expect("valid task identifier");
    let captured_logs = Arc::new(Mutex::new(Vec::<String>::new()));
    let log_sink = Arc::clone(&captured_logs);
    let handler = WasmTaskHandler::new(
        Arc::clone(&runtime),
        Arc::clone(&plugin),
        aetherflow_wasm::CapabilitySet::from_capabilities([aetherflow_wasm::Capability::LogWrite]),
        ExecutionLimits::default(),
    )?
    .with_log_sink(move |log| {
        log_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(log.message);
    });

    let mut registry = TaskRegistry::new();
    registry.register(task_id.clone(), handler)?;

    let retry_policy = RetryPolicy::new(NonZeroU32::new(1).expect("one attempt"), 0, 0)?;
    let scheduler_config = SchedulerConfig::new(
        NonZeroUsize::new(1).expect("one worker slot"),
        Duration::from_millis(1),
        Some(Duration::from_secs(5)),
        retry_policy,
    )?;
    let repository = Arc::new(InMemoryRepository::new());
    let scheduler = TaskScheduler::new(
        repository.clone(),
        Arc::new(MonotonicClock::new()),
        WorkerId::parse("plugin-example-worker").expect("valid worker identifier"),
        registry,
        scheduler_config,
    );
    let workflow = WorkflowDefinition::new(
        WorkflowId::parse("plugin-example-workflow").expect("valid workflow identifier"),
        vec![
            WorkflowTask::new(task_id, Vec::new())
                .with_input(TaskInput::new(input.to_vec())?),
        ],
    )?;
    let id_generator = SequenceTaskRunIdGenerator::new();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let result = scheduler
        .execute_workflow(
            workflow,
            WorkflowRunId::parse("plugin-example-run").expect("valid workflow-run identifier"),
            &id_generator,
            shutdown_receiver,
        )
        .await?;
    drop(shutdown_sender);

    ensure(
        result.status() == WorkflowStatus::Succeeded,
        "WASM-backed workflow did not succeed",
    )?;
    ensure(
        result.task_runs().len() == 1,
        "WASM-backed workflow did not materialize exactly one task",
    )?;
    let task_output = result.task_runs()[0]
        .output()
        .ok_or_else(|| io::Error::other("successful WASM task has no stored output"))?;
    ensure(
        task_output.as_bytes() == input,
        "WASM task output was not persisted unchanged",
    )?;

    let logs = captured_logs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ensure(
        logs.len() == 1 && logs[0].contains("attempt 1 echoed"),
        "TaskRegistry execution did not forward the granted guest log",
    )?;

    println!(
        "ABI v1 plugin passed: workflow=succeeded; output={} bytes; default-deny logs={}; scheduler logs={}",
        task_output.len(),
        denied.logs.len(),
        logs.len()
    );
    Ok(())
}

fn ensure(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("plugin example failed: {error:#}");
        std::process::exit(1);
    }
}
