use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

use aetherflow_core::EntityId;
use aetherflow_wasm::{
    Capability, CapabilitySet, ExecutionLimits, PluginExecutionContext, WasmPluginRuntime,
};
use wit_component::ComponentEncoder;

fn run() -> Result<(), Box<dyn Error>> {
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

    let runtime = WasmPluginRuntime::new()?;
    let plugin = runtime.compile_component(&component_bytes)?;
    let workflow_id = EntityId::parse("workflow-run-plugin-example")
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let task_id = EntityId::parse("task-run-plugin-example")
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let context = PluginExecutionContext::new(workflow_id, task_id, 1, None)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let input = b"AetherFlow ABI v1 example";

    let denied = runtime.execute(
        &plugin,
        &context,
        input,
        CapabilitySet::default(),
        ExecutionLimits::default(),
    )?;
    ensure(
        denied.output.as_slice() == input,
        "guest output changed when log.write was denied",
    )?;
    ensure(
        denied.logs.is_empty(),
        "default-deny invocation unexpectedly emitted a host log",
    )?;

    let granted = runtime.execute(
        &plugin,
        &context,
        input,
        CapabilitySet::from_capabilities([Capability::LogWrite]),
        ExecutionLimits::default(),
    )?;
    ensure(
        granted.output.as_slice() == input,
        "guest output changed when log.write was granted",
    )?;
    ensure(
        granted.logs.len() == 1 && granted.logs[0].message.contains("attempt 1 echoed"),
        "explicit log.write grant did not produce the expected bounded guest log",
    )?;

    println!(
        "ABI v1 plugin passed: output={} bytes; default-deny logs={}; granted logs={}",
        granted.output.len(),
        denied.logs.len(),
        granted.logs.len()
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

fn main() {
    if let Err(error) = run() {
        eprintln!("plugin example failed: {error:#}");
        std::process::exit(1);
    }
}
