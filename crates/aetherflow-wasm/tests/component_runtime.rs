//! End-to-end tests for components generated from the checked-in v1 WIT contract.

use std::path::PathBuf;

use aetherflow_core::EntityId;
use aetherflow_wasm::{CapabilitySet, ExecutionLimits, PluginExecutionContext, WasmPluginRuntime};

fn valid_component_from_checked_in_wit() -> Vec<u8> {
    let wit_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("wit");
    let mut resolve = wit_parser::Resolve::new();
    let (package, _source_map) = resolve
        .push_dir(&wit_dir)
        .expect("parse checked-in plugin WIT package");
    let world = resolve
        .select_world(&[package], None)
        .expect("select task-plugin WIT world");

    let mut module =
        wit_component::dummy_module(&resolve, world, wit_parser::ManglingAndAbi::Standard32);
    let printable = wasmprinter::print_bytes(&module).expect("print generated WIT core module");
    println!("AETHERFLOW_WIT_DUMMY_MODULE_BEGIN\n{printable}\nAETHERFLOW_WIT_DUMMY_MODULE_END");
    wit_component::embed_component_metadata(
        &mut module,
        &resolve,
        world,
        wit_component::StringEncoding::UTF8,
    )
    .expect("embed component type metadata");

    wit_component::ComponentEncoder::default()
        .module(&module)
        .expect("register WIT-conforming guest core module")
        .validate(true)
        .encode()
        .expect("encode and validate guest component")
}

#[test]
fn checked_in_wit_component_compiles_instantiates_and_executes() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = valid_component_from_checked_in_wit();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile valid v1 component");

    let context = PluginExecutionContext::new(
        EntityId::parse("workflow-run-fixture").expect("valid workflow run id"),
        EntityId::parse("task-run-fixture").expect("valid task run id"),
        1,
        None,
    )
    .expect("valid plugin execution context");

    let one_instance_limits = ExecutionLimits {
        max_core_instances: 1,
        ..ExecutionLimits::default()
    };
    assert!(matches!(
        runtime.execute(
            &plugin,
            &context,
            b"aetherflow-component-fixture",
            CapabilitySet::default(),
            one_instance_limits,
        ),
        Err(aetherflow_wasm::PluginRuntimeError::Instantiation(_))
    ));

    let result = runtime
        .execute(
            &plugin,
            &context,
            b"aetherflow-component-fixture",
            CapabilitySet::default(),
            ExecutionLimits::default(),
        )
        .expect("execute WIT-conforming component generated for ABI inspection");

    // The generated dummy guest deliberately returns the ABI's default empty
    // byte list. The purpose of this fixture is to cross the real component
    // boundary, including canonical ABI lowering/lifting and the host linker.
    assert!(result.output.is_empty());
    assert!(result.logs.is_empty());
}

#[test]
fn checked_in_wit_fixture_is_rejected_before_compilation_when_oversized() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let oversized = vec![0; aetherflow_wasm::MAX_PLUGIN_ARTIFACT_BYTES + 1];

    assert!(matches!(
        runtime.compile_component(&oversized),
        Err(aetherflow_wasm::PluginRuntimeError::ArtifactTooLarge {
            actual,
            maximum,
        }) if actual == aetherflow_wasm::MAX_PLUGIN_ARTIFACT_BYTES + 1
            && maximum == aetherflow_wasm::MAX_PLUGIN_ARTIFACT_BYTES
    ));
}
