//! End-to-end tests for components generated from the checked-in v1 WIT contract.

use std::path::PathBuf;

use aetherflow_core::EntityId;
use aetherflow_wasm::{
    Capability, CapabilitySet, ExecutionLimits, PluginExecutionContext, WasmPluginRuntime,
};

fn replace_once(source: &mut String, needle: &str, replacement: &str) {
    let offset = source
        .find(needle)
        .unwrap_or_else(|| panic!("generated WAT is missing expected fragment: {needle:?}"));
    source.replace_range(offset..offset + needle.len(), replacement);
}

fn valid_component_from_checked_in_wit() -> Vec<u8> {
    let wit_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("wit");
    let mut resolve = wit_parser::Resolve::new();
    let (package, _source_map) = resolve
        .push_dir(&wit_dir)
        .expect("parse checked-in plugin WIT package");
    let world = resolve
        .select_world(&[package], None)
        .expect("select task-plugin WIT world");

    let module =
        wit_component::dummy_module(&resolve, world, wit_parser::ManglingAndAbi::Standard32);
    let mut core_wat = wasmprinter::print_bytes(&module).expect("print generated WIT core module");

    // Keep the WIT-derived import/export signatures, but replace the dummy
    // module's intentionally trapping Guest and realloc bodies.
    replace_once(&mut core_wat, "(memory (;0;) 0)", "(memory (;0;) 1)");
    replace_once(
        &mut core_wat,
        "(memory (;0;) 1)",
        "(memory (;0;) 1)\n  (global $heap (mut i32) (i32.const 1024))",
    );

    // execute takes the WIT-flattened task context/input and returns a pointer
    // to result<list<u8>, plugin-error>. Zeroed memory at 0 is Ok(empty list).
    // Core import 0 is host.log: level, message pointer, length, result pointer.
    replace_once(
        &mut core_wat,
        "    unreachable\n  )",
        "    i32.const 2\n    i32.const 64\n    i32.const 20\n    i32.const 32\n    call 0\n    i32.const 0\n  )",
    );

    // A monotonic aligned allocator handles the fixture's small host inputs.
    replace_once(
        &mut core_wat,
        "    unreachable\n  )",
        "    (local $ptr i32)\n    (local.set $ptr\n      (i32.and\n        (i32.add (global.get $heap) (i32.sub (local.get 2) (i32.const 1)))\n        (i32.sub (i32.const 0) (local.get 2))))\n    (global.set $heap (i32.add (local.get $ptr) (local.get 3)))\n    (local.get $ptr)\n  )",
    );

    let end = core_wat.rfind(')').expect("core module closing delimiter");
    core_wat.insert_str(end, "\n  (data (i32.const 64) \"AetherFlow guest log\")\n");
    let mut module = wat::parse_str(&core_wat).expect("assemble executable WIT guest module");
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

    let denied = runtime
        .execute(
            &plugin,
            &context,
            b"aetherflow-component-fixture",
            CapabilitySet::default(),
            ExecutionLimits::default(),
        )
        .expect("execute guest with default-deny host capabilities");
    assert!(denied.output.is_empty());
    assert!(denied.logs.is_empty());

    let granted = runtime
        .execute(
            &plugin,
            &context,
            b"aetherflow-component-fixture",
            CapabilitySet::from_capabilities([Capability::LogWrite]),
            ExecutionLimits::default(),
        )
        .expect("execute guest with log.write capability");
    assert!(granted.output.is_empty());
    assert_eq!(granted.logs.len(), 1);
    assert_eq!(granted.logs[0].message, "AetherFlow guest log");

    let too_small_log_limit = ExecutionLimits {
        max_log_message_bytes: 4,
        ..ExecutionLimits::default()
    };
    let bounded = runtime
        .execute(
            &plugin,
            &context,
            b"aetherflow-component-fixture",
            CapabilitySet::from_capabilities([Capability::LogWrite]),
            too_small_log_limit,
        )
        .expect("oversized host log is rejected without trapping the guest");
    assert!(bounded.logs.is_empty());
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
