//! End-to-end tests for components generated from the checked-in v1 WIT contract.

use std::path::PathBuf;
use std::time::Duration;

use aetherflow_core::EntityId;
use aetherflow_wasm::{
    Capability, CapabilitySet, ExecutionLimits, MAX_PLUGIN_FUEL, PluginExecutionContext,
    PluginRuntimeError, WasmPluginRuntime,
};

fn replace_once(source: &mut String, needle: &str, replacement: &str) {
    let offset = source
        .find(needle)
        .unwrap_or_else(|| panic!("generated WAT is missing expected fragment: {needle:?}"));
    source.replace_range(offset..offset + needle.len(), replacement);
}

fn component_from_checked_in_wit(execute_body: &str, data_segments: &str) -> Vec<u8> {
    component_from_checked_in_wit_with_imports(execute_body, data_segments, "")
}

fn component_from_checked_in_wit_with_imports(
    execute_body: &str,
    data_segments: &str,
    extra_core_imports: &str,
) -> Vec<u8> {
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
    let memory_declaration = format!("{extra_core_imports}(memory (;0;) 1)");
    replace_once(
        &mut core_wat,
        "(memory (;0;) 0)",
        &memory_declaration,
    );
    replace_once(
        &mut core_wat,
        "(memory (;0;) 1)",
        "(memory (;0;) 1)\n  (global $heap (mut i32) (i32.const 1024))",
    );

    // execute takes the WIT-flattened task context/input and returns a pointer
    // to result<list<u8>, plugin-error>. Zeroed memory at 0 is Ok(empty list).
    // Core import 0 is host.log: level, message pointer, length, result pointer.
    let execute_replacement = format!("{execute_body}\n  )");
    replace_once(&mut core_wat, "    unreachable\n  )", &execute_replacement);

    // A monotonic aligned allocator handles the fixture's small host inputs.
    replace_once(
        &mut core_wat,
        "    unreachable\n  )",
        "    (local $ptr i32)\n    (local.set $ptr\n      (i32.and\n        (i32.add (global.get $heap) (i32.sub (local.get 2) (i32.const 1)))\n        (i32.sub (i32.const 0) (local.get 2))))\n    (global.set $heap (i32.add (local.get $ptr) (local.get 3)))\n    (local.get $ptr)\n  )",
    );

    let end = core_wat.rfind(')').expect("core module closing delimiter");
    core_wat.insert_str(end, data_segments);
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

fn valid_component_from_checked_in_wit() -> Vec<u8> {
    component_from_checked_in_wit(
        r#"    i32.const 2
    i32.const 64
    i32.const 20
    i32.const 32
    call 0
    i32.const 0"#,
        r#"
  (data (i32.const 64) "AetherFlow guest log")
"#,
    )
}

fn fixture_data_segment(offset: u32, bytes: &[u8]) -> String {
    let encoded = bytes
        .iter()
        .map(|byte| format!(r#"\{byte:02x}"#))
        .collect::<String>();
    format!(
        r#"  (data (i32.const {offset}) "{encoded}")
"#
    )
}

fn component_returning_output(output: &[u8]) -> Vec<u8> {
    let output_pointer = 128_u32;
    let mut result = [0_u8; 12];
    result[4..8].copy_from_slice(&output_pointer.to_le_bytes());
    result[8..12].copy_from_slice(&(output.len() as u32).to_le_bytes());

    let mut data_segments = fixture_data_segment(0, &result);
    data_segments.push_str(&fixture_data_segment(output_pointer, output));
    component_from_checked_in_wit("    i32.const 0", &data_segments)
}


fn component_reporting_memory_growth() -> Vec<u8> {
    let output_pointer = 128_u32;
    let mut result = [0_u8; 12];
    result[4..8].copy_from_slice(&output_pointer.to_le_bytes());
    result[8..12].copy_from_slice(&1_u32.to_le_bytes());

    let mut data_segments = fixture_data_segment(0, &result);
    data_segments.push_str(&fixture_data_segment(output_pointer, &[0]));

    let execute_body = r#"    i32.const 1
    memory.grow 0
    i32.const 0
    i32.lt_s
    if
      i32.const 128
      i32.const 1
      i32.store8
    else
      i32.const 128
      i32.const 0
      i32.store8
    end
    i32.const 0"#;

    component_from_checked_in_wit(execute_body, &data_segments)
}

fn component_with_ungranted_wasi_import() -> Vec<u8> {
    let extra_import = r#"  (import "wasi_snapshot_preview1" "fd_write"
    (func $wasi_fd_write (param i32 i32 i32 i32) (result i32)))
  "#;
    component_from_checked_in_wit_with_imports(
        "    i32.const 0",
        "",
        extra_import,
    )
}

fn component_returning_guest_error(code: &str, message: &str) -> Vec<u8> {
    let code_pointer = 128_u32;
    let message_pointer = 192_u32;
    let mut result = [0_u8; 20];
    result[0] = 1; // Err variant of result<list<u8>, plugin-error>.
    result[4..8].copy_from_slice(&code_pointer.to_le_bytes());
    result[8..12].copy_from_slice(&(code.len() as u32).to_le_bytes());
    result[12..16].copy_from_slice(&message_pointer.to_le_bytes());
    result[16..20].copy_from_slice(&(message.len() as u32).to_le_bytes());

    let mut data_segments = fixture_data_segment(0, &result);
    data_segments.push_str(&fixture_data_segment(code_pointer, code.as_bytes()));
    data_segments.push_str(&fixture_data_segment(message_pointer, message.as_bytes()));
    component_from_checked_in_wit("    i32.const 0", &data_segments)
}

fn component_spinning_forever() -> Vec<u8> {
    component_from_checked_in_wit(
        r#"    (loop $forever
      br $forever
    )
    i32.const 0"#,
        "",
    )
}

fn fixture_context() -> PluginExecutionContext {
    PluginExecutionContext::new(
        EntityId::parse("workflow-run-fixture").expect("valid workflow run id"),
        EntityId::parse("task-run-fixture").expect("valid task run id"),
        1,
        None,
    )
    .expect("valid plugin execution context")
}

#[test]
fn checked_in_wit_component_compiles_instantiates_and_executes() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = valid_component_from_checked_in_wit();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile valid v1 component");

    let context = fixture_context();

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
fn component_guest_errors_keep_their_typed_code_and_message() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_returning_guest_error("E_GUEST", "intentional guest failure");
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile guest-error component");

    let result = runtime.execute(
        &plugin,
        &fixture_context(),
        b"",
        CapabilitySet::default(),
        ExecutionLimits::default(),
    );

    assert!(matches!(
        result,
        Err(PluginRuntimeError::GuestError { code, message })
            if code == "E_GUEST" && message == "intentional guest failure"
    ));
}

#[test]
fn component_output_limit_is_enforced_after_guest_execution() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_returning_output(b"AB");
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile output component");
    let limits = ExecutionLimits {
        max_output_bytes: 1,
        ..ExecutionLimits::default()
    };

    assert!(matches!(
        runtime.execute(
            &plugin,
            &fixture_context(),
            b"",
            CapabilitySet::default(),
            limits,
        ),
        Err(PluginRuntimeError::OutputTooLarge(error))
            if error.actual == 2 && error.maximum == 1
    ));
}


#[test]
fn component_memory_growth_is_bounded_per_invocation() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_reporting_memory_growth();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile memory-growth component");

    let bounded_limits = ExecutionLimits {
        max_memory_bytes: 64 * 1024,
        ..ExecutionLimits::default()
    };
    let bounded = runtime
        .execute(
            &plugin,
            &fixture_context(),
            b"",
            CapabilitySet::default(),
            bounded_limits,
        )
        .expect("guest should handle a denied memory.grow without trapping");
    assert_eq!(bounded.output, [1], "memory.grow should fail at the 64 KiB cap");

    let default_limits = runtime
        .execute(
            &plugin,
            &fixture_context(),
            b"",
            CapabilitySet::default(),
            ExecutionLimits::default(),
        )
        .expect("guest should grow memory under the default budget");
    assert_eq!(default_limits.output, [0], "memory.grow should succeed under the default budget");
}

#[test]
fn component_cannot_import_ungranted_wasi_preview1_functions() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_with_ungranted_wasi_import();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("component binary should validate before import resolution");

    let result = runtime.execute(
        &plugin,
        &fixture_context(),
        b"",
        CapabilitySet::default(),
        ExecutionLimits::default(),
    );
    assert!(
        matches!(
            result,
            Err(PluginRuntimeError::Instantiation(_)) | Err(PluginRuntimeError::Linker(_))
        ),
        "an ungranted WASI import must fail closed, got {result:?}"
    );
}

#[test]
fn component_traps_are_mapped_to_a_typed_runtime_error() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_from_checked_in_wit("    unreachable", "");
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile trapping component");

    assert!(matches!(
        runtime.execute(
            &plugin,
            &fixture_context(),
            b"",
            CapabilitySet::default(),
            ExecutionLimits::default(),
        ),
        Err(PluginRuntimeError::Trap(_))
    ));
}

#[test]
fn component_instruction_fuel_exhaustion_is_enforced() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_spinning_forever();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile non-terminating component");
    let limits = ExecutionLimits {
        fuel: 100_000,
        ..ExecutionLimits::default()
    };

    assert!(matches!(
        runtime.execute(
            &plugin,
            &fixture_context(),
            b"",
            CapabilitySet::default(),
            limits,
        ),
        Err(PluginRuntimeError::FuelExhausted)
    ));
}

#[test]
fn component_epoch_deadline_interrupts_non_terminating_guest_code() {
    let runtime = WasmPluginRuntime::new().expect("create Wasmtime runtime");
    let component_bytes = component_spinning_forever();
    let plugin = runtime
        .compile_component(&component_bytes)
        .expect("compile non-terminating component");
    let limits = ExecutionLimits {
        fuel: MAX_PLUGIN_FUEL,
        wall_clock_timeout: Duration::from_millis(10),
        ..ExecutionLimits::default()
    };

    let result = runtime.execute(
        &plugin,
        &fixture_context(),
        b"",
        CapabilitySet::default(),
        limits,
    );
    assert!(
        matches!(&result, Err(PluginRuntimeError::DeadlineExceeded)),
        "unexpected result while testing epoch deadline: {result:?}"
    );
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
