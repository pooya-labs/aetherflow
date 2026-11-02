#![cfg(target_arch = "wasm32")]

wit_bindgen::generate!({
    path: "../../../crates/aetherflow-wasm/wit",
    world: "task-plugin",
});

/// A minimal guest that echoes its opaque input and attempts one bounded log.
struct EchoPlugin;

impl exports::aetherflow::plugin::guest::Guest for EchoPlugin {
    fn execute(
        context: aetherflow::plugin::types::TaskContext,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, aetherflow::plugin::types::PluginError> {
        let message = format!("attempt {} echoed {} bytes", context.attempt, input.len());
        let _ = aetherflow::plugin::host::log(aetherflow::plugin::types::LogLevel::Info, &message);

        Ok(input)
    }
}

export!(EchoPlugin);
