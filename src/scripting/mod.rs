mod plugin;
mod runtime;
mod host_functions;
mod types;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod example_test;

pub use plugin::{execute_wasm, execute_wasm_test, compile_wat_to_wasm, validate_wasm_bytes};
pub use runtime::{WasmRuntime, ExecutionResult, compile_wat, validate_wasm};
pub use types::{
    PluginContext, JsonEntityId, JsonFieldType, JsonValue, JsonRequest
};

use crate::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::data::StoreTrait;

/// Execute a WASM plugin with the given parameters
pub async fn execute<T: StoreTrait + Send + Sync + 'static>(
    store: Arc<RwLock<T>>,
    wasm_bytes: &[u8],
    function_name: Option<&str>,
    input: serde_json::Value,
) -> Result<ExecutionResult> {
    let mut runtime = WasmRuntime::new(store).await?;
    runtime.execute(wasm_bytes, function_name.unwrap_or("main"), input).await
}
