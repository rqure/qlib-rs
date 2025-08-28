use std::sync::Arc;
use tokio::sync::RwLock;
use crate::data::StoreTrait;
use crate::scripting::{
    runtime::{WasmRuntime, ExecutionResult},
};

/// Execute WASM code with store access
pub async fn execute_wasm<T: StoreTrait + Send + Sync + 'static>(
    wasm_bytes: &[u8],
    store: Arc<RwLock<T>>,
    input: serde_json::Value,
    function_name: Option<&str>,
) -> Result<ExecutionResult, crate::Error> {
    let mut runtime = WasmRuntime::new(store).await?;
    runtime.execute(wasm_bytes, function_name.unwrap_or("main"), input).await
}

/// Execute WASM code as a test function (returns boolean)
pub async fn execute_wasm_test<T: StoreTrait + Send + Sync + 'static>(
    wasm_bytes: &[u8],
    store: Arc<RwLock<T>>,
    test_data: serde_json::Value,
    function_name: Option<&str>,
) -> Result<bool, crate::Error> {
    let mut runtime = WasmRuntime::new(store).await?;
    runtime.execute_test(wasm_bytes, function_name.unwrap_or("main"), test_data).await
}

/// Compile WAT (WebAssembly Text) to WASM bytes
pub fn compile_wat_to_wasm(wat_source: &str) -> Result<Vec<u8>, crate::Error> {
    crate::scripting::runtime::compile_wat(wat_source)
}

/// Validate WASM bytecode
pub fn validate_wasm_bytes(wasm_bytes: &[u8]) -> Result<(), crate::Error> {
    crate::scripting::runtime::validate_wasm(wasm_bytes)
}