//! WebAssembly scripting support for qlib-rs using wasmtime
//! 
//! This module provides a WebAssembly runtime that can execute compiled scripts 
//! with access to the Store functionality. Scripts can perform async operations 
//! and return values to the Rust environment.

mod store_wrapper;
mod wasm_runtime;

use std::sync::Arc;

pub use store_wrapper::StoreWrapper;
pub use wasm_runtime::{WasmRuntime, WasmRuntimeOptions, WasmResult};
use tokio::sync::RwLock;

use crate::Result;
use crate::data::StoreTrait;
use serde_json::Value;

/// Execute a WebAssembly expression/module with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `wasm_bytes` - The compiled WebAssembly bytecode
/// * `entrypoint` - Optional function name to call as entrypoint (default: "main")
/// * `args` - Arguments to pass to the entrypoint function
/// 
/// # Returns
/// A `WasmResult` containing the result value and execution metadata
/// 
/// # Example
/// ```rust,ignore
/// let wasm_bytes = std::fs::read("script.wasm")?;
/// let result = execute(
///     store,
///     &wasm_bytes,
///     Some("create_user"),
///     json!("alice")
/// ).await?;
/// ```
pub async fn execute<T: StoreTrait + 'static>(
    store: Arc<RwLock<T>>,
    wasm_bytes: &[u8],
    entrypoint: Option<&str>,
    args: Value,
) -> Result<WasmResult> {
    let mut runtime = WasmRuntime::new(WasmRuntimeOptions::default())?;
    runtime.bind_store(store)?;
    runtime.execute_wasm_bytes(wasm_bytes, entrypoint, args).await
}

/// Execute a WebAssembly module with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `module_name` - Name for the module (used in error messages)
/// * `wasm_bytes` - The compiled WebAssembly bytecode
/// * `entrypoint` - Optional function name to call as entrypoint
/// * `args` - Arguments to pass to the entrypoint function
/// 
/// # Returns
/// A `WasmResult` containing the result value and execution metadata
/// 
/// # Example
/// ```rust,ignore
/// let wasm_bytes = compile_rust_to_wasm(r#"
///     #[no_mangle]
///     pub extern "C" fn create_user(name_ptr: *const u8, name_len: usize) -> i32 {
///         // ... implementation
///         0 // success
///     }
/// "#);
/// 
/// let result = execute_module(
///     store,
///     "user_module",
///     &wasm_bytes,
///     Some("create_user"),
///     json!("alice")
/// ).await?;
/// ```
pub async fn execute_module<T: StoreTrait + 'static>(
    store: Arc<RwLock<T>>,
    _module_name: &str,
    wasm_bytes: &[u8],
    entrypoint: Option<&str>,
    args: Value,
) -> Result<WasmResult> {
    let mut runtime = WasmRuntime::new(WasmRuntimeOptions::default())?;
    runtime.bind_store(store)?;
    runtime.execute_wasm_bytes(wasm_bytes, entrypoint, args).await
}

/// Load and execute a WebAssembly file with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `file_path` - Path to the WebAssembly (.wasm) file
/// * `entrypoint` - Optional function name to call as entrypoint
/// * `args` - Arguments to pass to the entrypoint function
/// 
/// # Returns
/// A `WasmResult` containing the result value and execution metadata
pub async fn execute_file<T: StoreTrait + 'static>(
    store: Arc<RwLock<T>>,
    file_path: &str,
    entrypoint: Option<&str>,
    args: Value,
) -> Result<WasmResult> {
    let mut runtime = WasmRuntime::new(WasmRuntimeOptions::default())?;
    runtime.bind_store(store)?;
    runtime.execute_wasm_file(file_path, entrypoint, args).await
}
