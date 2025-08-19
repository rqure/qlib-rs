//! JavaScript scripting support for qlib-rs using rustyscript
//! 
//! This module provides a JavaScript runtime that can execute scripts with access
//! to the Store functionality. Scripts can perform async operations and return
//! values to the Rust environment.

mod runtime;
mod store_wrapper;

pub use runtime::{ScriptRuntime, ScriptRuntimeOptions, ScriptResult};
pub use store_wrapper::StoreWrapper;

use crate::{Context, Error, Result, StoreInterface};
use rustyscript::Module;
use serde_json::Value;

/// Execute a JavaScript expression with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `context` - The security context for store operations
/// * `expression` - The JavaScript expression to evaluate
/// 
/// # Returns
/// A `ScriptResult` containing the result value and execution metadata
/// 
/// # Example
/// ```rust,ignore
/// let result = execute_expression(
///     store,
///     &context,
///     "await store.createEntity('User', null, 'testuser')"
/// ).await?;
/// ```
pub async fn execute_expression(
    store: &mut StoreInterface,
    context: Context,
    expression: &str,
) -> Result<ScriptResult> {
    let mut runtime = ScriptRuntime::new(ScriptRuntimeOptions::default())?;
    runtime.bind_store(store, context)?;
    runtime.execute_expression(expression).await
}

/// Execute a JavaScript module with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `context` - The security context for store operations
/// * `module_name` - Name for the module (used in error messages)
/// * `module_code` - The JavaScript/TypeScript module code
/// * `entrypoint` - Optional function name to call as entrypoint
/// * `args` - Arguments to pass to the entrypoint function
/// 
/// # Returns
/// A `ScriptResult` containing the result value and execution metadata
/// 
/// # Example
/// ```rust,ignore
/// let module_code = r#"
///     export async function createUser(name) {
///         const user = await store.createEntity('User', null, name);
///         console.log('Created user:', user.entity_id);
///         return user;
///     }
/// "#;
/// 
/// let result = execute_module(
///     store,
///     &context,
///     "user_module",
///     module_code,
///     Some("createUser"),
///     json_args!("alice")
/// ).await?;
/// ```
pub async fn execute_module(
    store: &mut StoreInterface,
    context: Context,
    module_name: &str,
    module_code: &str,
    entrypoint: Option<&str>,
    args: Value,
) -> Result<ScriptResult> {
    let mut runtime = ScriptRuntime::new(ScriptRuntimeOptions::default())?;
    runtime.bind_store(store, context)?;
    
    let module = Module::new(module_name, module_code);
    runtime.execute_module(module, entrypoint, args).await
}

/// Load and execute a JavaScript file with access to store operations
/// 
/// # Arguments
/// * `store` - The store implementation to make available to the script
/// * `context` - The security context for store operations
/// * `file_path` - Path to the JavaScript/TypeScript file
/// * `entrypoint` - Optional function name to call as entrypoint
/// * `args` - Arguments to pass to the entrypoint function
/// 
/// # Returns
/// A `ScriptResult` containing the result value and execution metadata
pub async fn execute_file(
    store: &mut StoreInterface,
    context: Context,
    file_path: &str,
    entrypoint: Option<&str>,
    args: Value,
) -> Result<ScriptResult> {
    let mut runtime = ScriptRuntime::new(ScriptRuntimeOptions::default())?;
    runtime.bind_store(store, context)?;
    
    let module = Module::load(file_path)
        .map_err(|e| Error::Scripting(format!("Failed to load module: {}", e)))?;
    runtime.execute_module(module, entrypoint, args).await
}

pub async fn execute(
    store: &mut StoreInterface,
    context: Context,
    code: &str,
    args: Value,
) -> Result<ScriptResult> {
    let module_name = "inline_script";
    let module_code = format!(r#"
        export async function main(args) {{
            {}
        }}
    "#, code);
    execute_module(store, context, module_name, module_code.as_str(), Some("main"), args).await
}
