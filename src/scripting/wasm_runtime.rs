use crate::{Error, Result};
use crate::data::StoreTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use wasmtime::*;

/// Configuration options for the WebAssembly runtime
#[derive(Debug, Clone)]
pub struct WasmRuntimeOptions {
    /// Maximum execution time for scripts (default: 30 seconds)
    pub timeout: Duration,
    /// Maximum memory usage in bytes (default: 50MB)
    pub memory_limit: Option<usize>,
    /// Stack size limit in bytes (default: 1MB)  
    pub stack_limit: Option<usize>,
    /// Whether to enable fuel-based execution limits
    pub enable_fuel: bool,
    /// Fuel limit for execution (instructions count)
    pub fuel_limit: Option<u64>,
}

impl Default for WasmRuntimeOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            memory_limit: Some(50 * 1024 * 1024), // 50MB
            stack_limit: Some(1024 * 1024), // 1MB
            enable_fuel: true,
            fuel_limit: Some(1_000_000), // 1M instructions
        }
    }
}

/// Result of WebAssembly script execution with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmResult {
    /// The value returned by the script (JSON-serialized)
    pub value: Value,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Memory usage during execution (if available)
    pub memory_used: Option<usize>,
    /// Fuel consumed during execution (if fuel is enabled)
    pub fuel_consumed: Option<u64>,
    /// Console output captured during execution
    pub console_output: Vec<String>,
    /// Whether the script completed successfully
    pub success: bool,
    /// Error message if execution failed
    pub error: Option<String>,
}

/// WebAssembly runtime for executing scripts with store access
pub struct WasmRuntime {
    engine: Engine,
    store: Store<WasmContext>,
    options: WasmRuntimeOptions,
}

/// Context that gets passed to WebAssembly modules
pub struct WasmContext {
    pub store_instance: Option<Arc<RwLock<dyn StoreTrait + Send + Sync>>>,
}

impl WasmRuntime {
    /// Create a new WebAssembly runtime with the given options
    pub fn new(options: WasmRuntimeOptions) -> Result<Self> {
        // Configure the engine
        let mut config = Config::new();
        config.wasm_memory64(false);
        config.wasm_multi_memory(false);
        config.async_support(true);
        
        // Enable fuel for execution limits
        if options.enable_fuel {
            config.consume_fuel(true);
        }

        let engine = Engine::new(&config)
            .map_err(|e| Error::Scripting(format!("Failed to create WASM engine: {}", e)))?;

        let context = WasmContext {
            store_instance: None,
        };

        let mut store = Store::new(&engine, context);
        
        // Set fuel limit
        if options.enable_fuel {
            if let Some(fuel_limit) = options.fuel_limit {
                store.set_fuel(fuel_limit)
                    .map_err(|e| Error::Scripting(format!("Failed to set fuel limit: {}", e)))?;
            }
        }

        Ok(Self {
            engine,
            store,
            options,
        })
    }

    /// Bind a store instance to the runtime, making it available to WebAssembly modules
    pub fn bind_store<T: StoreTrait + Send + Sync + 'static>(
        &mut self,
        store: Arc<RwLock<T>>,
    ) -> Result<()> {
        self.store.data_mut().store_instance = Some(store);
        Ok(())
    }

    /// Execute a WebAssembly module from bytes
    pub async fn execute_wasm_bytes(
        &mut self,
        wasm_bytes: &[u8],
        entrypoint: Option<&str>,
        args: Value,
    ) -> Result<WasmResult> {
        let start_time = Instant::now();

        let initial_fuel = if self.options.enable_fuel {
            self.store.get_fuel().ok()
        } else {
            None
        };

        let result = match self.execute_wasm_internal(wasm_bytes, entrypoint, args).await {
            Ok(value) => {
                let fuel_consumed = if let (Some(initial), Some(remaining)) = 
                    (initial_fuel, self.store.get_fuel().ok()) {
                    Some(initial - remaining)
                } else {
                    None
                };

                WasmResult {
                    value,
                    execution_time_ms: start_time.elapsed().as_millis() as u64,
                    memory_used: self.get_memory_usage(),
                    fuel_consumed,
                    console_output: Vec::new(), // No console output capture for now
                    success: true,
                    error: None,
                }
            }
            Err(e) => WasmResult {
                value: Value::Null,
                execution_time_ms: start_time.elapsed().as_millis() as u64,
                memory_used: self.get_memory_usage(),
                fuel_consumed: initial_fuel.and_then(|initial| 
                    self.store.get_fuel().ok().map(|remaining| initial - remaining)
                ),
                console_output: Vec::new(), // No console output capture for now
                success: false,
                error: Some(format!("{}", e)),
            }
        };

        Ok(result)
    }

    /// Execute a WebAssembly module from a file
    pub async fn execute_wasm_file(
        &mut self,
        file_path: &str,
        entrypoint: Option<&str>,
        args: Value,
    ) -> Result<WasmResult> {
        let wasm_bytes = std::fs::read(file_path)
            .map_err(|e| Error::Scripting(format!("Failed to read WASM file: {}", e)))?;
        
        self.execute_wasm_bytes(&wasm_bytes, entrypoint, args).await
    }

    /// Internal implementation of WebAssembly execution
    async fn execute_wasm_internal(
        &mut self,
        wasm_bytes: &[u8],
        entrypoint: Option<&str>,
        args: Value,
    ) -> Result<Value> {
        // Compile the module
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| Error::Scripting(format!("Failed to compile WASM module: {}", e)))?;

        // Create linker for host functions
        let mut linker = Linker::new(&self.engine);
        
        // Add store functions
        self.add_store_functions(&mut linker)?;
        self.add_console_functions(&mut linker)?;

        // Instantiate the module
        let instance = linker.instantiate_async(&mut self.store, &module).await
            .map_err(|e| Error::Scripting(format!("Failed to instantiate WASM module: {}", e)))?;

        // Call the entrypoint function
        let entrypoint_name = entrypoint.unwrap_or("main");
        
        let func = instance.get_typed_func::<(i32, i32), i32>(&mut self.store, entrypoint_name)
            .map_err(|e| Error::Scripting(format!("Entrypoint '{}' not found: {}", entrypoint_name, e)))?;

        // Serialize args to memory (simplified - in practice you'd have a more sophisticated ABI)
        let args_json = serde_json::to_string(&args)
            .map_err(|e| Error::Scripting(format!("Failed to serialize args: {}", e)))?;
        
        // For simplicity, we'll pass the length and assume the WASM module allocates memory
        // In a real implementation, you'd have proper memory management
        let result_ptr = func.call_async(&mut self.store, (args_json.len() as i32, 0)).await
            .map_err(|e| Error::Scripting(format!("WASM execution failed: {}", e)))?;

        // Read result from memory (simplified)
        // In practice, you'd have a proper ABI for reading results
        Ok(Value::Number(serde_json::Number::from(result_ptr)))
    }

    /// Add store-related host functions to the linker
    fn add_store_functions(&self, linker: &mut Linker<WasmContext>) -> Result<()> {
        // For now, add a simple test function to validate the approach
        linker.func_wrap("env", "test_function", |_caller: Caller<'_, WasmContext>| -> i32 {
            42
        }).map_err(|e| Error::Scripting(format!("Failed to add test_function: {}", e)))?;

        // TODO: Add actual store functions with proper async handling
        Ok(())
    }

    /// Add console-related host functions to the linker
    fn add_console_functions(&self, linker: &mut Linker<WasmContext>) -> Result<()> {
        linker.func_wrap("env", "console_log", 
            move |_caller: Caller<'_, WasmContext>, msg_ptr: i32, msg_len: i32| -> () {
                // Simple console output for debugging
                println!("WASM console.log called with ptr={}, len={}", msg_ptr, msg_len);
            }).map_err(|e| Error::Scripting(format!("Failed to add console_log: {}", e)))?;

        Ok(())
    }

    /// Get current memory usage
    fn get_memory_usage(&self) -> Option<usize> {
        // This is a simplified implementation
        // In practice, you'd track memory usage more accurately
        None
    }
}