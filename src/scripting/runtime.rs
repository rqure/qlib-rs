use crate::data::StoreTrait;
use crate::scripting::host_functions;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wasmtime::*;

/// Result of WASM plugin execution
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub success: bool,
    pub value: serde_json::Value,
    pub error: Option<String>,
}

impl ExecutionResult {
    pub fn success(value: serde_json::Value) -> Self {
        Self {
            success: true,
            value,
            error: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            value: serde_json::Value::Null,
            error: Some(message),
        }
    }
}

/// Compiled WASM module that can be reused across executions
#[derive(Clone)]
pub struct CompiledModule {
    module: Module,
}

/// WASM runtime for executing plugins with caching support
pub struct WasmRuntime<T: StoreTrait + Send + Sync + 'static> {
    engine: Engine,
    store: Arc<RwLock<T>>,
    module_cache: HashMap<Vec<u8>, CompiledModule>,
}

impl<T: StoreTrait + Send + Sync + 'static> WasmRuntime<T> {
    /// Create a new WASM runtime with store access
    pub async fn new(store: Arc<RwLock<T>>) -> Result<Self, crate::Error> {
        let mut config = Config::new();
        config.async_support(true);
        config.wasm_component_model(false); // Use core WASM for broader compatibility

        let engine = Engine::new(&config)
            .map_err(|e| crate::Error::Scripting(format!("Failed to create WASM engine: {}", e)))?;

        Ok(Self {
            engine,
            store,
            module_cache: HashMap::new(),
        })
    }

    /// Get or compile a WASM module, using cache for performance
    fn get_or_compile_module(&mut self, wasm_bytes: &[u8]) -> Result<CompiledModule, crate::Error> {
        // Use the hash of the bytecode as cache key
        if let Some(cached_module) = self.module_cache.get(wasm_bytes) {
            return Ok(cached_module.clone());
        }

        // Compile the WASM module
        let module = Module::new(&self.engine, wasm_bytes).map_err(|e| {
            crate::Error::Scripting(format!("Failed to compile WASM module: {}", e))
        })?;

        let compiled_module = CompiledModule { module };

        // Cache the compiled module
        self.module_cache
            .insert(wasm_bytes.to_vec(), compiled_module.clone());

        Ok(compiled_module)
    }

    /// Remove a compiled module from the cache
    /// Returns true if the module was found and removed, false if it wasn't in the cache
    pub fn remove_cached_module(&mut self, wasm_bytes: &[u8]) -> bool {
        self.module_cache.remove(wasm_bytes).is_some()
    }

    /// Clear all cached modules
    pub fn clear_module_cache(&mut self) {
        self.module_cache.clear();
    }

    /// Get the number of cached modules
    pub fn cached_module_count(&self) -> usize {
        self.module_cache.len()
    }

    /// Write data to WASM memory and return the pointer and length
    async fn write_to_memory(
        &self,
        store: &mut Store<host_functions::StoreContext<T>>,
        instance: &Instance,
        data: &[u8],
    ) -> Result<(i32, i32), crate::Error> {
        let data_len = data.len() as i32;

        // Get memory
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .ok_or_else(|| crate::Error::Scripting("WASM module must export memory".to_string()))?;

        // Get allocator function
        let alloc_func = instance
            .get_func(store.as_context_mut(), "alloc")
            .ok_or_else(|| {
                crate::Error::Scripting(
                    "WASM module must export 'alloc' function for memory allocation".to_string(),
                )
            })?
            .typed::<i32, i32>(store.as_context())
            .map_err(|e| {
                crate::Error::Scripting(format!("Invalid 'alloc' function signature: {}", e))
            })?;

        // Allocate memory
        let ptr = alloc_func
            .call_async(store.as_context_mut(), data_len)
            .await
            .map_err(|e| crate::Error::Scripting(format!("Failed to allocate memory: {}", e)))?;

        // Write data to allocated memory
        memory
            .write(store.as_context_mut(), ptr as usize, data)
            .map_err(|e| crate::Error::Scripting(format!("Failed to write to memory: {}", e)))?;

        Ok((ptr, data_len))
    }

    /// Execute a simple boolean test function (commonly used for authorization)
    pub async fn execute_test(
        &mut self,
        wasm_bytes: &[u8],
        function_name: &str,
        test_data: serde_json::Value,
    ) -> Result<bool, crate::Error> {
        let result = self.execute(wasm_bytes, function_name, test_data).await?;

        if !result.success {
            return Err(crate::Error::Scripting(
                result.error.unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        // Try to extract boolean result
        match result.value {
            serde_json::Value::Bool(b) => Ok(b),
            _ => Err(crate::Error::Scripting(
                "Expected boolean result from test function".to_string(),
            )),
        }
    }

    /// Execute a WASM plugin with the given input
    pub async fn execute(
        &mut self,
        wasm_bytes: &[u8],
        function_name: &str,
        input: serde_json::Value,
    ) -> Result<ExecutionResult, crate::Error> {
        // Get or compile the module (with caching)
        let compiled_module = self.get_or_compile_module(wasm_bytes)?;

        // Create store context for WASM execution
        let store_context = host_functions::StoreContext {
            store: self.store.clone(),
        };

        // Create a new WASM store for this execution
        let mut store = Store::new(&self.engine, store_context);

        // Create linker and add host functions
        let mut linker = Linker::new(&self.engine);
        host_functions::define_functions::<T>(&mut linker).map_err(|e| {
            crate::Error::Scripting(format!("Failed to define host functions: {}", e))
        })?;

        // Instantiate the module
        let instance = linker
            .instantiate_async(&mut store, &compiled_module.module)
            .await
            .map_err(|e| {
                crate::Error::Scripting(format!("Failed to instantiate WASM module: {}", e))
            })?;

        // Try different function signatures based on what's available

        // First try the full signature with JSON input/output: (input_ptr, input_len, output_ptr, output_max_len) -> output_len
        if let Some(func) = instance.get_func(&mut store, function_name) {
            if let Ok(full_func) = func.typed::<(i32, i32, i32, i32), i32>(&store) {
                // Serialize input to JSON
                let input_json = serde_json::to_string(&input).map_err(|e| {
                    crate::Error::Scripting(format!("Failed to serialize input: {}", e))
                })?;

                // Write input to WASM memory
                let (input_ptr, input_len) = self
                    .write_to_memory(&mut store, &instance, input_json.as_bytes())
                    .await?;

                // Allocate output buffer in WASM memory (4KB should be enough for most JSON responses)
                let output_size = 4096_i32;
                let (output_ptr, _) = self
                    .write_to_memory(&mut store, &instance, &vec![0u8; output_size as usize])
                    .await?;

                // Call the WASM function with input and output pointers
                let output_len = full_func
                    .call_async(&mut store, (input_ptr, input_len, output_ptr, output_size))
                    .await
                    .map_err(|e| {
                        crate::Error::Scripting(format!("WASM function execution failed: {}", e))
                    })?;

                // Read the result from WASM memory
                if output_len > 0 && output_len <= output_size {
                    let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
                        crate::Error::Scripting("WASM module must export memory".to_string())
                    })?;

                    let mut buffer = vec![0u8; output_len as usize];
                    memory
                        .read(&mut store, output_ptr as usize, &mut buffer)
                        .map_err(|e| {
                            crate::Error::Scripting(format!("Failed to read from memory: {}", e))
                        })?;

                    let output_str = String::from_utf8(buffer).map_err(|e| {
                        crate::Error::Scripting(format!("Failed to decode output as UTF-8: {}", e))
                    })?;

                    // Parse the JSON result
                    match serde_json::from_str::<serde_json::Value>(&output_str) {
                        Ok(value) => return Ok(ExecutionResult::success(value)),
                        Err(e) => {
                            return Err(crate::Error::Scripting(format!(
                                "Failed to parse output JSON: {}",
                                e
                            )))
                        }
                    }
                } else if output_len == 0 {
                    // No output means success with null value
                    return Ok(ExecutionResult::success(serde_json::Value::Null));
                } else {
                    return Err(crate::Error::Scripting(format!(
                        "Output length {} exceeds buffer size {}",
                        output_len, output_size
                    )));
                }
            }
        }

        // Fallback to simple boolean function: () -> i32
        if let Some(func) = instance.get_func(&mut store, function_name) {
            if let Ok(simple_func) = func.typed::<(), i32>(&store) {
                let result = simple_func.call_async(&mut store, ()).await.map_err(|e| {
                    crate::Error::Scripting(format!("WASM function execution failed: {}", e))
                })?;

                return Ok(ExecutionResult::success(serde_json::Value::Bool(
                    result != 0,
                )));
            }
        }

        Err(crate::Error::Scripting(format!(
            "Function '{}' not found or has unsupported signature",
            function_name
        )))
    }
}

/// Utility function to compile WAT (WebAssembly Text) to WASM bytes
pub fn compile_wat(wat_source: &str) -> Result<Vec<u8>, crate::Error> {
    wat::parse_str(wat_source)
        .map_err(|e| crate::Error::Scripting(format!("Failed to compile WAT: {}", e)))
}

/// Utility function to validate WASM bytecode
pub fn validate_wasm(wasm_bytes: &[u8]) -> Result<(), crate::Error> {
    let engine = Engine::default();
    Module::new(&engine, wasm_bytes)
        .map(|_| ())
        .map_err(|e| crate::Error::Scripting(format!("Invalid WASM bytecode: {}", e)))
}
