use wasmtime::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::data::StoreTrait;
use crate::scripting::host_functions;
use serde_json;

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

/// WASM runtime for executing plugins
pub struct WasmRuntime<T: StoreTrait + Send + Sync + 'static> {
    engine: Engine,
    store: Arc<RwLock<T>>,
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
            store
        })
    }

    /// Execute a WASM plugin with the given input
    pub async fn execute(
        &mut self,
        wasm_bytes: &[u8],
        function_name: &str,
        input: serde_json::Value,
    ) -> Result<ExecutionResult, crate::Error> {
        // Create store context for WASM execution
        let store_context = host_functions::StoreContext {
            store: self.store.clone(),
        };
        
        // Create a new WASM store for this execution
        let mut store = Store::new(&self.engine, store_context);

        // Create linker and add host functions
        let mut linker = Linker::new(&self.engine);
        
        // Add our host functions
        host_functions::define_functions::<T>(&mut linker)
            .map_err(|e| crate::Error::Scripting(format!("Failed to define host functions: {}", e)))?;

        // Compile the WASM module
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| crate::Error::Scripting(format!("Failed to compile WASM module: {}", e)))?;

        // Instantiate the module
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|e| crate::Error::Scripting(format!("Failed to instantiate WASM module: {}", e)))?;

        // Get the memory export if it exists
        let _memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| crate::Error::Scripting("WASM module must export memory".to_string()))?;

        // Serialize input to JSON
        let _input_json = serde_json::to_string(&input)
            .map_err(|e| crate::Error::Scripting(format!("Failed to serialize input: {}", e)))?;

        // Get the main function
        let main_func = instance
            .get_typed_func::<(), i32>(&mut store, function_name)
            .map_err(|e| crate::Error::Scripting(format!("Failed to get function '{}': {}", function_name, e)))?;

        // For now, we'll just call the function and return a simple result
        // In a full implementation, you'd want to pass the input through memory
        let result = main_func
            .call_async(&mut store, ())
            .await
            .map_err(|e| crate::Error::Scripting(format!("WASM function execution failed: {}", e)))?;

        // For simplicity, assume the result is a boolean (0 = false, non-zero = true)
        Ok(ExecutionResult::success(serde_json::Value::Bool(result != 0)))
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
                result.error.unwrap_or_else(|| "Unknown error".to_string())
            ));
        }

        // Try to extract boolean result
        match result.value {
            serde_json::Value::Bool(b) => Ok(b),
            _ => Err(crate::Error::Scripting(
                "Expected boolean result from test function".to_string()
            )),
        }
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