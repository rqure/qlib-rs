use crate::{Error, Result, StoreProxy};
use rustyscript::{Module, Runtime, RuntimeOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use super::StoreWrapper;

/// Configuration options for the script runtime
#[derive(Debug, Clone)]
pub struct ScriptRuntimeOptions {
    /// Maximum execution time for scripts (default: 30 seconds)
    pub timeout: Duration,
    /// Maximum memory usage in bytes (default: 50MB)
    pub memory_limit: Option<usize>,
    /// Whether to enable console output (default: true)
    pub enable_console: bool,
    /// Default entrypoint function name
    pub default_entrypoint: Option<String>,
}

impl Default for ScriptRuntimeOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            memory_limit: Some(50 * 1024 * 1024), // 50MB
            enable_console: true,
            default_entrypoint: None,
        }
    }
}

/// Result of script execution with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptResult {
    /// The value returned by the script
    pub value: Value,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Memory usage during execution (if available)
    pub memory_used: Option<usize>,
    /// Console output captured during execution
    pub console_output: Vec<String>,
    /// Whether the script completed successfully
    pub success: bool,
    /// Error message if execution failed
    pub error: Option<String>,
}

/// JavaScript runtime for executing scripts with store access
pub struct ScriptRuntime {
    runtime: Runtime,
    console_output: Arc<Mutex<Vec<String>>>,
}

impl ScriptRuntime {
    /// Create a new script runtime with the given options
    pub fn new(options: ScriptRuntimeOptions) -> Result<Self> {
        let console_output = Arc::new(Mutex::new(Vec::new()));
        
        let runtime_options = RuntimeOptions {
            timeout: options.timeout,
            default_entrypoint: options.default_entrypoint.clone(),
            ..Default::default()
        };

        // Create a new runtime - this may cause conflicts in test environments
        let mut runtime = Runtime::new(runtime_options)
            .map_err(|e| Error::Scripting(format!("Failed to create runtime: {}", e)))?;

        // Register console functions if enabled
        if options.enable_console {
            let console_output_clone = console_output.clone();
            runtime.register_function("console_log", move |args| {
                let output = console_output_clone.clone();
                let message = args.iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(" ");
                tokio::spawn(async move {
                    let mut output = output.lock().await;
                    output.push(format!("[LOG] {}", message));
                });
                Ok(Value::Null)
            }).map_err(|e| Error::Scripting(format!("Failed to register console.log: {}", e)))?;

            let console_output_clone = console_output.clone();
            runtime.register_function("console_error", move |args| {
                let output = console_output_clone.clone();
                let message = args.iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(" ");
                tokio::spawn(async move {
                    let mut output = output.lock().await;
                    output.push(format!("[ERROR] {}", message));
                });
                Ok(Value::Null)
            }).map_err(|e| Error::Scripting(format!("Failed to register console.error: {}", e)))?;

            let console_output_clone = console_output.clone();
            runtime.register_function("console_warn", move |args| {
                let output = console_output_clone.clone();
                let message = args.iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(" ");
                tokio::spawn(async move {
                    let mut output = output.lock().await;
                    output.push(format!("[WARN] {}", message));
                });
                Ok(Value::Null)
            }).map_err(|e| Error::Scripting(format!("Failed to register console.warn: {}", e)))?;

            // Override console in JavaScript to use our functions
            let console_setup = Module::new("console_setup", r#"
                globalThis.console = {
                    log: (...args) => rustyscript.functions.console_log(args),
                    error: (...args) => rustyscript.functions.console_error(args),
                    warn: (...args) => rustyscript.functions.console_warn(args),
                    info: (...args) => rustyscript.functions.console_log(args),
                };
            "#);
            
            runtime.load_module(&console_setup)
                .map_err(|e| Error::Scripting(format!("Failed to setup console: {}", e)))?;
        }

        Ok(Self {
            runtime,
            console_output,
        })
    }

    /// Bind a store instance to the runtime, making it available as `store` in JavaScript
    pub fn bind_store(
        &mut self,
        store: Arc<Mutex<StoreProxy>>,
    ) -> Result<()> {
        let store_wrapper = StoreWrapper::new(store);
        
        // Register store functions
        let store_clone = store_wrapper.clone();
        self.runtime.register_async_function("store_create_entity", move |args| {
            let mut store = store_clone.clone();
            Box::pin(async move {
                if args.len() != 3 {
                    return Err(rustyscript::Error::Runtime("createEntity requires 3 arguments: entityType, parentId, name".to_string()));
                }
                
                let entity_type = args[0].as_str()
                    .ok_or_else(|| rustyscript::Error::Runtime("entityType must be a string".to_string()))?;
                let parent_id = if args[1].is_null() { None } else {
                    Some(args[1].as_str()
                        .ok_or_else(|| rustyscript::Error::Runtime("parentId must be a string or null".to_string()))?
                        .to_string())
                };
                let name = args[2].as_str()
                    .ok_or_else(|| rustyscript::Error::Runtime("name must be a string".to_string()))?;

                match store.create_entity(entity_type, parent_id.as_deref(), name).await {
                    Ok(entity) => Ok(serde_json::to_value(entity).unwrap()),
                    Err(e) => Err(rustyscript::Error::Runtime(format!("Store error: {}", e))),
                }
            })
        }).map_err(|e| Error::Scripting(format!("Failed to register createEntity: {}", e)))?;

        let store_clone = store_wrapper.clone();
        self.runtime.register_async_function("store_delete_entity", move |args| {
            let mut store = store_clone.clone();
            Box::pin(async move {
                if args.len() != 1 {
                    return Err(rustyscript::Error::Runtime("deleteEntity requires 1 argument: entityId".to_string()));
                }
                
                let entity_id = args[0].as_str()
                    .ok_or_else(|| rustyscript::Error::Runtime("entityId must be a string".to_string()))?;

                match store.delete_entity(entity_id).await {
                    Ok(_) => Ok(Value::Null),
                    Err(e) => Err(rustyscript::Error::Runtime(format!("Store error: {}", e))),
                }
            })
        }).map_err(|e| Error::Scripting(format!("Failed to register deleteEntity: {}", e)))?;

        let store_clone = store_wrapper.clone();
        self.runtime.register_async_function("store_entity_exists", move |args| {
            let store = store_clone.clone();
            Box::pin(async move {
                if args.len() != 1 {
                    return Err(rustyscript::Error::Runtime("entityExists requires 1 argument: entityId".to_string()));
                }
                
                let entity_id = args[0].as_str()
                    .ok_or_else(|| rustyscript::Error::Runtime("entityId must be a string".to_string()))?;

                match store.entity_exists(entity_id).await {
                    Ok(exists) => Ok(Value::Bool(exists)),
                    Err(e) => Err(rustyscript::Error::Runtime(format!("Store error: {}", e))),
                }
            })
        }).map_err(|e| Error::Scripting(format!("Failed to register entityExists: {}", e)))?;

        let store_clone = store_wrapper.clone();
        self.runtime.register_async_function("store_find_entities", move |args| {
            let store = store_clone.clone();
            Box::pin(async move {
                if args.len() != 1 {
                    return Err(rustyscript::Error::Runtime("findEntities requires 1 argument: entityType".to_string()));
                }
                
                let entity_type = args[0].as_str()
                    .ok_or_else(|| rustyscript::Error::Runtime("entityType must be a string".to_string()))?;

                match store.find_entities(entity_type).await {
                    Ok(entities) => Ok(serde_json::to_value(entities).unwrap()),
                    Err(e) => Err(rustyscript::Error::Runtime(format!("Store error: {}", e))),
                }
            })
        }).map_err(|e| Error::Scripting(format!("Failed to register findEntities: {}", e)))?;

        let store_clone = store_wrapper.clone();
        self.runtime.register_async_function("store_perform", move |args| {
            let mut store = store_clone.clone();
            Box::pin(async move {
                if args.len() != 1 {
                    return Err(rustyscript::Error::Runtime("perform requires 1 argument: requests".to_string()));
                }

                match store.perform(&args[0]).await {
                    Ok(result) => Ok(result),
                    Err(e) => Err(rustyscript::Error::Runtime(format!("Store error: {}", e))),
                }
            })
        }).map_err(|e| Error::Scripting(format!("Failed to register perform: {}", e)))?;

        // Setup store object in JavaScript
        let store_setup = Module::new("store_setup", r#"
            globalThis.store = {
                createEntity: async (entityType, parentId, name) => {
                    return await rustyscript.functions.store_create_entity([entityType, parentId, name]);
                },
                deleteEntity: async (entityId) => {
                    return await rustyscript.functions.store_delete_entity([entityId]);
                },
                entityExists: async (entityId) => {
                    return await rustyscript.functions.store_entity_exists([entityId]);
                },
                findEntities: async (entityType) => {
                    return await rustyscript.functions.store_find_entities([entityType]);
                },
                perform: async (requests) => {
                    return await rustyscript.functions.store_perform([requests]);
                }
            };
        "#);
        
        self.runtime.load_module(&store_setup)
            .map_err(|e| Error::Scripting(format!("Failed to setup store: {}", e)))?;

        Ok(())
    }

    /// Execute a JavaScript expression and return the result
    pub async fn execute_expression(&mut self, expression: &str) -> Result<ScriptResult> {
        let start_time = Instant::now();
        let mut console_output = Vec::new();
        
        // Clear previous console output
        {
            let mut output = self.console_output.lock().await;
            output.clear();
        }

        let result = match self.runtime.eval_async::<Value>(expression).await {
            Ok(value) => {
                // Get console output
                {
                    let output = self.console_output.lock().await;
                    console_output = output.clone();
                }

                ScriptResult {
                    value,
                    execution_time_ms: start_time.elapsed().as_millis() as u64,
                    memory_used: None, // TODO: Implement memory tracking
                    console_output,
                    success: true,
                    error: None,
                }
            }
            Err(e) => ScriptResult {
                value: Value::Null,
                execution_time_ms: start_time.elapsed().as_millis() as u64,
                memory_used: None,
                console_output,
                success: false,
                error: Some(format!("{}", e)),
            }
        };

        Ok(result)
    }

    /// Execute a JavaScript module and return the result
    pub async fn execute_module(
        &mut self,
        module: Module,
        entrypoint: Option<&str>,
        args: Value,
    ) -> Result<ScriptResult> {
        let start_time = Instant::now();
        let mut console_output = Vec::new();
        
        // Clear previous console output
        {
            let mut output = self.console_output.lock().await;
            output.clear();
        }

        let result = match self.runtime.tokio_runtime().block_on(async {
            let handle = self.runtime.load_module_async(&module).await?;
            
            if let Some(entrypoint_name) = entrypoint {
                let args_array = if args.is_array() {
                    &args
                } else {
                    &serde_json::Value::Array(vec![args])
                };
                self.runtime.call_function_async::<Value>(Some(&handle), entrypoint_name, args_array).await
            } else {
                // Run default entrypoint
                self.runtime.call_entrypoint_async::<Value>(&handle, &args).await
            }
        }) {
            Ok(value) => {
                // Get console output
                {
                    let output = self.console_output.lock().await;
                    console_output = output.clone();
                }

                ScriptResult {
                    value,
                    execution_time_ms: start_time.elapsed().as_millis() as u64,
                    memory_used: None,
                    console_output,
                    success: true,
                    error: None,
                }
            }
            Err(e) => ScriptResult {
                value: Value::Null,
                execution_time_ms: start_time.elapsed().as_millis() as u64,
                memory_used: None,
                console_output,
                success: false,
                error: Some(format!("{}", e)),
            }
        };

        Ok(result)
    }
}
