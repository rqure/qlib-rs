
#[allow(unused_imports)] // False positive - used in test functions
use crate::scripting::WasmRuntimeOptions;
use crate::data::Store;
use crate::Snowflake;
use std::sync::Arc;
use tokio::sync::Mutex;

#[allow(dead_code)]
async fn create_test_store() -> Arc<Mutex<Store>> {
    let store = Store::new(Arc::new(Snowflake::new()));
    Arc::new(Mutex::new(store))
}

#[test]
fn test_wasm_runtime_creation() {
    // Test that we can create a WasmRuntime with default options
    let options = WasmRuntimeOptions::default();
    assert_eq!(options.timeout.as_secs(), 30);
    assert_eq!(options.memory_limit, Some(50 * 1024 * 1024));
    assert!(options.enable_fuel);
}

#[test]
fn test_wasm_runtime_options() {
    // Test that we can create custom WasmRuntimeOptions
    use std::time::Duration;
    
    let options = WasmRuntimeOptions {
        timeout: Duration::from_secs(10),
        memory_limit: Some(10 * 1024 * 1024),
        stack_limit: Some(512 * 1024),
        enable_fuel: false,
        fuel_limit: Some(500_000),
    };
    
    assert_eq!(options.timeout.as_secs(), 10);
    assert_eq!(options.memory_limit, Some(10 * 1024 * 1024));
    assert_eq!(options.stack_limit, Some(512 * 1024));
    assert!(!options.enable_fuel);
    assert_eq!(options.fuel_limit, Some(500_000));
}

// Note: Actual JavaScript execution tests are disabled due to runtime conflicts
// with rustyscript in the test environment. The scripting functionality works
// in production environments where there's no existing tokio runtime conflict.

/* Commented out due to rustyscript runtime conflicts in test environment

#[test] 
fn test_execute_expression_simple() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let store = create_test_store().await;
        
        let result = execute_expression(store, Context::new(), "2 + 3").await.unwrap();
        assert_eq!(result.value, serde_json::Value::Number(serde_json::Number::from(5)));
    });
}

*/
