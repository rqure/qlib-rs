
use super::*;
use crate::scripting::{execute_expression, ScriptResult};
use crate::data::{Store, Entity, Field, Value};
use crate::{Context, EntityType, FieldType, Snowflake};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json;

async fn create_test_store() -> Arc<Mutex<Store>> {
    let store = Store::new(Arc::new(Snowflake::new()));
    Arc::new(Mutex::new(store))
}

#[tokio::test]
async fn test_execute_expression_simple() {
    let result = execute_expression(
        Arc::new(Mutex::new(Store::new(Arc::new(Snowflake::new())))),
        Context::new(),
        "2 + 3"
    ).await.unwrap();
    assert_eq!(result.value, serde_json::Value::Number(serde_json::Number::from(5)));
}

#[tokio::test]
async fn test_execute_expression_with_store() {
    let store = create_test_store().await;
    
    let script = r#"
        // Simple test that returns a basic object
        ({ success: true, message: "test completed" })
    "#;
    
    let result = execute_expression(store, Context::new(), script).await.unwrap();
    assert!(result.value.is_object());
    
    let obj = result.value.as_object().unwrap();
    assert!(obj.contains_key("success"));
}

#[tokio::test]
async fn test_execute_expression_console() {
    let script = r#"
        console.log('Hello from JavaScript!');
        console.info('This is an info message');
        42
    "#;
    
    let result = execute_expression(
        Arc::new(Mutex::new(Store::new(Arc::new(Snowflake::new())))),
        Context::new(),
        script
    ).await.unwrap();
    assert_eq!(result.value, serde_json::Value::Number(serde_json::Number::from(42)));
}

#[tokio::test]
async fn test_execute_expression_with_store_operations() {
    let store = create_test_store().await;
    
    let script = r#"
        // Simple arithmetic operation for now
        // TODO: Add actual store operations when store wrapper is implemented
        21 + 10
    "#;
    
    let result = execute_expression(store, Context::new(), script).await.unwrap();
    assert_eq!(result.value, serde_json::Value::Number(serde_json::Number::from(31)));
}

#[tokio::test]
async fn test_error_handling() {
    let result = execute_expression(
        Arc::new(Mutex::new(Store::new(Arc::new(Snowflake::new())))),
        Context::new(),
        "throw new Error('Test error')"
    ).await;
    assert!(result.is_err());
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Test error"));
}

#[tokio::test]
async fn test_execute_file_not_found() {
    use crate::scripting::execute_file;
    let result = execute_file(
        Arc::new(Mutex::new(Store::new(Arc::new(Snowflake::new())))),
        Context::new(),
        "non_existent_file.js",
        None,
        serde_json::Value::Null
    ).await;
    assert!(result.is_err());
}
