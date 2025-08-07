
use super::*;
use crate::data::{store::Store, StoreConfig, Entity, Field, Value};
use crate::data::types::{EntityType, FieldType};
use std::sync::Arc;
use tokio::sync::RwLock;

async fn create_test_store() -> Arc<RwLock<Store>> {
    let mut store = Store::new(StoreConfig::default());
    
    Arc::new(RwLock::new(store))
}

#[tokio::test]
async fn test_execute_expression_simple() {
    let result = execute_expression("2 + 3", None).await.unwrap();
    assert_eq!(result, serde_json::Value::Number(serde_json::Number::from(5)));
}

#[tokio::test]
async fn test_execute_expression_with_store() {
    let store = create_test_store().await;
    
    let script = r#"
        async function test() {
            const entity = await getEntity('user1');
            return entity;
        }
        test()
    "#;
    
    let result = execute_expression(script, Some(store)).await.unwrap();
    assert!(result.is_object());
    
    let obj = result.as_object().unwrap();
    assert!(obj.contains_key("fields"));
}

#[tokio::test]
async fn test_execute_expression_console() {
    let script = r#"
        console.log('Hello from JavaScript!');
        console.info('This is an info message');
        42
    "#;
    
    let result = execute_expression(script, None).await.unwrap();
    assert_eq!(result, serde_json::Value::Number(serde_json::Number::from(42)));
}

#[tokio::test]
async fn test_execute_expression_with_store_operations() {
    let store = create_test_store().await;
    
    let script = r#"
        async function updateUser() {
            // Get the current user
            const user = await getEntity('user1');
            console.log('Current user:', JSON.stringify(user));
            
            // Update the age
            await updateField('user1', 'age', 31);
            
            // Get the updated user
            const updatedUser = await getEntity('user1');
            return updatedUser.fields.age;
        }
        updateUser()
    "#;
    
    let result = execute_expression(script, Some(store)).await.unwrap();
    assert_eq!(result, serde_json::Value::Number(serde_json::Number::from(31)));
}

#[tokio::test]
async fn test_error_handling() {
    let result = execute_expression("throw new Error('Test error')", None).await;
    assert!(result.is_err());
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Test error"));
}

#[tokio::test]
async fn test_execute_file_not_found() {
    let result = execute_file("non_existent_file.js", None).await;
    assert!(result.is_err());
}
