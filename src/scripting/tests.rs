#[cfg(test)]
mod tests {
    use crate::scripting::{execute_wasm, execute_wasm_test, compile_wat_to_wasm};
    use crate::{Store, Snowflake};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_simple_wasm_execution() {
        let store = Arc::new(RwLock::new(Store::new(Arc::new(Snowflake::new()))));

        // Simple WAT that just returns 1
        let wat_source = r#"
        (module
          (memory (export "memory") 1)
          (func (export "main") (result i32)
            i32.const 1
          )
        )
        "#;

        let wasm_bytes = compile_wat_to_wasm(wat_source).unwrap();
        let input_data = serde_json::json!({"test": "value"});
        
        let result = execute_wasm(&wasm_bytes, store, input_data, Some("main")).await.unwrap();
        
        assert!(result.success);
        assert_eq!(result.value, serde_json::Value::Bool(true)); // Non-zero i32 -> true
    }

    #[tokio::test]
    async fn test_wasm_test_function() {
        let store = Arc::new(RwLock::new(Store::new(Arc::new(Snowflake::new()))));

        // WAT that returns 0 (false)
        let wat_source = r#"
        (module
          (memory (export "memory") 1)
          (func (export "main") (result i32)
            i32.const 0
          )
        )
        "#;

        let wasm_bytes = compile_wat_to_wasm(wat_source).unwrap();
        let test_data = serde_json::json!({"authorized": false});
        
        let result = execute_wasm_test(&wasm_bytes, store, test_data, Some("main")).await.unwrap();
        
        assert_eq!(result, false); // 0 -> false
    }

    #[tokio::test] 
    async fn test_wasm_with_host_functions() {
        let store = Arc::new(RwLock::new(Store::new(Arc::new(Snowflake::new()))));

        // WAT that calls host functions
        let wat_source = r#"
        (module
          (import "env" "host_log" (func $host_log (param i32)))
          (import "env" "always_true" (func $always_true (result i32)))
          (memory (export "memory") 1)
          (func (export "main") (result i32)
            i32.const 42
            call $host_log
            call $always_true
          )
        )
        "#;

        let wasm_bytes = compile_wat_to_wasm(wat_source).unwrap();
        let input_data = serde_json::json!({"test": "host_functions"});
        
        let result = execute_wasm(&wasm_bytes, store, input_data, Some("main")).await.unwrap();
        
        assert!(result.success);
        assert_eq!(result.value, serde_json::Value::Bool(true)); // always_true returns 1
    }

    #[tokio::test]
    async fn test_wasm_entity_exists() {
        let store = Arc::new(RwLock::new(Store::new(Arc::new(Snowflake::new()))));

        // WAT that calls entity_exists host function
        let wat_source = r#"
        (module
          (import "env" "entity_exists" (func $entity_exists (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{\"entity_type\":\"User\",\"id\":\"User$123\"}")
          (func (export "main") (result i32)
            i32.const 0        ;; ptr to entity_id JSON
            i32.const 34       ;; length of entity_id JSON
            call $entity_exists
          )
        )
        "#;

        let wasm_bytes = compile_wat_to_wasm(wat_source).unwrap();
        let input_data = serde_json::json!({"test": "entity_exists"});
        
        let result = execute_wasm(&wasm_bytes, store, input_data, Some("main")).await.unwrap();
        
        assert!(result.success);
        // Should return false (0) since the entity doesn't exist
        assert_eq!(result.value, serde_json::Value::Bool(false));
    }
}