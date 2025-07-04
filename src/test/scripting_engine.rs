#[cfg(test)]
mod tests {
    use crate::*;
    use crate::scripting::{ScriptingEngine, IntoEvalError};
    use std::{cell::RefCell, rc::Rc, sync::Arc};
    use rhai::EvalAltResult;

    #[test]
    fn test_into_eval_error_trait() {
        // Test the IntoEvalError trait implementation for &str
        let error_message = "Test error message";
        let result: std::result::Result<i32, Box<EvalAltResult>> = error_message.err();
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        
        // Check that the error contains our message
        let error_string = format!("{}", error);
        assert!(error_string.contains("Test error message"));
        
        // Test with different error messages
        let another_error: std::result::Result<String, Box<EvalAltResult>> = "Another error".err();
        assert!(another_error.is_err());
        
        let numeric_error: std::result::Result<f64, Box<EvalAltResult>> = "Numeric conversion failed".err();
        assert!(numeric_error.is_err());
    }

    #[test]
    fn test_scripting_engine_new() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test that the engine was created successfully
        // We can verify this by executing a simple script
        let result = scripting_engine.execute_raw("42");
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert_eq!(value.as_int().unwrap(), 42);
    }

    #[test]
    fn test_scripting_engine_execute_basic_operations() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test basic arithmetic
        let result = scripting_engine.execute_raw("2 + 3 * 4");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_int().unwrap(), 14);
        
        // Test string operations
        let result = scripting_engine.execute_raw("\"Hello, \" + \"World!\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().into_string().unwrap(), "Hello, World!");
        
        // Test boolean operations
        let result = scripting_engine.execute_raw("true && false");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_bool().unwrap(), false);
    }

    #[test]
    fn test_scripting_engine_execute_variables() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        let script = r#"
            let x = 10;
            let y = 20;
            x + y
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_int().unwrap(), 30);
    }

    #[test]
    fn test_scripting_engine_execute_arrays() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        let script = r#"
            let arr = [1, 2, 3, 4, 5];
            arr.len()
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_int().unwrap(), 5);
    }

    #[test]
    fn test_scripting_engine_execute_functions() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        let script = r#"
            fn add(a, b) {
                a + b
            }
            
            add(15, 25)
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_int().unwrap(), 40);
    }

    #[test]
    fn test_scripting_engine_execute_registered_functions() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test the read function
        let script = r#"
            let read_request = read("User$1", "Name");
            read_request
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        
        let map = result.unwrap().try_cast::<rhai::Map>().unwrap();
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "read");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
    }

    #[test]
    fn test_scripting_engine_execute_write_functions() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test basic write function
        let script = r#"
            let write_request = write("User$1", "Name", "Alice");
            write_request
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        
        let map = result.unwrap().try_cast::<rhai::Map>().unwrap();
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<String>().unwrap(), "Alice");
    }

    #[test]
    fn test_scripting_engine_execute_add_sub_functions() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test add function
        let script = r#"
            let add_request = add("User$1", "Score", 100);
            add_request
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        
        let map = result.unwrap().try_cast::<rhai::Map>().unwrap();
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");
        
        // Test sub function
        let script = r#"
            let sub_request = sub("User$1", "Health", 10);
            sub_request
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        
        let map = result.unwrap().try_cast::<rhai::Map>().unwrap();
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");
    }

    #[test]
    fn test_scripting_engine_execute_error_handling() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Test syntax error
        let result = scripting_engine.execute_raw("let x = ;"); // Invalid syntax
        assert!(result.is_err());
        
        let error_message = format!("{}", result.unwrap_err());
        assert!(error_message.contains("Script execution error"));
    }

    #[test]
    fn test_scripting_engine_execute_complex_script() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        let script = r#"
            let requests = [];
            
            // Add multiple operations
            requests.push(write("Player$1", "Name", "TestPlayer"));
            requests.push(add("Player$1", "Score", 500));
            requests.push(sub("Player$1", "Health", 25));
            
            // Add conditional operations
            requests.push(write("Player$1", "Status", "active", "changes"));
            requests.push(add("Player$1", "Experience", 100, "always", "Game$1"));
            
            requests.len()
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_int().unwrap(), 5);
    }

    #[test]
    fn test_scripting_engine_execute_with_metadata() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        let script = r#"
            let request = write("User$1", "Gold", 1000, "always", "add", "Admin$1", 1625097600);
            request
        "#;
        
        let result = scripting_engine.execute_raw(script);
        assert!(result.is_ok());
        
        let map = result.unwrap().try_cast::<rhai::Map>().unwrap();
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Admin$1");
        assert_eq!(map.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1625097600);
    }

    #[test]
    fn test_scripting_engine_scope_isolation() {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());
        
        // Execute first script that defines a variable
        let script1 = "let x = 42; x";
        let result1 = scripting_engine.execute_raw(script1);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().as_int().unwrap(), 42);
        
        // Execute second script - variable x should not be accessible
        let script2 = "x"; // This should fail because x is not defined in this scope
        let result2 = scripting_engine.execute_raw(script2);
        assert!(result2.is_err()); // Should fail because x is not defined
    }
}
