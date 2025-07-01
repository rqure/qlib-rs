#[cfg(test)]
mod rhai_tests {
    use crate::{QScriptEngine, RhaiStoreWrapper, Store};
    use std::sync::Arc;

    #[test]
    fn test_rhai_engine_creation() {
        let engine = QScriptEngine::new();
        // Check that the engine exists and has registered functions
        assert!(engine.engine().eval::<i64>("1 + 1").unwrap() == 2);
    }

    #[test]
    fn test_store_wrapper_creation() {
        let snowflake = Arc::new(crate::Snowflake::new());
        let store = Store::new(snowflake);
        let _wrapper = RhaiStoreWrapper::new(store);
        // Just test that it creates without panicking
    }

    #[test]
    fn test_basic_perform_api() {
        // For now, just test that the request creation functions work
        // Full integration tests will be done with actual working examples
        let engine = QScriptEngine::new();

        let script = r#"
            // Test request creation helpers
            let read_req = create_read_request("User$123", "name");
            let write_req = create_write_request("User$123", "age", 30);
            
            #{
                read_type: read_req.type,
                write_type: write_req.type,
                write_value: write_req.value
            }
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        assert_eq!(result.get("read_type").unwrap().clone().cast::<String>(), "Read");
        assert_eq!(result.get("write_type").unwrap().clone().cast::<String>(), "Write");
        assert_eq!(result.get("write_value").unwrap().clone().cast::<i64>(), 30);
    }

    #[test]
    fn test_request_helpers() {
        let engine = QScriptEngine::new();

        let script = r#"
            // Test request creation helpers
            let read_req = create_read_request("User$123", "name");
            let write_req = create_write_request("User$123", "age", 30);
            let add_req = create_add_request("User$123", "score", 10);
            let sub_req = create_subtract_request("User$123", "credits", 5);
            
            #{
                read_type: read_req.type,
                write_type: write_req.type,
                write_behavior: write_req.adjust_behavior,
                add_behavior: add_req.adjust_behavior,
                sub_behavior: sub_req.adjust_behavior
            }
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        assert_eq!(result.get("read_type").unwrap().clone().cast::<String>(), "Read");
        assert_eq!(result.get("write_type").unwrap().clone().cast::<String>(), "Write");
        assert_eq!(result.get("write_behavior").unwrap().clone().cast::<String>(), "Set");
        assert_eq!(result.get("add_behavior").unwrap().clone().cast::<String>(), "Add");
        assert_eq!(result.get("sub_behavior").unwrap().clone().cast::<String>(), "Subtract");
    }

    #[test]
    fn test_entity_id_helpers() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            let entity_id = format_entity_id("User", 123);
            let parsed = parse_entity_id(entity_id);
            [entity_id, parsed["type"], parsed["id"]]
        "#;

        let result: rhai::Array = engine.engine().eval(script).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].clone().cast::<String>(), "User$123");
        assert_eq!(result[1].clone().cast::<String>(), "User");
        assert_eq!(result[2].clone().cast::<i64>(), 123);
    }

    #[test]
    fn test_arithmetic_operations() {
        // Test arithmetic request creation
        let engine = QScriptEngine::new();

        let script = r#"
            // Test arithmetic request helpers
            let add_req = create_add_request("Counter$1", "value", 5);
            let sub_req = create_subtract_request("Counter$1", "value", 3);
            
            #{
                add_behavior: add_req.adjust_behavior,
                sub_behavior: sub_req.adjust_behavior,
                add_value: add_req.value,
                sub_value: sub_req.value
            }
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        assert_eq!(result.get("add_behavior").unwrap().clone().cast::<String>(), "Add");
        assert_eq!(result.get("sub_behavior").unwrap().clone().cast::<String>(), "Subtract");
        assert_eq!(result.get("add_value").unwrap().clone().cast::<i64>(), 5);
        assert_eq!(result.get("sub_value").unwrap().clone().cast::<i64>(), 3);
    }

    #[test]
    fn test_entity_management() {
        // Test entity ID utilities
        let engine = QScriptEngine::new();

        let script = r#"
            // Test entity ID utilities
            let user_id = format_entity_id("User", 123);
            let task_id = format_entity_id("Task", 456);
            
            let parsed_user = parse_entity_id(user_id);
            let parsed_task = parse_entity_id(task_id);
            
            #{
                user_id: user_id,
                task_id: task_id,
                user_type: parsed_user["type"],
                user_numeric_id: parsed_user["id"],
                task_type: parsed_task["type"],
                task_numeric_id: parsed_task["id"]
            }
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        assert_eq!(result.get("user_id").unwrap().clone().cast::<String>(), "User$123");
        assert_eq!(result.get("task_id").unwrap().clone().cast::<String>(), "Task$456");
        assert_eq!(result.get("user_type").unwrap().clone().cast::<String>(), "User");
        assert_eq!(result.get("user_numeric_id").unwrap().clone().cast::<i64>(), 123);
        assert_eq!(result.get("task_type").unwrap().clone().cast::<String>(), "Task");
        assert_eq!(result.get("task_numeric_id").unwrap().clone().cast::<i64>(), 456);
    }

    #[test]
    fn test_utility_functions() {
        let engine = QScriptEngine::new();

        let script = r#"
            // Test utility functions
            let map = create_map();
            map["test"] = "value";
            
            let array = create_array();
            array.push(1);
            array.push(2);
            array.push(3);
            
            let entity_id = format_entity_id("User", 123);
            let parsed = parse_entity_id(entity_id);
            
            #{
                map_value: map["test"],
                array_length: array.len(),
                entity_id: entity_id,
                parsed_type: parsed["type"],
                parsed_id: parsed["id"]
            }
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        assert_eq!(result.get("map_value").unwrap().clone().cast::<String>(), "value");
        assert_eq!(result.get("array_length").unwrap().clone().cast::<i64>(), 3);
        assert_eq!(result.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
        assert_eq!(result.get("parsed_type").unwrap().clone().cast::<String>(), "User");
        assert_eq!(result.get("parsed_id").unwrap().clone().cast::<i64>(), 123);
    }
}
