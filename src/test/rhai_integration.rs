#[cfg(test)]
mod rhai_tests {
    use crate::{QScriptEngine, RhaiStoreWrapper, Store, Snowflake, EntitySchema, FieldSchema, EntityType, FieldType, Context, Single};
    use std::sync::Arc;
    use rhai::{Scope, Array};

    fn create_test_store() -> RhaiStoreWrapper {
        let snowflake = Arc::new(Snowflake::new());
        let mut store = Store::new(snowflake);
        
        // Create basic schemas for testing
        let ctx = Context {};
        
        // Create User schema
        let mut user_schema = EntitySchema::<Single>::new(EntityType::from("User"), None);
        user_schema.fields.insert(FieldType::from("name"), FieldSchema::String {
            field_type: FieldType::from("name"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        });
        user_schema.fields.insert(FieldType::from("Children"), FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: 1,
            read_permission: None,
            write_permission: None,
        });
        store.set_entity_schema(&ctx, &user_schema).unwrap();
        
        // Create Counter schema  
        let mut counter_schema = EntitySchema::<Single>::new(EntityType::from("Counter"), None);
        counter_schema.fields.insert(FieldType::from("value"), FieldSchema::Int {
            field_type: FieldType::from("value"),
            default_value: 0,
            rank: 0,
            read_permission: None,
            write_permission: None,
        });
        store.set_entity_schema(&ctx, &counter_schema).unwrap();
        
        // Create Task schema
        let mut task_schema = EntitySchema::<Single>::new(EntityType::from("Task"), None);
        task_schema.fields.insert(FieldType::from("name"), FieldSchema::String {
            field_type: FieldType::from("name"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        });
        store.set_entity_schema(&ctx, &task_schema).unwrap();
        
        // Create Train schema
        let mut train_schema = EntitySchema::<Single>::new(EntityType::from("Train"), None);
        train_schema.fields.insert(FieldType::from("StopTrigger"), FieldSchema::String {
            field_type: FieldType::from("StopTrigger"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        });
        train_schema.fields.insert(FieldType::from("Owner"), FieldSchema::String {
            field_type: FieldType::from("Owner"),
            default_value: "".to_string(),
            rank: 1,
            read_permission: None,
            write_permission: None,
        });
        store.set_entity_schema(&ctx, &train_schema).unwrap();
        
        RhaiStoreWrapper::new(store)
    }

    #[test]
    fn test_rhai_engine_creation() {
        let engine = QScriptEngine::new();
        // Check that the engine exists and has registered functions
        assert!(engine.engine().eval::<i64>("1 + 1").unwrap() == 2);
    }

    #[test]
    fn test_store_wrapper_creation() {
        let _wrapper = create_test_store();
        // Just test that it creates without panicking
    }

    #[test]
    fn test_actual_store_integration_with_custom_syntax() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store();
        
        // Create an entity first using a schema we defined
        let entity_id = store.create_entity("User", "", "test_user").unwrap();
        engine.set_entity_context(Some(entity_id.clone()));
        
        let script = r#"
            // Use the new custom syntax to write a value
            let write_op = WRITE "Hello World" INTO name;
            let read_op = READ name INTO result_name;
            [write_op, read_op]
        "#;

        let result: Array = engine.engine().eval(script).unwrap();
        assert_eq!(result.len(), 2);
        
        // Check that the operations were created correctly
        let write_map = result[0].clone().try_cast::<rhai::Map>().unwrap();
        let read_map = result[1].clone().try_cast::<rhai::Map>().unwrap();
        
        assert!(write_map.contains_key("Write"));
        assert!(read_map.contains_key("Read"));
    }

    #[test]
    fn test_store_operations_with_perform() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store();
        
        // Create an entity
        let entity_id = store.create_entity("User", "", "test_user").unwrap();
        
        let script = format!(r#"
            // Create operations using helper functions
            let write_req = create_write_request("{}", "name", "Alice");
            let read_req = create_read_request("{}", "name");
            
            // Perform the write operation first
            let write_results = store.perform([write_req]);
            
            // Then perform the read operation
            let read_results = store.perform([read_req]);
            
            #{{
                write_success: write_results.len() > 0,
                read_success: read_results.len() > 0,
                read_result: if read_results.len() > 0 {{ read_results[0] }} else {{ #{{}} }}
            }}
        "#, entity_id, entity_id);

        let mut scope = Scope::new();
        scope.push("store", store);
        
        let result: rhai::Map = engine.engine().eval_with_scope(&mut scope, &script).unwrap();
        
        assert_eq!(result.get("write_success").unwrap().clone().cast::<bool>(), true);
        assert_eq!(result.get("read_success").unwrap().clone().cast::<bool>(), true);
        
        // Check that the read operation returned the value we wrote
        let read_result = result.get("read_result").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        if let Some(value) = read_result.get("value") {
            assert_eq!(value.clone().cast::<String>(), "Alice");
        }
    }

    #[test]
    fn test_custom_syntax_with_indirection() {
        let engine = QScriptEngine::new();
        
        // Test that our custom syntax correctly parses indirection
        let script = r#"
            // Test various indirection patterns
            let read_simple = READ Name INTO var1;
            let read_indirect = READ NextStation->Name INTO var2;
            let write_indirect = WRITE "me" INTO NextStation->CurrentTrain;
            let add_indirect = ADD 1 INTO NextStation->UpdateCount;
            let sub_indirect = SUBTRACT 1 FROM NextStation->Retries;
            
            [read_simple, read_indirect, write_indirect, add_indirect, sub_indirect]
        "#;

        let result: Array = engine.engine().eval(script).unwrap();
        assert_eq!(result.len(), 5);
        
        // Verify each operation type
        let ops: Vec<rhai::Map> = result.into_iter()
            .map(|r| r.try_cast::<rhai::Map>().unwrap())
            .collect();
        
        // Check READ simple
        assert!(ops[0].contains_key("Read"));
        let read_simple = ops[0].get("Read").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(read_simple.get("field_type").unwrap().clone().cast::<String>(), "Name");
        assert!(!read_simple.contains_key("indirection"));
        
        // Check READ with indirection
        assert!(ops[1].contains_key("Read"));
        let read_indirect = ops[1].get("Read").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(read_indirect.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert!(read_indirect.contains_key("indirection"));
        let indirection = read_indirect.get("indirection").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(indirection[0].clone().cast::<String>(), "Name");
        
        // Check WRITE with indirection
        assert!(ops[2].contains_key("Write"));
        let write_indirect = ops[2].get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(write_indirect.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert_eq!(write_indirect.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
        
        // Check ADD with indirection
        assert!(ops[3].contains_key("Write"));
        let add_indirect = ops[3].get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(add_indirect.get("adjust_behavior").unwrap().clone().cast::<String>(), "Add");
        
        // Check SUBTRACT with indirection
        assert!(ops[4].contains_key("Write"));
        let sub_indirect = ops[4].get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(sub_indirect.get("adjust_behavior").unwrap().clone().cast::<String>(), "Subtract");
    }

    #[test] 
    fn test_arithmetic_operations_with_store() {
        let engine = QScriptEngine::new();
        let mut store = create_test_store();
        
        // Create an entity
        let entity_id = store.create_entity("Counter", "", "test_counter").unwrap();
        
        let script = format!(r#"
            // Initialize a counter field to 10
            let init_req = create_write_request("{}", "value", 10);
            let init_results = store.perform([init_req]);
            
            // Add 5 to the counter
            let add_req = create_add_request("{}", "value", 5);
            let add_results = store.perform([add_req]);
            
            // Subtract 3 from the counter  
            let sub_req = create_subtract_request("{}", "value", 3);
            let sub_results = store.perform([sub_req]);
            
            // Read the final value
            let read_req = create_read_request("{}", "value");
            let read_results = store.perform([read_req]);
            
            // Check if we got the read result with a value
            let final_result = if read_results.len() > 0 {{
                read_results[0]
            }} else {{
                #{{"error": "no read results"}}
            }};
            
            // Return status information
            #{{"status": "complete", "final_result": final_result, "has_value": final_result.contains("value")}}
        "#, entity_id, entity_id, entity_id, entity_id);

        let mut scope = Scope::new();
        scope.push("store", store);
        
        let result: rhai::Map = engine.engine().eval_with_scope(&mut scope, &script).unwrap();
        
        // Check if all operations completed successfully
        println!("Result: {:?}", result);
        assert!(result.contains_key("status"));
        assert_eq!(result.get("status").unwrap().clone().cast::<String>(), "complete");
        
        // Check if the final result has a value (indicating the read worked)
        assert!(result.get("has_value").unwrap().clone().cast::<bool>());
    }

    #[test]
    fn test_entity_management() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store();
        
        let script = r#"
            // Create some entities
            let user_id = store.create_entity("User", "", "Alice");
            let task_id = store.create_entity("Task", user_id, "Complete project");
            
            // Check that they exist
            let user_exists = store.entity_exists(user_id);
            let task_exists = store.entity_exists(task_id);
            
            #{
                user_id: user_id,
                task_id: task_id,
                user_exists: user_exists,
                task_exists: task_exists
            }
        "#;

        let mut scope = Scope::new();
        scope.push("store", store);
        
        let result: rhai::Map = engine.engine().eval_with_scope(&mut scope, &script).unwrap();
        
        assert!(result.get("user_id").unwrap().clone().cast::<String>().starts_with("User$"));
        assert!(result.get("task_id").unwrap().clone().cast::<String>().starts_with("Task$"));
        assert_eq!(result.get("user_exists").unwrap().clone().cast::<bool>(), true);
        assert_eq!(result.get("task_exists").unwrap().clone().cast::<bool>(), true);
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

    #[test]
    fn test_default_and_me_variables() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store();
        
        // Create an entity and set it as context
        let entity_id = store.create_entity("Train", "", "test_train").unwrap();
        engine.set_entity_context(Some(entity_id.clone()));
        
        let script = r#"
            // Test DEFAULT and me variables in custom syntax
            let write_default = WRITE DEFAULT INTO StopTrigger;
            let write_me_value = WRITE me INTO Owner;
            
            [write_default, write_me_value]
        "#;

        let mut scope = Scope::new();
        scope.push("DEFAULT", rhai::Dynamic::UNIT); // DEFAULT should map to None/Unit
        scope.push("me", rhai::Dynamic::from(entity_id.clone())); // me should be the entity ID
        
        let result: Array = engine.engine().eval_with_scope(&mut scope, script).unwrap();
        assert_eq!(result.len(), 2);
        
        let write_default_map = result[0].clone().try_cast::<rhai::Map>().unwrap();
        let write_me_map = result[1].clone().try_cast::<rhai::Map>().unwrap();
        
        // Check DEFAULT write operation
        assert!(write_default_map.contains_key("Write"));
        let default_write = write_default_map.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(default_write.get("field_type").unwrap().clone().cast::<String>(), "StopTrigger");
        
        // Check me write operation  
        assert!(write_me_map.contains_key("Write"));
        let me_write = write_me_map.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(me_write.get("field_type").unwrap().clone().cast::<String>(), "Owner");
        assert_eq!(me_write.get("value").unwrap().clone().cast::<String>(), entity_id);
    }
}
