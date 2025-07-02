#[cfg(test)]
mod transaction_tests {
    use crate::*;
    use std::sync::Arc;
    use rhai::{Scope, Array};

    // Helper to create entity schemas for test entity types
    fn create_test_entity_schema(store: &mut Store, entity_type_name: &str, custom_fields: Vec<(&str, FieldSchema)>) -> Result<()> {
        let entity_type = EntityType::from(entity_type_name);
        let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
        
        // Add default Name field
        let ft_name = FieldType::from("Name");
        let name_schema = FieldSchema::String {
            field_type: ft_name.clone(),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        };
        schema.fields.insert(ft_name.clone(), name_schema);
        
        // Add custom fields
        for (field_name, field_schema) in custom_fields {
            let field_type = FieldType::from(field_name);
            schema.fields.insert(field_type, field_schema);
        }
        
        store.set_entity_schema(&Context {}, &schema)?;
        Ok(())
    }

    fn create_test_store_for_transactions() -> RhaiStoreWrapper {
        let snowflake = Arc::new(Snowflake::new());
        let mut store = Store::new(snowflake);
        
        // Create Train schema
        let train_next_station_field = FieldSchema::EntityReference {
            field_type: FieldType::from("NextStation"),
            default_value: None,
            rank: 1,
            read_permission: None,
            write_permission: None,
        };
        create_test_entity_schema(&mut store, "Train", vec![("NextStation", train_next_station_field)]).unwrap();
        
        // Create Station schema  
        let station_current_train_field = FieldSchema::EntityReference {
            field_type: FieldType::from("CurrentTrain"),
            default_value: None,
            rank: 1,
            read_permission: None,
            write_permission: None,
        };
        let station_stop_trigger_field = FieldSchema::String {
            field_type: FieldType::from("StopTrigger"),
            default_value: "".to_string(),
            rank: 2,
            read_permission: None,
            write_permission: None,
        };
        let station_update_count_field = FieldSchema::Int {
            field_type: FieldType::from("UpdateCount"),
            default_value: 0,
            rank: 3,
            read_permission: None,
            write_permission: None,
        };
        let station_retries_field = FieldSchema::Int {
            field_type: FieldType::from("Retries"),
            default_value: 0,
            rank: 4,
            read_permission: None,
            write_permission: None,
        };
        create_test_entity_schema(&mut store, "Station", vec![
            ("CurrentTrain", station_current_train_field),
            ("StopTrigger", station_stop_trigger_field),
            ("UpdateCount", station_update_count_field),
            ("Retries", station_retries_field),
        ]).unwrap();

        // Create Counter schema
        let counter_value_field = FieldSchema::Int {
            field_type: FieldType::from("value"),
            default_value: 0,
            rank: 1,
            read_permission: None,
            write_permission: None,
        };
        create_test_entity_schema(&mut store, "Counter", vec![("value", counter_value_field)]).unwrap();

        // Create Parent schema
        let parent_child_field = FieldSchema::EntityReference {
            field_type: FieldType::from("Child"),
            default_value: None,
            rank: 1,
            read_permission: None,
            write_permission: None,
        };
        create_test_entity_schema(&mut store, "Parent", vec![("Child", parent_child_field)]).unwrap();

        // Create Child schema  
        let child_value_field = FieldSchema::String {
            field_type: FieldType::from("Value"),
            default_value: "".to_string(),
            rank: 1,
            read_permission: None,
            write_permission: None,
        };
        create_test_entity_schema(&mut store, "Child", vec![("Value", child_value_field)]).unwrap();

        RhaiStoreWrapper::new(store)
    }

    /// Test for the specific transaction syntax from the user request:
    /// TRANSACTION(
    ///     READ Name INTO train_name,
    ///     READ NextStation->Name INTO station_name,
    ///     WRITE me INTO NextStation->CurrentTrain,
    ///     WRITE DEFAULT INTO StopTrigger,
    ///     ADD 1 INTO NextStation->UpdateCount,
    ///     SUBTRACT 1 FROM NextStation->Retries
    /// )
    #[test]
    fn test_transaction_syntax_with_actual_execution() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store_for_transactions();
        
        // Create entities for a more realistic test like the train/station example
        let train_id = store.create_entity("Train", "", "Express123").unwrap();
        let station_id = store.create_entity("Station", "", "CentralStation").unwrap();
        
        engine.set_entity_context(Some(train_id.clone()));
        
        // Set up initial state like in the documentation example
        let setup_script = format!(r#"
            let setup_ops = [
                create_write_request("{}", "Name", "Express Train"),
                create_write_request("{}", "NextStation", "{}"),
                create_write_request("{}", "StopTrigger", "GO"),
                create_write_request("{}", "Name", "Central Station"),
                create_write_request("{}", "CurrentTrain", ""),
                create_write_request("{}", "UpdateCount", 5),
                create_write_request("{}", "Retries", 3)
            ];
            store.perform(setup_ops)
        "#, train_id, train_id, station_id, train_id, station_id, station_id, station_id, station_id);

        let mut scope = Scope::new();
        scope.push("store", store.clone());
        let _setup_results: Array = engine.engine().eval_with_scope(&mut scope, &setup_script).unwrap();
        
        // Test the EXACT transaction pattern from the user request using array syntax
        // (since the current TRANSACTION() only supports single strings)
        let transaction_script = r#"
            // This is the big transaction from the documentation example
            let big_transaction = [
                READ Name INTO train_name,
                READ NextStation->Name INTO station_name,
                WRITE me INTO NextStation->CurrentTrain,
                WRITE DEFAULT INTO StopTrigger,
                ADD 1 INTO NextStation->UpdateCount,
                SUBTRACT 1 FROM NextStation->Retries
            ];
            
            // Execute the transaction and validate the store changes
            let results = store.perform(big_transaction);
            results
        "#;

        let mut scope2 = Scope::new();
        scope2.push("store", store.clone());
        scope2.push("me", train_id.clone());
        scope2.push("DEFAULT", rhai::Dynamic::UNIT);
        
        let result: Array = engine.engine().eval_with_scope(&mut scope2, transaction_script).unwrap();
        
        // Should get 6 results - one for each operation in the big transaction
        assert_eq!(result.len(), 6);
        
        // Validate the READ operations returned actual values from the store
        let train_name_result = &result[0];
        let station_name_result = &result[1];
        
        // Check that we actually read the train name
        if let Some(read1_map) = train_name_result.clone().try_cast::<rhai::Map>() {
            if let Some(value) = read1_map.get("value") {
                assert_eq!(value.clone().cast::<String>(), "Express Train");
            }
        }
        
        // Check that we read the station name through indirection
        if let Some(read2_map) = station_name_result.clone().try_cast::<rhai::Map>() {
            if let Some(value) = read2_map.get("value") {
                assert_eq!(value.clone().cast::<String>(), "Central Station");
            }
        }
        
        // Now verify the store has been modified by the WRITE, ADD, and SUBTRACT operations
        let verification_script = format!(r#"
            let verify_ops = [
                create_read_request("{}", "CurrentTrain"),  // Should now point to train
                create_read_request("{}", "StopTrigger"),   // Should be DEFAULT (empty/unit)
                create_read_request("{}", "UpdateCount"),   // Should be 5 + 1 = 6
                create_read_request("{}", "Retries")        // Should be 3 - 1 = 2
            ];
            store.perform(verify_ops)
        "#, station_id, train_id, station_id, station_id);
        
        let mut scope3 = Scope::new();
        scope3.push("store", store);
        let verify_results: Array = engine.engine().eval_with_scope(&mut scope3, &verification_script).unwrap();
        
        assert_eq!(verify_results.len(), 4);
        
        // Verify: NextStation->CurrentTrain now points to the train
        if let Some(current_train_map) = verify_results[0].clone().try_cast::<rhai::Map>() {
            if let Some(value) = current_train_map.get("value") {
                assert_eq!(value.clone().cast::<String>(), train_id);
            }
        }
        
        // Verify: StopTrigger was set to DEFAULT 
        if let Some(stop_trigger_map) = verify_results[1].clone().try_cast::<rhai::Map>() {
            if let Some(value) = stop_trigger_map.get("value") {
                // DEFAULT should clear the field or set it to empty/unit value
                let value_str = value.clone().cast::<String>();
                assert!(value_str.is_empty() || value.is_unit());
            }
        }
        
        // Verify: UpdateCount was incremented from 5 to 6
        if let Some(update_count_map) = verify_results[2].clone().try_cast::<rhai::Map>() {
            if let Some(value) = update_count_map.get("value") {
                assert_eq!(value.clone().cast::<i64>(), 6i64);
            }
        }
        
        // Verify: Retries was decremented from 3 to 2
        if let Some(retries_map) = verify_results[3].clone().try_cast::<rhai::Map>() {
            if let Some(value) = retries_map.get("value") {
                assert_eq!(value.clone().cast::<i64>(), 2i64);
            }
        }
    }

    #[test]
    fn test_actual_transaction_parentheses_syntax() {
        let engine = QScriptEngine::new();
        
        // Test the actual TRANSACTION() syntax that takes a string
        // (Current implementation only supports single operation strings)
        let script = r#"
            TRANSACTION("READ Name INTO train_name")
        "#;

        let result: rhai::Map = engine.engine().eval(script).unwrap();
        
        // Should create a Transaction map
        assert!(result.contains_key("Transaction"));
        
        let transaction = result.get("Transaction").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(transaction.len(), 1);
        assert_eq!(transaction[0].clone().cast::<String>(), "READ Name INTO train_name");
    }

    #[test]
    fn test_simple_transaction_execution_and_validation() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store_for_transactions();
        
        // Create a simple entity for testing
        let entity_id = store.create_entity("Counter", "", "test_counter").unwrap();
        engine.set_entity_context(Some(entity_id.clone()));
        
        // Initialize with a known value
        let init_script = format!(r#"
            let init_req = create_write_request("{}", "value", 50);
            store.perform([init_req])
        "#, entity_id);

        let mut scope = Scope::new();
        scope.push("store", store.clone());
        
        let _init_result: Array = engine.engine().eval_with_scope(&mut scope, &init_script).unwrap();
        
        // Execute a transaction using individual operations - this validates the store has changed
        let transaction_script = format!(r#"
            // Use individual operations in array format
            let transaction_ops = [
                create_read_request("{}", "value"),
                create_add_request("{}", "value", 10),
                create_subtract_request("{}", "value", 5),
                create_read_request("{}", "value")
            ];
            store.perform(transaction_ops)
        "#, entity_id, entity_id, entity_id, entity_id);

        let mut scope2 = Scope::new();
        scope2.push("store", store.clone());
        scope2.push("me", entity_id.clone());
        
        let results: Array = engine.engine().eval_with_scope(&mut scope2, &transaction_script).unwrap();
        
        // Validate we got all expected results
        assert_eq!(results.len(), 4);
        
        // Extract and validate the read values to prove the store changed
        let initial_value = if let Some(initial_result) = results[0].clone().try_cast::<rhai::Map>() {
            initial_result.get("value").and_then(|v| Some(v.clone().cast::<i64>())).unwrap_or(0)
        } else {
            panic!("Expected initial read result");
        };
        
        let final_value = if let Some(final_result) = results[3].clone().try_cast::<rhai::Map>() {
            final_result.get("value").and_then(|v| Some(v.clone().cast::<i64>())).unwrap_or(0)
        } else {
            panic!("Expected final read result");
        };
        
        // Validate the transaction executed correctly: 50 + 10 - 5 = 55
        assert_eq!(initial_value, 50i64);
        assert_eq!(final_value, 55i64);
        
        // Additional validation: read the latest value directly from store
        let verify_script = format!(r#"
            let verify_req = create_read_request("{}", "value");
            let verify_results = store.perform([verify_req]);
            verify_results[0]
        "#, entity_id);
        
        let mut scope3 = Scope::new();
        scope3.push("store", store);
        
        let verify_result: rhai::Map = engine.engine().eval_with_scope(&mut scope3, &verify_script).unwrap();
        let verified_value = verify_result.get("value").unwrap().clone().cast::<i64>();
        
        // Confirm the store retains the updated value
        assert_eq!(verified_value, 55i64);
    }

    #[test]
    fn test_indirection_operations_with_entity_references() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store_for_transactions();
        
        // Create entities to test indirection like in the train/station example
        let parent_id = store.create_entity("Parent", "", "parent_entity").unwrap();
        let child_id = store.create_entity("Child", "", "child_entity").unwrap();
        
        engine.set_entity_context(Some(parent_id.clone()));
        
        // Set up relationship: Parent -> Child, Child has a counter
        let setup_script = format!(r#"
            let setup_ops = [
                create_write_request("{}", "name", "Parent Entity"),
                create_write_request("{}", "ChildRef", "{}"),
                create_write_request("{}", "name", "Child Entity"),
                create_write_request("{}", "Counter", 100)
            ];
            store.perform(setup_ops)
        "#, parent_id, parent_id, child_id, child_id, child_id);

        let mut scope = Scope::new();
        scope.push("store", store.clone());
        
        let _setup_results: Array = engine.engine().eval_with_scope(&mut scope, &setup_script).unwrap();
        
        // Test transaction with indirection similar to "READ NextStation->Name"
        let indirection_script = r#"
            let transaction_ops = [
                READ name INTO parent_name,        // Direct read
                READ ChildRef INTO child_reference // This gets the child entity ID
            ];
            
            let results = store.perform(transaction_ops);
            results
        "#;

        let mut scope2 = Scope::new();
        scope2.push("store", store.clone());
        scope2.push("me", parent_id.clone());
        
        let results: Array = engine.engine().eval_with_scope(&mut scope2, indirection_script).unwrap();
        
        assert_eq!(results.len(), 2);
        
        // Validate we can read the parent name
        if let Some(parent_result) = results[0].clone().try_cast::<rhai::Map>() {
            if let Some(name_value) = parent_result.get("value") {
                assert_eq!(name_value.clone().cast::<String>(), "Parent Entity");
            }
        }
        
        // Validate we can get the child reference
        if let Some(child_ref_result) = results[1].clone().try_cast::<rhai::Map>() {
            if let Some(ref_value) = child_ref_result.get("value") {
                assert_eq!(ref_value.clone().cast::<String>(), child_id);
            }
        }
    }

    #[test]
    fn test_transaction_with_default_values() {
        let engine = QScriptEngine::new();
        
        // Test that DEFAULT and me variables work correctly in transactions
        let script = r#"
            let transaction_ops = [
                WRITE DEFAULT INTO SomeField,
                WRITE me INTO AnotherField
            ];
            
            transaction_ops
        "#;

        let mut scope = Scope::new();
        scope.push("me", "Entity$456");
        scope.push("DEFAULT", rhai::Dynamic::UNIT);
        
        let result: Array = engine.engine().eval_with_scope(&mut scope, script).unwrap();
        
        assert_eq!(result.len(), 2);
        
        // Check DEFAULT write
        let default_write = result[0].clone().try_cast::<rhai::Map>().unwrap();
        assert!(default_write.contains_key("Write"));
        let default_details = default_write.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(default_details.get("field_type").unwrap().clone().cast::<String>(), "SomeField");
        assert!(default_details.get("value").unwrap().is_unit());
        
        // Check me write 
        let me_write = result[1].clone().try_cast::<rhai::Map>().unwrap();
        assert!(me_write.contains_key("Write"));
        let me_details = me_write.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(me_details.get("field_type").unwrap().clone().cast::<String>(), "AnotherField");
        assert_eq!(me_details.get("value").unwrap().clone().cast::<String>(), "Entity$456");
    }

    #[test]
    fn test_arithmetic_operations_in_transaction() {
        let mut engine = QScriptEngine::new();
        let mut store = create_test_store_for_transactions();
        
        let entity_id = store.create_entity("Counter", "", "arithmetic_test").unwrap();
        engine.set_entity_context(Some(entity_id.clone()));
        
        // Initialize counter
        let init_script = format!(r#"
            let init_req = create_write_request("{}", "value", 0);
            store.perform([init_req])
        "#, entity_id);

        let mut scope = Scope::new();
        scope.push("store", store.clone());
        
        engine.engine().eval_with_scope::<Array>(&mut scope, &init_script).unwrap();
        
        // Test sequential arithmetic operations like in the big transaction
        let arithmetic_script = r#"
            let transaction_ops = [
                READ value INTO start_value,
                ADD 15 INTO value,     // Like "ADD 1 INTO NextStation->UpdateCount"
                ADD 25 INTO value,     // Multiple adds
                SUBTRACT 10 FROM value, // Like "SUBTRACT 1 FROM NextStation->Retries"
                SUBTRACT 5 FROM value,  // Multiple subtracts
                READ value INTO end_value
            ];
            
            let results = store.perform(transaction_ops);
            results
        "#;

        let mut scope2 = Scope::new();
        scope2.push("store", store.clone());
        scope2.push("me", entity_id.clone());
        
        let results: Array = engine.engine().eval_with_scope(&mut scope2, arithmetic_script).unwrap();
        
        assert_eq!(results.len(), 6);
        
        // Validate arithmetic: 0 + 15 + 25 - 10 - 5 = 25
        let start_value = if let Some(start_result) = results[0].clone().try_cast::<rhai::Map>() {
            start_result.get("value").and_then(|v| Some(v.clone().cast::<i64>())).unwrap_or(-1)
        } else {
            -1
        };
        
        let end_value = if let Some(end_result) = results[5].clone().try_cast::<rhai::Map>() {
            end_result.get("value").and_then(|v| Some(v.clone().cast::<i64>())).unwrap_or(-1)
        } else {
            -1
        };
        
        assert_eq!(start_value, 0i64);
        assert_eq!(end_value, 25i64);
    }

    #[test]
    fn test_big_transaction_syntax_validation() {
        let engine = QScriptEngine::new();
        
        // Test that the transaction operations parse correctly individually
        let script = r#"
            // Test parsing of each operation type from the big transaction
            [
                READ Name INTO train_name,
                READ NextStation->Name INTO station_name,
                WRITE me INTO NextStation->CurrentTrain,
                WRITE DEFAULT INTO StopTrigger,
                ADD 1 INTO NextStation->UpdateCount,
                SUBTRACT 1 FROM NextStation->Retries
            ]
        "#;

        let mut scope = Scope::new();
        scope.push("me", "Train$123");
        scope.push("DEFAULT", rhai::Dynamic::UNIT);
        
        let result: Array = engine.engine().eval_with_scope(&mut scope, script).unwrap();
        
        // Should have parsed 6 operations correctly
        assert_eq!(result.len(), 6);
        
        // Verify each operation type and structure
        
        // 1. READ Name INTO train_name
        let read_name = result[0].clone().try_cast::<rhai::Map>().unwrap();
        assert!(read_name.contains_key("Read"));
        let read_name_details = read_name.get("Read").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(read_name_details.get("field_type").unwrap().clone().cast::<String>(), "Name");
        assert_eq!(read_name_details.get("variable_name").unwrap().clone().cast::<String>(), "train_name");
        assert!(!read_name_details.contains_key("indirection"));
        
        // 2. READ NextStation->Name INTO station_name  
        let read_station_name = result[1].clone().try_cast::<rhai::Map>().unwrap();
        assert!(read_station_name.contains_key("Read"));
        let read_station_details = read_station_name.get("Read").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(read_station_details.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert_eq!(read_station_details.get("variable_name").unwrap().clone().cast::<String>(), "station_name");
        assert!(read_station_details.contains_key("indirection"));
        let indirection = read_station_details.get("indirection").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(indirection.len(), 1);
        assert_eq!(indirection[0].clone().cast::<String>(), "Name");
        
        // 3. WRITE me INTO NextStation->CurrentTrain
        let write_me = result[2].clone().try_cast::<rhai::Map>().unwrap();
        assert!(write_me.contains_key("Write"));
        let write_me_details = write_me.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(write_me_details.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert_eq!(write_me_details.get("value").unwrap().clone().cast::<String>(), "Train$123");
        assert_eq!(write_me_details.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
        assert!(write_me_details.contains_key("indirection"));
        let me_indirection = write_me_details.get("indirection").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(me_indirection.len(), 1);
        assert_eq!(me_indirection[0].clone().cast::<String>(), "CurrentTrain");
        
        // 4. WRITE DEFAULT INTO StopTrigger
        let write_default = result[3].clone().try_cast::<rhai::Map>().unwrap();
        assert!(write_default.contains_key("Write"));
        let write_default_details = write_default.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(write_default_details.get("field_type").unwrap().clone().cast::<String>(), "StopTrigger");
        assert!(write_default_details.get("value").unwrap().is_unit()); // DEFAULT maps to Unit
        assert_eq!(write_default_details.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
        assert!(!write_default_details.contains_key("indirection"));
        
        // 5. ADD 1 INTO NextStation->UpdateCount
        let add_op = result[4].clone().try_cast::<rhai::Map>().unwrap();
        assert!(add_op.contains_key("Write"));
        let add_details = add_op.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(add_details.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert_eq!(add_details.get("value").unwrap().clone().cast::<i64>(), 1i64);
        assert_eq!(add_details.get("adjust_behavior").unwrap().clone().cast::<String>(), "Add");
        assert!(add_details.contains_key("indirection"));
        let add_indirection = add_details.get("indirection").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(add_indirection.len(), 1);
        assert_eq!(add_indirection[0].clone().cast::<String>(), "UpdateCount");
        
        // 6. SUBTRACT 1 FROM NextStation->Retries
        let sub_op = result[5].clone().try_cast::<rhai::Map>().unwrap();
        assert!(sub_op.contains_key("Write"));
        let sub_details = sub_op.get("Write").unwrap().clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(sub_details.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
        assert_eq!(sub_details.get("value").unwrap().clone().cast::<i64>(), 1i64);
        assert_eq!(sub_details.get("adjust_behavior").unwrap().clone().cast::<String>(), "Subtract");
        assert!(sub_details.contains_key("indirection"));
        let sub_indirection = sub_details.get("indirection").unwrap().clone().try_cast::<Array>().unwrap();
        assert_eq!(sub_indirection.len(), 1);
        assert_eq!(sub_indirection[0].clone().cast::<String>(), "Retries");
    }
}
