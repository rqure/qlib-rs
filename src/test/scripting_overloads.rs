#[cfg(test)]
mod tests {
    use crate::*;
    use crate::scripting::ScriptingEngine;
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    #[test]
    fn test_write_function_overloads() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test basic write function (3 parameters)
        let script1 = r#"
            let request1 = write("User$1", "Name", "John Doe");
            request1
        "#;
        
        let result1 = scripting_engine.execute(script1).unwrap();
        let map1 = result1.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map1.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map1.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map1.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
        assert_eq!(map1.get("value").unwrap().clone().try_cast::<String>().unwrap(), "John Doe");
        assert_eq!(map1.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map1.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "set");

        // Test write function with push_condition (4 parameters)
        let script2 = r#"
            let request2 = write("User$1", "Name", "Jane Doe", "changes");
            request2
        "#;
        
        let result2 = scripting_engine.execute(script2).unwrap();
        let map2 = result2.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map2.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map2.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "set");

        // Test write function with push_condition and adjust_behavior (5 parameters)
        let script3 = r#"
            let request3 = write("User$1", "Score", 100, "always", "add");
            request3
        "#;
        
        let result3 = scripting_engine.execute(script3).unwrap();
        let map3 = result3.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map3.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map3.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");

        Ok(())
    }

    #[test]
    fn test_add_function() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test basic add function (3 parameters)
        let script1 = r#"
            let request1 = add("User$1", "Score", 50);
            request1
        "#;
        
        let result1 = scripting_engine.execute(script1).unwrap();
        let map1 = result1.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map1.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map1.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map1.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Score");
        assert_eq!(map1.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 50);
        assert_eq!(map1.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map1.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");

        // Test add function with push_condition (4 parameters)
        let script2 = r#"
            let request2 = add("User$1", "Score", 25, "changes");
            request2
        "#;
        
        let result2 = scripting_engine.execute(script2).unwrap();
        let map2 = result2.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map2.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map2.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");

        Ok(())
    }

    #[test]
    fn test_sub_function() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test basic sub function (3 parameters)
        let script1 = r#"
            let request1 = sub("User$1", "Score", 10);
            request1
        "#;
        
        let result1 = scripting_engine.execute(script1).unwrap();
        let map1 = result1.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map1.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map1.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map1.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Score");
        assert_eq!(map1.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 10);
        assert_eq!(map1.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map1.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");

        // Test sub function with push_condition (4 parameters)
        let script2 = r#"
            let request2 = sub("User$1", "Health", 5, "changes");
            request2
        "#;
        
        let result2 = scripting_engine.execute(script2).unwrap();
        let map2 = result2.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map2.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map2.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");

        Ok(())
    }

    #[test]
    fn test_mixed_operations_script() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test a script that uses all function variants
        let script = r#"
            let requests = [];
            
            // Basic write
            requests.push(write("User$1", "Name", "Alice"));
            
            // Write with push condition
            requests.push(write("User$1", "Email", "alice@example.com", "changes"));
            
            // Write with push condition and adjust behavior
            requests.push(write("User$1", "LoginCount", 1, "always", "add"));
            
            // Add operation
            requests.push(add("User$1", "Score", 100));
            
            // Add with push condition
            requests.push(add("User$1", "Experience", 50, "changes"));
            
            // Subtract operation
            requests.push(sub("User$1", "Health", 10));
            
            // Subtract with push condition
            requests.push(sub("User$1", "Mana", 5, "changes"));
            
            requests
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let requests = result.try_cast::<rhai::Array>().unwrap();
        
        assert_eq!(requests.len(), 7);
        
        // Check first request (basic write)
        let req1 = requests[0].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req1.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "set");
        
        // Check add request
        let req4 = requests[3].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req4.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");
        
        // Check subtract request
        let req6 = requests[5].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req6.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");

        Ok(())
    }

    #[test]
    fn test_write_with_writer_id() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test write function with writer_id (6 parameters)
        let script = r#"
            let request = write("User$1", "Name", "John", "always", "set", "Admin$1");
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<String>().unwrap(), "John");
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "set");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Admin$1");

        Ok(())
    }

    #[test]
    fn test_write_with_writer_id_and_write_time() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test write function with writer_id and write_time (7 parameters)
        let script = r#"
            let request = write("User$1", "Name", "Jane", "changes", "set", "Admin$2", 1234567890);
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<String>().unwrap(), "Jane");
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "set");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Admin$2");
        assert_eq!(map.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1234567890);

        Ok(())
    }

    #[test]
    fn test_add_with_writer_id() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test add function with writer_id (5 parameters)
        let script = r#"
            let request = add("User$1", "Score", 100, "always", "System$1");
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Score");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 100);
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "System$1");

        Ok(())
    }

    #[test]
    fn test_add_with_writer_id_and_write_time() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test add function with writer_id and write_time (6 parameters)
        let script = r#"
            let request = add("User$1", "Experience", 250, "changes", "Game$1", 9876543210);
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Experience");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 250);
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Game$1");
        assert_eq!(map.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 9876543210);

        Ok(())
    }

    #[test]
    fn test_sub_with_writer_id() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test sub function with writer_id (5 parameters)
        let script = r#"
            let request = sub("User$1", "Health", 25, "always", "Enemy$1");
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Health");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 25);
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "always");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Enemy$1");

        Ok(())
    }

    #[test]
    fn test_sub_with_writer_id_and_write_time() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test sub function with writer_id and write_time (6 parameters)
        let script = r#"
            let request = sub("User$1", "Mana", 15, "changes", "Spell$1", 1111111111);
            request
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let map = result.try_cast::<rhai::Map>().unwrap();
        
        assert_eq!(map.get("action").unwrap().clone().try_cast::<String>().unwrap(), "write");
        assert_eq!(map.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(map.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Mana");
        assert_eq!(map.get("value").unwrap().clone().try_cast::<i64>().unwrap(), 15);
        assert_eq!(map.get("push_condition").unwrap().clone().try_cast::<String>().unwrap(), "changes");
        assert_eq!(map.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");
        assert_eq!(map.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Spell$1");
        assert_eq!(map.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1111111111);

        Ok(())
    }

    #[test]
    fn test_comprehensive_script_with_metadata() -> Result<()> {
        let store = Rc::new(RefCell::new(Store::new(Arc::new(Snowflake::new()))));
        let scripting_engine = ScriptingEngine::new(store.clone());

        // Test a comprehensive script that uses all variants with metadata
        let script = r#"
            let requests = [];
            let current_time = 1625097600; // Example timestamp
            
            // Basic operations
            requests.push(write("User$1", "Name", "Alice"));
            requests.push(add("User$1", "Score", 100));
            requests.push(sub("User$1", "Health", 10));
            
            // Operations with writer_id
            requests.push(write("User$1", "Status", "active", "always", "set", "Admin$1"));
            requests.push(add("User$1", "Experience", 500, "changes", "Game$1"));
            requests.push(sub("User$1", "Stamina", 20, "always", "Action$1"));
            
            // Operations with writer_id and write_time
            requests.push(write("User$1", "LastLogin", "2024-01-01", "changes", "set", "System$1", current_time));
            requests.push(add("User$1", "PlayTime", 3600, "always", "Timer$1", current_time + 1));
            requests.push(sub("User$1", "Lives", 1, "changes", "Game$1", current_time + 2));
            
            requests
        "#;
        
        let result = scripting_engine.execute(script).unwrap();
        let requests = result.try_cast::<rhai::Array>().unwrap();
        
        assert_eq!(requests.len(), 9);
        
        // Check basic write (no metadata)
        let req1 = requests[0].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req1.get("entity_id").unwrap().clone().try_cast::<String>().unwrap(), "User$1");
        assert_eq!(req1.get("field_type").unwrap().clone().try_cast::<String>().unwrap(), "Name");
        assert!(req1.get("writer_id").is_none());
        assert!(req1.get("write_time").is_none());
        
        // Check write with writer_id (no write_time)
        let req4 = requests[3].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req4.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Admin$1");
        assert!(req4.get("write_time").is_none());
        
        // Check write with both writer_id and write_time
        let req7 = requests[6].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req7.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "System$1");
        assert_eq!(req7.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1625097600);
        
        // Check add with writer_id and write_time
        let req8 = requests[7].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req8.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "add");
        assert_eq!(req8.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Timer$1");
        assert_eq!(req8.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1625097601);
        
        // Check sub with writer_id and write_time
        let req9 = requests[8].clone().try_cast::<rhai::Map>().unwrap();
        assert_eq!(req9.get("adjust_behavior").unwrap().clone().try_cast::<String>().unwrap(), "subtract");
        assert_eq!(req9.get("writer_id").unwrap().clone().try_cast::<String>().unwrap(), "Game$1");
        assert_eq!(req9.get("write_time").unwrap().clone().try_cast::<u64>().unwrap(), 1625097602);

        Ok(())
    }
}
