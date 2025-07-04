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
}
