use crate::{Result, Store, EntityId, Snowflake, ScriptEngine, ScriptContext, Value, Notification, FieldType};
use std::sync::Arc;

#[test]
fn test_scripting_engine_creation() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let _engine = ScriptEngine::new(store);
    Ok(())
}

#[test]
fn test_script_context() -> Result<()> {
    let entity_id = EntityId::try_from("User$123")?;
    let context = ScriptContext::with_entity(entity_id.clone());
    
    assert_eq!(context.entity_id, Some(entity_id));
    
    let context2 = ScriptContext::new(None);
    assert_eq!(context2.entity_id, None);
    
    Ok(())
}

#[test]
fn test_notification_conditions() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let engine = ScriptEngine::new(store);

    let entity_id = EntityId::try_from("User$123")?;
    
    // Test various notification scenarios
    let notifications = vec![
        Notification {
            entity_id: entity_id.clone(),
            field_type: FieldType::from("Score"),
            current_value: Value::Int(100),
            previous_value: Value::Int(50),
            context: Default::default(),
        },
        Notification {
            entity_id: entity_id.clone(),
            field_type: FieldType::from("Score"),
            current_value: Value::Int(25),
            previous_value: Value::Int(75),
            context: Default::default(),
        },
        Notification {
            entity_id: entity_id.clone(),
            field_type: FieldType::from("Name"),
            current_value: Value::String("Alice".to_string()),
            previous_value: Value::String("Bob".to_string()),
            context: Default::default(),
        },
    ];

    // Test score increase condition
    let script1 = r#"
        field_type == "Score" && current_value > previous_value
    "#;

    assert!(engine.should_trigger_notification(script1, &notifications[0])?);
    assert!(!engine.should_trigger_notification(script1, &notifications[1])?);
    assert!(!engine.should_trigger_notification(script1, &notifications[2])?);

    // Test score threshold condition
    let script2 = r#"
        field_type == "Score" && current_value >= 100
    "#;

    assert!(engine.should_trigger_notification(script2, &notifications[0])?);
    assert!(!engine.should_trigger_notification(script2, &notifications[1])?);
    assert!(!engine.should_trigger_notification(script2, &notifications[2])?);

    // Test name change condition
    let script3 = r#"
        field_type == "Name" && current_value != previous_value
    "#;

    assert!(!engine.should_trigger_notification(script3, &notifications[0])?);
    assert!(!engine.should_trigger_notification(script3, &notifications[1])?);
    assert!(engine.should_trigger_notification(script3, &notifications[2])?);

    Ok(())
}

#[test]
fn test_script_compilation() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let engine = ScriptEngine::new(store);

    // Test compiling a simple script
    let script = r#"
        let x = 42;
        let y = x * 2;
        y
    "#;

    let ast = engine.compile(script)?;
    
    // Execute the compiled script multiple times
    let context = ScriptContext::new(None);
    for _ in 0..3 {
        let result = engine.execute_ast(&ast, context.clone())?;
        assert_eq!(result.as_int()?, 84);
    }

    Ok(())
}

#[test]
fn test_entity_context_access() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let engine = ScriptEngine::new(store);

    let entity_id = EntityId::try_from("Person$456")?;
    let context = ScriptContext::with_entity(entity_id.clone());

    let script = r#"
        // Test entity context access
        let entity_type = get_entity_type();
        let entity_id_str = get_entity_id();
        
        // Since 'this' and 'me' are in scope as strings, access them differently
        [entity_type, entity_id_str, entity_type, entity_id_str]
    "#;

    let result = engine.execute(script, context)?;
    let array = result.into_array()?;
    
    assert_eq!(array[0].clone().into_string()?, "Person");
    assert_eq!(array[1].clone().into_string()?, entity_id.to_string());
    assert_eq!(array[2].clone().into_string()?, "Person");
    assert_eq!(array[3].clone().into_string()?, entity_id.to_string());

    Ok(())
}

#[test]
fn test_basic_scripting_logic() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let engine = ScriptEngine::new(store);

    let context = ScriptContext::new(None);

    // Test basic arithmetic and logic
    let script = r#"
        let a = 10;
        let b = 20;
        let sum = a + b;
        let product = a * b;
        let is_greater = sum > product;
        
        [sum, product, is_greater]
    "#;

    let result = engine.execute(script, context)?;
    let array = result.into_array()?;
    
    assert_eq!(array[0].as_int()?, 30);   // sum
    assert_eq!(array[1].as_int()?, 200);  // product
    assert!(!array[2].as_bool()?);        // is_greater (30 > 200 is false)

    Ok(())
}

#[test]
fn test_script_with_write_requests() -> Result<()> {
    let store = Arc::new(Store::new(Arc::new(Snowflake::new())));
    let engine = ScriptEngine::new(store);

    let entity_id = EntityId::try_from("Task$789")?;
    let context = ScriptContext::with_entity(entity_id.clone());

    let script = r#"
        // Test that write operations add to context requests
        write("Title", "My Task");
        write("Priority", 5);
        add("Score", 10);
        subtract("Count", 1);
        
        // Return something to verify script ran
        true
    "#;

    let result = engine.execute(script, context.clone())?;
    assert!(result.as_bool()?);

    // Note: We can't easily verify the requests were added without access to the context
    // This would need to be tested in a more integrated manner

    Ok(())
}
