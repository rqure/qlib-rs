#[allow(unused_imports)]
use crate::*;
use crate::data::StorageScope;

#[test]
fn test_direct_methods_basic_functionality() {
    let mut store = Store::new();
    
    // Test entity type resolution (existing functionality)
    let entity_type = store.get_entity_type("TestEntity").unwrap_or_else(|_| {
        // If it doesn't exist, we'd need to register it first via schema
        EntityType(1)
    });
    
    let field_type = store.get_field_type("test_field").unwrap_or_else(|_| {
        // If it doesn't exist, we'd need to register it first via schema
        FieldType(1)
    });

    // For this test, we'll create a simple entity ID directly
    let entity_id = EntityId::new(entity_type, 1);
    
    // Test write method directly
    let write_result = store.write(
        entity_id, 
        field_type, 
        sint!(42), 
        PushCondition::Always, 
        AdjustBehavior::Set
    );
    
    // For basic testing purposes, we expect this to fail because the entity doesn't exist
    // but we're testing that the method signature works and compiles correctly
    assert!(write_result.is_err());
    
    // Test read method directly
    let read_result = store.read(entity_id, field_type);
    assert!(read_result.is_err()); // Should fail because entity doesn't exist
    
    // Test create_entity method directly
    let create_result = store.create_entity(entity_type, None, "test_entity".to_string());
    // This should also fail because we don't have proper schema setup
    assert!(create_result.is_err());
}

#[test]
fn test_direct_methods_existence() {
    // This test simply verifies that all the direct methods exist and have correct signatures
    let mut store = Store::new();
    
    let entity_type = EntityType(1);
    let field_type = FieldType(1);
    let entity_id = EntityId::new(entity_type, 1);
    
    // Test that all methods exist with correct signatures
    let _: Result<(Option<Value>, Option<Timestamp>, Option<EntityId>)> = 
        store.read(entity_id, field_type);
        
    let _: Result<(bool, Option<Timestamp>, Option<EntityId>)> = 
        store.write(entity_id, field_type, None, PushCondition::Always, AdjustBehavior::Set);
        
    let _: Result<(EntityId, Option<Timestamp>)> = 
        store.create_entity(entity_type, None, "test".to_string());
        
    let _: Result<Option<Timestamp>> = 
        store.delete_entity(entity_id);
        
    let _: Result<Option<Timestamp>> = 
        store.create_snapshot(1);
    
    // If we get here, all method signatures are correct
}

#[test]
fn test_direct_methods_functional() {
    // This test demonstrates that the new direct methods work functionally
    // by creating a schema, entity, and performing operations using direct methods
    let mut store = Store::new();
    
    // First create a proper schema with all required fields using the existing schema update functionality
    let mut schema = EntitySchema::<Single, String, String>::new("TestEntity".to_string(), vec![]);
    
    // Add default fields common to all entities (required by the Store)
    schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );

    schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );

    schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: vec![],
            rank: 3,
            storage_scope: StorageScope::Configuration,
        }
    );
    
    schema.fields.insert(
        "test_field".to_string(),
        FieldSchema::Int {
            field_type: "test_field".to_string(),
            default_value: 0,
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Test the update_schema direct method
    let schema_result = store.update_schema(schema);
    assert!(schema_result.is_ok());
    let timestamp = schema_result.unwrap();
    assert!(timestamp.is_some());
    
    // Now get the registered entity and field types  
    let entity_type = store.get_entity_type("TestEntity").expect("Schema should be registered");
    let field_type = store.get_field_type("test_field").expect("Field should be registered");
    
    // Test the create_entity direct method
    let create_result = store.create_entity(entity_type, None, "test_entity".to_string());
    assert!(create_result.is_ok());
    let (entity_id, create_timestamp) = create_result.unwrap();
    assert!(create_timestamp.is_some());
    
    // Test the write direct method
    let write_result = store.write(
        entity_id, 
        field_type, 
        sint!(42), 
        PushCondition::Always, 
        AdjustBehavior::Set
    );
    assert!(write_result.is_ok());
    let (was_written, write_timestamp, _writer_id) = write_result.unwrap();
    assert!(was_written);
    assert!(write_timestamp.is_some());
    
    // Test the read direct method
    let read_result = store.read(entity_id, field_type);
    assert!(read_result.is_ok());
    let (value, read_write_time, _read_writer_id) = read_result.unwrap();
    assert!(value.is_some());
    if let Some(Value::Int(int_val)) = value {
        assert_eq!(int_val, 42);
    } else {
        panic!("Expected Int value of 42");
    }
    assert!(read_write_time.is_some());
    
    // Test the delete_entity direct method
    let delete_result = store.delete_entity(entity_id);
    assert!(delete_result.is_ok());
    let delete_timestamp = delete_result.unwrap();
    assert!(delete_timestamp.is_some());
    
    // Test the create_snapshot direct method
    let snapshot_result = store.create_snapshot(1);
    assert!(snapshot_result.is_ok());
    let snapshot_timestamp = snapshot_result.unwrap();
    assert!(snapshot_timestamp.is_some());
}