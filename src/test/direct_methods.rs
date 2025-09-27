use crate::*;

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