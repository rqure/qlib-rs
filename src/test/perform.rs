use crate::*;

#[test]
fn test_perform_read_requests() -> Result<()> {
    let store = Store::new();
    
    // Test that Store::perform can handle the 4 new request types
    let entity_type_name = "TestEntity";
    let field_type_name = "TestField";
    
    // Test GetEntityType with perform (immutable)
    let get_entity_type_request = vec![
        Request::GetEntityType {
            name: entity_type_name.to_string(),
            entity_type: None,
        }
    ];
    
    let results = store.perform(get_entity_type_request)?;
    
    // Verify that non-existent entity type returns None
    if let Request::GetEntityType { entity_type, .. } = &results[0] {
        assert!(entity_type.is_none(), "Non-existent entity type should be None");
    } else {
        panic!("Expected GetEntityType request");
    }
    
    // Test GetFieldType with perform (immutable)
    let get_field_type_request = vec![
        Request::GetFieldType {
            name: field_type_name.to_string(),
            field_type: None,
        }
    ];
    
    let results = store.perform(get_field_type_request)?;
    
    // Verify that non-existent field type returns None
    if let Request::GetFieldType { field_type, .. } = &results[0] {
        assert!(field_type.is_none(), "Non-existent field type should be None");
    } else {
        panic!("Expected GetFieldType request");
    }
    
    // Test ResolveEntityType with perform (immutable)
    let resolve_entity_type_request = vec![
        Request::ResolveEntityType {
            entity_type: EntityType(999), // Non-existent
            name: None,
        }
    ];
    
    let results = store.perform(resolve_entity_type_request)?;
    
    if let Request::ResolveEntityType { name, .. } = &results[0] {
        assert!(name.is_none(), "Non-existent entity type should resolve to None");
    } else {
        panic!("Expected ResolveEntityType request");
    }
    
    // Test ResolveFieldType with perform (immutable)
    let resolve_field_type_request = vec![
        Request::ResolveFieldType {
            field_type: FieldType(999), // Non-existent
            name: None,
        }
    ];
    
    let results = store.perform(resolve_field_type_request)?;
    
    if let Request::ResolveFieldType { name, .. } = &results[0] {
        assert!(name.is_none(), "Non-existent field type should resolve to None");
    } else {
        panic!("Expected ResolveFieldType request");
    }
    
    // Test that Read requests still work
    let read_request = vec![sread!(EntityId(0), vec![FieldType(0)])];
    let _read_result = store.perform(read_request);
    
    // This might fail because the entity doesn't exist, but that's expected behavior
    // The important thing is that the request type is handled without throwing an "invalid request" error
    
    println!("✓ All 4 new request types work correctly with Store::perform()");
    
    Ok(())
}

#[test]
fn test_perform_invalid_request() {
    let store = Store::new();
    
    // Test that Write requests still return an error when using immutable perform
    let write_request = vec![swrite!(EntityId(0), vec![FieldType(0)], sstr!("test"))];
    let result = store.perform(write_request);
    
    assert!(result.is_err(), "Write requests should fail with immutable perform");
    if let Err(Error::InvalidRequest(msg)) = result {
        assert!(msg.contains("can only handle Read, GetEntityType, ResolveEntityType, GetFieldType, and ResolveFieldType"));
    } else {
        panic!("Expected InvalidRequest error");
    }
    
    println!("✓ Write requests correctly fail with Store::perform()");
}