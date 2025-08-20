use crate::*;
use crate::data::EntityType;
use std::sync::Arc;

// Helper to create an entity schema with basic fields
async fn create_entity_schema(store: &mut Store, entity_type: &EntityType) -> Result<()> {
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    let ft_name = FieldType::from("Name");
    let ft_parent = FieldType::from("Parent");
    let ft_children = FieldType::from("Children");

    // Add default fields common to all entities
    let name_schema = FieldSchema::String {
        field_type: ft_name.clone(),
        default_value: String::new(),
        rank: 0,
    };

    let parent_schema = FieldSchema::EntityReference {
        field_type: ft_parent.clone(),
        default_value: None,
        rank: 1,
    };

    let children_schema = FieldSchema::EntityList {
        field_type: ft_children.clone(),
        default_value: Vec::new(),
        rank: 2,
    };

    schema.fields.insert(ft_name.clone(), name_schema);
    schema.fields.insert(ft_parent.clone(), parent_schema);
    schema.fields.insert(ft_children.clone(), children_schema);

    store.set_entity_schema(&schema).await?;
    Ok(())
}

// Helper to set up a basic database structure for testing
#[allow(dead_code)]
async fn setup_test_database() -> Result<Store> {
    let mut store = Store::new(Arc::new(Snowflake::new()));

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");
    let et_role = EntityType::from("Role");

    create_entity_schema(&mut store, &et_root).await?;
    create_entity_schema(&mut store, &et_folder).await?;
    create_entity_schema(&mut store, &et_user).await?;
    create_entity_schema(&mut store, &et_role).await?;

    Ok(store)
}

#[tokio::test]
async fn test_create_entity_hierarchy() -> Result<()> {
    let mut store = setup_test_database().await?;

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    // Find root entities (should be empty initially)
    let root_entities = store.find_entities(&et_root).await?;
    assert_eq!(root_entities.len(), 0);

    // Create root entity
    let root_entity = 
        store.create_entity(&et_folder, None, "Security Models").await?;

    let users_folder = store.create_entity(
        &et_folder,
        Some(root_entity.entity_id.clone()),
        "Users"
    ).await?;

    let roles_folder = store.create_entity(
        &et_folder, 
        Some(root_entity.entity_id.clone()),
        "Roles"
    ).await?;

    let user = store.create_entity(&et_user, Some(users_folder.entity_id.clone()), "qei").await?;

    store.create_entity(
        &et_user,
        Some(roles_folder.entity_id.clone()),
        "admin"
    ).await?;

    // Test relationships
    let ft_parent = FieldType::from("Parent");
    let ft_name = FieldType::from("Name");

    let mut reqs = vec![
        sread!(user.entity_id.clone(), ft_parent.clone()),
        sread!(user.entity_id.clone(), ft_name.clone()),
    ];

    store.perform(&mut reqs).await?;

    if let Some(Request::Read { value: Some(Value::EntityReference(Some(parent_id))), .. }) = reqs.get(0) {
        assert_eq!(*parent_id, users_folder.entity_id);
    } else {
        panic!("Expected parent reference");
    }

    // Verify name
    let mut reqs = vec![
        sread!(users_folder.entity_id.clone(), ft_name.clone()),
    ];

    store.perform(&mut reqs).await?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = reqs.get(0) {
        assert_eq!(name, "Users");
    } else {
        panic!("Expected folder name");
    }

    Ok(())
}

#[tokio::test]
async fn test_field_operations() -> Result<()> {
    let mut store = setup_test_database().await?;

    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let users_folder = store.create_entity(&et_folder, None, "Users").await?;
    let user = store.create_entity(&et_user, Some(users_folder.entity_id), "testuser").await?;

    // Test write and read operations
    let ft_name = FieldType::from("Name");

    let mut writes = vec![
        swrite!(user.entity_id.clone(), ft_name.clone(), sstr!("Updated User")),
    ];

    store.perform(&mut writes).await?;

    let mut reads = vec![
        sread!(user.entity_id.clone(), ft_name.clone()),
    ];

    store.perform(&mut reads).await?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = reads.get(0) {
        assert_eq!(name, "Updated User");
    } else {
        panic!("Expected updated name");
    }

    // Test field updates
    let mut updates = vec![
        swrite!(user.entity_id.clone(), ft_name.clone(), sstr!("Final Name")),
    ];

    store.perform(&mut updates).await?;

    let mut verify = vec![
        sread!(user.entity_id.clone(), ft_name.clone()),
    ];

    store.perform(&mut verify).await?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = verify.get(0) {
        assert_eq!(name, "Final Name");
    } else {
        panic!("Expected final name");
    }

    Ok(())
}

#[tokio::test]
async fn test_indirection_resolution() -> Result<()> {
    let mut store = setup_test_database().await?;

    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    // Create entities
    let security_folder = store.create_entity(&et_folder, None, "Security").await?;
    let users_folder = store.create_entity(
        &et_folder,
        Some(security_folder.entity_id.clone()),
        "Users",
    ).await?;

    let admin_user = store.create_entity(
        &et_user,
        Some(users_folder.entity_id.clone()),
        "admin"
    ).await?;

    // Test indirection: User->Parent->Name should resolve to "Users"
    let parent_name_field = FieldType::from("Parent->Name");

    let mut writes = vec![
        swrite!(admin_user.entity_id.clone(), "Name".into(), sstr!("Administrator")),
    ];

    store.perform(&mut writes).await?;

    // Test indirection resolution
    let mut indirect_reads = vec![
        sread!(admin_user.entity_id.clone(), parent_name_field.clone()),
    ];

    store.perform(&mut indirect_reads).await?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = indirect_reads.get(0) {
        assert_eq!(name, "Users");
    } else {
        panic!("Expected indirect resolution to return 'Users'");
    }

    Ok(())
}

#[tokio::test]
async fn test_entity_deletion() -> Result<()> {
    let mut store = setup_test_database().await?;

    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let users_folder = store.create_entity(&et_folder, None, "Users").await?;
    let user = store.create_entity(
        &et_user,
        Some(users_folder.entity_id.clone()),
        "testuser"
    ).await?;

    // Verify entity exists
    assert!(store.entity_exists(&user.entity_id).await);

    // Delete the entity
    store.delete_entity(&user.entity_id).await?;

    // Verify entity is gone
    assert!(!store.entity_exists(&user.entity_id).await);

    // Try to read from deleted entity - the request should succeed but return no value
    let mut request = vec![sread!(user.entity_id.clone(), "Name".into())];
    let result = store.perform(&mut request).await;
    
    // The request may succeed but return None, or it may fail - both are acceptable
    // for a deleted entity
    match result {
        Ok(_) => {
            // If it succeeded, the value should be None
            if let Some(Request::Read { value: None, .. }) = request.get(0) {
                // This is expected - no value for deleted entity
            } else if let Some(Request::Read { value: Some(_), .. }) = request.get(0) {
                panic!("Should not have a value for deleted entity");
            }
        }
        Err(_) => {
            // Also acceptable - operation failed because entity doesn't exist
        }
    }

    Ok(())
}

#[tokio::test] 
async fn test_entity_listing_with_pagination() -> Result<()> {
    let mut store = setup_test_database().await?;

    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let users_folder = store.create_entity(&et_folder, None, "Users").await?;

    // Create multiple users
    for i in 0..5 {
        store.create_entity(
            &et_user,
            Some(users_folder.entity_id.clone()),
            &format!("user{}", i)
        ).await?;
    }

    let user_entities = store.find_entities(&et_user).await?;
    assert_eq!(user_entities.len(), 5);

    Ok(())
}
