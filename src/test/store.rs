use crate::*;
use crate::data::EntityType;
use std::sync::Arc;

// Helper to create an entity schema with basic fields
#[allow(dead_code)]
fn create_entity_schema(store: &mut MapStore, entity_type: &EntityType) -> Result<()> {
    let mut schema = EntitySchema::new(entity_type.clone());
    let ft_name = FieldType::from("Name");
    let ft_parent = FieldType::from("Parent");
    let ft_children = FieldType::from("Children");

    // Add default fields common to all entities
    let name_schema = FieldSchema {
        entity_type: entity_type.clone(),
        field_type: ft_name.clone(),
        default_value: Value::String("".into()),
        rank: 0,
        read_permission: None,
        write_permission: None,
        choices: None,
    };

    let parent_schema = FieldSchema {
        entity_type: entity_type.clone(),
        field_type: ft_parent.clone(),
        default_value: Value::EntityReference(None),
        rank: 1,
        read_permission: None,
        write_permission: None,
        choices: None,
    };

    let children_schema = FieldSchema {
        entity_type: entity_type.clone(),
        field_type: ft_children.clone(),
        default_value: Value::EntityList(Vec::new()),
        rank: 2,
        read_permission: None,
        write_permission: None,
        choices: None,
    };

    schema.fields.insert(ft_name.clone(), name_schema);
    schema.fields.insert(ft_parent.clone(), parent_schema);
    schema.fields.insert(ft_children.clone(), children_schema);

    store.set_entity_schema(&Context {}, &schema)?;
    Ok(())
}

// Helper to set up a basic database structure for testing
#[allow(dead_code)]
fn setup_test_database() -> Result<MapStore> {
    let mut store = MapStore::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");
    let et_role = EntityType::from("Role");

    let ft_email = FieldType::from("Email");

    // Create schemas for different entity types
    create_entity_schema(&mut store, &et_root)?;
    create_entity_schema(&mut store, &et_folder)?;
    create_entity_schema(&mut store, &et_user)?;
    create_entity_schema(&mut store, &et_role)?;

    // Add custom fields to User schema
    let email_schema = FieldSchema {
        entity_type: et_user.clone(),
        field_type: ft_email.clone(),
        default_value: Value::String("".into()),
        rank: 3,
        read_permission: None,
        write_permission: None,
        choices: None,
    };

    store.set_field_schema(&ctx, &et_user, &ft_email, email_schema)?;

    // Create root entity
    store.create_entity(&ctx, &et_root, None, "Root")?;

    Ok(store)
}

#[test]
fn test_create_entity_hierarchy() -> Result<()> {
    let mut store = setup_test_database()?;
    let ctx = Context {};

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");
    let et_role = EntityType::from("Role");

    let ft_children = FieldType::from("Children");
    let ft_parent = FieldType::from("Parent");

    // Get the Root entity
    let root_entities = store.find_entities(&ctx, &et_root, None)?;
    assert_eq!(root_entities.items.len(), 1);
    let root_id = root_entities.items[0].clone();

    // Create a folder under root
    let security_models =
        store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security Models")?;

    // Create subfolders
    let users_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_models.entity_id.clone()),
        "Users",
    )?;

    let roles_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_models.entity_id.clone()),
        "Roles",
    )?;

    // Create a user and role
    let user = store.create_entity(&ctx, &et_user, Some(users_folder.entity_id.clone()), "qei")?;

    store.create_entity(
        &ctx,
        &et_role,
        Some(roles_folder.entity_id.clone()),
        "Admin",
    )?;

    // Read children of security models folder
    let mut reqs = vec![sread!(security_models.entity_id, ft_children.clone())];
    store.perform(&ctx, &mut reqs)?;

    if let Request::Read { value, .. } = &reqs[0] {
        if let Some(Value::EntityList(children)) = value {
            assert_eq!(children.len(), 2);
        } else {
            panic!("Expected Children to be an EntityList");
        }
    }

    // Verify user's parent is the users folder
    let mut reqs = vec![sread!(user.entity_id, ft_parent.clone())];
    store.perform(&ctx, &mut reqs)?;

    if let Request::Read { value, .. } = &reqs[0] {
        if let Some(Value::EntityReference(parent)) = value {
            assert_eq!(*parent, Some(users_folder.entity_id));
        } else {
            panic!("Expected Parent to be an EntityReference");
        }
    }

    Ok(())
}

#[test]
fn test_field_operations() -> Result<()> {
    let mut store = setup_test_database()?;
    let ctx = Context {};

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_email = FieldType::from("Email");

    let root_entities = store.find_entities(&ctx, &et_root, None)?;
    let root_id = root_entities.items[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

    let user = store.create_entity(&ctx, &et_user, Some(users_folder.entity_id), "testuser")?;

    // Test writing to a field
    let mut writes = vec![swrite!(
        user.entity_id.clone(),
        ft_email.clone(),
        sstr!("test@example.com")
    )];
    store.perform(&ctx, &mut writes)?;

    // Test reading the field
    let mut reads = vec![sread!(user.entity_id.clone(), "Email".into())];
    store.perform(&ctx, &mut reads)?;

    if let Request::Read { value, .. } = &reads[0] {
        assert_eq!(*value, Some(Value::String("test@example.com".to_string())));
    }

    // Test field update with write option
    let mut updates = vec![swrite!(
        user.entity_id.clone(),
        ft_email.clone(),
        sstr!("updated@example.com"),
        PushCondition::Changes
    )];
    store.perform(&ctx, &mut updates)?;

    // Verify update
    let mut verify = vec![sread!(user.entity_id.clone(), ft_email.clone())];
    store.perform(&ctx, &mut verify)?;

    if let Request::Read { value, .. } = &verify[0] {
        assert_eq!(
            *value,
            Some(Value::String("updated@example.com".to_string()))
        );
    }

    Ok(())
}

#[test]
fn test_indirection_resolution() -> Result<()> {
    let mut store = setup_test_database()?;
    let ctx = Context {};

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_email = FieldType::from("Email");

    // Create entities
    let root_entities = store.find_entities(&ctx, &et_root, None)?;
    let root_id = root_entities.items[0].clone();

    let security_folder =
        store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security")?;

    let users_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_folder.entity_id.clone()),
        "Users",
    )?;

    let admin_user = store.create_entity(
        &ctx,
        &et_user,
        Some(users_folder.entity_id.clone()),
        "admin",
    )?;

    // Set email
    let mut writes = vec![swrite!(
        admin_user.entity_id.clone(),
        ft_email.clone(),
        sstr!("admin@example.com")
    )];
    store.perform(&ctx, &mut writes)?;

    // Test indirection
    let mut reqs = vec![sread!(
        security_folder.entity_id.clone(),
        format!("Children->0->Children->0->Email").into()
    )];

    store.perform(&ctx, &mut reqs)?;

    if let Request::Read { value, .. } = &reqs[0] {
        assert_eq!(*value, Some(Value::String("admin@example.com".to_string())));
    }

    Ok(())
}

#[test]
fn test_entity_deletion() -> Result<()> {
    let mut store = setup_test_database()?;
    let ctx = Context {};

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_children = FieldType::from("Children");

    // Create a folder and a user
    let root_entities = store.find_entities(&ctx, &et_root, None)?;
    let root_id = root_entities.items[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

    let user = store.create_entity(
        &ctx,
        &et_user,
        Some(users_folder.entity_id.clone()),
        "temp_user",
    )?;

    // Verify user exists
    assert!(store.entity_exists(&ctx, &user.entity_id));

    // Delete the user
    store.delete_entity(&ctx, &user.entity_id)?;

    // Verify user no longer exists
    assert!(!store.entity_exists(&ctx, &user.entity_id));

    // Check if the user was removed from the parent's children list
    let mut request = vec![sread!(users_folder.entity_id.clone(), ft_children.clone())];
    store.perform(&ctx, &mut request)?;

    if let Request::Read { value, .. } = &request[0] {
        if let Some(Value::EntityList(children)) = value {
            assert!(
                !children.contains(&user.entity_id),
                "User should have been removed from parent's children list"
            );
        }
    }

    Ok(())
}

#[test]
fn test_entity_listing_with_pagination() -> Result<()> {
    let mut store = setup_test_database()?;
    let ctx = Context {};

    // Create multiple entities of the same type
    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let root_entities = store.find_entities(&ctx, &et_root, None)?;
    let root_id = root_entities.items[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

    // Create 10 users
    for i in 1..=10 {
        store.create_entity(
            &ctx,
            &et_user,
            Some(users_folder.entity_id.clone()),
            &format!("user{}", i),
        )?;
    }

    // Test pagination - first page (5 items)
    let page_opts = PageOpts::new(5, None);
    let page1 = store.find_entities(&ctx, &et_user, Some(page_opts))?;

    assert_eq!(page1.items.len(), 5);
    assert_eq!(page1.total, 10);
    assert!(page1.next_cursor.is_some());

    // Test pagination - second page
    let page_opts = PageOpts::new(5, page1.next_cursor.clone());
    let page2 = store.find_entities(&ctx, &et_user, Some(page_opts))?;

    assert_eq!(page2.items.len(), 5);
    assert_eq!(page2.total, 10);
    assert!(page2.next_cursor.is_none());

    // Verify we got different sets of users
    for item in &page1.items {
        assert!(!page2.items.contains(item));
    }

    Ok(())
}
