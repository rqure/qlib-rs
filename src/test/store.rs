use crate::*;
use crate::data::EntityType;
use std::sync::Arc;

// Helper to create an entity schema with basic fields
async fn create_entity_schema(store_interface: &mut StoreInterface, ctx: &Context, entity_type: &EntityType) -> Result<()> {
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    let ft_name = FieldType::from("Name");
    let ft_parent = FieldType::from("Parent");
    let ft_children = FieldType::from("Children");

    // Add default fields common to all entities
    let name_schema = FieldSchema::String {
        field_type: ft_name.clone(),
        default_value: "".to_string(),
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

    store_interface.set_entity_schema(ctx, &schema).await?;
    Ok(())
}

// Helper to set up a basic database structure for testing
#[allow(dead_code)]
async fn setup_test_database() -> Result<(StoreInterface, Context)> {
    let store_interface = StoreInterface::new_shared_local(Store::new(Arc::new(Snowflake::new())));
    let ctx = Context::new(store_interface.clone());
    let mut store_interface_mut = store_interface.clone();

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");
    let et_role = EntityType::from("Role");

    let ft_email = FieldType::from("Email");

    // Create schemas for different entity types
    create_entity_schema(&mut store_interface_mut, &ctx, &et_root).await?;
    create_entity_schema(&mut store_interface_mut, &ctx, &et_folder).await?;
    create_entity_schema(&mut store_interface_mut, &ctx, &et_user).await?;
    create_entity_schema(&mut store_interface_mut, &ctx, &et_role).await?;

    // Add custom fields to User schema
    let email_schema = FieldSchema::String {
        field_type: ft_email.clone(),
        default_value: "".to_string(),
        rank: 3,
    };

    store_interface_mut.set_field_schema(&ctx, &et_user, &ft_email, email_schema).await?;

    // Create root entity
    store_interface_mut.create_entity(&ctx, &et_root, None, "Root").await?;

    Ok((store_interface, ctx))
}

#[tokio::test]
async fn test_create_entity_hierarchy() -> Result<()> {
    let (store_interface, ctx) = setup_test_database().await?;
    let mut store = store_interface;

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");
    let et_role = EntityType::from("Role");

    let ft_children = FieldType::from("Children");
    let ft_parent = FieldType::from("Parent");

    // Get the Root entity
    let root_entities = store.find_entities(&ctx, &et_root).await?;
    assert_eq!(root_entities.len(), 1);
    let root_id = root_entities[0].clone();

    // Create a folder under root
    let security_models =
        store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security Models").await?;

    // Create subfolders
    let users_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_models.entity_id.clone()),
        "Users",
    ).await?;

    let roles_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_models.entity_id.clone()),
        "Roles",
    ).await?;

    // Create a user and role
    let user = store.create_entity(&ctx, &et_user, Some(users_folder.entity_id.clone()), "qei").await?;

    store.create_entity(
        &ctx,
        &et_role,
        Some(roles_folder.entity_id.clone()),
        "Admin",
    ).await?;

    // Read children of security models folder
    let mut reqs = vec![sread!(security_models.entity_id, ft_children.clone())];
    store.perform(&ctx, &mut reqs).await?;

    if let Request::Read { value, .. } = &reqs[0] {
        if let Some(Value::EntityList(children)) = value {
            assert_eq!(children.len(), 2);
        } else {
            panic!("Expected Children to be an EntityList");
        }
    }

    // Verify user's parent is the users folder
    let mut reqs = vec![sread!(user.entity_id, ft_parent.clone())];
    store.perform(&ctx, &mut reqs).await?;

    if let Request::Read { value, .. } = &reqs[0] {
        if let Some(Value::EntityReference(parent)) = value {
            assert_eq!(*parent, Some(users_folder.entity_id));
        } else {
            panic!("Expected Parent to be an EntityReference");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_field_operations() -> Result<()> {
    let (store_interface, ctx) = setup_test_database().await?;
    let mut store = store_interface;

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_email = FieldType::from("Email");

    let root_entities = store.find_entities(&ctx, &et_root).await?;
    let root_id = root_entities[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users").await?;

    let user = store.create_entity(&ctx, &et_user, Some(users_folder.entity_id), "testuser").await?;

    // Test writing to a field
    let mut writes = vec![swrite!(
        user.entity_id.clone(),
        ft_email.clone(),
        sstr!("test@example.com")
    )];
    store.perform(&ctx, &mut writes).await?;

    // Test reading the field
    let mut reads = vec![sread!(user.entity_id.clone(), "Email".into())];
    store.perform(&ctx, &mut reads).await?;

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
    store.perform(&ctx, &mut updates).await?;

    // Verify update
    let mut verify = vec![sread!(user.entity_id.clone(), ft_email.clone())];
    store.perform(&ctx, &mut verify).await?;

    if let Request::Read { value, .. } = &verify[0] {
        assert_eq!(
            *value,
            Some(Value::String("updated@example.com".to_string()))
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_indirection_resolution() -> Result<()> {
    let (store_interface, ctx) = setup_test_database().await?;
    let mut store = store_interface;

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_email = FieldType::from("Email");

    // Create entities
    let root_entities = store.find_entities(&ctx, &et_root).await?;
    let root_id = root_entities[0].clone();

    let security_folder =
        store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security").await?;

    let users_folder = store.create_entity(
        &ctx,
        &et_folder,
        Some(security_folder.entity_id.clone()),
        "Users",
    ).await?;

    let admin_user = store.create_entity(
        &ctx,
        &et_user,
        Some(users_folder.entity_id.clone()),
        "admin",
    ).await?;

    // Set email
    let mut writes = vec![swrite!(
        admin_user.entity_id.clone(),
        ft_email.clone(),
        sstr!("admin@example.com")
    )];
    store.perform(&ctx, &mut writes).await?;

    // Test indirection
    let mut reqs = vec![sread!(
        security_folder.entity_id.clone(),
        format!("Children->0->Children->0->Email").into()
    )];

    store.perform(&ctx, &mut reqs).await?;

    if let Request::Read { value, .. } = &reqs[0] {
        assert_eq!(*value, Some(Value::String("admin@example.com".to_string())));
    }

    Ok(())
}

#[tokio::test]
async fn test_entity_deletion() -> Result<()> {
    let (store_interface, ctx) = setup_test_database().await?;
    let mut store = store_interface;

    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let ft_children = FieldType::from("Children");

    // Create a folder and a user
    let root_entities = store.find_entities(&ctx, &et_root).await?;
    let root_id = root_entities[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users").await?;

    let user = store.create_entity(
        &ctx,
        &et_user,
        Some(users_folder.entity_id.clone()),
        "temp_user",
    ).await?;

    // Verify user exists
    assert!(store.entity_exists(&ctx, &user.entity_id).await);

    // Delete the user
    store.delete_entity(&ctx, &user.entity_id).await?;

    // Verify user no longer exists
    assert!(!store.entity_exists(&ctx, &user.entity_id).await);

    // Check if the user was removed from the parent's children list
    let mut request = vec![sread!(users_folder.entity_id.clone(), ft_children.clone())];
    store.perform(&ctx, &mut request).await?;

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

#[tokio::test]
async fn test_entity_listing_with_pagination() -> Result<()> {
    let (store_interface, ctx) = setup_test_database().await?;
    let mut store = store_interface;

    // Create multiple entities of the same type
    let et_root = EntityType::from("Root");
    let et_folder = EntityType::from("Folder");
    let et_user = EntityType::from("User");

    let root_entities = store.find_entities(&ctx, &et_root).await?;
    let root_id = root_entities[0].clone();

    let users_folder = store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users").await?;

    // Create 10 users
    for i in 1..=10 {
        store.create_entity(
            &ctx,
            &et_user,
            Some(users_folder.entity_id.clone()),
            &format!("user{}", i),
        ).await?;
    }

    // Test finding entities without pagination
    let user_entities = store.find_entities(&ctx, &et_user).await?;

    assert_eq!(user_entities.len(), 10);

    Ok(())
}
