#[allow(unused_imports)]
use crate::*;

#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::auth::{authenticate, find_user_by_name, create_user, set_user_password, AuthConfig, AuthMethod};

#[allow(unused_imports)]
use std::sync::Arc;

#[tokio::test]
async fn test_create_and_authenticate_user() -> Result<()> {
    let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
    
    // Create the Object entity schema with Name field first
    let mut object_schema = EntitySchema::<Single>::new(EntityType::from("Object"), vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    let mut requests = vec![sschemaupdate!(object_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create the Subject entity schema with required authentication fields
    let mut subject_schema = EntitySchema::<Single>::new(EntityType::from("Subject"), vec![EntityType::from("Object")]);
    subject_schema.fields.insert(
        FieldType::from("Secret"),
        FieldSchema::String {
            field_type: FieldType::from("Secret"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("AuthMethod"),
        FieldSchema::Choice {
            field_type: FieldType::from("AuthMethod"),
            default_value: 0, // Native
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        FieldType::from("Active"),
        FieldSchema::Bool {
            field_type: FieldType::from("Active"),
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("FailedAttempts"),
        FieldSchema::Int {
            field_type: FieldType::from("FailedAttempts"),
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("LockedUntil"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LockedUntil"),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("LastLogin"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LastLogin"),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    let mut requests = vec![sschemaupdate!(subject_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create the User entity schema (inheriting from Subject)
    let user_schema = EntitySchema::<Single>::new(EntityType::from("User"), vec![EntityType::from("Subject")]);
    let mut requests = vec![sschemaupdate!(user_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create an object entity to serve as parent
    let parent_id = EntityId::new("Object", 1);
    let mut create_requests = vec![Request::Create {
        entity_type: EntityType::from("Object"),
        parent_id: None,
        name: "TestParent".to_string(),
        created_entity_id: Some(parent_id.clone()),
        timestamp: None,
        originator: None,
    }];
    store.perform_mut(&mut create_requests).await?;
    
    // Create a test user
    let username = "testuser";
    let password = "TestPassword123!"; // Meet password complexity requirements
    
    let user_id = create_user(&mut store, username, AuthMethod::Native, &parent_id).await?;
    println!("Created user with ID: {}", user_id);
    
    // Set the user password
    let auth_config = AuthConfig::default();
    set_user_password(&mut store, &user_id, password, &auth_config).await?;
    
    // Test finding the user by name
    let found_user = find_user_by_name(&mut store, username).await?;
    assert!(found_user.is_some());
    assert_eq!(found_user.unwrap(), user_id);
    
    // Test authentication
    let authenticated_user = authenticate(&mut store, username, password, &auth_config).await?;
    assert_eq!(authenticated_user, user_id);
    
    // Test authentication with wrong password
    let wrong_auth = authenticate(&mut store, username, "wrongpassword", &auth_config).await;
    assert!(wrong_auth.is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_authentication_with_factory_restore_format() -> Result<()> {
    let mut store = AsyncStore::new(Arc::new(Snowflake::new()));
    
    // Create schemas as they would be loaded from factory restore
    // (this should match what's in base-topology.json)
    
    // Create Object schema
    let mut object_schema = EntitySchema::<Single>::new(EntityType::from("Object"), vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    let mut requests = vec![sschemaupdate!(object_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create Subject schema
    let mut subject_schema = EntitySchema::<Single>::new(EntityType::from("Subject"), vec![EntityType::from("Object")]);
    subject_schema.fields.insert(
        FieldType::from("Secret"),
        FieldSchema::String {
            field_type: FieldType::from("Secret"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("AuthMethod"),
        FieldSchema::Choice {
            field_type: FieldType::from("AuthMethod"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        FieldType::from("Active"),
        FieldSchema::Bool {
            field_type: FieldType::from("Active"),
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("FailedAttempts"),
        FieldSchema::Int {
            field_type: FieldType::from("FailedAttempts"),
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("LockedUntil"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LockedUntil"),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        FieldType::from("LastLogin"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LastLogin"),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    let mut requests = vec![sschemaupdate!(subject_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create User schema
    let user_schema = EntitySchema::<Single>::new(EntityType::from("User"), vec![EntityType::from("Subject")]);
    let mut requests = vec![sschemaupdate!(user_schema)];
    store.perform_mut(&mut requests).await?;
    
    // Create a user entity as it would be created by factory restore
    let username = "qei";
    let password = "qei";
    
    // Create the entity manually (simulating factory restore)
    let user_type = EntityType::from("User");
    let user_id = EntityId::new("User", 0); // This matches our factory restore format: User$0
    
    // Add the user entity to the store
    let mut create_requests = vec![Request::Create {
        entity_type: user_type.clone(),
        parent_id: None,
        name: username.to_string(),
        created_entity_id: Some(user_id.clone()),
        timestamp: None,
        originator: None,
    }];
    store.perform_mut(&mut create_requests).await?;
    
    // Set the user fields as factory restore would
    let auth_config = AuthConfig::default();
    let password_hash = crate::auth::hash_password(password, &auth_config)?;
    
    let mut field_requests = vec![
        Request::Write {
            entity_id: user_id.clone(),
            field_type: FieldType::from("Name"),
            value: Some(Value::String(username.to_string())),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
        Request::Write {
            entity_id: user_id.clone(),
            field_type: FieldType::from("Secret"),
            value: Some(Value::String(password_hash)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
        Request::Write {
            entity_id: user_id.clone(),
            field_type: FieldType::from("Active"),
            value: Some(Value::Bool(true)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
    ];
    store.perform_mut(&mut field_requests).await?;
    
    // Test finding the user by name
    let found_user = find_user_by_name(&mut store, username).await?;
    assert!(found_user.is_some(), "Should find user by name");
    assert_eq!(found_user.unwrap(), user_id, "Should return correct user ID");
    
    // Test authentication
    let auth_config = AuthConfig::default();
    let authenticated_user = authenticate(&mut store, username, password, &auth_config).await?;
    assert_eq!(authenticated_user, user_id, "Authentication should succeed and return correct user ID");
    
    Ok(())
}