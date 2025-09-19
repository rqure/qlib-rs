#[allow(unused_imports)]
use crate::*;

#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::auth::{authenticate_user, find_user_by_name, create_user, set_user_password, AuthConfig, AuthMethod};

#[test]
fn test_create_and_authenticate_user() -> Result<()> {
    let mut store = Store::new();
    
    // Create the Object entity schema with Name field first
    let object_entity_type = store.get_entity_type("Object")?;
    let name_field_type = store.get_field_type("Name")?;
    let mut object_schema = EntitySchema::<Single>::new(object_entity_type, vec![]);
    object_schema.fields.insert(
        name_field_type,
        FieldSchema::String {
            field_type: name_field_type,
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = vec![sschemaupdate!(object_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create the Subject entity schema with required authentication fields
    let subject_entity_type = store.get_entity_type("Subject")?;
    let secret_field_type = store.get_field_type("Secret")?;
    let auth_method_field_type = store.get_field_type("AuthMethod")?;
    let active_field_type = store.get_field_type("Active")?;
    let failed_attempts_field_type = store.get_field_type("FailedAttempts")?;
    let locked_until_field_type = store.get_field_type("LockedUntil")?;
    let last_login_field_type = store.get_field_type("LastLogin")?;
    let mut subject_schema = EntitySchema::<Single>::new(subject_entity_type, vec![object_entity_type]);
    subject_schema.fields.insert(
        secret_field_type,
        FieldSchema::String {
            field_type: secret_field_type,
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        auth_method_field_type,
        FieldSchema::Choice {
            field_type: auth_method_field_type,
            default_value: 0, // Native
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        active_field_type,
        FieldSchema::Bool {
            field_type: active_field_type,
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        failed_attempts_field_type,
        FieldSchema::Int {
            field_type: failed_attempts_field_type,
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        locked_until_field_type,
        FieldSchema::Timestamp {
            field_type: locked_until_field_type,
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        last_login_field_type,
        FieldSchema::Timestamp {
            field_type: last_login_field_type,
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(subject_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create the User entity schema (inheriting from Subject)
    let user_entity_type = store.get_entity_type("User")?;
    let user_schema = EntitySchema::<Single>::new(user_entity_type, vec![subject_entity_type]);
    let requests = vec![sschemaupdate!(user_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create an object entity to serve as parent
    let parent_id = EntityId::new(object_entity_type, 1);
    let create_requests = vec![Request::Create {
        entity_type: object_entity_type,
        parent_id: None,
        name: "TestParent".to_string(),
        created_entity_id: Some(parent_id),
        timestamp: None,
        originator: None,
    }];
    store.perform_mut(create_requests)?;
    
    // Create a test user
    let username = "testuser";
    let password = "TestPassword123!"; // Meet password complexity requirements
    
    let user_id = create_user(&mut store, username, AuthMethod::Native, parent_id)?;
    println!("Created user with ID: {:?}", user_id);
    
    // Set the user password
    let auth_config = AuthConfig::default();
    set_user_password(&mut store, user_id, password, &auth_config)?;
    
    // Test finding the user by name
    let found_user = find_user_by_name(&mut store, username)?;
    assert!(found_user.is_some());
    assert_eq!(found_user.unwrap(), user_id);
    
    // Test authentication
    let authenticated_user = authenticate_user(&mut store, username, password, &auth_config)?;
    assert_eq!(authenticated_user, user_id);
    
    // Test authentication with wrong password
    let wrong_auth = authenticate_user(&mut store, username, "wrongpassword", &auth_config);
    assert!(wrong_auth.is_err());
    
    Ok(())
}

#[test]
fn test_authentication_with_factory_restore_format() -> Result<()> {
    let mut store = Store::new();
    
    // Create schemas as they would be loaded from factory restore
    // (this should match what's in base-topology.json)
    
    // Create Object schema
    let object_entity_type = store.get_entity_type("Object")?;
    let name_field_type = store.get_field_type("Name")?;
    let mut object_schema = EntitySchema::<Single>::new(object_entity_type, vec![]);
    object_schema.fields.insert(
        name_field_type,
        FieldSchema::String {
            field_type: name_field_type,
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = vec![sschemaupdate!(object_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create Subject schema
    let subject_entity_type = store.get_entity_type("Subject")?;
    let secret_field_type = store.get_field_type("Secret")?;
    let auth_method_field_type = store.get_field_type("AuthMethod")?;
    let active_field_type = store.get_field_type("Active")?;
    let failed_attempts_field_type = store.get_field_type("FailedAttempts")?;
    let locked_until_field_type = store.get_field_type("LockedUntil")?;
    let last_login_field_type = store.get_field_type("LastLogin")?;
    let mut subject_schema = EntitySchema::<Single>::new(subject_entity_type, vec![object_entity_type]);
    subject_schema.fields.insert(
        secret_field_type,
        FieldSchema::String {
            field_type: secret_field_type,
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        auth_method_field_type,
        FieldSchema::Choice {
            field_type: auth_method_field_type,
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        active_field_type,
        FieldSchema::Bool {
            field_type: active_field_type,
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        failed_attempts_field_type,
        FieldSchema::Int {
            field_type: failed_attempts_field_type,
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        locked_until_field_type,
        FieldSchema::Timestamp {
            field_type: locked_until_field_type,
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        last_login_field_type,
        FieldSchema::Timestamp {
            field_type: last_login_field_type,
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(subject_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create User schema
    let user_entity_type = store.get_entity_type("User")?;
    let user_schema = EntitySchema::<Single>::new(user_entity_type, vec![subject_entity_type]);
    let requests = vec![sschemaupdate!(user_schema.to_string_schema(&store))];
    store.perform_mut(requests)?;
    
    // Create a user entity as it would be created by factory restore
    let username = "qei";
    let password = "qei";
    
    // Create the entity manually (simulating factory restore)
    let user_id = EntityId::new(user_entity_type, 0); // This matches our factory restore format: User$0
    
    // Add the user entity to the store
    let create_requests = vec![Request::Create {
        entity_type: user_entity_type,
        parent_id: None,
        name: username.to_string(),
        created_entity_id: Some(user_id),
        timestamp: None,
        originator: None,
    }];
    store.perform_mut(create_requests)?;
    
    // Set the user fields as factory restore would
    let auth_config = AuthConfig::default();
    let password_hash = crate::auth::hash_password(password, &auth_config)?;
    
    let field_requests = vec![
        Request::Write {
            entity_id: user_id,
            field_types: vec![name_field_type],
            value: Some(Value::String(username.to_string())),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
        Request::Write {
            entity_id: user_id,
            field_types: vec![secret_field_type],
            value: Some(Value::String(password_hash)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
        Request::Write {
            entity_id: user_id,
            field_types: vec![active_field_type],
            value: Some(Value::Bool(true)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
            originator: None,
        },
    ];
    store.perform_mut(field_requests)?;
    
    // Test finding the user by name
    let found_user = find_user_by_name(&mut store, username)?;
    assert!(found_user.is_some(), "Should find user by name");
    assert_eq!(found_user.unwrap(), user_id, "Should return correct user ID");
    
    // Test authentication
    let auth_config = AuthConfig::default();
    let authenticated_user = authenticate_user(&mut store, username, password, &auth_config)?;
    assert_eq!(authenticated_user, user_id, "Authentication should succeed and return correct user ID");
    
    Ok(())
}