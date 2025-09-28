#[allow(unused_imports)]
use crate::*;

#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::auth::{authenticate_user, find_user_by_name, create_user, set_user_password, AuthConfig, AuthMethod};

#[test]
fn test_create_and_authenticate_user() -> Result<()> {
    let mut store = Store::new();
    
    // Create the Object entity schema with Name, Parent, and Children fields first
    // Define the schema using strings - perform_mut will intern the types
    let mut object_schema = EntitySchema::<Single, String, String>::new("Object".to_string(), vec![]);
    object_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    object_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    object_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    store.update_schema(object_schema)?;
    
    // Now we can get the interned types
    let object_entity_type = store.get_entity_type("Object")?;
    
    // Create the Subject entity schema with required authentication fields
    // Define the schema using strings - perform_mut will intern the types
    let mut subject_schema = EntitySchema::<Single, String, String>::new("Subject".to_string(), vec!["Object".to_string()]);
    subject_schema.fields.insert(
        "Secret".to_string(),
        FieldSchema::String {
            field_type: "Secret".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        "AuthMethod".to_string(),
        FieldSchema::Choice {
            field_type: "AuthMethod".to_string(),
            default_value: 0, // Native
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        "Active".to_string(),
        FieldSchema::Bool {
            field_type: "Active".to_string(),
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        "FailedAttempts".to_string(),
        FieldSchema::Int {
            field_type: "FailedAttempts".to_string(),
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        "LockedUntil".to_string(),
        FieldSchema::Timestamp {
            field_type: "LockedUntil".to_string(),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        "LastLogin".to_string(),
        FieldSchema::Timestamp {
            field_type: "LastLogin".to_string(),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.update_schema(subject_schema)?;
    
    // Create the User entity schema (inheriting from Subject)
    let user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec!["Subject".to_string()]);
    store.update_schema(user_schema)?;
    
    // Create an object entity to serve as parent
    let parent_id = store.create_entity(object_entity_type, None, "TestParent")?;
    
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
    
    // Create Object schema with all required fields
    let mut object_schema = EntitySchema::<Single, String, String>::new("Object".to_string(), vec![]);
    object_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    object_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    object_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    store.update_schema(object_schema)?;
    
    // Now we can get the interned types
    let _object_entity_type = store.get_entity_type("Object")?;
    
    // Create Subject schema
    let mut subject_schema = EntitySchema::<Single, String, String>::new("Subject".to_string(), vec!["Object".to_string()]);
    subject_schema.fields.insert(
        "Secret".to_string(),
        FieldSchema::String {
            field_type: "Secret".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        "AuthMethod".to_string(),
        FieldSchema::Choice {
            field_type: "AuthMethod".to_string(),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Native".to_string(), "LDAP".to_string(), "OpenID Connect".to_string()],
        }
    );
    subject_schema.fields.insert(
        "Active".to_string(),
        FieldSchema::Bool {
            field_type: "Active".to_string(),
            default_value: true,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    subject_schema.fields.insert(
        "FailedAttempts".to_string(),
        FieldSchema::Int {
            field_type: "FailedAttempts".to_string(),
            default_value: 0,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        "LockedUntil".to_string(),
        FieldSchema::Timestamp {
            field_type: "LockedUntil".to_string(),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    subject_schema.fields.insert(
        "LastLogin".to_string(),
        FieldSchema::Timestamp {
            field_type: "LastLogin".to_string(),
            default_value: crate::Timestamp::from_unix_timestamp(0).unwrap(),
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.update_schema(subject_schema)?;
    
    // Create User schema
    let user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec!["Subject".to_string()]);
    store.update_schema(user_schema)?;
    
    // Now we can get the interned types
    let user_entity_type = store.get_entity_type("User")?;
    
    // Create a user entity as it would be created by factory restore
    let username = "qei";
    let password = "qei";
    
    // Create the entity using direct method instead of macro
    let user_id = store.create_entity(user_entity_type, None, username)?;
    
    // Set the user fields as factory restore would
    let auth_config = AuthConfig::default();
    let password_hash = crate::auth::hash_password(password, &auth_config)?;
    
    // Get field types for setting values
    let name_field_type = store.get_field_type("Name")?;
    let secret_field_type = store.get_field_type("Secret")?;
    let _auth_method_field_type = store.get_field_type("AuthMethod")?;
    let active_field_type = store.get_field_type("Active")?;
    let _failed_attempts_field_type = store.get_field_type("FailedAttempts")?;
    let _locked_until_field_type = store.get_field_type("LockedUntil")?;
    let _last_login_field_type = store.get_field_type("LastLogin")?;
    
    store.write(user_id, &[name_field_type], Value::from_string(username.to_string()), None, None, None, None)?;
    store.write(user_id, &[secret_field_type], Value::from_string(password_hash), None, None, None, None)?;
    store.write(user_id, &[active_field_type], Value::Bool(true), None, None, None, None)?;
    
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