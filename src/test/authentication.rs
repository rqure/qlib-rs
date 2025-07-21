use crate::*;
use crate::auth::*;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_store() -> Store {
        Store::new(Arc::new(Snowflake::new()))
    }

    fn create_test_context() -> Context {
        Context {}
    }

    #[test]
    fn test_create_user_success() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        assert!(!user_id.to_string().is_empty());
    }

    #[test]
    fn test_create_user_invalid_name() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Try to create user with empty name
        let result = auth_manager.create_user(&mut store, &ctx, "", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::InvalidName)));

        // Try to create user with whitespace-only name
        let result = auth_manager.create_user(&mut store, &ctx, "   ", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::InvalidName)));
    }

    #[test]
    fn test_create_user_weak_password() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Try to create user with weak password
        let result = auth_manager.create_user(&mut store, &ctx, "testuser", "123");
        assert!(matches!(result, Err(AuthError::InvalidPassword(_))));
    }

    #[test]
    fn test_create_duplicate_user() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create first user
        auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Try to create duplicate user
        let result = auth_manager.create_user(&mut store, &ctx, "testuser", "AnotherPass456!");
        assert!(matches!(result, Err(AuthError::UserAlreadyExists)));
    }

    #[test]
    fn test_authenticate_success() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let created_user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Authenticate user
        let auth_user_id = auth_manager
            .authenticate(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        assert_eq!(created_user_id, auth_user_id);
    }

    #[test]
    fn test_authenticate_wrong_password() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Try to authenticate with wrong password
        let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "WrongPassword");
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[test]
    fn test_authenticate_nonexistent_user() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Try to authenticate nonexistent user
        let result = auth_manager.authenticate(&mut store, &ctx, "nonexistent", "AnyPassword");
        assert!(matches!(result, Err(AuthError::UserNotFound)));
    }

    #[test]
    fn test_change_password() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Change password
        auth_manager
            .change_password(&mut store, &ctx, &user_id, "NewSecurePass456!")
            .unwrap();

        // Verify old password doesn't work
        let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));

        // Verify new password works
        let auth_user_id = auth_manager
            .authenticate(&mut store, &ctx, "testuser", "NewSecurePass456!")
            .unwrap();
        assert_eq!(user_id, auth_user_id);
    }

    #[test]
    fn test_disable_user() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Disable user
        auth_manager.disable_user(&mut store, &ctx, &user_id).unwrap();

        // Try to authenticate disabled user
        let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::AccountDisabled)));
    }

    #[test]
    fn test_enable_user() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Disable user
        auth_manager.disable_user(&mut store, &ctx, &user_id).unwrap();

        // Re-enable user
        auth_manager.enable_user(&mut store, &ctx, &user_id).unwrap();

        // Authentication should work again
        let auth_user_id = auth_manager
            .authenticate(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();
        assert_eq!(user_id, auth_user_id);
    }

    #[test]
    fn test_account_lockout() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        
        // Create auth manager with lower max attempts for testing
        let config = AuthConfig {
            max_failed_attempts: 3,
            lockout_duration: Duration::from_secs(60),
            min_password_length: 8,
            require_password_complexity: true,
        };
        let auth_manager = AuthenticationManager::with_config(config);

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Make failed attempts
        for _ in 0..3 {
            let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "WrongPassword");
            assert!(matches!(result, Err(AuthError::InvalidCredentials)));
        }

        // Account should now be locked
        let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::AccountLocked)));
    }

    #[test]
    fn test_unlock_user() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        
        // Create auth manager with lower max attempts for testing
        let config = AuthConfig {
            max_failed_attempts: 2,
            lockout_duration: Duration::from_secs(60),
            min_password_length: 8,
            require_password_complexity: true,
        };
        let auth_manager = AuthenticationManager::with_config(config);

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user
        let user_id = auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Make failed attempts to lock account
        for _ in 0..2 {
            let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "WrongPassword");
            assert!(matches!(result, Err(AuthError::InvalidCredentials)));
        }

        // Verify account is locked
        let result = auth_manager.authenticate(&mut store, &ctx, "testuser", "SecurePass123!");
        assert!(matches!(result, Err(AuthError::AccountLocked)));

        // Manually unlock the account
        auth_manager.unlock_user(&mut store, &ctx, &user_id).unwrap();

        // Authentication should work again
        let auth_user_id = auth_manager
            .authenticate(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();
        assert_eq!(user_id, auth_user_id);
    }

    #[test]
    fn test_case_insensitive_name_lookup() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthenticationManager::new();

        // Initialize schema
        auth_manager.initialize_user_schema(&mut store, &ctx).unwrap();

        // Create user with lowercase name
        auth_manager
            .create_user(&mut store, &ctx, "testuser", "SecurePass123!")
            .unwrap();

        // Should be able to authenticate with different cases
        let result = auth_manager.authenticate(&mut store, &ctx, "TESTUSER", "SecurePass123!");
        assert!(result.is_ok());

        let result = auth_manager.authenticate(&mut store, &ctx, "TestUser", "SecurePass123!");
        assert!(result.is_ok());
    }
}

#[test]
fn test_auth_manager_initialization() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    Ok(())
}

#[test]
fn test_create_user() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Verify the user was created
    assert!(!user_id.get_id().is_empty());

    Ok(())
}

#[test]
fn test_authenticate_valid_user() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Authenticate the user
    let auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Verify it's the same user
    assert_eq!(user_id, auth_user_id);

    Ok(())
}

#[test]
fn test_authenticate_invalid_password() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let _user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Try to authenticate with wrong password
    let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "WrongPassword");

    // Should fail with invalid credentials
    assert!(matches!(result, Err(AuthError::InvalidCredentials)));

    Ok(())
}

#[test]
fn test_authenticate_non_existent_user() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Try to authenticate non-existent user
    let result = auth_manager.authenticate(&mut store, &ctx, "nonexistent@example.com", "AnyPassword");

    // Should fail with user not found
    assert!(matches!(result, Err(AuthError::UserNotFound)));

    Ok(())
}

#[test]
fn test_password_validation() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Test weak password (too short)
    let result = auth_manager.create_user(&mut store, &ctx, "test1@example.com", "weak");
    assert!(matches!(result, Err(AuthError::InvalidPassword(_))));

    // Test password without complexity
    let result = auth_manager.create_user(&mut store, &ctx, "test2@example.com", "simplemono");
    assert!(matches!(result, Err(AuthError::InvalidPassword(_))));

    // Test strong password
    let result = auth_manager.create_user(&mut store, &ctx, "test3@example.com", "Strong123!");
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_user_already_exists() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let _user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Try to create the same user again
    let result = auth_manager.create_user(&mut store, &ctx, "test@example.com", "AnotherPass123!");

    // Should fail with user already exists
    assert!(matches!(result, Err(AuthError::UserAlreadyExists)));

    Ok(())
}

#[test]
fn test_disable_and_enable_user() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Authenticate initially (should work)
    let _auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Disable the user
    auth_manager.disable_user(&mut store, &ctx, &user_id)?;

    // Try to authenticate (should fail)
    let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!");
    assert!(matches!(result, Err(AuthError::AccountDisabled)));

    // Enable the user
    auth_manager.enable_user(&mut store, &ctx, &user_id)?;

    // Try to authenticate again (should work)
    let _auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    Ok(())
}

#[test]
fn test_change_password() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let auth_manager = AuthenticationManager::new();
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "OldPass123!")?;

    // Authenticate with old password
    let _auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "OldPass123!")?;

    // Change password
    auth_manager.change_password(&mut store, &ctx, &user_id, "NewPass456!")?;

    // Try to authenticate with old password (should fail)
    let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "OldPass123!");
    assert!(matches!(result, Err(AuthError::InvalidCredentials)));

    // Authenticate with new password (should work)
    let _auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "NewPass456!")?;

    Ok(())
}

#[test]
fn test_account_lockout() -> Result<()> {
    // Create auth manager with low max attempts for testing
    let mut config = AuthConfig::default();
    config.max_failed_attempts = 3;
    config.lockout_duration = Duration::from_secs(1); // Short duration for testing
    let auth_manager = AuthenticationManager::with_config(config);
    
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let _user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Make 3 failed attempts
    for _ in 0..3 {
        let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "WrongPassword");
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    // Next attempt should be locked
    let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!");
    assert!(matches!(result, Err(AuthError::AccountLocked)));

    Ok(())
}

#[test]
fn test_unlock_user() -> Result<()> {
    // Create auth manager with low max attempts for testing
    let mut config = AuthConfig::default();
    config.max_failed_attempts = 2;
    config.lockout_duration = Duration::from_secs(60); // Long duration
    let auth_manager = AuthenticationManager::with_config(config);
    
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Create a user
    let user_id = auth_manager.create_user(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    // Make 2 failed attempts to lock the account
    for _ in 0..2 {
        let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "WrongPassword");
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    // Next attempt should be locked
    let result = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!");
    assert!(matches!(result, Err(AuthError::AccountLocked)));

    // Manually unlock the user
    auth_manager.unlock_user(&mut store, &ctx, &user_id)?;

    // Authentication should now work
    let _auth_user_id = auth_manager.authenticate(&mut store, &ctx, "test@example.com", "SecurePass123!")?;

    Ok(())
}

#[test]
fn test_custom_auth_config() -> Result<()> {
    // Create custom config
    let config = AuthConfig {
        max_failed_attempts: 10,
        lockout_duration: Duration::from_secs(5 * 60), // 5 minutes
        min_password_length: 12,
        require_password_complexity: false,
    };
    
    let auth_manager = AuthenticationManager::with_config(config);
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    // Initialize the user schema
    auth_manager.initialize_user_schema(&mut store, &ctx)?;

    // Test that 12-character simple password is accepted
    let result = auth_manager.create_user(&mut store, &ctx, "test@example.com", "simplepassword");
    assert!(result.is_ok());

    // Test that shorter password is rejected
    let result = auth_manager.create_user(&mut store, &ctx, "test2@example.com", "short");
    assert!(matches!(result, Err(AuthError::InvalidPassword(_))));

    Ok(())
}