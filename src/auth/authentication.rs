use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::rngs::OsRng;
use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::{
    now, EntityId, Error, Result,
    Value, StoreProxy,
};

const USER_ENTITY_NAME: &str = "User";
const SUBJECT_ENTITY_NAME: &str = "Subject";
const NAME_FIELD_NAME: &str = "Name";
const SECRET_FIELD_NAME: &str = "Secret";
const AUTH_METHOD_FIELD_NAME: &str = "AuthMethod";
const ACTIVE_FIELD_NAME: &str = "Active";
const LOCKED_UNTIL_FIELD_NAME: &str = "LockedUntil";
const FAILED_ATTEMPTS_FIELD_NAME: &str = "FailedAttempts";
const LAST_LOGIN_FIELD_NAME: &str = "LastLogin";

/// Authentication methods supported by the system
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMethod {
    /// Native authentication using stored password hash in Secret field
    Native = 0,
    /// LDAP authentication 
    LDAP = 1,
    /// OpenID Connect authentication
    OpenIDConnect = 2,
}

impl From<i64> for AuthMethod {
    fn from(value: i64) -> Self {
        match value {
            0 => AuthMethod::Native,
            1 => AuthMethod::LDAP,
            2 => AuthMethod::OpenIDConnect,
            _ => AuthMethod::Native, // Default to Native for unknown values
        }
    }
}

impl From<AuthMethod> for i64 {
    fn from(method: AuthMethod) -> Self {
        method as i64
    }
}

/// Configuration for authentication behavior
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Maximum number of failed login attempts before account lockout
    pub max_failed_attempts: i64,
    /// Duration to lock account after max failed attempts
    pub lockout_duration: Duration,
    /// Minimum password length
    pub min_password_length: usize,
    /// Require password complexity (uppercase, lowercase, numbers, symbols)
    pub require_password_complexity: bool,

    pub argon2: Argon2<'static>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(30 * 60), // 30 minutes
            min_password_length: 8,
            require_password_complexity: true,
            argon2: Argon2::default(),
        }
    }
}

/// Authenticate a user with name and password
/// This function determines the authentication method from the user's AuthMethod field
/// and delegates to the appropriate authentication mechanism
pub fn authenticate_user(
    store: &StoreProxy,
    name: &str,
    password: &str,
    config: &AuthConfig,
) -> Result<EntityId> {
    let user_id = find_user_by_name(store, name)?
        .ok_or(Error::SubjectNotFound)?;

    // Check if account is active
    if !is_user_active(store, user_id)? {
        return Err(Error::AccountDisabled);
    }

    // Check if account is locked
    if is_user_locked(store, user_id)? {
        return Err(Error::AccountLocked);
    }

    // Get authentication method
    let auth_method = get_user_auth_method(store, user_id)?;

    // Authenticate based on method
    match auth_method {
        AuthMethod::Native => {
            authenticate_native(store, user_id, password, config)?;
        }
        AuthMethod::LDAP => {
            authenticate_ldap(store, user_id, name, password, config)?;
        }
        AuthMethod::OpenIDConnect => {
            // OpenID Connect authentication should be handled elsewhere
            // This is typically done via token validation, not username/password
            return Err(Error::InvalidCredentials);
        }
    }

    // Reset failed attempts and update last login
    reset_failed_attempts(store, user_id)?;
    update_last_login(store, user_id)?;
    Ok(user_id)
}

/// Authenticate a user using native (password hash) authentication
pub fn authenticate_native(
    store: &StoreProxy,
    user_id: EntityId,
    password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Get stored password hash from Secret field
    let stored_hash = get_user_secret(store, user_id)?;

    // Verify password
    if verify_password(password, &stored_hash, config)? {
        Ok(())
    } else {
        // Increment failed attempts
        increment_failed_attempts(store, user_id, config)?;
        Err(Error::InvalidCredentials)
    }
}

/// Authenticate a user using LDAP
/// Note: This is a placeholder - actual LDAP implementation would require LDAP client
pub fn authenticate_ldap(
    _store: &StoreProxy,
    _user_id: EntityId,
    _name: &str,
    _password: &str,
    _config: &AuthConfig,
) -> Result<()> {
    // TODO: Implement actual LDAP authentication
    // For now, return an error indicating LDAP is not implemented
    Err(Error::AuthenticationMethodNotImplemented("LDAP".to_string()))
}

/// Authenticate a user using OpenID Connect token validation
pub fn authenticate_openid_connect(
    _store: &StoreProxy,
    _user_id: EntityId,
    _id_token: &str,
    _config: &AuthConfig,
) -> Result<()> {
    // TODO: Implement actual OpenID Connect token validation
    // For now, return an error indicating OIDC is not implemented
    Err(Error::AuthenticationMethodNotImplemented("OpenID Connect".to_string()))
}

/// Get user authentication method
pub fn get_user_auth_method(store: &StoreProxy, user_id: EntityId) -> Result<AuthMethod> {
    let auth_method_ft = store.get_field_type(AUTH_METHOD_FIELD_NAME)?;
    let result = store.read(user_id, &[auth_method_ft]);
    
    // If the store operation failed, default to Native
    let (value, _, _) = match result {
        Ok(value) => value,
        Err(_) => return Ok(AuthMethod::Native),
    };

    match value {
        Value::Choice(method) => Ok(AuthMethod::from(method)),
        _ => Ok(AuthMethod::Native), // Default to Native if AuthMethod is not set
    }
}

/// Get user secret (password hash for native auth, or other secret data)
pub fn get_user_secret(store: &StoreProxy, user_id: EntityId) -> Result<String> {
    let secret_ft = store.get_field_type(SECRET_FIELD_NAME)?;
    let (value, _, _) = store.read(user_id, &[secret_ft])?;

    match value {
        Value::String(secret) => Ok(secret.to_string()),
        _ => Err(Error::SubjectNotFound),
    }
}

/// Change a user's password (only for Native authentication)
pub fn change_password(
    store: &StoreProxy,
    user_id: EntityId,
    new_password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Check if user uses native authentication
    let auth_method = get_user_auth_method(store, user_id)?;
    if auth_method != AuthMethod::Native {
        return Err(Error::InvalidAuthenticationMethod);
    }

    // Validate new password
    validate_password(new_password, config)?;

    // Hash the new password
    let password_hash = hash_password(new_password, config)?;

    let secret_ft = store.get_field_type(SECRET_FIELD_NAME)?;
    store.write(user_id, &[secret_ft], Value::String(password_hash.into()), None, None, None, None)?;

    Ok(())
}

/// Find a user by name
pub fn find_user_by_name(store: &StoreProxy, name: &str) -> Result<Option<EntityId>> {
    let user_et = store.get_entity_type(USER_ENTITY_NAME)?;
    let entities = store.find_entities(user_et, None)?;
    let name_ft = store.get_field_type(NAME_FIELD_NAME)?;
    for entity_id in entities {
        let (value, _, _) = store.read(entity_id, &[name_ft])?;

        if let Value::String(stored_name) = value {
            if stored_name.eq_ignore_ascii_case(name) {
                return Ok(Some(entity_id));
            }
        }
    }

    Ok(None)
}

/// Check if a user is active
pub fn is_user_active(store: &StoreProxy, user_id: EntityId) -> Result<bool> {
    let active_ft = store.get_field_type(ACTIVE_FIELD_NAME)?;
    
    let (value, _, _) = store.read(user_id, &[active_ft])?;

    match value {
        Value::Bool(active) => Ok(active),
        _ => Ok(false), // Default to inactive if field not found
    }
}

/// Check if a user is locked
pub fn is_user_locked(store: &StoreProxy, user_id: EntityId) -> Result<bool> {
    let locked_until_ft = store.get_field_type(LOCKED_UNTIL_FIELD_NAME)?;
    let (value, _, _) = match store.read(user_id, &[locked_until_ft]) {
        Ok(value) => value,
        Err(_) => return Ok(false), // If read fails, assume not locked
    };

    match value {
        Value::Timestamp(locked_until) => Ok(now() < locked_until),
        _ => Ok(false),
    }
}

/// Hash a password using Argon2
pub fn hash_password(password: &str, config: &AuthConfig) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let password_hash = config.argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| Error::PasswordHashError(e.to_string()))?;
    Ok(password_hash.to_string())
}

/// Verify a password against a hash
pub fn verify_password(password: &str, hash: &str, config: &AuthConfig) -> Result<bool> {
    let parsed_hash =
        PasswordHash::new(hash).map_err(|e| Error::PasswordHashError(e.to_string()))?;

    Ok(config.argon2
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

/// Validate password requirements
pub fn validate_password(password: &str, config: &AuthConfig) -> Result<()> {
    if password.len() < config.min_password_length {
        return Err(Error::InvalidPassword(format!(
            "Password must be at least {} characters long",
            config.min_password_length
        )));
    }

    if config.require_password_complexity {
        let has_upper = password.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = password.chars().any(|c| c.is_ascii_lowercase());
        let has_digit = password.chars().any(|c| c.is_ascii_digit());
        let has_symbol = password.chars().any(|c| !c.is_alphanumeric());

        if !has_upper || !has_lower || !has_digit || !has_symbol {
            return Err(Error::InvalidPassword(
                "Password must contain uppercase, lowercase, digit, and symbol characters".to_string()
            ));
        }
    }

    Ok(())
}

/// Increment failed login attempts and lock account if needed
pub fn increment_failed_attempts(
    store: &StoreProxy,
    user_id: EntityId,
    config: &AuthConfig,
) -> Result<()> {
    // Get current failed attempts
    let failed_attempts_ft = store.get_field_type(FAILED_ATTEMPTS_FIELD_NAME)?;
    let locked_until_ft = store.get_field_type(LOCKED_UNTIL_FIELD_NAME)?;
    let current_attempts = store.read(user_id, &[failed_attempts_ft]).map_or(0, |(value, _, _)| {
        if let Value::Int(attempts) = value {
            attempts
        } else {
            0
        }
    });

    let new_attempts = current_attempts + 1;

    // Update failed attempts
    store.write(user_id, &[failed_attempts_ft], Value::Int(new_attempts), None, None, None, None)?;

    // Lock account if max attempts reached
    if new_attempts >= config.max_failed_attempts {
        let lock_until = now() + config.lockout_duration;
        store.write(user_id, &[locked_until_ft], Value::Timestamp(lock_until), None, None, None, None)?;
    }

    Ok(())
}

/// Reset failed login attempts
pub fn reset_failed_attempts(store: &StoreProxy, user_id: EntityId) -> Result<()> {
    let failed_attempts_ft = store.get_field_type(FAILED_ATTEMPTS_FIELD_NAME)?;
    store.write(user_id, &[failed_attempts_ft], Value::Int(0), None, None, None, None)?;

    Ok(())
}

/// Update last login timestamp
pub fn update_last_login(store: &StoreProxy, user_id: EntityId) -> Result<()> {
    let last_login_ft = store.get_field_type(LAST_LOGIN_FIELD_NAME)?;
    store.write(user_id, &[last_login_ft], Value::Timestamp(now()), None, None, None, None)?;

    Ok(())
}

/// Create a new user with specified authentication method
pub fn create_user(
    store: &StoreProxy,
    name: &str,
    auth_method: AuthMethod,
    parent_id: EntityId,
) -> Result<EntityId> {
    // Check if user already exists
    if let Ok(Some(_)) = find_user_by_name(store, name) {
        return Err(Error::SubjectAlreadyExists);
    }

    // Create the user entity
    let entity_type = store.get_entity_type(USER_ENTITY_NAME)?;
    let user_id = store.create_entity(entity_type, Some(parent_id), name)?;

    // Set authentication method and default values
    let auth_method_ft = store.get_field_type(AUTH_METHOD_FIELD_NAME)?;
    let active_ft = store.get_field_type(ACTIVE_FIELD_NAME)?;
    let failed_attempts_ft = store.get_field_type(FAILED_ATTEMPTS_FIELD_NAME)?;
    store.write(user_id, &[auth_method_ft], Value::Choice(i64::from(auth_method)), None, None, None, None)?;
    store.write(user_id, &[active_ft], Value::Bool(true), None, None, None, None)?;
    store.write(user_id, &[failed_attempts_ft], Value::Int(0), None, None, None, None)?;

    Ok(user_id)
}

/// Set user password (only for Native authentication method)
pub fn set_user_password(
    store: &StoreProxy,
    user_id: EntityId,
    password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Check if user uses native authentication
    let auth_method = get_user_auth_method(store, user_id)?;
    if auth_method != AuthMethod::Native {
        return Err(Error::InvalidAuthenticationMethod);
    }

    // Validate password
    validate_password(password, config)?;

    // Hash the password
    let password_hash = hash_password(password, config)?;

    // Store in Secret field
    let secret_ft = store.get_field_type(SECRET_FIELD_NAME)?;
    store.write(user_id, &[secret_ft], Value::String(password_hash.into()), None, None, None, None)?;

    Ok(())
}

/// Set user authentication method
pub fn set_user_auth_method(
    store: &StoreProxy,
    user_id: EntityId,
    auth_method: AuthMethod,
) -> Result<()> {
    let auth_method_ft = store.get_field_type(AUTH_METHOD_FIELD_NAME)?;
    store.write(user_id, &[auth_method_ft], Value::Choice(i64::from(auth_method)), None, None, None, None)?;

    Ok(())
}

/// Authenticate a service using its secret key
pub fn authenticate_service(
    store: &StoreProxy,
    name: &str,
    secret_key: &str,
) -> Result<EntityId> {
    let service_id = find_subject_by_name(store, name)?
        .ok_or(Error::SubjectNotFound)?; // Reusing user error for services

    // Check if service is active
    if !is_service_active(store, service_id)? {
        return Err(Error::AccountDisabled);
    }

    // Get stored secret key
    let stored_secret = get_service_secret(store, service_id)?;

    // Compare secret keys (simple string comparison for services)
    if stored_secret == secret_key {
        Ok(service_id)
    } else {
        Err(Error::InvalidCredentials)
    }
}

/// Find a subject by name
pub fn find_subject_by_name(store: &StoreProxy, name: &str) -> Result<Option<EntityId>> {
    let entities = store.find_entities(store.get_entity_type(SUBJECT_ENTITY_NAME)?, None)?;

    let name_ft = store.get_field_type(NAME_FIELD_NAME)?;
    for entity_id in entities {
        let (value, _, _) = store.read(entity_id, &[name_ft])?;
        if let Value::String(stored_name) = value {
            if stored_name.eq_ignore_ascii_case(name) {
                return Ok(Some(entity_id));
            }
        }
    }

    Ok(None)
}

/// Check if a service is active
pub fn is_service_active(store: &StoreProxy, service_id: EntityId) -> Result<bool> {
    let active_ft = store.get_field_type(ACTIVE_FIELD_NAME)?;
    let (value, _, _) = store.read(service_id, &[active_ft])?;
    match value {
        Value::Bool(active) => Ok(active),
        _ => Ok(false), // Default to inactive if field not found
    }
}

/// Get service secret key
pub fn get_service_secret(store: &StoreProxy, service_id: EntityId) -> Result<String> {
    let secret_ft = store.get_field_type(SECRET_FIELD_NAME)?;
    let (value, _, _) = store.read(service_id, &[secret_ft])?;
    match value {
        Value::String(secret) => Ok(secret.to_string()),
        _ => Err(Error::SubjectNotFound),
    }
}

/// Set service secret key
pub fn set_service_secret(
    store: &StoreProxy,
    service_id: EntityId,
    secret_key: &str,
) -> Result<()> {
    let secret_ft = store.get_field_type(SECRET_FIELD_NAME)?;
    store.write(service_id, &[secret_ft], Value::String(secret_key.to_string().into()), None, None, None, None)?;
    Ok(())
}

/// Generic subject authentication - determines if it's a user or service and authenticates accordingly
pub fn authenticate_subject(
    store: &StoreProxy,
    name: &str,
    credential: &str,
    config: &AuthConfig,
) -> Result<EntityId> {
    match authenticate_user(store, name, credential, config) {
        Ok(user_id) => return Ok(user_id),
        Err(Error::SubjectNotFound) => {
            match authenticate_service(store, name, credential) {
                Ok(service_id) => return Ok(service_id),
                Err(e) => return Err(e),
            }
        }
        Err(e) => return Err(e),
    }
}
