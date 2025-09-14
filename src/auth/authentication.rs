use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::rngs::OsRng;
use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::{
    et, ft, now, sread, swrite, EntityId, Error, Request, Result,
    Value, Store,
};

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
    store: &mut Store,
    name: &str,
    password: &str,
    config: &AuthConfig,
) -> Result<EntityId> {
    let user_id = find_user_by_name(store, name)?
        .ok_or(Error::SubjectNotFound)?;

    // Check if account is active
    if !is_user_active(store, &user_id)? {
        return Err(Error::AccountDisabled);
    }

    // Check if account is locked
    if is_user_locked(store, &user_id)? {
        return Err(Error::AccountLocked);
    }

    // Get authentication method
    let auth_method = get_user_auth_method(store, &user_id)?;

    // Authenticate based on method
    match auth_method {
        AuthMethod::Native => {
            authenticate_native(store, &user_id, password, config)?;
        }
        AuthMethod::LDAP => {
            authenticate_ldap(store, &user_id, name, password, config)?;
        }
        AuthMethod::OpenIDConnect => {
            // OpenID Connect authentication should be handled elsewhere
            // This is typically done via token validation, not username/password
            return Err(Error::InvalidCredentials);
        }
    }

    // Reset failed attempts and update last login
    reset_failed_attempts(store, &user_id)?;
    update_last_login(store, &user_id)?;
    Ok(user_id)
}

/// Authenticate a user using native (password hash) authentication
pub fn authenticate_native(
    store: &mut Store,
    user_id: &EntityId,
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
    _store: &mut Store,
    _user_id: &EntityId,
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
    _store: &mut Store,
    _user_id: &EntityId,
    _id_token: &str,
    _config: &AuthConfig,
) -> Result<()> {
    // TODO: Implement actual OpenID Connect token validation
    // For now, return an error indicating OIDC is not implemented
    Err(Error::AuthenticationMethodNotImplemented("OpenID Connect".to_string()))
}

/// Get user authentication method
pub fn get_user_auth_method(store: &mut Store, user_id: &EntityId) -> Result<AuthMethod> {
    let requests = vec![sread!(user_id.clone(), ft::auth_method())];

    let result = store.perform_mut(requests);
    
    // If the store operation failed, default to Native
    let requests = match result {
        Ok(requests) => requests,
        Err(_) => return Ok(AuthMethod::Native),
    };

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::Choice(method)),
            ..
        } = request
        {
            Ok(AuthMethod::from(*method))
        } else {
            // Default to Native if AuthMethod is not set
            Ok(AuthMethod::Native)
        }
    } else {
        // Default to Native if field doesn't exist
        Ok(AuthMethod::Native)
    }
}

/// Get user secret (password hash for native auth, or other secret data)
pub fn get_user_secret(store: &mut Store, user_id: &EntityId) -> Result<String> {
    let requests = vec![sread!(user_id.clone(), ft::secret())];

    let requests = store.perform_mut(requests)?;

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::String(secret)),
            ..
        } = request
        {
            Ok(secret.clone())
        } else {
            Err(Error::SubjectNotFound)
        }
    } else {
        Err(Error::SubjectNotFound)
    }
}

/// Change a user's password (only for Native authentication)
pub fn change_password(
    store: &mut Store,
    user_id: &EntityId,
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

    let requests = vec![swrite!(
        user_id.clone(),
        ft::secret(),
        Some(Value::String(password_hash))
    )];

    store.perform_mut(requests)?;

    Ok(())
}

/// Find a user by name
pub fn find_user_by_name(store: &mut Store, name: &str) -> Result<Option<EntityId>> {
    // Use the store's find_entities method to search for users with matching name
    let entities = store.find_entities(&et::user(), None)?;

    for entity_id in entities {
        let requests = vec![sread!(entity_id.clone(), ft::name())];

        let requests = store.perform_mut(requests)?;

        if let Some(request) = requests.first() {
            if let Request::Read {
                value: Some(Value::String(stored_name)),
                ..
            } = request
            {
                if stored_name.eq_ignore_ascii_case(name) {
                    return Ok(Some(entity_id));
                }
            }
        }
    }

    Ok(None)
}

/// Check if a user is active
pub fn is_user_active(store: &mut Store, user_id: &EntityId) -> Result<bool> {
    let requests = vec![sread!(user_id.clone(), ft::active())];

    let requests = store.perform_mut(requests)?;

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::Bool(active)),
            ..
        } = request
        {
            Ok(*active)
        } else {
            Ok(false) // Default to inactive if field not found
        }
    } else {
        Ok(false)
    }
}

/// Check if a user is locked
pub fn is_user_locked(store: &mut Store, user_id: &EntityId) -> Result<bool> {
    let requests = vec![sread!(user_id.clone(), ft::locked_until())];

    let result = store.perform_mut(requests);
    
    // If the store operation failed, it might be because the field doesn't exist
    let requests = match result {
        Ok(requests) => requests,
        Err(_) => return Ok(false),
    };

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::Timestamp(locked_until)),
            ..
        } = request
        {
            Ok(now() < *locked_until)
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
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
    store: &mut Store,
    user_id: &EntityId,
    config: &AuthConfig,
) -> Result<()> {
    // Get current failed attempts
    let read_requests = vec![sread!(user_id.clone(), ft::failed_attempts())];

    let read_requests = store.perform_mut(read_requests)?;

    let current_attempts = if let Some(request) = read_requests.first() {
        if let Request::Read {
            value: Some(Value::Int(attempts)),
            ..
        } = request
        {
            *attempts
        } else {
            0
        }
    } else {
        0
    };

    let new_attempts = current_attempts + 1;

    // Update failed attempts
    let mut write_requests = vec![swrite!(
        user_id.clone(),
        ft::failed_attempts(),
        Some(Value::Int(new_attempts))
    )];

    // Lock account if max attempts reached
    if new_attempts >= config.max_failed_attempts {
        let lock_until = now() + config.lockout_duration;
        write_requests.push(swrite!(
            user_id.clone(),
            ft::locked_until(),
            Some(Value::Timestamp(lock_until))
        ));
    }

    store.perform_mut(write_requests)?;

    Ok(())
}

/// Reset failed login attempts
pub fn reset_failed_attempts(store: &mut Store, user_id: &EntityId) -> Result<()> {
    let requests = vec![swrite!(user_id.clone(), ft::failed_attempts(), Some(Value::Int(0)))];

    store.perform_mut(requests)?;

    Ok(())
}

/// Update last login timestamp
pub fn update_last_login(store: &mut Store, user_id: &EntityId) -> Result<()> {
    let requests = vec![swrite!(
        user_id.clone(),
        ft::last_login(),
        Some(Value::Timestamp(now()))
    )];

    store.perform_mut(requests)?;

    Ok(())
}

/// Create a new user with specified authentication method
pub fn create_user(
    store: &mut Store,
    name: &str,
    auth_method: AuthMethod,
    parent_id: &EntityId,
) -> Result<EntityId> {
    // Check if user already exists
    if let Ok(Some(_)) = find_user_by_name(store, name) {
        return Err(Error::SubjectAlreadyExists);
    }

    // Create the user entity
    let requests = vec![Request::Create {
        entity_type: et::user(),
        parent_id: Some(parent_id.clone()),
        name: name.to_string(),
        created_entity_id: None,
        timestamp: None,
        originator: None,
    }];
    let requests = store.perform_mut(requests)?;

    // Get the created user ID
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = requests.first() {
        id.clone()
    } else {
        return Err(Error::EntityNotFound(EntityId::new("User", 0)));
    };

    // Set authentication method and default values
    let requests = vec![
        swrite!(user_id.clone(), ft::auth_method(), Some(Value::Choice(i64::from(auth_method)))),
        swrite!(user_id.clone(), ft::active(), Some(Value::Bool(true))),
        swrite!(user_id.clone(), ft::failed_attempts(), Some(Value::Int(0))),
    ];

    store.perform_mut(requests)?;

    Ok(user_id)
}

/// Set user password (only for Native authentication method)
pub fn set_user_password(
    store: &mut Store,
    user_id: &EntityId,
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
    let requests = vec![swrite!(
        user_id.clone(),
        ft::secret(),
        Some(Value::String(password_hash))
    )];

    store.perform_mut(requests)?;

    Ok(())
}

/// Set user authentication method
pub fn set_user_auth_method(
    store: &mut Store,
    user_id: &EntityId,
    auth_method: AuthMethod,
) -> Result<()> {
    let requests = vec![swrite!(
        user_id.clone(),
        ft::auth_method(),
        Some(Value::Choice(i64::from(auth_method)))
    )];

    store.perform_mut(requests)?;

    Ok(())
}

/// Authenticate a service using its secret key
pub fn authenticate_service(
    store: &mut Store,
    name: &str,
    secret_key: &str,
) -> Result<EntityId> {
    let service_id = find_subject_by_name(store, name)?
        .ok_or(Error::SubjectNotFound)?; // Reusing user error for services

    // Check if service is active
    if !is_service_active(store, &service_id)? {
        return Err(Error::AccountDisabled);
    }

    // Get stored secret key
    let stored_secret = get_service_secret(store, &service_id)?;

    // Compare secret keys (simple string comparison for services)
    if stored_secret == secret_key {
        Ok(service_id)
    } else {
        Err(Error::InvalidCredentials)
    }
}

/// Find a subject by name
pub fn find_subject_by_name(store: &mut Store, name: &str) -> Result<Option<EntityId>> {
    let entities = store.find_entities(&et::subject(), None)?;

    for entity_id in entities {
        let requests = vec![sread!(entity_id.clone(), ft::name())];
        let requests = store.perform_mut(requests)?;

        if let Some(request) = requests.first() {
            if let Request::Read {
                value: Some(Value::String(stored_name)),
                ..
            } = request
            {
                if stored_name.eq_ignore_ascii_case(name) {
                    return Ok(Some(entity_id));
                }
            }
        }
    }

    Ok(None)
}

/// Check if a service is active
pub fn is_service_active(store: &mut Store, service_id: &EntityId) -> Result<bool> {
    let requests = vec![sread!(service_id.clone(), ft::active())];
    let requests = store.perform_mut(requests)?;

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::Bool(active)),
            ..
        } = request
        {
            Ok(*active)
        } else {
            Ok(false) // Default to inactive if field not found
        }
    } else {
        Ok(false)
    }
}

/// Get service secret key
pub fn get_service_secret(store: &mut Store, service_id: &EntityId) -> Result<String> {
    let requests = vec![sread!(service_id.clone(), ft::secret())];
    let requests = store.perform_mut(requests)?;

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::String(secret)),
            ..
        } = request
        {
            Ok(secret.clone())
        } else {
            Err(Error::SubjectNotFound)
        }
    } else {
        Err(Error::SubjectNotFound)
    }
}

/// Set service secret key
pub fn set_service_secret(
    store: &mut Store,
    service_id: &EntityId,
    secret_key: &str,
) -> Result<()> {
    let requests = vec![swrite!(
        service_id.clone(),
        ft::secret(),
        Some(Value::String(secret_key.to_string()))
    )];

    store.perform_mut(requests)?;
    Ok(())
}

/// Generic subject authentication - determines if it's a user or service and authenticates accordingly
pub fn authenticate_subject(
    store: &mut Store,
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
