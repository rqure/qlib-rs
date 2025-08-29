use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::rngs::OsRng;
use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::{
    et, ft, now, sread, swrite, EntityId, Error, Request, Result,
    Value, AsyncStore,
};
use crate::data::StoreTrait;

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
pub async fn authenticate(
    store: &mut AsyncStore,
    name: &str,
    password: &str,
    config: &AuthConfig,
) -> Result<EntityId> {
    let user_id = find_user_by_name(store, name).await?
        .ok_or(Error::SubjectNotFound)?;

    // Check if account is active
    if !is_user_active(store, &user_id).await? {
        return Err(Error::AccountDisabled);
    }

    // Check if account is locked
    if is_user_locked(store, &user_id).await? {
        return Err(Error::AccountLocked);
    }

    // Get authentication method
    let auth_method = get_user_auth_method(store, &user_id).await?;

    // Authenticate based on method
    match auth_method {
        AuthMethod::Native => {
            authenticate_native(store, &user_id, password, config).await?;
        }
        AuthMethod::LDAP => {
            authenticate_ldap(store, &user_id, name, password, config).await?;
        }
        AuthMethod::OpenIDConnect => {
            // OpenID Connect authentication should be handled elsewhere
            // This is typically done via token validation, not username/password
            return Err(Error::InvalidCredentials);
        }
    }

    // Reset failed attempts and update last login
    reset_failed_attempts(store, &user_id).await?;
    update_last_login(store, &user_id).await?;
    Ok(user_id)
}

/// Authenticate a user using native (password hash) authentication
pub async fn authenticate_native(
    store: &mut AsyncStore,
    user_id: &EntityId,
    password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Get stored password hash from Secret field
    let stored_hash = get_user_secret(store, user_id).await?;

    // Verify password
    if verify_password(password, &stored_hash, config)? {
        Ok(())
    } else {
        // Increment failed attempts
        increment_failed_attempts(store, user_id, config).await?;
        Err(Error::InvalidCredentials)
    }
}

/// Authenticate a user using LDAP
/// Note: This is a placeholder - actual LDAP implementation would require LDAP client
pub async fn authenticate_ldap(
    _store: &mut AsyncStore,
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
pub async fn authenticate_openid_connect(
    _store: &mut AsyncStore,
    _user_id: &EntityId,
    _id_token: &str,
    _config: &AuthConfig,
) -> Result<()> {
    // TODO: Implement actual OpenID Connect token validation
    // For now, return an error indicating OIDC is not implemented
    Err(Error::AuthenticationMethodNotImplemented("OpenID Connect".to_string()))
}

/// Get user authentication method
pub async fn get_user_auth_method(store: &mut AsyncStore, user_id: &EntityId) -> Result<AuthMethod> {
    let mut requests = vec![sread!(user_id.clone(), ft::auth_method())];

    store.perform(&mut requests).await?;

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
pub async fn get_user_secret(store: &mut AsyncStore, user_id: &EntityId) -> Result<String> {
    let mut requests = vec![sread!(user_id.clone(), ft::secret())];

    store.perform(&mut requests).await?;

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
pub async fn change_password(
    store: &mut AsyncStore,
    user_id: &EntityId,
    new_password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Check if user uses native authentication
    let auth_method = get_user_auth_method(store, user_id).await?;
    if auth_method != AuthMethod::Native {
        return Err(Error::InvalidAuthenticationMethod);
    }

    // Validate new password
    validate_password(new_password, config)?;

    // Hash the new password
    let password_hash = hash_password(new_password, config)?;

    let mut requests = vec![swrite!(
        user_id.clone(),
        ft::secret(),
        Some(Value::String(password_hash))
    )];

    store.perform(&mut requests).await?;

    Ok(())
}

/// Find a user by name
pub async fn find_user_by_name(store: &mut AsyncStore, name: &str) -> Result<Option<EntityId>> {
    // Use the store's find_entities method to search for users with matching name
    let entities = store.find_entities(&et::user()).await?;

    for entity_id in entities {
        let mut requests = vec![sread!(entity_id.clone(), ft::name())];

        store.perform(&mut requests).await?;

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
pub async fn is_user_active(store: &mut AsyncStore, user_id: &EntityId) -> Result<bool> {
    let mut requests = vec![sread!(user_id.clone(), ft::active())];

    store.perform(&mut requests).await?;

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
pub async fn is_user_locked(store: &mut AsyncStore, user_id: &EntityId) -> Result<bool> {
    let mut requests = vec![sread!(user_id.clone(), ft::locked_until())];

    store.perform(&mut requests).await?;

    if let Some(request) = requests.first() {
        if let Request::Read {
            value: Some(Value::Timestamp(locked_until)),
            ..
        } = request
        {
            return Ok(now() < *locked_until);
        }
    }

    Ok(false)
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
pub async fn increment_failed_attempts(
    store: &mut AsyncStore,
    user_id: &EntityId,
    config: &AuthConfig,
) -> Result<()> {
    // Get current failed attempts
    let mut read_requests = vec![sread!(user_id.clone(), ft::failed_attempts())];

    store.perform(&mut read_requests).await?;

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

    store.perform(&mut write_requests).await?;

    Ok(())
}

/// Reset failed login attempts
pub async fn reset_failed_attempts(store: &mut AsyncStore, user_id: &EntityId) -> Result<()> {
    let mut requests = vec![swrite!(user_id.clone(), ft::failed_attempts(), Some(Value::Int(0)))];

    store.perform(&mut requests).await?;

    Ok(())
}

/// Update last login timestamp
pub async fn update_last_login(store: &mut AsyncStore, user_id: &EntityId) -> Result<()> {
    let mut requests = vec![swrite!(
        user_id.clone(),
        ft::last_login(),
        Some(Value::Timestamp(now()))
    )];

    store.perform(&mut requests).await?;

    Ok(())
}

/// Create a new user with specified authentication method
pub async fn create_user(
    store: &mut AsyncStore,
    name: &str,
    auth_method: AuthMethod,
    parent_id: &EntityId,
) -> Result<EntityId> {
    // Check if user already exists
    if let Ok(Some(_)) = find_user_by_name(store, name).await {
        return Err(Error::SubjectAlreadyExists);
    }

    // Create the user entity
    let mut requests = vec![Request::Create {
        entity_type: et::user(),
        parent_id: Some(parent_id.clone()),
        name: name.to_string(),
        created_entity_id: None,
        originator: None,
    }];
    store.perform(&mut requests).await?;

    // Get the created user ID
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = requests.first() {
        id.clone()
    } else {
        return Err(Error::EntityNotFound(EntityId::new("User", 0)));
    };

    // Set authentication method and default values
    let mut requests = vec![
        swrite!(user_id.clone(), ft::auth_method(), Some(Value::Choice(i64::from(auth_method)))),
        swrite!(user_id.clone(), ft::active(), Some(Value::Bool(true))),
        swrite!(user_id.clone(), ft::failed_attempts(), Some(Value::Int(0))),
    ];

    store.perform(&mut requests).await?;

    Ok(user_id)
}

/// Set user password (only for Native authentication method)
pub async fn set_user_password(
    store: &mut AsyncStore,
    user_id: &EntityId,
    password: &str,
    config: &AuthConfig,
) -> Result<()> {
    // Check if user uses native authentication
    let auth_method = get_user_auth_method(store, user_id).await?;
    if auth_method != AuthMethod::Native {
        return Err(Error::InvalidAuthenticationMethod);
    }

    // Validate password
    validate_password(password, config)?;

    // Hash the password
    let password_hash = hash_password(password, config)?;

    // Store in Secret field
    let mut requests = vec![swrite!(
        user_id.clone(),
        ft::secret(),
        Some(Value::String(password_hash))
    )];

    store.perform(&mut requests).await?;

    Ok(())
}

/// Set user authentication method
pub async fn set_user_auth_method(
    store: &mut AsyncStore,
    user_id: &EntityId,
    auth_method: AuthMethod,
) -> Result<()> {
    let mut requests = vec![swrite!(
        user_id.clone(),
        ft::auth_method(),
        Some(Value::Choice(i64::from(auth_method)))
    )];

    store.perform(&mut requests).await?;

    Ok(())
}

/// Authenticate a service using its secret key
pub async fn authenticate_service(
    store: &mut AsyncStore,
    name: &str,
    secret_key: &str,
) -> Result<EntityId> {
    let service_id = find_service_by_name(store, name).await?
        .ok_or(Error::SubjectNotFound)?; // Reusing user error for services

    // Check if service is active
    if !is_service_active(store, &service_id).await? {
        return Err(Error::AccountDisabled);
    }

    // Get stored secret key
    let stored_secret = get_service_secret(store, &service_id).await?;

    // Compare secret keys (simple string comparison for services)
    if stored_secret == secret_key {
        Ok(service_id)
    } else {
        Err(Error::InvalidCredentials)
    }
}

/// Find a service by name
pub async fn find_service_by_name(store: &mut AsyncStore, name: &str) -> Result<Option<EntityId>> {
    let entities = store.find_entities(&et::service()).await?;

    for entity_id in entities {
        let mut requests = vec![sread!(entity_id.clone(), ft::name())];
        store.perform(&mut requests).await?;

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
pub async fn is_service_active(store: &mut AsyncStore, service_id: &EntityId) -> Result<bool> {
    let mut requests = vec![sread!(service_id.clone(), ft::active())];
    store.perform(&mut requests).await?;

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
pub async fn get_service_secret(store: &mut AsyncStore, service_id: &EntityId) -> Result<String> {
    let mut requests = vec![sread!(service_id.clone(), ft::secret())];
    store.perform(&mut requests).await?;

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
pub async fn set_service_secret(
    store: &mut AsyncStore,
    service_id: &EntityId,
    secret_key: &str,
) -> Result<()> {
    let mut requests = vec![swrite!(
        service_id.clone(),
        ft::secret(),
        Some(Value::String(secret_key.to_string()))
    )];

    store.perform(&mut requests).await?;
    Ok(())
}

/// Generic subject authentication - determines if it's a user or service and authenticates accordingly
pub async fn authenticate_subject(
    store: &mut AsyncStore,
    name: &str,
    credential: &str,
    config: &AuthConfig,
) -> Result<EntityId> {
    // Try to authenticate as user first
    if let Ok(user_id) = authenticate(store, name, credential, config).await {
        return Ok(user_id);
    }

    // If user authentication fails, try service authentication
    if let Ok(service_id) = authenticate_service(store, name, credential).await {
        return Ok(service_id);
    }

    Err(Error::InvalidCredentials)
}
