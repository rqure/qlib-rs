use argon2::Argon2;
use std::time::Duration;

// Note: Uncomment these imports when the macros are actually implemented
// use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
// use rand::rngs::OsRng;
// use crate::{
//     et, ft, now, sint, sread, sstr, stimestamp, swrite, EntityId, Error, Request, Result,
//     Value,
// };

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
#[macro_export]
macro_rules! authenticate {
    ($store:expr, $name:expr, $password:expr, $config:expr) => {{
        async {
            let user_id = find_user_by_name!($store, $name).await?
                .ok_or(Error::UserNotFound)?;

            // Check if account is active
            if !is_user_active!($store, &user_id).await? {
                return Err(Error::AccountDisabled);
            }

            // Check if account is locked
            if is_user_locked!($store, &user_id).await? {
                return Err(Error::AccountLocked);
            }

            // Get stored password hash
            let stored_hash = get_user_password_hash!($store, &user_id).await?;

            // Verify password
            if verify_password!($password, &stored_hash, $config)? {
                // Reset failed attempts and update last login
                reset_failed_attempts!($store, &user_id).await?;
                update_last_login!($store, &user_id).await?;
                Ok(user_id)
            } else {
                // Increment failed attempts
                increment_failed_attempts!($store, &user_id, $config).await?;
                Err(Error::InvalidCredentials)
            }
        }
    }}
}

/// Change a user's password
#[macro_export]
macro_rules! change_password {
    ($store:expr, $user_id:expr, $new_password:expr, $config:expr) => {{
        async {
            // Validate new password
            validate_password!($new_password, $config)?;

            // Hash the new password
            let password_hash = hash_password!($new_password, $config)?;

            let mut requests = vec![swrite!(
                $user_id.clone(),
                ft::password(),
                sstr!(password_hash)
            )];

            $store.perform(&mut requests).await?;

            Ok(())
        }
    }}
}

/// Find a user by name
#[macro_export]
macro_rules! find_user_by_name {
    ($store:expr, $name:expr) => {{
        async {
            // Use the store's find_entities method to search for users with matching name
            let entities = $store.find_entities(&et::user()).await?;

            for entity_id in entities {
                let mut requests = vec![sread!(entity_id.clone(), ft::name())];

                $store.perform(&mut requests).await?;

                if let Some(request) = requests.first() {
                    if let Request::Read {
                        value: Some(Value::String(stored_name)),
                        ..
                    } = request
                    {
                        if stored_name.eq_ignore_ascii_case($name) {
                            return Ok(Some(entity_id));
                        }
                    }
                }
            }

            Ok(None)
        }
    }}
}

/// Check if a user is active
#[macro_export]
macro_rules! is_user_active {
    ($store:expr, $user_id:expr) => {{
        async {
            let mut requests = vec![sread!($user_id.clone(), ft::active())];

            $store.perform(&mut requests).await?;

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
    }}
}

/// Check if a user is locked
#[macro_export]
macro_rules! is_user_locked {
    ($store:expr, $user_id:expr) => {{
        async {
            let mut requests = vec![sread!($user_id.clone(), ft::locked_until())];

            $store.perform(&mut requests).await?;

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
    }}
}

/// Get user password hash
#[macro_export]
macro_rules! get_user_password_hash {
    ($store:expr, $user_id:expr) => {{
        async {
            let mut requests = vec![sread!($user_id.clone(), ft::password())];

            $store.perform(&mut requests).await?;

            if let Some(request) = requests.first() {
                if let Request::Read {
                    value: Some(Value::String(hash)),
                    ..
                } = request
                {
                    Ok(hash.clone())
                } else {
                    Err(Error::UserNotFound)
                }
            } else {
                Err(Error::UserNotFound)
            }
        }
    }}
}

/// Hash a password using Argon2
#[macro_export]
macro_rules! hash_password {
    ($password:expr, $config:expr) => {{
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = $config.argon2
            .hash_password($password.as_bytes(), &salt)
            .map_err(|e| Error::PasswordHashError(e.to_string()))?;
        Ok(password_hash.to_string())
    }}
}

/// Verify a password against a hash
#[macro_export]
macro_rules! verify_password {
    ($password:expr, $hash:expr, $config:expr) => {{
        let parsed_hash =
            PasswordHash::new($hash).map_err(|e| Error::PasswordHashError(e.to_string()))?;

        Ok($config.argon2
            .verify_password($password.as_bytes(), &parsed_hash)
            .is_ok())
    }}
}

/// Validate password requirements
#[macro_export]
macro_rules! validate_password {
    ($password:expr, $config:expr) => {{
        if $password.len() < $config.min_password_length {
            return Err(Error::InvalidPassword(format!(
                "Password must be at least {} characters long",
                $config.min_password_length
            )));
        }

        if $config.require_password_complexity {
            let has_upper = $password.chars().any(|c| c.is_ascii_uppercase());
            let has_lower = $password.chars().any(|c| c.is_ascii_lowercase());
            let has_digit = $password.chars().any(|c| c.is_ascii_digit());
            let has_symbol = $password.chars().any(|c| !c.is_alphanumeric());

            if !has_upper || !has_lower || !has_digit || !has_symbol {
                return Err(Error::InvalidPassword(
                    "Password must contain uppercase, lowercase, digit, and symbol characters"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }}
}

/// Increment failed login attempts and lock account if needed
#[macro_export]
macro_rules! increment_failed_attempts {
    ($store:expr, $user_id:expr, $config:expr) => {{
        async {
            // Get current failed attempts
            let mut read_requests = vec![sread!($user_id.clone(), ft::failed_attempts())];

            $store.perform(&mut read_requests).await?;

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
                $user_id.clone(),
                ft::failed_attempts(),
                sint!(new_attempts)
            )];

            // Lock account if max attempts reached
            if new_attempts >= $config.max_failed_attempts {
                let lock_until = now() + $config.lockout_duration;
                write_requests.push(swrite!(
                    $user_id.clone(),
                    ft::locked_until(),
                    stimestamp!(lock_until)
                ));
            }

            $store.perform(&mut write_requests).await?;

            Ok(())
        }
    }}
}

/// Reset failed login attempts
#[macro_export]
macro_rules! reset_failed_attempts {
    ($store:expr, $user_id:expr) => {{
        async {
            let mut requests = vec![swrite!($user_id.clone(), ft::failed_attempts(), sint!(0))];

            $store.perform(&mut requests).await?;

            Ok(())
        }
    }}
}

/// Update last login timestamp
#[macro_export]
macro_rules! update_last_login {
    ($store:expr, $user_id:expr) => {{
        async {
            let mut requests = vec![swrite!(
                $user_id.clone(),
                ft::last_login(),
                stimestamp!(now())
            )];

            $store.perform(&mut requests).await?;

            Ok(())
        }
    }}
}
