use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use std::time::{Duration};

use crate::{
    et, ft, now, sint, sread, sstr, stimestamp, swrite, Context, EntityId, Error, Request, Result, Value
};

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
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(30 * 60), // 30 minutes
            min_password_length: 8,
            require_password_complexity: true,
        }
    }
}

/// Authentication manager for handling user authentication
pub struct AuthenticationManager {
    config: AuthConfig,
    argon2: Argon2<'static>,
}

impl AuthenticationManager {
    /// Create a new authentication manager
    pub fn new() -> Self {
        Self::with_config(AuthConfig::default())
    }

    /// Create a new authentication manager with custom configuration
    pub fn with_config(config: AuthConfig) -> Self {
        Self {
            config,
            argon2: Argon2::default(),
        }
    }

    /// Authenticate a user with name and password
    pub async fn authenticate(&self, store: &mut StoreInterface, ctx: &Context, name: &str, password: &str) -> Result<EntityId> {
        let user_id = self
            .find_user_by_name(store, ctx, name)
            .await?
            .ok_or(Error::UserNotFound)?;

        // Check if account is active
        if !self.is_user_active(store, ctx, &user_id).await? {
            return Err(Error::AccountDisabled);
        }

        // Check if account is locked
        if self.is_user_locked(store, ctx, &user_id).await? {
            return Err(Error::AccountLocked);
        }

        // Get stored password hash
        let stored_hash = self.get_user_password_hash(store, ctx, &user_id).await?;

        // Verify password
        if self.verify_password(password, &stored_hash)? {
            // Reset failed attempts and update last login
            self.reset_failed_attempts(store, ctx, &user_id).await?;
            self.update_last_login(store, ctx, &user_id).await?;
            Ok(user_id)
        } else {
            // Increment failed attempts
            self.increment_failed_attempts(store, ctx, &user_id).await?;
            Err(Error::InvalidCredentials)
        }
    }

    /// Change a user's password
    pub async fn change_password(
        &self,
        store: &mut StoreInterface,
        ctx: &Context,
        user_id: &EntityId,
        new_password: &str,
    ) -> Result<()> {
        // Validate new password
        self.validate_password(new_password)?;

        // Hash the new password
        let password_hash = self.hash_password(new_password)?;

        let mut requests = vec![
            swrite!(user_id.clone(), ft::password(), sstr!(password_hash))
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        Ok(())
    }

    // Private helper methods
    async fn find_user_by_name(&self, store: &mut StoreInterface, ctx: &Context, name: &str) -> Result<Option<EntityId>> {
        // Use the store's find_entities method to search for users with matching name
        let entities = store
            .find_entities(ctx, &et::user())
            .await?;

        for entity_id in entities {
            let mut requests = vec![
                sread!(entity_id.clone(), ft::name()),
            ];

            store
                .perform(ctx, &mut requests)
                .await?;

            if let Some(request) = requests.first() {
                if let Request::Read { value: Some(Value::String(stored_name)), .. } = request {
                    if stored_name.eq_ignore_ascii_case(name) {
                        return Ok(Some(entity_id));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn is_user_active(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<bool> {
        let mut requests = vec![
            sread!(user_id.clone(), ft::active()),
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        if let Some(request) = requests.first() {
            if let Request::Read { value: Some(Value::Bool(active)), .. } = request {
                Ok(*active)
            } else {
                Ok(false) // Default to inactive if field not found
            }
        } else {
            Ok(false)
        }
    }

    async fn is_user_locked(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<bool> {
        let mut requests = vec![
            sread!(user_id.clone(), ft::locked_until()),
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        if let Some(request) = requests.first() {
            if let Request::Read { value: Some(Value::Timestamp(locked_until)), .. } = request {
                return Ok(now() < *locked_until);
            }
        }

        Ok(false)
    }

    async fn get_user_password_hash(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<String> {
        let mut requests = vec![
            sread!(user_id.clone(), ft::password()),
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        if let Some(request) = requests.first() {
            if let Request::Read { value: Some(Value::String(hash)), .. } = request {
                Ok(hash.clone())
            } else {
                Err(Error::UserNotFound)
            }
        } else {
            Err(Error::UserNotFound)
        }
    }

    fn hash_password(&self, password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = self.argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| Error::PasswordHashError(e.to_string()))?;
        Ok(password_hash.to_string())
    }

    fn verify_password(&self, password: &str, hash: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| Error::PasswordHashError(e.to_string()))?;

        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    fn validate_password(&self, password: &str) -> Result<()> {
        if password.len() < self.config.min_password_length {
            return Err(Error::InvalidPassword(format!(
                "Password must be at least {} characters long",
                self.config.min_password_length
            )));
        }

        if self.config.require_password_complexity {
            let has_upper = password.chars().any(|c| c.is_ascii_uppercase());
            let has_lower = password.chars().any(|c| c.is_ascii_lowercase());
            let has_digit = password.chars().any(|c| c.is_ascii_digit());
            let has_symbol = password.chars().any(|c| !c.is_alphanumeric());

            if !has_upper || !has_lower || !has_digit || !has_symbol {
                return Err(Error::InvalidPassword(
                    "Password must contain uppercase, lowercase, digit, and symbol characters"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn increment_failed_attempts(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<()> {
        // Get current failed attempts
        let mut read_requests = vec![sread!(user_id.clone(), ft::failed_attempts())];

        store
            .perform(ctx, &mut read_requests)
            .await?;

        let current_attempts = if let Some(request) = read_requests.first() {
            if let Request::Read { value: Some(Value::Int(attempts)), .. } = request {
                *attempts
            } else {
                0
            }
        } else {
            0
        };

        let new_attempts = current_attempts + 1;

        // Update failed attempts
        let mut write_requests = vec![
            swrite!(user_id.clone(), ft::failed_attempts(), sint!(new_attempts)),
        ];

        // Lock account if max attempts reached
        if new_attempts >= self.config.max_failed_attempts {
            let lock_until = now() + self.config.lockout_duration;
            write_requests.push(
                swrite!(user_id.clone(), ft::locked_until(), stimestamp!(lock_until)),
            );
        }

        store
            .perform(ctx, &mut write_requests)
            .await?;

        Ok(())
    }

    async fn reset_failed_attempts(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<()> {
        let mut requests = vec![
            swrite!(user_id.clone(), ft::failed_attempts(), sint!(0)),
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        Ok(())
    }

    async fn update_last_login(&self, store: &mut StoreInterface, ctx: &Context, user_id: &EntityId) -> Result<()> {
        let mut requests = vec![
            swrite!(user_id.clone(), ft::last_login(), stimestamp!(now())),
        ];

        store
            .perform(ctx, &mut requests)
            .await?;

        Ok(())
    }
}