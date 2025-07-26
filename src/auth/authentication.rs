use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use std::time::{Duration, SystemTime};

use crate::{
    auth::{
        error::{AuthError, AuthResult},
        *,
    },
    Context, EntityId, EntitySchema, FieldSchema, Single, Store, Value,
    now, AdjustBehavior, PushCondition, Request,
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

    /// Initialize the User entity schema with authentication fields
    pub fn initialize_user_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        // Check if User schema already exists
        if store.get_entity_schema(ctx, &user_entity_type()).is_ok() {
            return Ok(()); // Schema already exists
        }

        // First ensure Object schema exists
        self.initialize_object_schema(store, ctx)?;

        // Create User schema that inherits from Object
        let mut user_schema = EntitySchema::<Single>::new(user_entity_type(), Some(EntityType::from("Object")));

        // Add authentication fields (Note: Name field is inherited from Object)
        user_schema.fields.insert(
            password_field(),
            FieldSchema::String {
                field_type: password_field(),
                default_value: String::new(),
                rank: 0,
            },
        );

        user_schema.fields.insert(
            active_field(),
            FieldSchema::Bool {
                field_type: active_field(),
                default_value: true,
                rank: 1,
            },
        );

        user_schema.fields.insert(
            last_login_field(),
            FieldSchema::Timestamp {
                field_type: last_login_field(),
                default_value: SystemTime::UNIX_EPOCH,
                rank: 2,
            },
        );

        user_schema.fields.insert(
            created_at_field(),
            FieldSchema::Timestamp {
                field_type: created_at_field(),
                default_value: now(),
                rank: 3,
            },
        );

        user_schema.fields.insert(
            failed_attempts_field(),
            FieldSchema::Int {
                field_type: failed_attempts_field(),
                default_value: 0,
                rank: 4,
            },
        );

        user_schema.fields.insert(
            locked_until_field(),
            FieldSchema::Timestamp {
                field_type: locked_until_field(),
                default_value: SystemTime::UNIX_EPOCH,
                rank: 5,
            },
        );

        store
            .set_entity_schema(ctx, &user_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Initialize the Object entity schema that serves as the base for User
    fn initialize_object_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        // Check if Object schema already exists
        if store.get_entity_schema(ctx, &EntityType::from("Object")).is_ok() {
            return Ok(()); // Schema already exists
        }

        let mut object_schema = EntitySchema::<Single>::new(EntityType::from("Object"), None);

        // Add base Object fields
        object_schema.fields.insert(
            name_field(),
            FieldSchema::String {
                field_type: name_field(),
                default_value: String::new(),
                rank: 0,
            },
        );

        object_schema.fields.insert(
            FieldType::from("Parent"),
            FieldSchema::EntityReference {
                field_type: FieldType::from("Parent"),
                default_value: None,
                rank: 1,
            },
        );

        object_schema.fields.insert(
            FieldType::from("Children"),
            FieldSchema::EntityList {
                field_type: FieldType::from("Children"),
                default_value: Vec::new(),
                rank: 2,
            },
        );

        store
            .set_entity_schema(ctx, &object_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Create a new user with name and password
    pub fn create_user(&self, store: &mut Store, ctx: &Context, name: &str, password: &str) -> AuthResult<EntityId> {
        // Validate name
        if name.trim().is_empty() {
            return Err(AuthError::InvalidName);
        }

        // Validate password
        self.validate_password(password)?;

        // Check if user already exists
        if self.find_user_by_name(store, ctx, name)?.is_some() {
            return Err(AuthError::UserAlreadyExists);
        }

        // Hash the password
        let password_hash = self.hash_password(password)?;

        // Create the user entity (the name will be set as the Object's Name field)
        let user = store
            .create_entity(ctx, &user_entity_type(), None, name)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        // Set user fields
        let mut requests = vec![
            Request::Write {
                entity_id: user.entity_id.clone(),
                field_type: password_field(),
                value: Some(Value::String(password_hash)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: user.entity_id.clone(),
                field_type: active_field(),
                value: Some(Value::Bool(true)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: user.entity_id.clone(),
                field_type: created_at_field(),
                value: Some(Value::Timestamp(now())),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: user.entity_id.clone(),
                field_type: failed_attempts_field(),
                value: Some(Value::Int(0)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
        ];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(user.entity_id)
    }

    /// Authenticate a user with name and password
    pub fn authenticate(&self, store: &mut Store, ctx: &Context, name: &str, password: &str) -> AuthResult<EntityId> {
        let user_id = self
            .find_user_by_name(store, ctx, name)?
            .ok_or(AuthError::UserNotFound)?;

        // Check if account is active
        if !self.is_user_active(store, ctx, &user_id)? {
            return Err(AuthError::AccountDisabled);
        }

        // Check if account is locked
        if self.is_user_locked(store, ctx, &user_id)? {
            return Err(AuthError::AccountLocked);
        }

        // Get stored password hash
        let stored_hash = self.get_user_password_hash(store, ctx, &user_id)?;

        // Verify password
        if self.verify_password(password, &stored_hash)? {
            // Reset failed attempts and update last login
            self.reset_failed_attempts(store, ctx, &user_id)?;
            self.update_last_login(store, ctx, &user_id)?;
            Ok(user_id)
        } else {
            // Increment failed attempts
            self.increment_failed_attempts(store, ctx, &user_id)?;
            Err(AuthError::InvalidCredentials)
        }
    }

    /// Change a user's password
    pub fn change_password(
        &self,
        store: &mut Store,
        ctx: &Context,
        user_id: &EntityId,
        new_password: &str,
    ) -> AuthResult<()> {
        // Validate new password
        self.validate_password(new_password)?;

        // Hash the new password
        let password_hash = self.hash_password(new_password)?;

        let mut requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: password_field(),
            value: Some(Value::String(password_hash)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Disable a user account
    pub fn disable_user(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        let mut requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: active_field(),
            value: Some(Value::Bool(false)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Enable a user account
    pub fn enable_user(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        let mut requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: active_field(),
            value: Some(Value::Bool(true)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Unlock a user account manually
    pub fn unlock_user(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        let mut requests = vec![
            Request::Write {
                entity_id: user_id.clone(),
                field_type: failed_attempts_field(),
                value: Some(Value::Int(0)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: user_id.clone(),
                field_type: locked_until_field(),
                value: Some(Value::Timestamp(SystemTime::UNIX_EPOCH)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
        ];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    // Private helper methods

    fn find_user_by_name(&self, store: &mut Store, ctx: &Context, name: &str) -> AuthResult<Option<EntityId>> {
        // Use the store's find_entities method to search for users with matching name
        let entities = store
            .find_entities(ctx, &user_entity_type(), None)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        for entity_id in entities.items {
            // Read the name field for this entity (from Object)
            let mut requests = vec![Request::Read {
                entity_id: entity_id.clone(),
                field_type: name_field(),
                value: None,
                write_time: None,
                writer_id: None,
            }];

            store
                .perform(ctx, &mut requests)
                .map_err(|e| AuthError::StoreError(e.to_string()))?;

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

    fn is_user_active(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<bool> {
        let mut requests = vec![Request::Read {
            entity_id: user_id.clone(),
            field_type: active_field(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

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

    fn is_user_locked(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<bool> {
        let mut requests = vec![Request::Read {
            entity_id: user_id.clone(),
            field_type: locked_until_field(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        if let Some(request) = requests.first() {
            if let Request::Read { value: Some(Value::Timestamp(locked_until)), .. } = request {
                return Ok(now() < *locked_until);
            }
        }

        Ok(false)
    }

    fn get_user_password_hash(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<String> {
        let mut requests = vec![Request::Read {
            entity_id: user_id.clone(),
            field_type: password_field(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        if let Some(request) = requests.first() {
            if let Request::Read { value: Some(Value::String(hash)), .. } = request {
                Ok(hash.clone())
            } else {
                Err(AuthError::UserNotFound)
            }
        } else {
            Err(AuthError::UserNotFound)
        }
    }

    fn hash_password(&self, password: &str) -> AuthResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = self.argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;
        Ok(password_hash.to_string())
    }

    fn verify_password(&self, password: &str, hash: &str) -> AuthResult<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;

        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    fn validate_password(&self, password: &str) -> AuthResult<()> {
        if password.len() < self.config.min_password_length {
            return Err(AuthError::InvalidPassword(format!(
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
                return Err(AuthError::InvalidPassword(
                    "Password must contain uppercase, lowercase, digit, and symbol characters"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }

    fn increment_failed_attempts(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        // Get current failed attempts
        let mut read_requests = vec![Request::Read {
            entity_id: user_id.clone(),
            field_type: failed_attempts_field(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut read_requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

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
        let mut write_requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: failed_attempts_field(),
            value: Some(Value::Int(new_attempts)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        // Lock account if max attempts reached
        if new_attempts >= self.config.max_failed_attempts {
            let lock_until = now() + self.config.lockout_duration;
            write_requests.push(Request::Write {
                entity_id: user_id.clone(),
                field_type: locked_until_field(),
                value: Some(Value::Timestamp(lock_until)),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            });
        }

        store
            .perform(ctx, &mut write_requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    fn reset_failed_attempts(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        let mut requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: failed_attempts_field(),
            value: Some(Value::Int(0)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    fn update_last_login(&self, store: &mut Store, ctx: &Context, user_id: &EntityId) -> AuthResult<()> {
        let mut requests = vec![Request::Write {
            entity_id: user_id.clone(),
            field_type: last_login_field(),
            value: Some(Value::Timestamp(now())),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }
}
