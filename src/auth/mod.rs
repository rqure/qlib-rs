mod authentication_manager;
mod error;

pub use authentication_manager::{AuthenticationManager, AuthConfig};
pub use error::{AuthError, AuthResult};

use crate::{FieldType, EntityType};

/// Constants for authentication-related field types
pub const PASSWORD_FIELD: &str = "Password";
pub const NAME_FIELD: &str = "Name"; // Inherited from Object
pub const ACTIVE_FIELD: &str = "Active";
pub const LAST_LOGIN_FIELD: &str = "LastLogin";
pub const CREATED_AT_FIELD: &str = "CreatedAt";
pub const FAILED_ATTEMPTS_FIELD: &str = "FailedAttempts";
pub const LOCKED_UNTIL_FIELD: &str = "LockedUntil";

/// Helper functions to get field types
pub fn password_field() -> FieldType {
    FieldType::from(PASSWORD_FIELD)
}

pub fn name_field() -> FieldType {
    FieldType::from(NAME_FIELD)
}

pub fn active_field() -> FieldType {
    FieldType::from(ACTIVE_FIELD)
}

pub fn last_login_field() -> FieldType {
    FieldType::from(LAST_LOGIN_FIELD)
}

pub fn created_at_field() -> FieldType {
    FieldType::from(CREATED_AT_FIELD)
}

pub fn failed_attempts_field() -> FieldType {
    FieldType::from(FAILED_ATTEMPTS_FIELD)
}

pub fn locked_until_field() -> FieldType {
    FieldType::from(LOCKED_UNTIL_FIELD)
}

/// Helper function to get the User entity type
pub fn user_entity_type() -> EntityType {
    EntityType::from("User")
}
