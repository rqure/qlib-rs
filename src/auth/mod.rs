mod authentication;
mod authorization;
mod error;
mod security;

pub use authentication::{AuthenticationManager, AuthConfig};
pub use error::{AuthError, AuthResult};
pub use security::{
    SecurityContext, JwtClaims, JwtManager
};

use crate::{FieldType, EntityType};

/// Constants for authentication-related field types
pub const PASSWORD_FIELD: &str = "Password";
pub const NAME_FIELD: &str = "Name"; // Inherited from Object
pub const ACTIVE_FIELD: &str = "Active";
pub const LAST_LOGIN_FIELD: &str = "LastLogin";
pub const CREATED_AT_FIELD: &str = "CreatedAt";
pub const FAILED_ATTEMPTS_FIELD: &str = "FailedAttempts";
pub const LOCKED_UNTIL_FIELD: &str = "LockedUntil";

/// Constants for authorization-related field types
pub const TEST_FN_FIELD: &str = "TestFn";
pub const SCOPE_FIELD: &str = "Scope";
pub const RESOURCE_TYPE_FIELD: &str = "ResourceType";
pub const RESOURCE_FIELD_FIELD: &str = "ResourceField";
pub const PERMISSION_FIELD: &str = "Permission";
pub const PERMISSION_TEST_FN_FIELD: &str = "Permission->TestFn";

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

/// Helper function to get the Subject entity type
pub fn subject_entity_type() -> EntityType {
    EntityType::from("Subject")
}

/// Helper function to get the Permission entity type
pub fn permission_entity_type() -> EntityType {
    EntityType::from("Permission")
}

/// Helper function to get the AuthorizationRule entity type
pub fn authorization_rule_entity_type() -> EntityType {
    EntityType::from("AuthorizationRule")
}

/// Helper function to get the TestFn field type
pub fn test_fn_field() -> FieldType {
    FieldType::from(TEST_FN_FIELD)
}

/// Helper function to get the Scope field type
pub fn scope_field() -> FieldType {
    FieldType::from(SCOPE_FIELD)
}

/// Helper function to get the ResourceType field type
pub fn resource_type_field() -> FieldType {
    FieldType::from(RESOURCE_TYPE_FIELD)
}

/// Helper function to get the ResourceField field type
pub fn resource_field_field() -> FieldType {
    FieldType::from(RESOURCE_FIELD_FIELD)
}

/// Helper function to get the Permission field type (for authorization rules)
pub fn permission_field() -> FieldType {
    FieldType::from(PERMISSION_FIELD)
}

pub fn permission_test_fn_field() -> FieldType {
    FieldType::from(PERMISSION_TEST_FN_FIELD)
}