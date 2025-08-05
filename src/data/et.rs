use crate::EntityType;

pub const OBJECT: &str = "Object";
pub const USER: &str = "User";
pub const SUBJECT: &str = "Subject";
pub const PERMISSION: &str = "Permission";
pub const AUTHORIZATION_RULE: &str = "AuthorizationRule";

pub fn object() -> EntityType {
    EntityType::from(OBJECT)
}

pub fn user() -> EntityType {
    EntityType::from(USER)
}

pub fn subject() -> EntityType {
    EntityType::from(SUBJECT)
}

pub fn permission() -> EntityType {
    EntityType::from(PERMISSION)
}

pub fn authorization_rule() -> EntityType {
    EntityType::from(AUTHORIZATION_RULE)
}