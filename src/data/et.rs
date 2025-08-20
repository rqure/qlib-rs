use crate::EntityType;

pub const OBJECT: &str = "Object";
pub const USER: &str = "User";
pub const SUBJECT: &str = "Subject";
pub const PERMISSION: &str = "Permission";
pub const AUTHORIZATION_RULE: &str = "AuthorizationRule";
pub const FAULT_TOLERANCE: &str = "FaultTolerance";
pub const SERVICE: &str = "Service";
pub const MACHINE: &str = "Machine";

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

pub fn fault_tolerance() -> EntityType {
    EntityType::from(FAULT_TOLERANCE)
}

pub fn service() -> EntityType {
    EntityType::from(SERVICE)
}

pub fn machine() -> EntityType {
    EntityType::from(MACHINE)
}
