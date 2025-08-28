use crate::EntityType;

pub const FAULT_TOLERANCE: &str = "FaultTolerance";
pub const FOLDER: &str = "Folder";
pub const MACHINE: &str = "Machine";
pub const OBJECT: &str = "Object";
pub const PERMISSION: &str = "Permission";
pub const ROOT: &str = "Root";
pub const SERVICE: &str = "Service";
pub const SUBJECT: &str = "Subject";
pub const USER: &str = "User";
pub const PROGRAM: &str = "Program";

pub fn fault_tolerance() -> EntityType {
    EntityType::from(FAULT_TOLERANCE)
}

pub fn folder() -> EntityType {
    EntityType::from(FOLDER)
}

pub fn machine() -> EntityType {
    EntityType::from(MACHINE)
}

pub fn object() -> EntityType {
    EntityType::from(OBJECT)
}

pub fn permission() -> EntityType {
    EntityType::from(PERMISSION)
}

pub fn root() -> EntityType {
    EntityType::from(ROOT)
}

pub fn service() -> EntityType {
    EntityType::from(SERVICE)
}

pub fn subject() -> EntityType {
    EntityType::from(SUBJECT)
}

pub fn user() -> EntityType {
    EntityType::from(USER)
}

pub fn program() -> EntityType {
    EntityType::from(PROGRAM)
}