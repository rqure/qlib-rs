use crate::{EntityType, StoreTrait};

pub const FAULT_TOLERANCE: &str = "FaultTolerance";
pub const FOLDER: &str = "Folder";
pub const MACHINE: &str = "Machine";
pub const OBJECT: &str = "Object";
pub const PERMISSION: &str = "Permission";
pub const ROOT: &str = "Root";
pub const SERVICE: &str = "Service";
pub const SUBJECT: &str = "Subject";
pub const USER: &str = "User";
pub const CANDIDATE: &str = "Candidate";

pub struct ET {
    pub fault_tolerance: EntityType,
    pub folder: EntityType,
    pub machine: EntityType,
    pub object: EntityType,
    pub permission: EntityType,
    pub root: EntityType,
    pub service: EntityType,
    pub subject: EntityType,
    pub user: EntityType,
    pub candidate: EntityType,
}

impl ET {
    pub fn new(store: &impl StoreTrait) -> Self {
        ET {
            fault_tolerance: store.get_entity_type(FAULT_TOLERANCE).unwrap(),
            folder: store.get_entity_type(FOLDER).unwrap(),
            machine: store.get_entity_type(MACHINE).unwrap(),
            object: store.get_entity_type(OBJECT).unwrap(),
            permission: store.get_entity_type(PERMISSION).unwrap(),
            root: store.get_entity_type(ROOT).unwrap(),
            service: store.get_entity_type(SERVICE).unwrap(),
            subject: store.get_entity_type(SUBJECT).unwrap(),
            user: store.get_entity_type(USER).unwrap(),
            candidate: store.get_entity_type(CANDIDATE).unwrap(),
        }
    }
}