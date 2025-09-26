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
    pub fault_tolerance: Option<EntityType>,
    pub folder: Option<EntityType>,
    pub machine: Option<EntityType>,
    pub object: Option<EntityType>,
    pub permission: Option<EntityType>,
    pub root: Option<EntityType>,
    pub service: Option<EntityType>,
    pub subject: Option<EntityType>,
    pub user: Option<EntityType>,
    pub candidate: Option<EntityType>,
}

impl ET {
    pub fn new(store: &impl StoreTrait) -> Self {
        ET {
            fault_tolerance: store.get_entity_type(FAULT_TOLERANCE).ok(),
            folder: store.get_entity_type(FOLDER).ok(),
            machine: store.get_entity_type(MACHINE).ok(),
            object: store.get_entity_type(OBJECT).ok(),
            permission: store.get_entity_type(PERMISSION).ok(),
            root: store.get_entity_type(ROOT).ok(),
            service: store.get_entity_type(SERVICE).ok(),
            subject: store.get_entity_type(SUBJECT).ok(),
            user: store.get_entity_type(USER).ok(),
            candidate: store.get_entity_type(CANDIDATE).ok(),
        }
    }
}
