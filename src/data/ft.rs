use crate::{FieldType, StoreTrait};

pub const ACTIVE: &str = "Active";
pub const AUTH_METHOD: &str = "AuthMethod";
pub const AVAILABLE_LIST: &str = "AvailableList";
pub const CANDIDATE_LIST: &str = "CandidateList";
pub const CHILDREN: &str = "Children";
pub const CONDITION: &str = "Condition";
pub const CURRENT_LEADER: &str = "CurrentLeader";
pub const DEATH_DETECTION_TIMEOUT: &str = "DeathDetectionTimeout";
pub const DESCRIPTION: &str = "Description";
pub const FAILED_ATTEMPTS: &str = "FailedAttempts";
pub const HEARTBEAT: &str = "Heartbeat";
pub const LAST_LOGIN: &str = "LastLogin";
pub const LOCKED_UNTIL: &str = "LockedUntil";
pub const MAKE_ME: &str = "MakeMe";
pub const NAME: &str = "Name";
pub const PARENT: &str = "Parent";
pub const PASSWORD: &str = "Password";
pub const RESOURCE_FIELD: &str = "ResourceField";
pub const RESOURCE_TYPE: &str = "ResourceType";
pub const SCOPE: &str = "Scope";
pub const SECRET: &str = "Secret";
pub const START_TIME: &str = "StartTime";
pub const STATUS: &str = "Status";
pub const SYNC_STATUS: &str = "SyncStatus";

pub struct FT {
    pub active: FieldType,
    pub auth_method: FieldType,
    pub available_list: FieldType,
    pub candidate_list: FieldType,
    pub children: FieldType,
    pub condition: FieldType,
    pub current_leader: FieldType,
    pub death_detection_timeout: FieldType,
    pub description: FieldType,
    pub failed_attempts: FieldType,
    pub heartbeat: FieldType,
    pub last_login: FieldType,
    pub locked_until: FieldType,
    pub make_me: FieldType,
    pub name: FieldType,
    pub parent: FieldType,
    pub password: FieldType,
    pub resource_field: FieldType,
    pub resource_type: FieldType,
    pub scope: FieldType,
    pub secret: FieldType,
    pub start_time: FieldType,
    pub status: FieldType,
    pub sync_status: FieldType,
}

impl FT {
    pub fn new(store: &impl StoreTrait) -> Self {
        FT {
            active: store.get_field_type(ACTIVE).unwrap(),
            auth_method: store.get_field_type(AUTH_METHOD).unwrap(),
            available_list: store.get_field_type(AVAILABLE_LIST).unwrap(),
            candidate_list: store.get_field_type(CANDIDATE_LIST).unwrap(),
            children: store.get_field_type(CHILDREN).unwrap(),
            condition: store.get_field_type(CONDITION).unwrap(),
            current_leader: store.get_field_type(CURRENT_LEADER).unwrap(),
            death_detection_timeout: store.get_field_type(DEATH_DETECTION_TIMEOUT).unwrap(),
            description: store.get_field_type(DESCRIPTION).unwrap(),
            failed_attempts: store.get_field_type(FAILED_ATTEMPTS).unwrap(),
            heartbeat: store.get_field_type(HEARTBEAT).unwrap(),
            last_login: store.get_field_type(LAST_LOGIN).unwrap(),
            locked_until: store.get_field_type(LOCKED_UNTIL).unwrap(),
            make_me: store.get_field_type(MAKE_ME).unwrap(),
            name: store.get_field_type(NAME).unwrap(),
            parent: store.get_field_type(PARENT).unwrap(),
            password: store.get_field_type(PASSWORD).unwrap(),
            resource_field: store.get_field_type(RESOURCE_FIELD).unwrap(),
            resource_type: store.get_field_type(RESOURCE_TYPE).unwrap(),
            scope: store.get_field_type(SCOPE).unwrap(),
            secret: store.get_field_type(SECRET).unwrap(),
            start_time: store.get_field_type(START_TIME).unwrap(),
            status: store.get_field_type(STATUS).unwrap(),
            sync_status: store.get_field_type(SYNC_STATUS).unwrap(),
        }
    }
}