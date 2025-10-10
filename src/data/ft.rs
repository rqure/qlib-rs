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
pub const FAIL_OVER: &str = "FailOver";
pub const FAIL_OVER_GRACE_PERIOD: &str = "FailOverGracePeriod";
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

#[derive(Clone)]
pub struct FT {
    pub active: Option<FieldType>,
    pub auth_method: Option<FieldType>,
    pub available_list: Option<FieldType>,
    pub candidate_list: Option<FieldType>,
    pub children: Option<FieldType>,
    pub condition: Option<FieldType>,
    pub current_leader: Option<FieldType>,
    pub death_detection_timeout: Option<FieldType>,
    pub description: Option<FieldType>,
    pub fail_over: Option<FieldType>,
    pub fail_over_grace_period: Option<FieldType>,
    pub failed_attempts: Option<FieldType>,
    pub heartbeat: Option<FieldType>,
    pub last_login: Option<FieldType>,
    pub locked_until: Option<FieldType>,
    pub make_me: Option<FieldType>,
    pub name: Option<FieldType>,
    pub parent: Option<FieldType>,
    pub password: Option<FieldType>,
    pub resource_field: Option<FieldType>,
    pub resource_type: Option<FieldType>,
    pub scope: Option<FieldType>,
    pub secret: Option<FieldType>,
    pub start_time: Option<FieldType>,
    pub status: Option<FieldType>,
    pub sync_status: Option<FieldType>,
}

impl FT {
    pub fn new(store: &impl StoreTrait) -> Self {
        FT {
            active: store.get_field_type(ACTIVE).ok(),
            auth_method: store.get_field_type(AUTH_METHOD).ok(),
            available_list: store.get_field_type(AVAILABLE_LIST).ok(),
            candidate_list: store.get_field_type(CANDIDATE_LIST).ok(),
            children: store.get_field_type(CHILDREN).ok(),
            condition: store.get_field_type(CONDITION).ok(),
            current_leader: store.get_field_type(CURRENT_LEADER).ok(),
            death_detection_timeout: store.get_field_type(DEATH_DETECTION_TIMEOUT).ok(),
            description: store.get_field_type(DESCRIPTION).ok(),
            fail_over: store.get_field_type(FAIL_OVER).ok(),
            fail_over_grace_period: store.get_field_type(FAIL_OVER_GRACE_PERIOD).ok(),
            failed_attempts: store.get_field_type(FAILED_ATTEMPTS).ok(),
            heartbeat: store.get_field_type(HEARTBEAT).ok(),
            last_login: store.get_field_type(LAST_LOGIN).ok(),
            locked_until: store.get_field_type(LOCKED_UNTIL).ok(),
            make_me: store.get_field_type(MAKE_ME).ok(),
            name: store.get_field_type(NAME).ok(),
            parent: store.get_field_type(PARENT).ok(),
            password: store.get_field_type(PASSWORD).ok(),
            resource_field: store.get_field_type(RESOURCE_FIELD).ok(),
            resource_type: store.get_field_type(RESOURCE_TYPE).ok(),
            scope: store.get_field_type(SCOPE).ok(),
            secret: store.get_field_type(SECRET).ok(),
            start_time: store.get_field_type(START_TIME).ok(),
            status: store.get_field_type(STATUS).ok(),
            sync_status: store.get_field_type(SYNC_STATUS).ok(),
        }
    }
}