use crate::FieldType;

pub const ACTIVE: &str = "Active";
pub const AUTH_METHOD: &str = "AuthMethod";
pub const CHILDREN: &str = "Children";
pub const DESCRIPTION: &str = "Description";
pub const FAILED_ATTEMPTS: &str = "FailedAttempts";
pub const LAST_LOGIN: &str = "LastLogin";
pub const LOCKED_UNTIL: &str = "LockedUntil";
pub const NAME: &str = "Name";
pub const PARENT: &str = "Parent";
pub const PASSWORD: &str = "Password";
pub const PERMISSION: &str = "Permission";
pub const RESOURCE_FIELD: &str = "ResourceField";
pub const RESOURCE_TYPE: &str = "ResourceType";
pub const SCOPE: &str = "Scope";
pub const SECRET: &str = "Secret";
pub const TEST_FN: &str = "TestFn";
pub const CANDIDATE_LIST: &str = "CandidateList";
pub const AVAILABLE_LIST: &str = "AvailableList";
pub const SYNC_STATUS: &str = "SyncStatus";
pub const CURRENT_LEADER: &str = "CurrentLeader";
pub const STATUS: &str = "Status";
pub const START_TIME: &str = "StartTime";
pub const MAKE_ME_AVAILABLE: &str = "MakeMeAvailable";
pub const MAKE_ME_UNAVAILABLE: &str = "MakeMeUnavailable";
pub const HEARTBEAT: &str = "Heartbeat";
pub const DEATH_DETECTION_TIMEOUT: &str = "DeathDetectionTimeout";

pub fn children() -> FieldType {
    FieldType::from(CHILDREN)
}

pub fn parent() -> FieldType {
    FieldType::from(PARENT)
}

pub fn description() -> FieldType {
    FieldType::from(DESCRIPTION)
}

pub fn password() -> FieldType {
    FieldType::from(PASSWORD)
}

pub fn secret() -> FieldType {
    FieldType::from(SECRET)
}

pub fn auth_method() -> FieldType {
    FieldType::from(AUTH_METHOD)
}

pub fn name() -> FieldType {
    FieldType::from(NAME)
}

pub fn active() -> FieldType {
    FieldType::from(ACTIVE)
}

pub fn last_login() -> FieldType {
    FieldType::from(LAST_LOGIN)
}

pub fn failed_attempts() -> FieldType {
    FieldType::from(FAILED_ATTEMPTS)
}

pub fn locked_until() -> FieldType {
    FieldType::from(LOCKED_UNTIL)
}

pub fn test_fn() -> FieldType {
    FieldType::from(TEST_FN)
}

pub fn scope() -> FieldType {
    FieldType::from(SCOPE)
}

pub fn resource_type() -> FieldType {
    FieldType::from(RESOURCE_TYPE)
}

pub fn resource_field() -> FieldType {
    FieldType::from(RESOURCE_FIELD)
}

pub fn permission() -> FieldType {
    FieldType::from(PERMISSION)
}

pub fn candidate_list() -> FieldType {
    FieldType::from(CANDIDATE_LIST)
}

pub fn available_list() -> FieldType {
    FieldType::from(AVAILABLE_LIST)
}

pub fn sync_status() -> FieldType {
    FieldType::from(SYNC_STATUS)
}

pub fn current_leader() -> FieldType {
    FieldType::from(CURRENT_LEADER)
}

pub fn status() -> FieldType {
    FieldType::from(STATUS)
}

pub fn start_time() -> FieldType {
    FieldType::from(START_TIME)
}

pub fn make_me_available() -> FieldType {
    FieldType::from(MAKE_ME_AVAILABLE)
}

pub fn make_me_unavailable() -> FieldType {
    FieldType::from(MAKE_ME_UNAVAILABLE)
}

pub fn heartbeat() -> FieldType {
    FieldType::from(HEARTBEAT)
}

pub fn death_detection_timeout() -> FieldType {
    FieldType::from(DEATH_DETECTION_TIMEOUT)
}
