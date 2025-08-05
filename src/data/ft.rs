use crate::FieldType;

pub const ACTIVE: &str = "Active";
pub const CHILDREN: &str = "Children";
pub const FAILED_ATTEMPTS: &str = "FailedAttempts";
pub const LAST_LOGIN: &str = "LastLogin";
pub const LOCKED_UNTIL: &str = "LockedUntil";
pub const NAME: &str = "Name";
pub const PARENT: &str = "Parent";
pub const PASSWORD: &str = "Password";
pub const PERMISSION_TEST_FN: &str = "Permission->TestFn";
pub const PERMISSION: &str = "Permission";
pub const RESOURCE_FIELD: &str = "ResourceField";
pub const RESOURCE_TYPE: &str = "ResourceType";
pub const SCOPE: &str = "Scope";
pub const TEST_FN: &str = "TestFn";

pub fn children() -> FieldType {
    FieldType::from(CHILDREN)
}

pub fn parent() -> FieldType {
    FieldType::from(PARENT)
}

pub fn password() -> FieldType {
    FieldType::from(PASSWORD)
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

pub fn permission_test_fn() -> FieldType {
    FieldType::from(PERMISSION_TEST_FN)
}