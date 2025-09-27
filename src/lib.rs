pub mod data;
pub mod auth;
pub mod protocol;
mod test;
pub mod expr;

pub use data::{
    BadIndirectionReason, Store, PageOpts,
    PageResult, NotificationQueue, hash_notify_config, Snapshot, EntityId, EntitySchema, Single, Complete, 
    Field, FieldSchema, AdjustBehavior, PushCondition, Request, Requests,
    StoreProxy, AsyncStoreProxy, StoreMessage, extract_message_id, Value, INDIRECTION_DELIMITER, NotifyConfig, Notification, NotifyInfo,
    JsonSnapshot, JsonEntitySchema, JsonEntity, value_to_json_value, json_value_to_value, value_to_json_value_with_paths, build_json_entity_tree, take_json_snapshot, restore_json_snapshot,
    restore_entity_recursive, factory_restore_json_snapshot, restore_json_snapshot_via_proxy,
    EntityType, FieldType, Timestamp, now, epoch, nanos_to_timestamp, secs_to_timestamp, 
    millis_to_timestamp, micros_to_timestamp, ft, et, Cache, path, path_to_entity_id,
    StoreTrait, from_base64, to_base64, IndirectFieldType
};

pub use auth::{
    AuthConfig, AuthMethod,
    authenticate_user, find_user_by_name, create_user, set_user_password,
    change_password, validate_password, hash_password, verify_password,
};

pub use protocol::{
    MessageHeader, MessageType, ProtocolMessage, ProtocolCodec, MessageBuffer, 
    encode_store_message,
};

pub use expr::CelExecutor;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Error {
    // Store related errors
    BadIndirection(EntityId, Vec<FieldType>, BadIndirectionReason),
    EntityAlreadyExists(EntityId),
    EntityNotFound(EntityId),
    EntityNameNotFound(String),
    EntityTypeNotFound(EntityType),
    EntityTypeStrNotFound(String),
    CacheFieldNotFound(FieldType),
    FieldTypeNotFound(EntityId, FieldType),
    FieldTypeStrNotFound(String),
    InvalidFieldType(String),
    InvalidFieldValue(String),
    InvalidNotifyConfig(String),
    UnsupportedAdjustBehavior(EntityId, FieldType, AdjustBehavior),
    ValueTypeMismatch(EntityId, FieldType, Value, Value),
    BadValueCast(Value, Value),
    InvalidRequest(String),

    // Auth related errors
    InvalidCredentials,
    AccountDisabled,
    AccountLocked,
    SubjectNotFound,
    PasswordHashError(String),
    InvalidName,
    InvalidPassword(String),
    SubjectAlreadyExists,
    InvalidAuthenticationMethod,
    AuthenticationMethodNotImplemented(String),

    // StoreProxy related errors
    StoreProxyError(String),

    // Scripting related errors
    ExecutionError(String),
}
impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::BadIndirection(id, field, reason) => write!(f, "Bad indirection for {:?}.{:?}: {}", id, field, reason),
            Error::EntityAlreadyExists(id) => write!(f, "Entity already exists: {:?}", id),
            Error::EntityNotFound(id) => write!(f, "Entity not found: {:?}", id),
            Error::EntityNameNotFound(name) => write!(f, "Entity name not found: {}", name),
            Error::EntityTypeNotFound(et) => write!(f, "Entity type not found: {:?}", et),
            Error::EntityTypeStrNotFound(et) => write!(f, "Entity type not found: {}", et),
            Error::CacheFieldNotFound(field) => write!(f, "Cache field not found: {:?}", field),
            Error::FieldTypeNotFound(id, field) => write!(f, "Field not found for {:?}: {:?}", id, field),
            Error::FieldTypeStrNotFound(field) => write!(f, "Field not found: {}", field),
            Error::InvalidFieldType(msg) => write!(f, "Invalid field type: {}", msg),
            Error::InvalidFieldValue(msg) => write!(f, "Invalid field value: {}", msg),
            Error::InvalidNotifyConfig(msg) => write!(f, "Invalid notification config: {}", msg),
            Error::UnsupportedAdjustBehavior(id, field, behavior) => write!(f, "Unsupported adjust behavior {:?} for {:?}.{:?}", behavior, id, field),
            Error::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
            Error::ValueTypeMismatch(id, field, got, expected) => write!(f, "Value type mismatch for {:?}.{:?}: got value type {:?}, expected value type {:?}", id, field, got, expected),
            Error::BadValueCast(got, expected) => write!(f, "Bad value cast: got value type {:?}, expected value type {:?}", got, expected),
            Error::InvalidCredentials => write!(f, "Invalid credentials"),
            Error::AccountDisabled => write!(f, "Account is disabled"),
            Error::AccountLocked => write!(f, "Account is locked due to too many failed attempts"),
            Error::SubjectNotFound => write!(f, "User not found"),
            Error::PasswordHashError(msg) => write!(f, "Password hashing error: {}", msg),
            Error::InvalidName => write!(f, "Invalid name format"),
            Error::InvalidPassword(msg) => write!(f, "Invalid password: {}", msg),
            Error::SubjectAlreadyExists => write!(f, "User already exists"),
            Error::InvalidAuthenticationMethod => write!(f, "Invalid authentication method for this operation"),
            Error::AuthenticationMethodNotImplemented(method) => write!(f, "Authentication method '{}' is not implemented", method),
            Error::StoreProxyError(msg) => write!(f, "Store proxy error: {}", msg),
            Error::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
        }
    }
}

/// Creates a SmallVec of FieldType for use in read/write requests.
///
/// This macro creates a `IndirectFieldType` that can be used with
/// the `sread!` and `swrite!` macros. It functions like `vec!` but creates
/// a SmallVec instead for better performance with small field lists.
///
/// # Arguments
///
/// * Elements can be provided as comma-separated FieldType values
///
/// # Returns
///
/// * `IndirectFieldType` - A SmallVec containing the field types
#[macro_export]
macro_rules! sfield {
    ($($x:expr),* $(,)?) => {
        {
            use smallvec::smallvec;
            smallvec![$($x),*]
        }
    };
}
