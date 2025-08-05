mod data;
mod auth;
mod test;

pub use data::{
    resolve_indirection, BadIndirectionReason, Context, Store, StoreTrait, PageOpts,
    PageResult, NotificationCallback, Snapshot, Entity, EntityId, EntitySchema, Single, Complete, 
    Field, FieldSchema, AdjustBehavior, PushCondition, Request, Snowflake, 
    StoreProxy, StoreMessage, Value, INDIRECTION_DELIMITER, NotifyConfig, Notification,
    EntityType, FieldType, Timestamp, now, epoch, nanos_to_timestamp, secs_to_timestamp, 
    millis_to_timestamp, micros_to_timestamp
};

pub use auth::{
    AuthenticationManager, AuthConfig, AuthError, AuthResult,
    SecurityContext, JwtClaims, JwtManager
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Error {
    // Store related errors
    BadIndirection(EntityId, FieldType, BadIndirectionReason),
    EntityExists(EntityId),
    EntityNotFound(EntityId),
    EntityTypeNotFound(EntityType),
    FieldNotFound(EntityId, FieldType),
    InvalidNotifyConfig(String),
    UnsupportedAdjustBehavior(EntityId, FieldType, AdjustBehavior),
    ValueTypeMismatch(EntityId, FieldType, Value, Value),

    // Auth related errors
    InvalidCredentials,
    AccountDisabled,
    AccountLocked,
    UserNotFound,
    PasswordHashError(String),
    InvalidName,
    InvalidPassword(String),
    UserAlreadyExists,
}
impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::BadIndirection(id, field, reason) => write!(f, "Bad indirection for {}.{}: {}", id, field, reason),
            Error::EntityExists(id) => write!(f, "Entity already exists: {}", id),
            Error::EntityNotFound(id) => write!(f, "Entity not found: {}", id),
            Error::EntityTypeNotFound(et) => write!(f, "Entity type not found: {}", et),
            Error::FieldNotFound(id, field) => write!(f, "Field not found for {}: {}", id, field),
            Error::InvalidNotifyConfig(msg) => write!(f, "Invalid notification config: {}", msg),
            Error::UnsupportedAdjustBehavior(id, field, behavior) => write!(f, "Unsupported adjust behavior {} for {}.{}", behavior, id, field),
            Error::ValueTypeMismatch(id, field, current_value, previous_value) => write!(f, "Value type mismatch for {}.{}: current value {:?}, previous value {:?}", id, field, current_value, previous_value),
            Error::InvalidCredentials => write!(f, "Invalid credentials"),
            Error::AccountDisabled => write!(f, "Account is disabled"),
            Error::AccountLocked => write!(f, "Account is locked due to too many failed attempts"),
            Error::UserNotFound => write!(f, "User not found"),
            Error::PasswordHashError(msg) => write!(f, "Password hashing error: {}", msg),
            Error::InvalidName => write!(f, "Invalid name format"),
            Error::InvalidPassword(msg) => write!(f, "Invalid password: {}", msg),
            Error::UserAlreadyExists => write!(f, "User already exists"),
        }
    }
}

/// Create a Read request with minimal syntax
///
/// # Arguments
///
/// * `entity_id` - The entity ID to read from
/// * `field_type` - The field type to read
#[macro_export]
macro_rules! sread {
    ($entity_id:expr, $field_type:expr) => {
        $crate::Request::Read {
            entity_id: $entity_id,
            field_type: $field_type,
            value: None,
            write_time: None,
            writer_id: None,
        }
    };
}

/// Create a Write request with minimal syntax
///
/// # Arguments
///
/// * `entity_id` - The entity ID to write to
/// * `field_type` - The field type to write
/// * `value` - The value to write (must be a Some(Value) or None)
/// * `push_condition` - (optional) The write option, defaults to Normal
/// * `write_time` - (optional) The write time
/// * `writer_id` - (optional) The writer ID
#[macro_export]
macro_rules! swrite {
    // Basic version with no value: handle Some/None
    ($entity_id:expr, $field_type:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: None,
            push_condition: $crate::PushCondition::Always,
            adjust_behavior: $crate::AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }
    };

    // Basic version with just value: handle Some/None
    ($entity_id:expr, $field_type:expr, $value:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $crate::PushCondition::Always,
            adjust_behavior: $crate::AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option and write time
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Set,
            write_time: $write_time,
            writer_id: None,
        }
    };

    // With write option, write time, and writer ID
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr, $writer_id:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Set,
            write_time: $write_time,
            writer_id: $writer_id,
        }
    };
}

/// Create a Write request with Add adjustment behavior
///
/// This macro creates a `Request::Write` with `AdjustBehavior::Add`, which is useful for
/// incrementing values, appending to lists, or concatenating strings.
///
/// # Arguments
///
/// * `entity_id` - The entity ID to write to
/// * `field_type` - The field type to write
/// * `value` - The value to add (must be a Some(Value) or None)
/// * `push_condition` - (optional) The write option, defaults to Always
/// * `write_time` - (optional) The write time
/// * `writer_id` - (optional) The writer ID
#[macro_export]
macro_rules! sadd {
    // Basic version with just value
    ($entity_id:expr, $field_type:expr, $value:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $crate::PushCondition::Always,
            adjust_behavior: $crate::AdjustBehavior::Add,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Add,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option and write time
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Add,
            write_time: $write_time,
            writer_id: None,
        }
    };

    // With write option, write time, and writer ID
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr, $writer_id:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Add,
            write_time: $write_time,
            writer_id: $writer_id,
        }
    };
}

/// Create a Write request with Subtract adjustment behavior
///
/// This macro creates a `Request::Write` with `AdjustBehavior::Subtract`, which is useful for
/// decrementing values or removing items from lists.
///
/// # Arguments
///
/// * `entity_id` - The entity ID to write to
/// * `field_type` - The field type to write
/// * `value` - The value to subtract (must be a Some(Value) or None)
/// * `push_condition` - (optional) The write option, defaults to Always
/// * `write_time` - (optional) The write time
/// * `writer_id` - (optional) The writer ID
#[macro_export]
macro_rules! ssub {
    // Basic version with just value
    ($entity_id:expr, $field_type:expr, $value:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $crate::PushCondition::Always,
            adjust_behavior: $crate::AdjustBehavior::Subtract,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Subtract,
            write_time: None,
            writer_id: None,
        }
    };

    // With write option and write time
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Subtract,
            write_time: $write_time,
            writer_id: None,
        }
    };

    // With write option, write time, and writer ID
    ($entity_id:expr, $field_type:expr, $value:expr, $push_condition:expr, $write_time:expr, $writer_id:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id,
            field_type: $field_type,
            value: $value,
            push_condition: $push_condition,
            adjust_behavior: $crate::AdjustBehavior::Subtract,
            write_time: $write_time,
            writer_id: $writer_id,
        }
    };
}

/// Creates a `Some(Value::Bool)` for use in write requests.
///
/// This macro wraps a boolean value in `Some(Value::Bool)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - A boolean value (`true` or `false`)
///
/// # Returns
///
/// * `Some(Value::Bool)` - The wrapped boolean value
#[macro_export]
macro_rules! sbool {
    ($value:expr) => {
        Some($crate::Value::Bool($value))
    };
}

/// Creates a `Some(Value::Int)` for use in write requests.
///
/// This macro wraps an integer value in `Some(Value::Int)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - An integer value (will be converted to i64)
///
/// # Returns
///
/// * `Some(Value::Int)` - The wrapped integer value
#[macro_export]
macro_rules! sint {
    ($value:expr) => {
        Some($crate::Value::Int($value))
    };
}

/// Creates a `Some(Value::Float)` for use in write requests.
///
/// This macro wraps a floating-point value in `Some(Value::Float)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - A floating-point value (will be converted to f64)
///
/// # Returns
///
/// * `Some(Value::Float)` - The wrapped floating-point value
#[macro_export]
macro_rules! sfloat {
    ($value:expr) => {
        Some($crate::Value::Float($value))
    };
}

/// Creates a `Some(Value::String)` for use in write requests.
///
/// This macro wraps a string value in `Some(Value::String)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
/// The input will be converted to a String using `to_string()`.
///
/// # Arguments
///
/// * `$value` - A string-like value that can be converted to String
///
/// # Returns
///
/// * `Some(Value::String)` - The wrapped string value
#[macro_export]
macro_rules! sstr {
    ($value:expr) => {
        Some($crate::Value::String($value.into()))
    };
}

/// Creates a `Some(Value::EntityReference)` for use in write requests.
///
/// This macro wraps an entity reference string in `Some(Value::EntityReference)`,
/// making it ready for use with `swrite!` macro or any function expecting an `Option<Value>`.
/// The input will be converted to a String using `to_string()`.
///
/// # Arguments
///
/// * `$value` - A string-like value representing an entity reference
///
/// # Returns
///
/// * `Some(Value::EntityReference)` - The wrapped entity reference
#[macro_export]
macro_rules! sref {
    ($value:expr) => {
        Some($crate::Value::EntityReference($value))
    };
}

/// Creates a `Some(Value::EntityList)` for use in write requests.
///
/// This macro wraps a list of entity references in `Some(Value::EntityList)`,
/// making it ready for use with `swrite!` macro or any function expecting an
/// `Option<Value>`. It can be used in three ways:
/// 1. With no arguments: creates an empty list
/// 2. With multiple arguments: creates a list from those arguments
/// 3. With a single Vec: wraps the existing Vec
///
/// Each input item will be converted to a String using `to_string()`.
///
/// # Arguments
///
/// * `$value` - Either nothing, a Vec<String>, or a comma-separated list of values
///
/// # Returns
///
/// * `Some(Value::EntityList)` - The wrapped entity list
#[macro_export]
macro_rules! sreflist {
    [] => {
        Some($crate::Value::EntityList(Vec::new()))
    };
    [$($value:expr),*] => {
        {
            let mut v = Vec::<EntityId>::new();
            $(
                v.push($value);
            )*
            Some($crate::Value::EntityList(v))
        }
    };
    ($value:expr) => {
        Some($crate::Value::EntityList($value.clone()))
    };
}

/// Creates a `Some(Value::Choice)` for use in write requests.
///
/// This macro wraps an integer value in `Some(Value::Choice)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
/// The Choice variant typically represents a selection from a predefined set of options.
///
/// # Arguments
///
/// * `$value` - An integer value representing the selected choice (will be converted to i64)
///
/// # Returns
///
/// * `Some(Value::Choice)` - The wrapped choice value
#[macro_export]
macro_rules! schoice {
    ($value:expr) => {
        Some($crate::Value::Choice($value))
    };
}

/// Creates a `Some(Value::Timestamp)` for use in write requests.
///
/// This macro wraps a timestamp value in `Some(Value::Timestamp)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - A SystemTime value
///
/// # Returns
///
/// * `Some(Value::Timestamp)` - The wrapped timestamp value
#[macro_export]
macro_rules! stimestamp {
    ($value:expr) => {
        Some($crate::Value::Timestamp($value))
    };
}

/// Creates a `Some(Value::Blob)` for use in write requests.
///
/// This macro wraps binary data in `Some(Value::Blob)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - A Vec<u8> containing binary data
///
/// # Returns
///
/// * `Some(Value::Blob)` - The wrapped binary data
#[macro_export]
macro_rules! sblob {
    ($value:expr) => {
        Some($crate::Value::Blob($value))
    };
}
