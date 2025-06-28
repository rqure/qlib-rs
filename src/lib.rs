mod data;
mod test;

pub use data::{
    epoch, now, resolve_indirection, AdjustBehavior, BadIndirection, BadIndirectionReason, Context,
    Entity, EntityId, EntityType, EntitySchema, Field, FieldSchema, FieldType, Store, PageOpts, PageResult,
    PushCondition, Request, Snowflake, Timestamp, Value, INDIRECTION_DELIMITER,
    Single, Complete,
};

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

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
