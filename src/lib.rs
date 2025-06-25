mod data;
mod test;

pub use data::{
    epoch, now, resolve_indirection, AdjustBehavior, BadIndirection, BadIndirectionReason, Context,
    Entity, EntityId, EntitySchema, Field, FieldSchema, FieldType, MapStore, PageOpts, PageResult,
    PushCondition, Request, Snowflake, Timestamp, Value, INDIRECTION_DELIMITER,
};

/// Create a Read request with minimal syntax
///
/// # Arguments
///
/// * `entity_id` - The entity ID to read from
/// * `field_type` - The field type to read
///
/// # Example
///
/// ```
/// let request = sread!(entity_id, "Name");
/// ```
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
///
/// # Examples
///
/// ```
/// // Use with sb* macros that automatically wrap values in Some()
/// let request = swrite!(entity_id, "Name", sstr!("Test"));
/// let request = swrite!(entity_id, "Age", sint!(42));
/// let request = swrite!(entity_id, "Active", sbool!(true));
///
/// // With None for deletion
/// let request = swrite!(entity_id, "Name", None);
///
/// // With write option
/// let request = swrite!(entity_id, "Name", sstr!("Test"), WriteOption::Changes);
///
/// // With write time
/// let request = swrite!(entity_id, "Name", sstr!("Test"),
///                      WriteOption::Normal, Some(now()));
///
/// // With all options
/// let request = swrite!(entity_id, "Name", sstr!("Test"),
///                      WriteOption::Normal, Some(now()), Some(writer_id));
/// ```
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
///
/// # Examples
///
/// ```
/// // Increment a counter
/// let request = sadd!(entity_id, "Counter", sint!(1));
///
/// // Append to a list
/// let request = sadd!(entity_id, "Tags", sreflist!["tag1", "tag2"]);
///
/// // With write option
/// let request = sadd!(entity_id, "Counter", sint!(1), PushCondition::Changes);
///
/// // With all options
/// let request = sadd!(entity_id, "Counter", sint!(1),
///                    PushCondition::Always, Some(now()), Some(writer_id));
/// ```
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
///
/// # Examples
///
/// ```
/// // Decrement a counter
/// let request = ssub!(entity_id, "Counter", sint!(1));
///
/// // Remove from a list
/// let request = ssub!(entity_id, "Tags", sreflist!["tag1"]);
///
/// // With write option
/// let request = ssub!(entity_id, "Counter", sint!(1), PushCondition::Changes);
///
/// // With all options
/// let request = ssub!(entity_id, "Counter", sint!(1),
///                    PushCondition::Always, Some(now()), Some(writer_id));
/// ```
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
///
/// # Examples
///
/// ```
/// let bool_value = sbool!(true);
/// assert_eq!(bool_value, Some(Value::Bool(true)));
///
/// // Use in a write request
/// swrite!(entity_id, "IsActive", sbool!(true));
/// ```
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
///
/// # Examples
///
/// ```
/// let int_value = sint!(42);
/// assert_eq!(int_value, Some(Value::Int(42)));
///
/// // Use in a write request
/// swrite!(entity_id, "Count", sint!(100));
/// ```
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
///
/// # Examples
///
/// ```
/// let float_value = sfloat!(3.14);
/// assert_eq!(float_value, Some(Value::Float(3.14)));
///
/// // Use in a write request
/// swrite!(entity_id, "Price", sfloat!(29.99));
/// ```
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
///
/// # Examples
///
/// ```
/// let string_value = sstr!("Hello");
/// assert_eq!(string_value, Some(Value::String("Hello".to_string())));
///
/// // Works with different string types
/// let static_str = sstr!("Static");
/// let string_type = sstr!(String::from("Dynamic"));
///
/// // Use in a write request
/// swrite!(entity_id, "Name", sstr!("Alice"));
/// ```
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
///
/// # Examples
///
/// ```
/// let ref_value = sref!("User$123");
/// assert_eq!(ref_value, Some(Value::EntityReference("User$123".to_string())));
///
/// // Use in a write request
/// swrite!(entity_id, "Owner", sref!("User$456"));
/// ```
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
///
/// # Examples
///
/// ```
/// // Empty list
/// let empty_list = sreflist![];
/// assert_eq!(empty_list, Some(Value::EntityList(Vec::new())));
///
/// // List from multiple arguments
/// let multi_list = sreflist!["User$1", "User$2", "User$3"];
/// assert_eq!(multi_list, Some(Value::EntityList(vec![
///     "User$1".to_string(),
///     "User$2".to_string(),
///     "User$3".to_string()
/// ])));
///
/// // Use in a write request
/// swrite!(entity_id, "Members", sreflist!["User$1", "User$2"]);
/// ```
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
///
/// # Examples
///
/// ```
/// let choice_value = schoice!(2);
/// assert_eq!(choice_value, Some(Value::Choice(2)));
///
/// // Use in a write request
/// swrite!(entity_id, "Status", schoice!(1)); // 1 might represent "Active" in the application
/// ```
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
///
/// # Examples
///
/// ```
/// use std::time::SystemTime;
///
/// let now = SystemTime::now();
/// let timestamp_value = stimestamp!(now);
///
/// // Use in a write request
/// let created_at = SystemTime::now();
/// swrite!(entity_id, "CreatedAt", stimestamp!(created_at));
/// ```
#[macro_export]
macro_rules! stimestamp {
    ($value:expr) => {
        Some($crate::Value::Timestamp($value))
    };
}

/// Creates a `Some(Value::BinaryFile)` for use in write requests.
///
/// This macro wraps binary data in `Some(Value::BinaryFile)`, making it ready
/// for use with `swrite!` macro or any function expecting an `Option<Value>`.
///
/// # Arguments
///
/// * `$value` - A Vec<u8> containing binary data
///
/// # Returns
///
/// * `Some(Value::BinaryFile)` - The wrapped binary data
///
/// # Examples
///
/// ```
/// let data = vec![0x48, 0x65, 0x6C, 0x6C, 0x6F]; // "Hello" in bytes
/// let binary_value = sbinfile!(data);
///
/// // Use in a write request
/// let file_contents = std::fs::read("example.dat").unwrap();
/// swrite!(entity_id, "FileData", sbinfile!(file_contents));
/// ```
#[macro_export]
macro_rules! sbinfile {
    ($value:expr) => {
        Some($crate::Value::BinaryFile($value))
    };
}
