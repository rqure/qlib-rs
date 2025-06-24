mod data;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::UNIX_EPOCH};

    #[test]
    fn it_works() {
        let snowflake = Arc::new(Snowflake::new());
        println!("{}", EntityId::new("Root", snowflake.generate()));
        let _store = MapStore::new(snowflake);
    }

    #[test]
    fn test_value_bool_macro() {
        // Test true value
        let true_val = sbool!(true);
        assert!(matches!(true_val, Some(Value::Bool(true))));

        // Test false value
        let false_val = sbool!(false);
        assert!(matches!(false_val, Some(Value::Bool(false))));
    }

    #[test]
    fn test_value_int_macro() {
        // Test positive integer
        let positive = sint!(42);
        assert!(matches!(positive, Some(Value::Int(42))));

        // Test zero
        let zero = sint!(0);
        assert!(matches!(zero, Some(Value::Int(0))));

        // Test negative integer
        let negative = sint!(-10);
        assert!(matches!(negative, Some(Value::Int(-10))));

        // Test larger integers
        let large = sint!(i64::MAX);
        assert!(matches!(large, Some(Value::Int(i64::MAX))));
    }

    #[test]
    fn test_value_float_macro() {
        // Test positive float
        let positive = sfloat!(3.14);
        if let Some(Value::Float(val)) = positive {
            assert!((val - 3.14).abs() < f64::EPSILON);
        } else {
            panic!("Expected Some(Value::Float)");
        }

        // Test zero
        let zero = sfloat!(0.0);
        if let Some(Value::Float(val)) = zero {
            assert!(val == 0.0);
        } else {
            panic!("Expected Some(Value::Float)");
        }

        // Test negative float
        let negative = sfloat!(-2.5);
        if let Some(Value::Float(val)) = negative {
            assert!((val - (-2.5)).abs() < f64::EPSILON);
        } else {
            panic!("Expected Some(Value::Float)");
        }
    }

    #[test]
    fn test_value_string_macro() {
        // Test with string literal
        let str_lit = sstr!("Hello");
        assert!(matches!(str_lit, Some(Value::String(s)) if s == "Hello"));

        // Test with String
        let string = String::from("World");
        let str_obj = sstr!(string);
        assert!(matches!(str_obj, Some(Value::String(s)) if s == "World"));

        // Test with &str
        let str_ref = "Reference";
        let str_ref_val = sstr!(str_ref);
        assert!(matches!(str_ref_val, Some(Value::String(s)) if s == "Reference"));

        // Test with string containing special chars
        let special = sstr!("Special & Chars: !@#$%^&*()");
        assert!(matches!(special, Some(Value::String(s)) if s == "Special & Chars: !@#$%^&*()"));
    }

    #[test]
    fn test_entity_reference_macro() {
        // Test with string literal
        let entity_id = Some(EntityId::try_from("User$123").unwrap());
        let ref_lit = sref!(entity_id.clone());
        assert!(matches!(ref_lit, Some(Value::EntityReference(s)) if s == entity_id));
    }

    #[test]
    fn test_entity_list_macro() {
        // Test empty list
        let empty = sreflist![];
        assert!(matches!(empty, Some(Value::EntityList(v)) if v.is_empty()));

        // Test list with multiple items
        let user1 = EntityId::try_from("User$1").unwrap();
        let user2 = EntityId::try_from("User$2").unwrap();
        let user3 = EntityId::try_from("User$3").unwrap();
        let multi = sreflist![user1.clone(), user2.clone(), user3.clone()];
        if let Some(Value::EntityList(list)) = multi {
            assert_eq!(list.len(), 3);
            assert_eq!(list[0], user1);
            assert_eq!(list[1], user2);
            assert_eq!(list[2], user3);
        } else {
            panic!("Expected Some(Value::EntityList)");
        }
    }

    #[test]
    fn test_choice_macro() {
        // Test choice values
        let choice1 = schoice!(0);
        assert!(matches!(choice1, Some(Value::Choice(0))));

        let choice2 = schoice!(1);
        assert!(matches!(choice2, Some(Value::Choice(1))));

        let choice3 = schoice!(100);
        assert!(matches!(choice3, Some(Value::Choice(100))));

        let choice_neg = schoice!(-1);
        assert!(matches!(choice_neg, Some(Value::Choice(-1))));
    }

    #[test]
    fn test_timestamp_macro() {
        // Test with current time
        let now = now();
        let ts_now = stimestamp!(now);
        if let Some(Value::Timestamp(ts)) = ts_now {
            assert_eq!(ts, now);
        } else {
            panic!("Expected Some(Value::Timestamp)");
        }

        // Test with epoch
        let epoch = UNIX_EPOCH;
        let ts_epoch = stimestamp!(epoch);
        if let Some(Value::Timestamp(ts)) = ts_epoch {
            assert_eq!(ts, epoch);
        } else {
            panic!("Expected Some(Value::Timestamp)");
        }
    }

    #[test]
    fn test_binary_file_macro() {
        // Test with empty vector
        let empty = sbinfile!(Vec::<u8>::new());
        if let Some(Value::BinaryFile(data)) = empty {
            assert!(data.is_empty());
        } else {
            panic!("Expected Some(Value::BinaryFile)");
        }

        // Test with some data
        let hello = vec![0x48, 0x65, 0x6C, 0x6C, 0x6F]; // "Hello" in ASCII
        let bin_hello = sbinfile!(hello.clone());
        if let Some(Value::BinaryFile(data)) = bin_hello {
            assert_eq!(data, hello);
        } else {
            panic!("Expected Some(Value::BinaryFile)");
        }

        // Test with larger binary data
        let large_data = vec![0u8; 1024]; // 1KB of zeros
        let bin_large = sbinfile!(large_data.clone());
        if let Some(Value::BinaryFile(data)) = bin_large {
            assert_eq!(data.len(), 1024);
            assert_eq!(data, large_data);
        } else {
            panic!("Expected Some(Value::BinaryFile)");
        }
    }

    #[test]
    fn test_sread_macro() {
        let entity_id = EntityId::new("User", 123);
        let request = sread!(entity_id.clone(), "Username".into());

        match request {
            Request::Read {
                entity_id: req_entity_id,
                field_type,
                ..
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, "Username".into());
            }
            _ => panic!("Expected Request::Read"),
        }
    }

    #[test]
    fn test_swrite_macro() {
        let entity_id = EntityId::new("User", 456);
        let ft_username = FieldType::from("Username");

        // Basic write with just a value
        let request1 = swrite!(entity_id.clone(), ft_username.clone(), sstr!("alice"));
        match request1 {
            Request::Write {
                entity_id: req_entity_id,
                field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time,
                writer_id,
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, ft_username);
                assert!(matches!(value, Some(Value::String(s)) if s == "alice"));
                assert!(matches!(push_condition, PushCondition::Always));
                assert!(matches!(adjust_behavior, AdjustBehavior::Set));
                assert!(write_time.is_none());
                assert!(writer_id.is_none());
            }
            _ => panic!("Expected Request::Write"),
        }

        // Write with None (deletion)
        let request2 = swrite!(entity_id.clone(), ft_username.clone(), None);
        match request2 {
            Request::Write { value, .. } => assert!(value.is_none()),
            _ => panic!("Expected Request::Write"),
        }

        // Write with custom write option
        let request3 = swrite!(
            entity_id.clone(),
            ft_username.clone(),
            sstr!("bob"),
            PushCondition::Changes
        );
        match request3 {
            Request::Write { push_condition, .. } => {
                assert!(matches!(push_condition, PushCondition::Changes))
            }
            _ => panic!("Expected Request::Write"),
        }

        // Write with time
        let now = now();
        let ft_last_login = FieldType::from("LastLogin");
        let request4 = swrite!(
            entity_id.clone(),
            ft_last_login.clone(),
            stimestamp!(now),
            PushCondition::Always,
            Some(now)
        );
        match request4 {
            Request::Write { write_time, .. } => assert_eq!(write_time, Some(now)),
            _ => panic!("Expected Request::Write"),
        }

        // Write with writer
        let writer_id = EntityId::new("Admin", 1);
        let request5 = swrite!(
            entity_id.clone(),
            ft_username.clone(),
            sstr!("carol"),
            PushCondition::Always,
            Some(now),
            Some(writer_id.clone())
        );
        match request5 {
            Request::Write {
                writer_id: req_writer_id,
                ..
            } => {
                assert_eq!(req_writer_id, Some(writer_id));
            }
            _ => panic!("Expected Request::Write"),
        }
    }

    #[test]
    fn test_sadd_macro() {
        let entity_id = EntityId::new("User", 456);
        let ft_counter = FieldType::from("Counter");

        // Basic add with just a value
        let request1 = sadd!(entity_id.clone(), ft_counter.clone(), sint!(5));
        match request1 {
            Request::Write {
                entity_id: req_entity_id,
                field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time,
                writer_id,
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, ft_counter);
                assert!(matches!(value, Some(Value::Int(5))));
                assert!(matches!(push_condition, PushCondition::Always));
                assert!(matches!(adjust_behavior, AdjustBehavior::Add));
                assert!(write_time.is_none());
                assert!(writer_id.is_none());
            }
            _ => panic!("Expected Request::Write"),
        }

        // Add with write option
        let request2 = sadd!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(10),
            PushCondition::Changes
        );
        match request2 {
            Request::Write {
                push_condition,
                adjust_behavior,
                ..
            } => {
                assert!(matches!(push_condition, PushCondition::Changes));
                assert!(matches!(adjust_behavior, AdjustBehavior::Add));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Add with time
        let now = now();
        let request3 = sadd!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(15),
            PushCondition::Always,
            Some(now)
        );
        match request3 {
            Request::Write {
                write_time,
                adjust_behavior,
                ..
            } => {
                assert_eq!(write_time, Some(now));
                assert!(matches!(adjust_behavior, AdjustBehavior::Add));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Add with writer
        let writer_id = EntityId::new("Admin", 1);
        let request4 = sadd!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(20),
            PushCondition::Always,
            Some(now),
            Some(writer_id.clone())
        );
        match request4 {
            Request::Write {
                writer_id: req_writer_id,
                adjust_behavior,
                ..
            } => {
                assert_eq!(req_writer_id, Some(writer_id));
                assert!(matches!(adjust_behavior, AdjustBehavior::Add));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Add with entity list (testing different value types)
        let ft_tags = FieldType::from("Tags");
        let tag1 = EntityId::new("Tag", 1);
        let tag2 = EntityId::new("Tag", 2);
        let request5 = sadd!(
            entity_id.clone(),
            ft_tags.clone(),
            sreflist![tag1.clone(), tag2.clone()]
        );
        match request5 {
            Request::Write {
                adjust_behavior,
                value,
                ..
            } => {
                assert!(matches!(adjust_behavior, AdjustBehavior::Add));
                if let Some(Value::EntityList(list)) = value {
                    assert_eq!(list.len(), 2);
                    assert_eq!(list[0], tag1);
                    assert_eq!(list[1], tag2);
                } else {
                    panic!("Expected Some(Value::EntityList)");
                }
            }
            _ => panic!("Expected Request::Write"),
        }
    }

    #[test]
    fn test_ssub_macro() {
        let entity_id = EntityId::new("User", 789);
        let ft_counter = FieldType::from("Counter");

        // Basic subtract with just a value
        let request1 = ssub!(entity_id.clone(), ft_counter.clone(), sint!(3));
        match request1 {
            Request::Write {
                entity_id: req_entity_id,
                field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time,
                writer_id,
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, ft_counter);
                assert!(matches!(value, Some(Value::Int(3))));
                assert!(matches!(push_condition, PushCondition::Always));
                assert!(matches!(adjust_behavior, AdjustBehavior::Subtract));
                assert!(write_time.is_none());
                assert!(writer_id.is_none());
            }
            _ => panic!("Expected Request::Write"),
        }

        // Subtract with write option
        let request2 = ssub!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(5),
            PushCondition::Changes
        );
        match request2 {
            Request::Write {
                push_condition,
                adjust_behavior,
                ..
            } => {
                assert!(matches!(push_condition, PushCondition::Changes));
                assert!(matches!(adjust_behavior, AdjustBehavior::Subtract));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Subtract with time
        let now = now();
        let request3 = ssub!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(8),
            PushCondition::Always,
            Some(now)
        );
        match request3 {
            Request::Write {
                write_time,
                adjust_behavior,
                ..
            } => {
                assert_eq!(write_time, Some(now));
                assert!(matches!(adjust_behavior, AdjustBehavior::Subtract));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Subtract with writer
        let writer_id = EntityId::new("Admin", 1);
        let request4 = ssub!(
            entity_id.clone(),
            ft_counter.clone(),
            sint!(10),
            PushCondition::Always,
            Some(now),
            Some(writer_id.clone())
        );
        match request4 {
            Request::Write {
                writer_id: req_writer_id,
                adjust_behavior,
                ..
            } => {
                assert_eq!(req_writer_id, Some(writer_id));
                assert!(matches!(adjust_behavior, AdjustBehavior::Subtract));
            }
            _ => panic!("Expected Request::Write"),
        }

        // Subtract with entity list (testing different value types)
        let ft_tags = FieldType::from("Tags");
        let tag1 = EntityId::new("Tag", 1);
        let request5 = ssub!(entity_id.clone(), ft_tags.clone(), sreflist![tag1.clone()]);
        match request5 {
            Request::Write {
                adjust_behavior,
                value,
                ..
            } => {
                assert!(matches!(adjust_behavior, AdjustBehavior::Subtract));
                if let Some(Value::EntityList(list)) = value {
                    assert_eq!(list.len(), 1);
                    assert_eq!(list[0], tag1);
                } else {
                    panic!("Expected Some(Value::EntityList)");
                }
            }
            _ => panic!("Expected Request::Write"),
        }
    }

    #[cfg(test)]
    mod mapstore_tests {
        use super::*;
        use crate::data::EntityType;
        use std::sync::Arc;

        // Helper to create an entity schema with basic fields
        fn create_entity_schema(store: &mut MapStore, entity_type: &EntityType) -> Result<()> {
            let mut schema = EntitySchema::new(entity_type.clone());
            let ft_name = FieldType::from("Name");
            let ft_parent = FieldType::from("Parent");
            let ft_children = FieldType::from("Children");

            // Add default fields common to all entities
            let name_schema = FieldSchema {
                entity_type: entity_type.clone(),
                field_type: ft_name.clone(),
                default_value: Value::String("".into()),
                rank: 0,
                read_permission: None,
                write_permission: None,
                choices: None,
            };

            let parent_schema = FieldSchema {
                entity_type: entity_type.clone(),
                field_type: ft_parent.clone(),
                default_value: Value::EntityReference(None),
                rank: 1,
                read_permission: None,
                write_permission: None,
                choices: None,
            };

            let children_schema = FieldSchema {
                entity_type: entity_type.clone(),
                field_type: ft_children.clone(),
                default_value: Value::EntityList(Vec::new()),
                rank: 2,
                read_permission: None,
                write_permission: None,
                choices: None,
            };

            schema.fields.insert(ft_name.clone(), name_schema);
            schema.fields.insert(ft_parent.clone(), parent_schema);
            schema.fields.insert(ft_children.clone(), children_schema);

            store.set_entity_schema(&Context {}, &schema)?;
            Ok(())
        }

        // Helper to set up a basic database structure for testing
        fn setup_test_database() -> Result<MapStore> {
            let mut store = MapStore::new(Arc::new(Snowflake::new()));
            let ctx = Context {};

            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");
            let et_role = EntityType::from("Role");

            let ft_email = FieldType::from("Email");

            // Create schemas for different entity types
            create_entity_schema(&mut store, &et_root)?;
            create_entity_schema(&mut store, &et_folder)?;
            create_entity_schema(&mut store, &et_user)?;
            create_entity_schema(&mut store, &et_role)?;

            // Add custom fields to User schema
            let email_schema = FieldSchema {
                entity_type: et_user.clone(),
                field_type: ft_email.clone(),
                default_value: Value::String("".into()),
                rank: 3,
                read_permission: None,
                write_permission: None,
                choices: None,
            };

            store.set_field_schema(&ctx, &et_user, &ft_email, email_schema)?;

            // Create root entity
            store.create_entity(&ctx, &et_root, None, "Root")?;

            Ok(store)
        }

        #[test]
        fn test_create_entity_hierarchy() -> Result<()> {
            let mut store = setup_test_database()?;
            let ctx = Context {};

            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");
            let et_role = EntityType::from("Role");

            let ft_children = FieldType::from("Children");
            let ft_parent = FieldType::from("Parent");

            // Get the Root entity
            let root_entities = store.find_entities(&ctx, &et_root, None)?;
            assert_eq!(root_entities.items.len(), 1);
            let root_id = root_entities.items[0].clone();

            // Create a folder under root
            let security_models =
                store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security Models")?;

            // Create subfolders
            let users_folder = store.create_entity(
                &ctx,
                &et_folder,
                Some(security_models.entity_id.clone()),
                "Users",
            )?;

            let roles_folder = store.create_entity(
                &ctx,
                &et_folder,
                Some(security_models.entity_id.clone()),
                "Roles",
            )?;

            // Create a user and role
            let user =
                store.create_entity(&ctx, &et_user, Some(users_folder.entity_id.clone()), "qei")?;

            store.create_entity(
                &ctx,
                &et_role,
                Some(roles_folder.entity_id.clone()),
                "Admin",
            )?;

            // Read children of security models folder
            let mut reqs = vec![sread!(security_models.entity_id, ft_children.clone())];
            store.perform(&ctx, &mut reqs)?;

            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityList(children)) = value {
                    assert_eq!(children.len(), 2);
                } else {
                    panic!("Expected Children to be an EntityList");
                }
            }

            // Verify user's parent is the users folder
            let mut reqs = vec![sread!(user.entity_id, ft_parent.clone())];
            store.perform(&ctx, &mut reqs)?;

            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityReference(parent)) = value {
                    assert_eq!(*parent, Some(users_folder.entity_id));
                } else {
                    panic!("Expected Parent to be an EntityReference");
                }
            }

            Ok(())
        }

        #[test]
        fn test_field_operations() -> Result<()> {
            let mut store = setup_test_database()?;
            let ctx = Context {};

            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");

            let ft_email = FieldType::from("Email");

            let root_entities = store.find_entities(&ctx, &et_root, None)?;
            let root_id = root_entities.items[0].clone();

            let users_folder =
                store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

            let user =
                store.create_entity(&ctx, &et_user, Some(users_folder.entity_id), "testuser")?;

            // Test writing to a field
            let mut writes = vec![swrite!(
                user.entity_id.clone(),
                ft_email.clone(),
                sstr!("test@example.com")
            )];
            store.perform(&ctx, &mut writes)?;

            // Test reading the field
            let mut reads = vec![sread!(user.entity_id.clone(), "Email".into())];
            store.perform(&ctx, &mut reads)?;

            if let Request::Read { value, .. } = &reads[0] {
                assert_eq!(*value, Some(Value::String("test@example.com".to_string())));
            }

            // Test field update with write option
            let mut updates = vec![swrite!(
                user.entity_id.clone(),
                ft_email.clone(),
                sstr!("updated@example.com"),
                PushCondition::Changes
            )];
            store.perform(&ctx, &mut updates)?;

            // Verify update
            let mut verify = vec![sread!(user.entity_id.clone(), ft_email.clone())];
            store.perform(&ctx, &mut verify)?;

            if let Request::Read { value, .. } = &verify[0] {
                assert_eq!(
                    *value,
                    Some(Value::String("updated@example.com".to_string()))
                );
            }

            Ok(())
        }

        #[test]
        fn test_indirection_resolution() -> Result<()> {
            let mut store = setup_test_database()?;
            let ctx = Context {};

            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");

            let ft_email = FieldType::from("Email");

            // Create entities
            let root_entities = store.find_entities(&ctx, &et_root, None)?;
            let root_id = root_entities.items[0].clone();

            let security_folder =
                store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Security")?;

            let users_folder = store.create_entity(
                &ctx,
                &et_folder,
                Some(security_folder.entity_id.clone()),
                "Users",
            )?;

            let admin_user = store.create_entity(
                &ctx,
                &et_user,
                Some(users_folder.entity_id.clone()),
                "admin",
            )?;

            // Set email
            let mut writes = vec![swrite!(
                admin_user.entity_id.clone(),
                ft_email.clone(),
                sstr!("admin@example.com")
            )];
            store.perform(&ctx, &mut writes)?;

            // Test indirection
            let mut reqs = vec![sread!(
                security_folder.entity_id.clone(),
                format!("Children->0->Children->0->Email").into()
            )];

            store.perform(&ctx, &mut reqs)?;

            if let Request::Read { value, .. } = &reqs[0] {
                assert_eq!(*value, Some(Value::String("admin@example.com".to_string())));
            }

            Ok(())
        }

        #[test]
        fn test_entity_deletion() -> Result<()> {
            let mut store = setup_test_database()?;
            let ctx = Context {};

            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");

            let ft_children = FieldType::from("Children");

            // Create a folder and a user
            let root_entities = store.find_entities(&ctx, &et_root, None)?;
            let root_id = root_entities.items[0].clone();

            let users_folder =
                store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

            let user = store.create_entity(
                &ctx,
                &et_user,
                Some(users_folder.entity_id.clone()),
                "temp_user",
            )?;

            // Verify user exists
            assert!(store.entity_exists(&ctx, &user.entity_id));

            // Delete the user
            store.delete_entity(&ctx, &user.entity_id)?;

            // Verify user no longer exists
            assert!(!store.entity_exists(&ctx, &user.entity_id));

            // Check if the user was removed from the parent's children list
            let mut request = vec![sread!(users_folder.entity_id.clone(), ft_children.clone())];
            store.perform(&ctx, &mut request)?;

            if let Request::Read { value, .. } = &request[0] {
                if let Some(Value::EntityList(children)) = value {
                    assert!(
                        !children.contains(&user.entity_id),
                        "User should have been removed from parent's children list"
                    );
                }
            }

            Ok(())
        }

        #[test]
        fn test_entity_listing_with_pagination() -> Result<()> {
            let mut store = setup_test_database()?;
            let ctx = Context {};

            // Create multiple entities of the same type
            let et_root = EntityType::from("Root");
            let et_folder = EntityType::from("Folder");
            let et_user = EntityType::from("User");

            let root_entities = store.find_entities(&ctx, &et_root, None)?;
            let root_id = root_entities.items[0].clone();

            let users_folder =
                store.create_entity(&ctx, &et_folder, Some(root_id.clone()), "Users")?;

            // Create 10 users
            for i in 1..=10 {
                store.create_entity(
                    &ctx,
                    &et_user,
                    Some(users_folder.entity_id.clone()),
                    &format!("user{}", i),
                )?;
            }

            // Test pagination - first page (5 items)
            let page_opts = PageOpts::new(5, None);
            let page1 = store.find_entities(&ctx, &et_user, Some(page_opts))?;

            assert_eq!(page1.items.len(), 5);
            assert_eq!(page1.total, 10);
            assert!(page1.next_cursor.is_some());

            // Test pagination - second page
            let page_opts = PageOpts::new(5, page1.next_cursor.clone());
            let page2 = store.find_entities(&ctx, &et_user, Some(page_opts))?;

            assert_eq!(page2.items.len(), 5);
            assert_eq!(page2.total, 10);
            assert!(page2.next_cursor.is_none());

            // Verify we got different sets of users
            for item in &page1.items {
                assert!(!page2.items.contains(item));
            }

            Ok(())
        }
    }
}
