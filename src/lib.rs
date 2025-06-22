mod data;

pub use data::{Entity, EntitySchema, EntityId, Field, FieldSchema, Request, Snowflake, Value, 
    MapStore, resolve_indirection, INDIRECTION_DELIMITER, BadIndirection, BadIndirectionReason,
    WriteOption, Timestamp, FieldType, Shared, now, epoch};

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
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: $crate::data::Shared::new(None),
            write_time: $crate::data::Shared::new(None),
            writer_id: $crate::data::Shared::new(None),
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
/// * `write_option` - (optional) The write option, defaults to Normal
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
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: None,
            write_option: $crate::data::request::WriteOption::Normal,
            write_time: None,
            writer_id: None,
        }
    };

    // Basic version with just value: handle Some/None
    ($entity_id:expr, $field_type:expr, $value:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: $value,
            write_option: $crate::data::request::WriteOption::Normal,
            write_time: None,
            writer_id: None,
        }
    };
    
    // With write option
    ($entity_id:expr, $field_type:expr, $value:expr, $write_option:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: $value,
            write_option: $write_option,
            write_time: None,
            writer_id: None,
        }
    };
    
    // With write option and write time
    ($entity_id:expr, $field_type:expr, $value:expr, $write_option:expr, $write_time:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: $value,
            write_option: $write_option,
            write_time: $write_time,
            writer_id: None,
        }
    };
    
    // With write option, write time, and writer ID
    ($entity_id:expr, $field_type:expr, $value:expr, $write_option:expr, $write_time:expr, $writer_id:expr) => {
        $crate::Request::Write {
            entity_id: $entity_id.clone(),
            field_type: $field_type.into(),
            value: $value,
            write_option: $write_option,
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
        Some($crate::Value::String($value.to_string()))
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
        Some($crate::Value::EntityReference($value.to_string()))
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
            let mut v = Vec::<String>::new();
            $(
                v.push($value.to_string());
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn it_works() {
        let snowflake = Snowflake::new();
        println!("{}", EntityId::new("Root", snowflake.generate()));

        let _store = MapStore::new();
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
        let ref_lit = sref!("User$123");
        assert!(matches!(ref_lit, Some(Value::EntityReference(s)) if s == "User$123"));

        // Test with String
        let string = String::from("Group$456");
        let ref_obj = sref!(string);
        assert!(matches!(ref_obj, Some(Value::EntityReference(s)) if s == "Group$456"));

        // Test with EntityId converted to string
        let entity_id = EntityId::new("Item", 789);
        let entity_str = entity_id.to_string();
        let ref_eid = sref!(entity_str);
        assert!(matches!(ref_eid, Some(Value::EntityReference(s)) if s == entity_id.to_string()));
    }

    #[test]
    fn test_entity_list_macro() {
        // Test empty list
        let empty = sreflist![];
        assert!(matches!(empty, Some(Value::EntityList(v)) if v.is_empty()));

        // Test list with multiple items
        let multi = sreflist!["User$1", "User$2", "User$3"];
        if let Some(Value::EntityList(list)) = multi {
            assert_eq!(list.len(), 3);
            assert_eq!(list[0], "User$1");
            assert_eq!(list[1], "User$2");
            assert_eq!(list[2], "User$3");
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
        let now = SystemTime::now();
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
        let request = sread!(entity_id, "Username");
        
        match request {
            Request::Read { 
                entity_id: req_entity_id, 
                field_type, 
                ..
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, "Username");
            },
            _ => panic!("Expected Request::Read"),
        }
    }

    #[test]
    fn test_swrite_macro() {
        let entity_id = EntityId::new("User", 456);
        
        // Basic write with just a value
        let request1 = swrite!(entity_id, "Username", sstr!("alice"));
        match request1 {
            Request::Write { 
                entity_id: req_entity_id, 
                field_type, 
                value, 
                write_option,
                write_time,
                writer_id 
            } => {
                assert_eq!(req_entity_id, entity_id);
                assert_eq!(field_type, "Username");
                assert!(matches!(value, Some(Value::String(s)) if s == "alice"));
                assert!(matches!(write_option, data::request::WriteOption::Normal));
                assert!(write_time.is_none());
                assert!(writer_id.is_none());
            },
            _ => panic!("Expected Request::Write"),
        }
        
        // Write with None (deletion)
        let request2 = swrite!(entity_id, "Username", None);
        match request2 {
            Request::Write { value, .. } => assert!(value.is_none()),
            _ => panic!("Expected Request::Write"),
        }
        
        // Write with custom write option
        use data::request::WriteOption;
        let request3 = swrite!(entity_id, "Username", sstr!("bob"), WriteOption::Changes);
        match request3 {
            Request::Write { write_option, .. } => assert!(matches!(write_option, WriteOption::Changes)),
            _ => panic!("Expected Request::Write"),
        }
        
        // Write with time
        let now = SystemTime::now();
        let request4 = swrite!(entity_id, "LastLogin", stimestamp!(now), WriteOption::Normal, Some(now));
        match request4 {
            Request::Write { write_time, .. } => assert_eq!(write_time, Some(now)),
            _ => panic!("Expected Request::Write"),
        }
        
        // Write with writer
        let writer_id = EntityId::new("Admin", 1);
        let request5 = swrite!(
            entity_id, 
            "Username", 
            sstr!("carol"), 
            WriteOption::Normal, 
            Some(now), 
            Some(writer_id.clone())
        );
        match request5 {
            Request::Write { writer_id: req_writer_id, .. } => {
                assert_eq!(req_writer_id, Some(writer_id));
            },
            _ => panic!("Expected Request::Write"),
        }
    }
}
