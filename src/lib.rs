mod data;

pub use data::{Entity, EntitySchema, EntityId, Field, FieldSchema, Request, Snowflake, Value, 
    MapStore, resolve_indirection, INDIRECTION_DELIMITER, BadIndirection, BadIndirectionReason};

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
/// * `value` - The value to write (optional)
/// * `write_option` - (optional) The write option, defaults to Normal
/// * `write_time` - (optional) The write time
/// * `writer_id` - (optional) The writer ID
///
/// # Examples
///
/// ```
/// // Basic usage
/// let request = swrite!(entity_id, "Name", Some(Value::String("Test".to_string())));
///
/// // With write option
/// let request = swrite!(entity_id, "Name", Some(Value::String("Test".to_string())), WriteOption::Changes);
///
/// // With write time
/// let request = swrite!(entity_id, "Name", Some(Value::String("Test".to_string())), 
///                      WriteOption::Normal, Some(now()));
///
/// // With all options
/// let request = swrite!(entity_id, "Name", Some(Value::String("Test".to_string())), 
///                      WriteOption::Normal, Some(now()), Some(writer_id));
/// ```
#[macro_export]
macro_rules! swrite {
    // Basic version with just value
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

/// Create boolean store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = sbool!(true);
/// ```
#[macro_export]
macro_rules! sbool {
    ($value:expr) => {
        $crate::Value::Bool($value)
    };
}

/// Create integer store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = sint!(42);
/// ```
#[macro_export]
macro_rules! sint {
    ($value:expr) => {
        $crate::Value::Int($value)
    };
}

/// Create float store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = sfloat!(3.14);
/// ```
#[macro_export]
macro_rules! sfloat {
    ($value:expr) => {
        $crate::Value::Float($value)
    };
}

/// Create string store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = sstr!("hello");
/// let value = sstr!(format!("hello {}", name));
/// ```
#[macro_export]
macro_rules! sstr {
    ($value:expr) => {
        $crate::Value::String($value.to_string())
    };
}

/// Create entity reference store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = sref!(entity_id);
/// // or with string
/// let value = sref!("User$123456");
/// ```
#[macro_export]
macro_rules! sref {
    ($value:expr) => {
        $crate::Value::EntityReference($value.to_string())
    };
}

/// Create entity list store values more elegantly
/// 
/// # Example
/// 
/// ```
/// // Create empty list
/// let value = slist![];
/// 
/// // Create with values
/// let value = slist!["User$123", "User$456"];
/// 
/// // Create from a vector
/// let ids = vec!["User$123".to_string(), "User$456".to_string()];
/// let value = slist!(ids);
/// ```
#[macro_export]
macro_rules! slist {
    [] => {
        $crate::Value::EntityList(Vec::new())
    };
    [$($value:expr),* $(,)?] => {
        {
            let mut v = Vec::new();
            $(
                v.push($value.to_string());
            )*
            $crate::Value::EntityList(v)
        }
    };
    ($value:expr) => {
        $crate::Value::EntityList($value.clone())
    };
}

/// Create choice store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let value = schoice!(2);
/// ```
#[macro_export]
macro_rules! schoice {
    ($value:expr) => {
        $crate::Value::Choice($value)
    };
}

/// Create timestamp store values more elegantly
/// 
/// # Example
/// 
/// ```
/// // Current time
/// let value = stimestamp!(now());
/// 
/// // UNIX epoch
/// let value = stimestamp!(epoch());
/// ```
#[macro_export]
macro_rules! stimestamp {
    ($value:expr) => {
        $crate::Value::Timestamp($value)
    };
}

/// Create binary file store values more elegantly
/// 
/// # Example
/// 
/// ```
/// let data = vec![0, 1, 2, 3];
/// let value = sbin!(data);
/// ```
#[macro_export]
macro_rules! sbin {
    ($value:expr) => {
        $crate::Value::BinaryFile($value)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let snowflake = Snowflake::new();
        println!("{}", EntityId::new("Root", snowflake.generate()));

        let _store = MapStore::new();
    }
}
