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
/// * `value` - The value to write (must be a Some(Value) or None)
/// * `write_option` - (optional) The write option, defaults to Normal
/// * `write_time` - (optional) The write time
/// * `writer_id` - (optional) The writer ID
///
/// # Examples
///
/// ```
/// // Use with sw* macros that automatically wrap values in Some()
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

/// Create a Some(Value::Bool) for direct use in write requests
#[macro_export]
macro_rules! sbool {
    ($value:expr) => {
        Some($crate::Value::Bool($value))
    };
}

/// Create a Some(Value::Int) for direct use in write requests
#[macro_export]
macro_rules! sint {
    ($value:expr) => {
        Some($crate::Value::Int($value))
    };
}

/// Create a Some(Value::Float) for direct use in write requests
#[macro_export]
macro_rules! sfloat {
    ($value:expr) => {
        Some($crate::Value::Float($value))
    };
}

/// Create a Some(Value::String) for direct use in write requests
#[macro_export]
macro_rules! sstr {
    ($value:expr) => {
        Some($crate::Value::String($value.to_string()))
    };
}

/// Create a Some(Value::EntityReference) for direct use in write requests
#[macro_export]
macro_rules! sref {
    ($value:expr) => {
        Some($crate::Value::EntityReference($value.to_string()))
    };
}

/// Create a Some(Value::EntityList) for direct use in write requests
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

/// Create a Some(Value::Choice) for direct use in write requests
#[macro_export]
macro_rules! schoice {
    ($value:expr) => {
        Some($crate::Value::Choice($value))
    };
}

/// Create a Some(Value::Timestamp) for direct use in write requests
#[macro_export]
macro_rules! stimestamp {
    ($value:expr) => {
        Some($crate::Value::Timestamp($value))
    };
}

/// Create a Some(Value::BinaryFile) for direct use in write requests
#[macro_export]
macro_rules! sbinfile {
    ($value:expr) => {
        Some($crate::Value::BinaryFile($value))
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
