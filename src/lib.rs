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
/// let request = read!(entity_id, "Name");
/// ```
#[macro_export]
macro_rules! read {
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
/// let request = write!(entity_id, "Name", Some(Value::String("Test".to_string())));
///
/// // With write option
/// let request = write!(entity_id, "Name", Some(Value::String("Test".to_string())), WriteOption::Changes);
///
/// // With write time
/// let request = write!(entity_id, "Name", Some(Value::String("Test".to_string())), 
///                      WriteOption::Normal, Some(now()));
///
/// // With all options
/// let request = write!(entity_id, "Name", Some(Value::String("Test".to_string())), 
///                      WriteOption::Normal, Some(now()), Some(writer_id));
/// ```
#[macro_export]
macro_rules! write {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read, write};
    use crate::data::request::WriteOption;

    #[test]
    fn it_works() {
        let snowflake = Snowflake::new();
        println!("{}", EntityId::new("Root", snowflake.generate()));

        let _store = MapStore::new();
    }
    
    #[test]
    fn test_request_macros() {
        let entity_id = EntityId::new("User", 12345);
        
        // Test read macro
        let read_request = read!(entity_id, "Name");
        if let Request::Read { field_type, .. } = read_request {
            assert_eq!(field_type, "Name");
        } else {
            panic!("Expected Read request");
        }
        
        // Test write macro with just value
        let write_request1 = write!(entity_id, "Age", Some(Value::Int(30)));
        if let Request::Write { field_type, value, write_option, .. } = write_request1 {
            assert_eq!(field_type, "Age");
            assert_eq!(value, Some(Value::Int(30)));
            assert_eq!(write_option, WriteOption::Normal);
        } else {
            panic!("Expected Write request");
        }
        
        // Test write macro with value and write option
        let write_request2 = write!(entity_id, "Score", Some(Value::Float(95.5)), WriteOption::Changes);
        if let Request::Write { field_type, value, write_option, .. } = write_request2 {
            assert_eq!(field_type, "Score");
            assert_eq!(value, Some(Value::Float(95.5)));
            assert_eq!(write_option, WriteOption::Changes);
        } else {
            panic!("Expected Write request");
        }
    }
}
