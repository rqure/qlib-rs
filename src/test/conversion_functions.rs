#[cfg(test)]
mod tests {
    use crate::*;
    use crate::scripting::{convert_rhai_to_value, convert_value_to_rhai};
    use crate::data::nanos_to_timestamp;
    use rhai::Dynamic;
    use std::time::UNIX_EPOCH;

    #[test]
    fn test_convert_rhai_to_value_string() {
        let rhai_string = Dynamic::from("Hello, World!");
        let type_hint = Value::String("".to_string());
        
        let result = convert_rhai_to_value(&rhai_string, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_string());
        assert_eq!(value.as_string().unwrap(), "Hello, World!");
    }

    #[test]
    fn test_convert_rhai_to_value_int() {
        let rhai_int = Dynamic::from(42i64);
        let type_hint = Value::Int(0);
        
        let result = convert_rhai_to_value(&rhai_int, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_int());
        assert_eq!(value.as_int().unwrap(), 42);
    }

    #[test]
    fn test_convert_rhai_to_value_bool() {
        let rhai_bool = Dynamic::from(true);
        let type_hint = Value::Bool(false);
        
        let result = convert_rhai_to_value(&rhai_bool, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_bool());
        assert_eq!(value.as_bool().unwrap(), true);
    }

    #[test]
    fn test_convert_rhai_to_value_float() {
        let rhai_float = Dynamic::from(3.14159f64);
        let type_hint = Value::Float(0.0);
        
        let result = convert_rhai_to_value(&rhai_float, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_float());
        assert!((value.as_float().unwrap() - 3.14159).abs() < f64::EPSILON);
    }

    #[test]
    fn test_convert_rhai_to_value_blob() {
        let blob_data = vec![1u8, 2, 3, 4, 5];
        let rhai_blob = Dynamic::from(blob_data.clone());
        let type_hint = Value::Blob(vec![]);
        
        let result = convert_rhai_to_value(&rhai_blob, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_blob());
        assert_eq!(value.as_blob().unwrap(), &blob_data);
    }

    #[test]
    fn test_convert_rhai_to_value_entity_reference() {
        let rhai_entity = Dynamic::from("TestEntity$42");
        let type_hint = Value::EntityReference(None);
        
        let result = convert_rhai_to_value(&rhai_entity, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_entity_reference());
        let entity_ref = value.as_entity_reference().unwrap();
        assert!(entity_ref.is_some());
        assert_eq!(entity_ref.as_ref().unwrap().to_string(), "TestEntity$42");
    }

    #[test]
    fn test_convert_rhai_to_value_entity_list() {
        let entity_list = vec![
            Dynamic::from("Entity$1"),
            Dynamic::from("Entity$2"),
            Dynamic::from("Entity$3"),
        ];
        let rhai_array = Dynamic::from(entity_list);
        let type_hint = Value::EntityList(vec![]);
        
        let result = convert_rhai_to_value(&rhai_array, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_entity_list());
        let list = value.as_entity_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].to_string(), "Entity$1");
        assert_eq!(list[1].to_string(), "Entity$2");
        assert_eq!(list[2].to_string(), "Entity$3");
    }

    #[test]
    fn test_convert_rhai_to_value_choice() {
        let rhai_choice = Dynamic::from(2i64);
        let type_hint = Value::Choice(0);
        
        let result = convert_rhai_to_value(&rhai_choice, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        assert!(value.is_choice());
        assert_eq!(value.as_choice().unwrap(), 2);
    }

    #[test]
    fn test_convert_rhai_to_value_timestamp() {
        let timestamp_nanos = 1625097600_000_000_000i64;
        let rhai_timestamp = Dynamic::from(timestamp_nanos);
        let type_hint = Value::Timestamp(UNIX_EPOCH);
        
        let result = convert_rhai_to_value(&rhai_timestamp, type_hint);
        assert!(result.is_ok());
        
        let value = result.unwrap();
        let timestamp = value.as_timestamp().unwrap();
        let expected = nanos_to_timestamp(timestamp_nanos as u64);
        assert_eq!(timestamp, expected);
    }

    #[test]
    fn test_convert_value_to_rhai_string() {
        let value = Value::String("Test String".to_string());
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<String>());
        assert_eq!(rhai_value.into_string().unwrap(), "Test String");
    }

    #[test]
    fn test_convert_value_to_rhai_int() {
        let value = Value::Int(123);
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<i64>());
        assert_eq!(rhai_value.as_int().unwrap(), 123);
    }

    #[test]
    fn test_convert_value_to_rhai_bool() {
        let value = Value::Bool(true);
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<bool>());
        assert_eq!(rhai_value.as_bool().unwrap(), true);
    }

    #[test]
    fn test_convert_value_to_rhai_float() {
        let value = Value::Float(2.718);
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<f64>());
        assert!((rhai_value.as_float().unwrap() - 2.718).abs() < f64::EPSILON);
    }

    #[test]
    fn test_convert_value_to_rhai_blob() {
        let blob_data = vec![1u8, 2, 3, 4, 5];
        let value = Value::Blob(blob_data.clone());
        let rhai_value = convert_value_to_rhai(&value);
        
        // Blob should be converted to array
        assert!(rhai_value.is::<Vec<u8>>());
        assert_eq!(rhai_value.cast::<Vec<u8>>(), blob_data);
    }

    #[test]
    fn test_convert_value_to_rhai_entity_reference() {
        let entity_id = EntityId::new("TestEntity", 42);
        let value = Value::EntityReference(Some(entity_id.clone()));
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<String>());
        assert_eq!(rhai_value.into_string().unwrap(), entity_id.to_string());
        
        // Test None case
        let none_value = Value::EntityReference(None);
        let none_rhai = convert_value_to_rhai(&none_value);
        assert!(none_rhai.is::<String>());
        assert_eq!(none_rhai.into_string().unwrap(), "");
    }

    #[test]
    fn test_convert_value_to_rhai_entity_list() {
        let entities = vec![
            EntityId::new("Entity", 1),
            EntityId::new("Entity", 2),
            EntityId::new("Entity", 3),
        ];
        let value = Value::EntityList(entities.clone());
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<rhai::Array>());
        let array = rhai_value.cast::<rhai::Array>();
        assert_eq!(array.len(), 3);
        
        for (i, item) in array.iter().enumerate() {
            assert!(item.is::<String>());
            assert_eq!(item.clone().into_string().unwrap(), entities[i].to_string());
        }
    }

    #[test]
    fn test_convert_value_to_rhai_choice() {
        let value = Value::Choice(5);
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<i64>());
        assert_eq!(rhai_value.as_int().unwrap(), 5);
    }

    #[test]
    fn test_convert_value_to_rhai_timestamp() {
        let timestamp = nanos_to_timestamp(1625097600_000_000_000);
        let value = Value::Timestamp(timestamp);
        let rhai_value = convert_value_to_rhai(&value);
        
        assert!(rhai_value.is::<i64>());
        let converted_nanos = rhai_value.as_int().unwrap();
        assert_eq!(converted_nanos, 1625097600_000_000_000);
    }

    #[test]
    fn test_convert_rhai_to_value_type_mismatch() {
        let rhai_string = Dynamic::from("not a number");
        let type_hint = Value::Int(0);
        
        let result = convert_rhai_to_value(&rhai_string, type_hint);
        assert!(result.is_err());
        
        let error_msg = format!("{}", result.unwrap_err());
        assert!(error_msg.contains("Expected an integer value"));
    }

    #[test]
    fn test_convert_rhai_to_value_invalid_entity_id() {
        let rhai_invalid = Dynamic::from("invalid_entity_format");
        let type_hint = Value::EntityReference(None);
        
        let result = convert_rhai_to_value(&rhai_invalid, type_hint);
        assert!(result.is_err());
        
        let error_msg = format!("{}", result.unwrap_err());
        assert!(error_msg.contains("Invalid entity ID format"));
    }

    #[test]
    fn test_roundtrip_conversion() {
        // Test that converting from Value to Rhai and back preserves the original value
        let original_values = vec![
            Value::String("Roundtrip Test".to_string()),
            Value::Int(999),
            Value::Bool(false),
            Value::Float(1.618),
            Value::Choice(3),
            Value::EntityReference(Some(EntityId::new("Test", 123))),
            Value::EntityReference(None),
            Value::EntityList(vec![EntityId::new("List", 1), EntityId::new("List", 2)]),
            Value::Blob(vec![10u8, 20, 30]),
            Value::Timestamp(nanos_to_timestamp(1234567890_000_000_000)),
        ];
        
        for original in original_values {
            let rhai_converted = convert_value_to_rhai(&original);
            let back_converted = convert_rhai_to_value(&rhai_converted, original.clone());
            
            assert!(back_converted.is_ok(), "Failed to convert back: {:?}", original);
            let final_value = back_converted.unwrap();
            
            // Note: Due to the nature of some conversions (like timestamps), 
            // we check type compatibility rather than exact equality
            match (&original, &final_value) {
                (Value::String(_), Value::String(_)) => assert_eq!(original, final_value),
                (Value::Int(_), Value::Int(_)) => assert_eq!(original, final_value),
                (Value::Bool(_), Value::Bool(_)) => assert_eq!(original, final_value),
                (Value::Float(_), Value::Float(_)) => {
                    // For floats, check approximate equality
                    let orig_f = original.as_float().unwrap();
                    let final_f = final_value.as_float().unwrap();
                    assert!((orig_f - final_f).abs() < f64::EPSILON);
                }
                (Value::Choice(_), Value::Choice(_)) => assert_eq!(original, final_value),
                (Value::EntityReference(_), Value::EntityReference(_)) => assert_eq!(original, final_value),
                (Value::EntityList(_), Value::EntityList(_)) => assert_eq!(original, final_value),
                (Value::Blob(_), Value::Blob(_)) => assert_eq!(original, final_value),
                (Value::Timestamp(_), Value::Timestamp(_)) => {
                    // Timestamps might have precision differences, so check they're close
                    let orig_ts = original.as_timestamp().unwrap();
                    let final_ts = final_value.as_timestamp().unwrap();
                    let diff = orig_ts.duration_since(final_ts)
                        .unwrap_or_else(|_| final_ts.duration_since(orig_ts).unwrap());
                    assert!(diff.as_nanos() < 1000); // Within 1 microsecond
                }
                _ => panic!("Type mismatch in roundtrip conversion: {:?} vs {:?}", original, final_value),
            }
        }
    }
}
