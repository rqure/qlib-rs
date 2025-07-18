#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_fieldschema_underlying_types() {
        // Test creating FieldSchema with underlying types instead of Value enum
        let string_schema = FieldSchema::String {
            field_type: FieldType::from("test_string"),
            default_value: "hello".to_string(),  // String instead of Value::String
            rank: 0,
        };
        
        let bool_schema = FieldSchema::Bool {
            field_type: FieldType::from("test_bool"),
            default_value: true,  // bool instead of Value::Bool
            rank: 1,
        };
        
        let int_schema = FieldSchema::Int {
            field_type: FieldType::from("test_int"),
            default_value: 42,  // i64 instead of Value::Int
            rank: 2,
        };

        let entity_ref_schema = FieldSchema::EntityReference {
            field_type: FieldType::from("test_ref"),
            default_value: None,  // Option<EntityId> instead of Value::EntityReference
            rank: 3,
        };

        let entity_list_schema = FieldSchema::EntityList {
            field_type: FieldType::from("test_list"),
            default_value: Vec::new(),  // Vec<EntityId> instead of Value::EntityList
            rank: 4,
        };
        
        // Test that default_value() method still returns Value enum
        assert!(matches!(string_schema.default_value(), Value::String(s) if s == "hello"));
        assert!(matches!(bool_schema.default_value(), Value::Bool(true)));
        assert!(matches!(int_schema.default_value(), Value::Int(42)));
        assert!(matches!(entity_ref_schema.default_value(), Value::EntityReference(None)));
        assert!(matches!(entity_list_schema.default_value(), Value::EntityList(list) if list.is_empty()));
        
        // Test that we can't accidentally pass wrong types (this prevents type mismatches)
        // This would cause a compile error now:
        // let bad_schema = FieldSchema::String {
        //     field_type: FieldType::from("bad"),
        //     default_value: Value::Int(42),  // This would be a compile error!
        //     rank: 0,
        // };
    }

    #[test]
    fn test_fieldschema_choice_variant() {
        // Test the Choice variant which has an additional field
        let choice_schema = FieldSchema::Choice {
            field_type: FieldType::from("test_choice"),
            default_value: 1,  // i64 instead of Value::Choice
            rank: 0,
            choices: vec!["option1".to_string(), "option2".to_string()],
        };
        
        assert!(matches!(choice_schema.default_value(), Value::Choice(1)));
    }
}
