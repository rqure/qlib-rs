#![allow(unused_imports)]
use crate::*;

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
    // Test with EntityId
    let user_type = EntityType(1); // User
    let entity_id = Some(EntityId::new(user_type, 123));
    let ref_lit = sref!(entity_id);
    assert!(matches!(ref_lit, Some(Value::EntityReference(s)) if s == entity_id));
}

#[test]
fn test_entity_list_macro() {
    // Test empty list
    let empty = sreflist![];
    assert!(matches!(empty, Some(Value::EntityList(v)) if v.is_empty()));

    // Test list with multiple items
    let user_type = EntityType(1); // User
    let user1 = EntityId::new(user_type, 1);
    let user2 = EntityId::new(user_type, 2);
    let user3 = EntityId::new(user_type, 3);
    let multi = sreflist![user1, user2, user3];
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
    let epoch = epoch();
    let ts_epoch = stimestamp!(epoch);
    if let Some(Value::Timestamp(ts)) = ts_epoch {
        assert_eq!(ts, epoch);
    } else {
        panic!("Expected Some(Value::Timestamp)");
    }
}

#[test]
fn test_blob_macro() {
    // Test with empty vector
    let empty = sblob!(Vec::<u8>::new());
    if let Some(Value::Blob(data)) = empty {
        assert!(data.is_empty());
    } else {
        panic!("Expected Some(Value::Blob)");
    }

    // Test with some data
    let hello = vec![0x48, 0x65, 0x6C, 0x6C, 0x6F]; // "Hello" in ASCII
    let bin_hello = sblob!(hello.clone());
    if let Some(Value::Blob(data)) = bin_hello {
        assert_eq!(data, hello);
    } else {
        panic!("Expected Some(Value::Blob)");
    }

    // Test with larger binary data
    let large_data = vec![0u8; 1024]; // 1KB of zeros
    let bin_large = sblob!(large_data.clone());
    if let Some(Value::Blob(data)) = bin_large {
        assert_eq!(data.len(), 1024);
        assert_eq!(data, large_data);
    } else {
        panic!("Expected Some(Value::Blob)");
    }
}

#[test]
fn test_sread_macro() {
    // Use EntityType(1) and FieldType(1) directly for testing
    let entity_type = EntityType(1); // User
    let entity_id = EntityId::new(entity_type, 123);
    let field_type = FieldType(1); // Username
    let request = sread!(entity_id, vec![field_type]);

    match request {
        Request::Read {
            entity_id: req_entity_id,
            field_types: field_types,
            ..
        } => {
            assert_eq!(req_entity_id, entity_id);
            assert_eq!(field_types, vec![field_type]);
        }
        _ => panic!("Expected Request::Read"),
    }
}

#[test]
fn test_swrite_macro() {
    let entity_type = EntityType(1); // User
    let entity_id = EntityId::new(entity_type, 456);
    let ft_username = FieldType(1); // Username

    // Basic write with just a value
    let request1 = swrite!(entity_id, vec![ft_username], sstr!("alice"));
    match request1 {
        Request::Write {
            entity_id: req_entity_id,
            field_types: field_types,
            value,
            push_condition,
            adjust_behavior,
            write_time,
            writer_id,
            ..
        } => {
            assert_eq!(req_entity_id, entity_id);
            assert_eq!(field_types, vec![ft_username]);
            assert!(matches!(value, Some(Value::String(s)) if s == "alice"));
            assert!(matches!(push_condition, PushCondition::Always));
            assert!(matches!(adjust_behavior, AdjustBehavior::Set));
            assert!(write_time.is_none());
            assert!(writer_id.is_none());
        }
        _ => panic!("Expected Request::Write"),
    }

    // Write with None (deletion)
    let request2 = swrite!(entity_id, vec![ft_username], None);
    match request2 {
        Request::Write { value, .. } => assert!(value.is_none()),
        _ => panic!("Expected Request::Write"),
    }

    // Write with custom write option
    let request3 = swrite!(
        entity_id,
        vec![ft_username],
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
    let ft_last_login = FieldType(2); // LastLogin
    let request4 = swrite!(
        entity_id,
        vec![ft_last_login],
        stimestamp!(now),
        PushCondition::Always,
        Some(now)
    );
    match request4 {
        Request::Write { write_time, .. } => assert_eq!(write_time, Some(now)),
        _ => panic!("Expected Request::Write"),
    }

    // Write with writer
    let admin_type = EntityType(3); // Admin
    let writer_id = EntityId::new(admin_type, 1);
    let request5 = swrite!(
        entity_id,
        vec![ft_username],
        sstr!("carol"),
        PushCondition::Always,
        Some(now),
        Some(writer_id)
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
    let entity_type = EntityType(1); // User
    let entity_id = EntityId::new(entity_type, 456);
    let ft_counter = FieldType(3); // Counter

    // Basic add with just a value
    let request1 = sadd!(entity_id, vec![ft_counter], sint!(5));
    match request1 {
        Request::Write {
            entity_id: req_entity_id,
            field_types: field_types,
            value,
            push_condition,
            adjust_behavior,
            write_time,
            writer_id,
            ..
        } => {
            assert_eq!(req_entity_id, entity_id);
            assert_eq!(field_types, vec![ft_counter]);
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
        entity_id,
        vec![ft_counter],
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
        entity_id,
        vec![ft_counter],
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
    let admin_type = EntityType(3); // Admin
    let writer_id = EntityId::new(admin_type, 1);
    let request4 = sadd!(
        entity_id,
        vec![ft_counter],
        sint!(20),
        PushCondition::Always,
        Some(now),
        Some(writer_id)
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
    let ft_tags = FieldType(4); // Tags
    let tag_type = EntityType(4); // Tag
    let tag1 = EntityId::new(tag_type, 1);
    let tag2 = EntityId::new(tag_type, 2);
    let request5 = sadd!(
        entity_id,
        vec![ft_tags],
        sreflist![tag1, tag2]
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
    let entity_type = EntityType(1); // User
    let entity_id = EntityId::new(entity_type, 789);
    let ft_counter = FieldType(3); // Counter

    // Basic subtract with just a value
    let request1 = ssub!(entity_id, vec![ft_counter], sint!(3));
    match request1 {
        Request::Write {
            entity_id: req_entity_id,
            field_types: field_types,
            value,
            push_condition,
            adjust_behavior,
            write_time,
            writer_id,
            ..
        } => {
            assert_eq!(req_entity_id, entity_id);
            assert_eq!(field_types, vec![ft_counter]);
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
        entity_id,
        vec![ft_counter],
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
        entity_id,
        vec![ft_counter],
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
    let admin_type = EntityType(3); // Admin
    let writer_id = EntityId::new(admin_type, 1);
    let request4 = ssub!(
        entity_id,
        vec![ft_counter],
        sint!(10),
        PushCondition::Always,
        Some(now),
        Some(writer_id)
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
    let ft_tags = FieldType(4); // Tags
    let tag_type = EntityType(4); // Tag
    let tag1 = EntityId::new(tag_type, 1);
    let request5 = ssub!(entity_id, vec![ft_tags], sreflist![tag1]);
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

#[test]
fn test_screate_macro() {
    let et_user = EntityType(1); // User
    let parent_type = EntityType(5); // Parent
    let parent_id = EntityId::new(parent_type, 123);
    let entity_type = EntityType(2); // Entity
    let entity_id = EntityId::new(entity_type, 456);

    // Test basic create with just type and name
    let basic_create = screate!(et_user, "test_user".to_string());
    match basic_create {
        Request::Create { entity_type, parent_id: None, name, created_entity_id: None, timestamp: None, originator: None } => {
            assert_eq!(entity_type, et_user);
            assert_eq!(name, "test_user");
        }
        _ => panic!("Expected Request::Create with basic parameters"),
    }

    // Test create with parent ID
    let create_with_parent = screate!(et_user, "child_user".to_string(), parent_id);
    match create_with_parent {
        Request::Create { entity_type, parent_id: Some(pid), name, created_entity_id: None, timestamp: None, originator: None } => {
            assert_eq!(entity_type, et_user);
            assert_eq!(pid, parent_id);
            assert_eq!(name, "child_user");
        }
        _ => panic!("Expected Request::Create with parent ID"),
    }

    // Test create with parent ID and desired entity ID
    let create_with_ids = screate!(et_user, "specific_user".to_string(), parent_id, entity_id);
    match create_with_ids {
        Request::Create { entity_type, parent_id: Some(pid), name, created_entity_id: Some(eid), timestamp: None, originator: None } => {
            assert_eq!(entity_type, et_user);
            assert_eq!(pid, parent_id);
            assert_eq!(name, "specific_user");
            assert_eq!(eid, entity_id);
        }
        _ => panic!("Expected Request::Create with both IDs"),
    }
}

#[test]
fn test_sdelete_macro() {
    let entity_type = EntityType(2); // Entity
    let entity_id = EntityId::new(entity_type, 123);

    let delete_request = sdelete!(entity_id);
    match delete_request {
        Request::Delete { entity_id: eid, timestamp: None, originator: None } => {
            assert_eq!(eid, entity_id);
        }
        _ => panic!("Expected Request::Delete"),
    }
}

#[test]
fn test_sschemaupdate_macro() {
    // SchemaUpdate expects EntitySchema<Single, String, String>
    let schema = EntitySchema::<Single, String, String> {
        entity_type: "User".to_string(),
        inherit: vec![],
        fields: rustc_hash::FxHashMap::default(),
        _marker: std::marker::PhantomData,
    };

    let schema_update = sschemaupdate!(schema.clone());
    match schema_update {
        Request::SchemaUpdate { schema: s, timestamp: None, originator: None } => {
            assert_eq!(s.entity_type, "User".to_string());
        }
        _ => panic!("Expected Request::SchemaUpdate"),
    }
}
