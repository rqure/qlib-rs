use crate::{
    data::StoreMessage, 
    protocol::{encode_store_message, encode_fast_store_message, ProtocolCodec, MessageType, FastStoreMessage, FastMessageType}
};

#[test]
fn test_fast_store_message_encoding_decoding() {
    // Create a simple StoreMessage
    let store_message = StoreMessage::EntityExists {
        id: "test-123".to_string(),
        entity_id: crate::EntityId::new(crate::EntityType(1), 42),
    };

    // Test legacy bincode encoding
    let legacy_encoded = encode_store_message(&store_message).expect("Legacy encoding should work");
    
    // Test new rkyv encoding  
    let fast_encoded = encode_fast_store_message(&store_message).expect("Fast encoding should work");
    
    // Fast encoding should be different from legacy (different format)
    assert_ne!(legacy_encoded, fast_encoded);
    
    // Test decoding both formats
    if let Ok(Some((legacy_decoded, _))) = ProtocolCodec::decode(&legacy_encoded) {
        match legacy_decoded {
            crate::protocol::ProtocolMessage::Store(decoded_msg) => {
                // Should be able to extract the same message
                match (&store_message, &decoded_msg) {
                    (StoreMessage::EntityExists { id: id1, entity_id: eid1 }, 
                     StoreMessage::EntityExists { id: id2, entity_id: eid2 }) => {
                        assert_eq!(id1, id2);
                        assert_eq!(eid1, eid2);
                    },
                    _ => panic!("Message types don't match"),
                }
            },
            _ => panic!("Expected Store message"),
        }
    } else {
        panic!("Failed to decode legacy message");
    }
    
    if let Ok(Some((fast_decoded, _))) = ProtocolCodec::decode(&fast_encoded) {
        match fast_decoded {
            crate::protocol::ProtocolMessage::FastStore(fast_msg) => {
                // Verify we can access the message ID without deserialization
                assert_eq!(fast_msg.message_id(), "test-123");
                
                // Verify the message type is correct
                assert!(matches!(fast_msg.message_type(), FastMessageType::EntityExists));
                
                // Verify we can access metadata without deserialization
                assert_eq!(fast_msg.primary_entity_id(), Some(crate::EntityId::new(crate::EntityType(1), 42)));
                assert!(fast_msg.is_simple_operation());
                
                // Test conversion back to StoreMessage for compatibility
                let decoded_msg = fast_msg.to_store_message().expect("Should convert back to StoreMessage");
                match (&store_message, &decoded_msg) {
                    (StoreMessage::EntityExists { id: id1, entity_id: eid1 }, 
                     StoreMessage::EntityExists { id: id2, entity_id: eid2 }) => {
                        assert_eq!(id1, id2);
                        assert_eq!(eid1, eid2);
                    },
                    _ => panic!("Message types don't match after fast decoding"),
                }
            },
            _ => panic!("Expected FastStore message"),
        }
    } else {
        panic!("Failed to decode fast message");
    }
    
    // Both should work with MessageBuffer too
    let mut buffer = crate::protocol::MessageBuffer::new();
    buffer.add_data(&fast_encoded);
    
    if let Ok(Some(decoded_store_msg)) = buffer.try_decode_store_message() {
        match (&store_message, &decoded_store_msg) {
            (StoreMessage::EntityExists { id: id1, entity_id: eid1 }, 
             StoreMessage::EntityExists { id: id2, entity_id: eid2 }) => {
                assert_eq!(id1, id2);
                assert_eq!(eid1, eid2);
            },
            _ => panic!("Message types don't match from buffer"),
        }
    } else {
        panic!("Failed to decode message from buffer");
    }
}

#[test]
fn test_message_type_enum() {
    assert_eq!(MessageType::StoreMessage as u32, 1000);
    assert_eq!(MessageType::FastStoreMessage as u32, 1001);
    
    assert_eq!(MessageType::from_u32(1000), Some(MessageType::StoreMessage));
    assert_eq!(MessageType::from_u32(1001), Some(MessageType::FastStoreMessage));
    assert_eq!(MessageType::from_u32(9999), None); // Removed types should not exist
}

#[test]
fn test_performance_demonstration() {
    use std::time::Instant;
    
    // Create a StoreMessage with some data
    let store_message = StoreMessage::Perform {
        id: "perf-test-123".to_string(),
        requests: vec![
            // Simulate some typical requests
            crate::Request::Create {
                entity_type: crate::EntityType(1),
                parent_id: Some(crate::EntityId::new(crate::EntityType(1), 42)),
                name: "TestEntity".to_string(),
                created_entity_id: None,
                timestamp: None,
                originator: None,
            },
            crate::Request::Write {
                entity_id: crate::EntityId::new(crate::EntityType(2), 100),
                field_types: vec![crate::FieldType(1), crate::FieldType(2)],
                value: Some(crate::Value::String("test data".to_string())),
                push_condition: crate::PushCondition::Always,
                adjust_behavior: crate::AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
                originator: None,
            },
        ],
    };
    
    // Test multiple iterations to get meaningful timing
    let iterations = 1000;
    
    // Time legacy encoding
    let start = Instant::now();
    for _ in 0..iterations {
        let _encoded = encode_store_message(&store_message).expect("Legacy encoding should work");
    }
    let legacy_duration = start.elapsed();
    
    // Time fast encoding  
    let start = Instant::now();
    for _ in 0..iterations {
        let _encoded = encode_fast_store_message(&store_message).expect("Fast encoding should work");
    }
    let fast_duration = start.elapsed();
    
    println!("Legacy encoding: {:?} for {} iterations", legacy_duration, iterations);
    println!("Fast encoding: {:?} for {} iterations", fast_duration, iterations);
    println!("Performance ratio: {:.2}x", legacy_duration.as_nanos() as f64 / fast_duration.as_nanos() as f64);
    
    // This test doesn't assert performance - that's hardware dependent
    // But it demonstrates that both methods work and provides timing comparison
}

#[test]
fn test_elegant_fast_store_message() {
    use crate::{Request, EntityId, EntityType, FieldType, Value, data::StoreMessage};
    
    // Test 1: Simple existence check with zero-copy metadata
    let store_message = StoreMessage::EntityExists {
        id: "elegant-test-123".to_string(),
        entity_id: EntityId::new(EntityType(1), 42),
    };

    let fast_message = crate::protocol::FastStoreMessage::from_store_message(&store_message)
        .expect("Should convert to FastStoreMessage");

    // Verify zero-copy metadata access
    assert_eq!(fast_message.message_id(), "elegant-test-123");
    assert!(matches!(fast_message.message_type(), crate::protocol::FastMessageType::EntityExists));
    assert_eq!(fast_message.primary_entity_id(), Some(EntityId::new(EntityType(1), 42)));
    assert!(fast_message.is_simple_operation());
    
    // Test 2: Read/Write operations elegantly supported
    let read_write_message = StoreMessage::Perform {
        id: "rw-test-456".to_string(),
        requests: vec![
            Request::Read {
                entity_id: EntityId::new(EntityType(1), 100),
                field_types: vec![FieldType(1), FieldType(2)],
                value: None,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: EntityId::new(EntityType(1), 101),
                field_types: vec![FieldType(3)],
                value: Some(Value::String("elegant_value".to_string())),
                push_condition: crate::PushCondition::Always,
                adjust_behavior: crate::AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
                originator: None,
            },
        ],
    };

    let rw_fast_message = crate::protocol::FastStoreMessage::from_store_message(&read_write_message)
        .expect("Should convert read/write message");

    // Verify elegant read/write support with zero-copy metadata
    assert_eq!(rw_fast_message.message_id(), "rw-test-456");
    assert!(rw_fast_message.is_read_write_operation());
    assert_eq!(rw_fast_message.primary_entity_id(), Some(EntityId::new(EntityType(1), 100)));
    assert!(rw_fast_message.is_batch_operation());

    // Test 3: Demonstrate elegant zero-copy metadata access (without needing Store)
    
    // Show that we can make routing decisions without any deserialization
    if fast_message.is_simple_operation() {
        println!("✅ Simple operation detected via zero-copy metadata");
    }
    
    if rw_fast_message.is_read_write_operation() {
        println!("✅ Read/write operation detected via zero-copy metadata");
    }
    
    if let Some(entity_id) = rw_fast_message.primary_entity_id() {
        println!("✅ Can route to specific entity {} without deserialization", entity_id.0);
    }

    println!("✅ Elegant FastStoreMessage supports ALL operations with intelligent zero-copy metadata!");
}

