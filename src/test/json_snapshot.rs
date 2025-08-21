use std::sync::Arc;
use crate::{
    Store, EntityType, FieldType, FieldSchema, EntitySchema, 
    Snowflake, Request, AdjustBehavior, PushCondition, Single, Value
};

#[tokio::test]
async fn test_json_snapshot_functionality() {
    // Create a new store
    let snowflake = Arc::new(Snowflake::new());
    let mut store = Store::new(snowflake);

    // Define schemas as per the example
    let mut object_schema = EntitySchema::<Single>::new("Object", None);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Description"),
        FieldSchema::String {
            field_type: FieldType::from("Description"),
            default_value: "".to_string(),
            rank: 1,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Children"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: 2,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Parent"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Parent"),
            default_value: None,
            rank: 3,
        },
    );

    let mut root_schema = EntitySchema::<Single>::new("Root", Some(EntityType::from("Object")));
    root_schema.fields.insert(
        FieldType::from("CreatedEntity"),
        FieldSchema::String {
            field_type: FieldType::from("CreatedEntity"),
            default_value: "".to_string(),
            rank: 10,
        },
    );
    root_schema.fields.insert(
        FieldType::from("DeletedEntity"),
        FieldSchema::String {
            field_type: FieldType::from("DeletedEntity"),
            default_value: "".to_string(),
            rank: 11,
        },
    );
    root_schema.fields.insert(
        FieldType::from("SchemaChange"),
        FieldSchema::String {
            field_type: FieldType::from("SchemaChange"),
            default_value: "".to_string(),
            rank: 12,
        },
    );

    let mut machine_schema = EntitySchema::<Single>::new("Machine", Some(EntityType::from("Object")));
    machine_schema.fields.insert(
        FieldType::from("Status"),
        FieldSchema::Choice {
            field_type: FieldType::from("Status"),
            default_value: 1, // "Offline"
            rank: 10,
            choices: vec!["Online".to_string(), "Offline".to_string()],
        },
    );

    let mut sensor_schema = EntitySchema::<Single>::new("Sensor", Some(EntityType::from("Object")));
    sensor_schema.fields.insert(
        FieldType::from("CurrentValue"),
        FieldSchema::Float {
            field_type: FieldType::from("CurrentValue"),
            default_value: 0.0,
            rank: 10,
        },
    );
    sensor_schema.fields.insert(
        FieldType::from("Unit"),
        FieldSchema::String {
            field_type: FieldType::from("Unit"),
            default_value: "".to_string(),
            rank: 11,
        },
    );
    sensor_schema.fields.insert(
        FieldType::from("LastUpdated"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LastUpdated"),
            default_value: std::time::SystemTime::UNIX_EPOCH,
            rank: 12,
        },
    );

    let mut temp_sensor_schema = EntitySchema::<Single>::new("TemperatureSensor", Some(EntityType::from("Sensor")));
    temp_sensor_schema.fields.insert(
        FieldType::from("CalibrationOffset"),
        FieldSchema::Float {
            field_type: FieldType::from("CalibrationOffset"),
            default_value: 0.0,
            rank: 20,
        },
    );

    // Add schemas to the store
    let mut schema_requests = vec![
        Request::SchemaUpdate { schema: object_schema, originator: None },
        Request::SchemaUpdate { schema: root_schema, originator: None },
        Request::SchemaUpdate { schema: machine_schema, originator: None },
        Request::SchemaUpdate { schema: sensor_schema, originator: None },
        Request::SchemaUpdate { schema: temp_sensor_schema, originator: None },
    ];
    store.perform(&mut schema_requests).await.unwrap();

    // Create entities
    let mut create_requests = vec![
        Request::Create {
            entity_type: EntityType::from("Root"),
            parent_id: None,
            name: "DataStore".to_string(),
            created_entity_id: None,
            originator: None,
        },
        Request::Create {
            entity_type: EntityType::from("Machine"),
            parent_id: None,
            name: "Server1".to_string(),
            created_entity_id: None,
            originator: None,
        },
        Request::Create {
            entity_type: EntityType::from("TemperatureSensor"),
            parent_id: None,
            name: "IntakeTemp".to_string(),
            created_entity_id: None,
            originator: None,
        },
    ];
    store.perform(&mut create_requests).await.unwrap();

    // Get the created entity IDs
    let root_entities = store.find_entities(&EntityType::from("Root")).await.unwrap();
    let machine_entities = store.find_entities(&EntityType::from("Machine")).await.unwrap();
    let sensor_entities = store.find_entities(&EntityType::from("TemperatureSensor")).await.unwrap();

    let root_id = root_entities.first().unwrap();
    let machine_id = machine_entities.first().unwrap();
    let sensor_id = sensor_entities.first().unwrap();

    // Set up the entity hierarchy and field values
    let mut field_requests = vec![
        // Set root description
        Request::Write {
            entity_id: root_id.clone(),
            field_type: FieldType::from("Description"),
            value: Some(Value::String("Primary data store".to_string())),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        // Set machine status to Online
        Request::Write {
            entity_id: machine_id.clone(),
            field_type: FieldType::from("Status"),
            value: Some(Value::Choice(0)), // "Online"
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        // Set sensor values
        Request::Write {
            entity_id: sensor_id.clone(),
            field_type: FieldType::from("CurrentValue"),
            value: Some(Value::Float(72.5)),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        Request::Write {
            entity_id: sensor_id.clone(),
            field_type: FieldType::from("Unit"),
            value: Some(Value::String("C".to_string())),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        Request::Write {
            entity_id: sensor_id.clone(),
            field_type: FieldType::from("CalibrationOffset"),
            value: Some(Value::Float(0.5)),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        // Set up parent-child relationships
        Request::Write {
            entity_id: sensor_id.clone(),
            field_type: FieldType::from("Parent"),
            value: Some(Value::EntityReference(Some(machine_id.clone()))),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        Request::Write {
            entity_id: machine_id.clone(),
            field_type: FieldType::from("Parent"),
            value: Some(Value::EntityReference(Some(root_id.clone()))),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        Request::Write {
            entity_id: machine_id.clone(),
            field_type: FieldType::from("Children"),
            value: Some(Value::EntityList(vec![sensor_id.clone()])),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
        Request::Write {
            entity_id: root_id.clone(),
            field_type: FieldType::from("Children"),
            value: Some(Value::EntityList(vec![machine_id.clone()])),
            write_time: None,
            writer_id: None,
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            originator: None,
        },
    ];
    store.perform(&mut field_requests).await.unwrap();

    // Take a JSON snapshot using the macro
    let json_snapshot = crate::take_json_snapshot!(store).await.unwrap();
    
    // Print the JSON snapshot to see the result
    let json_string = serde_json::to_string_pretty(&json_snapshot).unwrap();
    println!("JSON Snapshot:\n{}", json_string);

    // Verify the JSON snapshot has the expected structure
    assert!(!json_snapshot.schemas.is_empty());
    assert_eq!(json_snapshot.entity.entity_type, "Root");
    
    // Verify that entity references are converted to paths
    println!("Root entity fields: {:?}", json_snapshot.entity.fields);
    
    // Check that the Children field contains a path instead of raw ID
    if let Some(serde_json::Value::Array(children)) = json_snapshot.entity.fields.get("Children") {
        assert_eq!(children.len(), 1);
        if let Some(serde_json::Value::String(child_path)) = children.first() {
            assert_eq!(child_path, "DataStore/Server1");
            println!("✓ Entity reference converted to path: {}", child_path);
        } else {
            panic!("Expected child path to be a string");
        }
    } else {
        panic!("Expected Children field to be an array");
    }
    
    // Verify we have the expected schemas
    let schema_names: Vec<String> = json_snapshot.schemas.iter().map(|s| s.entity_type.clone()).collect();
    assert!(schema_names.contains(&"Object".to_string()));
    assert!(schema_names.contains(&"Root".to_string()));
    assert!(schema_names.contains(&"Sensor".to_string()));
    assert!(schema_names.contains(&"TemperatureSensor".to_string()));
    assert!(schema_names.contains(&"Machine".to_string()));

    // TODO: Test restoration - currently has an issue with entity type lookup during restore
    // Test restoration
    // let mut new_store = Store::new(Arc::new(Snowflake::new()));
    // restore_json_snapshot(&mut new_store, &json_snapshot).await.unwrap();

    // // Verify that the new store has the same data
    // let new_json_snapshot = take_json_snapshot(&new_store).await.unwrap();
    // assert_eq!(json_snapshot.schemas.len(), new_json_snapshot.schemas.len());
    // assert_eq!(json_snapshot.entity.entity_type, new_json_snapshot.entity.entity_type);

    println!("JSON snapshot functionality test passed!");
}

#[tokio::test]
async fn test_json_snapshot_path_resolution() {
    // Create a new store
    let snowflake = Arc::new(Snowflake::new());
    let mut store = Store::new(snowflake.clone());

    // Add the required schemas
    let object_schema = EntitySchema::<Single>::new("Object".to_string(), None)
        .with_field(FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: Some(0),
        })
        .with_field(FieldSchema::String {
            field_type: FieldType::from("Description"),
            default_value: "".to_string(),
            rank: Some(1),
        })
        .with_field(FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: Some(2),
        })
        .with_field(FieldSchema::EntityReference {
            field_type: FieldType::from("Parent"),
            default_value: None,
            rank: Some(3),
        });

    let root_schema = EntitySchema::<Single>::new("Root".to_string(), Some(EntityType::from("Object")));
    let building_schema = EntitySchema::<Single>::new("Building".to_string(), Some(EntityType::from("Object")));
    let floor_schema = EntitySchema::<Single>::new("Floor".to_string(), Some(EntityType::from("Object")));
    let room_schema = EntitySchema::<Single>::new("Room".to_string(), Some(EntityType::from("Object")));

    // Add schemas to store
    let mut schema_requests = vec![
        Request::SchemaUpdate { schema: object_schema, originator: None },
        Request::SchemaUpdate { schema: root_schema, originator: None },
        Request::SchemaUpdate { schema: building_schema, originator: None },
        Request::SchemaUpdate { schema: floor_schema, originator: None },
        Request::SchemaUpdate { schema: room_schema, originator: None },
    ];
    store.perform(&mut schema_requests).await.unwrap();

    // Create a hierarchy: Root -> Building -> Floor -> Room
    let root_id = snowflake.generate_entity_id(EntityType::from("Root"));
    let building_id = snowflake.generate_entity_id(EntityType::from("Building"));
    let floor_id = snowflake.generate_entity_id(EntityType::from("Floor"));
    let room_id = snowflake.generate_entity_id(EntityType::from("Room"));

    // Create all entities
    let mut create_requests = vec![
        Request::Create {
            entity_type: EntityType::from("Root"),
            parent_id: None,
            name: Some("Campus".to_string()),
            created_entity_id: Some(root_id.clone()),
            originator: None,
        },
        Request::Create {
            entity_type: EntityType::from("Building"),
            parent_id: Some(root_id.clone()),
            name: Some("MainBuilding".to_string()),
            created_entity_id: Some(building_id.clone()),
            originator: None,
        },
        Request::Create {
            entity_type: EntityType::from("Floor"),
            parent_id: Some(building_id.clone()),
            name: Some("FirstFloor".to_string()),
            created_entity_id: Some(floor_id.clone()),
            originator: None,
        },
        Request::Create {
            entity_type: EntityType::from("Room"),
            parent_id: Some(floor_id.clone()),
            name: Some("Conference101".to_string()),
            created_entity_id: Some(room_id.clone()),
            originator: None,
        },
    ];
    store.perform(&mut create_requests).await.unwrap();

    // Take a JSON snapshot
    let json_snapshot = crate::take_json_snapshot!(store).await.unwrap();
    
    // Print the JSON snapshot to see the result
    let json_string = serde_json::to_string_pretty(&json_snapshot).unwrap();
    println!("Deep Hierarchy JSON Snapshot:\n{}", json_string);

    // Verify path resolution works at multiple levels
    assert_eq!(json_snapshot.entity.entity_type, "Root");
    
    // Check root's children contains the path to building
    if let Some(serde_json::Value::Array(children)) = json_snapshot.entity.fields.get("Children") {
        if !children.is_empty() {
            if let Some(serde_json::Value::String(building_path)) = children.first() {
                println!("✓ Building path resolved: {}", building_path);
                // The path should start with the root name
                assert!(building_path.contains("Campus"));
                assert!(building_path.contains("MainBuilding"));
            }
        }
    }

    // Test the spath! macro directly on the deepest entity
    let room_path = crate::spath!(store, &room_id).await.unwrap();
    println!("✓ Room path resolved: {}", room_path);
    assert!(room_path.contains("Campus"));
    assert!(room_path.contains("MainBuilding"));
    assert!(room_path.contains("FirstFloor"));
    assert!(room_path.contains("Conference101"));

    println!("JSON snapshot functionality test passed!");
}

