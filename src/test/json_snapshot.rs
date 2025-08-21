use std::sync::Arc;
use crate::{
    Store, EntityType, FieldType, FieldSchema, EntitySchema, EntityId, Request,
    Snowflake, Single, take_json_snapshot, sschemaupdate, screate, swrite, Value
};

#[tokio::test]
async fn test_json_snapshot_functionality() {
    // Create a new store
    let snowflake = Arc::new(Snowflake::new());
    let mut store = Store::new(snowflake.clone());

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
            default_value: std::time::UNIX_EPOCH,
            rank: 12,
        },
    );

    let mut temp_sensor_schema = EntitySchema::<Single>::new("TemperatureSensor", Some(EntityType::from("Sensor")));
    temp_sensor_schema.fields.insert(
        FieldType::from("CalibrationOffset"),
        FieldSchema::Float {
            field_type: FieldType::from("CalibrationOffset"),
            default_value: 0.0,
            rank: 13,
        },
    );

    // Add schemas to the store
    let mut schema_requests = vec![
        sschemaupdate!(object_schema),
        sschemaupdate!(root_schema),
        sschemaupdate!(machine_schema),
        sschemaupdate!(sensor_schema),
        sschemaupdate!(temp_sensor_schema),
    ];
    store.perform(&mut schema_requests).await.unwrap();

    // Create entities
    let root_id = EntityId::new("Root", snowflake.generate());
    let machine_id = EntityId::new("Machine", snowflake.generate());
    let sensor_id = EntityId::new("TemperatureSensor", snowflake.generate());

    let mut create_requests = vec![
        Request::Create {
            entity_type: EntityType::from("Root"),
            parent_id: None,
            name: "DataStore".to_string(),
            created_entity_id: Some(root_id.clone()),
            originator: None,
        },
        screate!(EntityType::from("Machine"), "Server1".to_string(), root_id.clone(), machine_id.clone()),
        screate!(EntityType::from("TemperatureSensor"), "IntakeTemp".to_string(), machine_id.clone(), sensor_id.clone()),
    ];
    store.perform(&mut create_requests).await.unwrap();

    // Set field values
    let mut field_requests = vec![
        swrite!(root_id.clone(), FieldType::from("Name"), Some(Value::String("DataStore".to_string()))),
        swrite!(root_id.clone(), FieldType::from("Description"), Some(Value::String("Primary data store".to_string()))),
        swrite!(root_id.clone(), FieldType::from("Children"), Some(Value::EntityList(vec![machine_id.clone()]))),
        
        swrite!(machine_id.clone(), FieldType::from("Name"), Some(Value::String("Server1".to_string()))),
        swrite!(machine_id.clone(), FieldType::from("Status"), Some(Value::Choice(0))), // "Online"
        swrite!(machine_id.clone(), FieldType::from("Children"), Some(Value::EntityList(vec![sensor_id.clone()]))),
        
        swrite!(sensor_id.clone(), FieldType::from("Name"), Some(Value::String("IntakeTemp".to_string()))),
        swrite!(sensor_id.clone(), FieldType::from("CurrentValue"), Some(Value::Float(72.5))),
        swrite!(sensor_id.clone(), FieldType::from("Unit"), Some(Value::String("C".to_string()))),
        swrite!(sensor_id.clone(), FieldType::from("CalibrationOffset"), Some(Value::Float(0.5))),
    ];
    store.perform(&mut field_requests).await.unwrap();

    // Take JSON snapshot
    let snapshot = take_json_snapshot!(store).await.unwrap();
    
    // Verify the snapshot structure matches the expected format
    assert_eq!(snapshot.schemas.len(), 5);
    
    // Check that schemas are properly sorted
    let schema_names: Vec<&str> = snapshot.schemas.iter().map(|s| s.entity_type.as_str()).collect();
    let mut expected_names = vec!["Machine", "Object", "Root", "Sensor", "TemperatureSensor"];
    expected_names.sort();
    assert_eq!(schema_names, expected_names);
    
    // Verify the tree structure
    assert_eq!(snapshot.tree.entity_type, "Root");
    assert_eq!(snapshot.tree.fields.get("Name").unwrap().as_str().unwrap(), "DataStore");
    assert_eq!(snapshot.tree.fields.get("Description").unwrap().as_str().unwrap(), "Primary data store");
    
    // Verify children are nested entities, not paths
    let children = snapshot.tree.fields.get("Children").unwrap().as_array().unwrap();
    assert_eq!(children.len(), 1);
    
    let machine = &children[0];
    assert_eq!(machine.get("_entityType").unwrap().as_str().unwrap(), "Machine");
    assert_eq!(machine.get("Name").unwrap().as_str().unwrap(), "Server1");
    assert_eq!(machine.get("Status").unwrap().as_str().unwrap(), "Online");
    
    let machine_children = machine.get("Children").unwrap().as_array().unwrap();
    assert_eq!(machine_children.len(), 1);
    
    let sensor = &machine_children[0];
    assert_eq!(sensor.get("_entityType").unwrap().as_str().unwrap(), "TemperatureSensor");
    assert_eq!(sensor.get("Name").unwrap().as_str().unwrap(), "IntakeTemp");
    assert_eq!(sensor.get("CurrentValue").unwrap().as_f64().unwrap(), 72.5);
    assert_eq!(sensor.get("Unit").unwrap().as_str().unwrap(), "C");
    assert_eq!(sensor.get("CalibrationOffset").unwrap().as_f64().unwrap(), 0.5);

    // Print the snapshot for visual verification
    let json_str = serde_json::to_string_pretty(&snapshot).unwrap();
    println!("JSON Snapshot:\n{}", json_str);
}

#[tokio::test]
async fn test_json_snapshot_path_resolution() {
    // This test ensures that normal entity references (not Children) show paths
    // while Children show nested entity objects
    
    // TODO: Add test for path resolution vs nested object behavior
    // This would test that EntityReference fields show paths like "Root/Server1"
    // while Children fields show nested entity objects
}
