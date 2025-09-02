
#[allow(unused_imports)]
use std::sync::Arc;

#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::StoreTrait;

#[allow(unused_imports)]
use crate::{restore_json_snapshot, screate, sschemaupdate, swrite, take_json_snapshot, EntitySchema, EntityType, FieldSchema, FieldType, Request, Single, Snowflake, AsyncStore, Value};


#[tokio::test]
async fn test_json_snapshot_functionality() {
    // Create a new store
    let snowflake = Arc::new(Snowflake::new());
    let mut store = AsyncStore::new(snowflake.clone());

    // Define schemas as per the example
    let mut object_schema = EntitySchema::<Single>::new("Object", vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Description"),
        FieldSchema::String {
            field_type: FieldType::from("Description"),
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Children"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Configuration,
        },
    );

    let mut root_schema = EntitySchema::<Single>::new("Root", vec![EntityType::from("Object")]);
    root_schema.fields.insert(
        FieldType::from("CreatedEntity"),
        FieldSchema::String {
            field_type: FieldType::from("CreatedEntity"),
            default_value: "".to_string(),
            rank: 10,
            storage_scope: StorageScope::Runtime,
        },
    );
    root_schema.fields.insert(
        FieldType::from("DeletedEntity"),
        FieldSchema::String {
            field_type: FieldType::from("DeletedEntity"),
            default_value: "".to_string(),
            rank: 11,
            storage_scope: StorageScope::Runtime,
        },
    );
    root_schema.fields.insert(
        FieldType::from("SchemaChange"),
        FieldSchema::String {
            field_type: FieldType::from("SchemaChange"),
            default_value: "".to_string(),
            rank: 12,
            storage_scope: StorageScope::Runtime,
        },
    );

    let mut machine_schema = EntitySchema::<Single>::new("Machine", vec![EntityType::from("Object")]);
    machine_schema.fields.insert(
        FieldType::from("Status"),
        FieldSchema::Choice {
            field_type: FieldType::from("Status"),
            default_value: 1, // "Offline"
            rank: 10,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Online".to_string(), "Offline".to_string()],
        },
    );

    let mut sensor_schema = EntitySchema::<Single>::new("Sensor", vec![EntityType::from("Object")]);
    sensor_schema.fields.insert(
        FieldType::from("CurrentValue"),
        FieldSchema::Float {
            field_type: FieldType::from("CurrentValue"),
            default_value: 0.0,
            rank: 10,
            storage_scope: StorageScope::Runtime,
        },
    );
    sensor_schema.fields.insert(
        FieldType::from("Unit"),
        FieldSchema::String {
            field_type: FieldType::from("Unit"),
            default_value: "".to_string(),
            rank: 11,
            storage_scope: StorageScope::Configuration,
        },
    );
    sensor_schema.fields.insert(
        FieldType::from("LastUpdated"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("LastUpdated"),
            default_value: time::OffsetDateTime::UNIX_EPOCH,
            rank: 12,
            storage_scope: StorageScope::Runtime,
        },
    );

    let mut temp_sensor_schema = EntitySchema::<Single>::new("TemperatureSensor", vec![EntityType::from("Sensor")]);
    temp_sensor_schema.fields.insert(
        FieldType::from("CalibrationOffset"),
        FieldSchema::Float {
            field_type: FieldType::from("CalibrationOffset"),
            default_value: 0.0,
            rank: 13,
            storage_scope: StorageScope::Configuration,
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
    store.perform_mut(&mut schema_requests).await.unwrap();

    // Create entities - let the store generate IDs
    let mut create_requests = vec![
        Request::Create {
            entity_type: EntityType::from("Root"),
            parent_id: None,
            name: "DataStore".to_string(),
            created_entity_id: None,
            timestamp: None,
            originator: None,
        },
    ];
    store.perform_mut(&mut create_requests).await.unwrap();
    
    // Get the actual created root ID
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let mut machine_create_requests = vec![
        screate!(EntityType::from("Machine"), "Server1".to_string(), root_id.clone()),
    ];
    store.perform_mut(&mut machine_create_requests).await.unwrap();
    
    // Get the actual created machine ID
    let machine_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = machine_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created machine entity ID");
    };

    let mut sensor_create_requests = vec![
        screate!(EntityType::from("TemperatureSensor"), "IntakeTemp".to_string(), machine_id.clone()),
    ];
    store.perform_mut(&mut sensor_create_requests).await.unwrap();
    
    // Get the actual created sensor ID
    let sensor_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = sensor_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created sensor entity ID");
    };

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
    store.perform_mut(&mut field_requests).await.unwrap();

    // Take JSON snapshot
    let snapshot = take_json_snapshot(&mut store).await.unwrap();
    
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
    assert_eq!(machine.get("entityType").unwrap().as_str().unwrap(), "Machine");
    assert_eq!(machine.get("Name").unwrap().as_str().unwrap(), "Server1");
    assert_eq!(machine.get("Status").unwrap().as_str().unwrap(), "Online");
    
    let machine_children = machine.get("Children").unwrap().as_array().unwrap();
    assert_eq!(machine_children.len(), 1);
    
    let sensor = &machine_children[0];
    assert_eq!(sensor.get("entityType").unwrap().as_str().unwrap(), "TemperatureSensor");
    assert_eq!(sensor.get("Name").unwrap().as_str().unwrap(), "IntakeTemp");
    // CurrentValue is a Runtime field and not included in snapshots
    assert_eq!(sensor.get("Unit").unwrap().as_str().unwrap(), "C");
    assert_eq!(sensor.get("CalibrationOffset").unwrap().as_f64().unwrap(), 0.5);

    // Print the snapshot for visual verification
    let json_str = serde_json::to_string_pretty(&snapshot).unwrap();
    println!("JSON Snapshot:\n{}", json_str);
}

#[tokio::test]
async fn test_json_snapshot_restore() {
    // Create and populate the first store
    let snowflake1 = Arc::new(Snowflake::new());
    let mut store1 = AsyncStore::new(snowflake1.clone());

    // Define schemas
    let mut object_schema = EntitySchema::<Single>::new("Object", vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Description"),
        FieldSchema::String {
            field_type: FieldType::from("Description"),
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Children"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Configuration,
        },
    );

    let mut root_schema = EntitySchema::<Single>::new("Root", vec![EntityType::from("Object")]);
    root_schema.fields.insert(
        FieldType::from("Status"),
        FieldSchema::String {
            field_type: FieldType::from("Status"),
            default_value: "Active".to_string(),
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );

    let mut document_schema = EntitySchema::<Single>::new("Document", vec![EntityType::from("Object")]);
    document_schema.fields.insert(
        FieldType::from("Content"),
        FieldSchema::String {
            field_type: FieldType::from("Content"),
            default_value: "".to_string(),
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas to store1
    let mut schema_requests = vec![
        sschemaupdate!(object_schema),
        sschemaupdate!(root_schema),
        sschemaupdate!(document_schema),
    ];
    store1.perform_mut(&mut schema_requests).await.unwrap();

    // Create entities in store1
    let mut create_requests = vec![
        Request::Create {
            entity_type: EntityType::from("Root"),
            parent_id: None,
            name: "TestRoot".to_string(),
            created_entity_id: None,
            timestamp: None,
            originator: None,
        },
    ];
    store1.perform_mut(&mut create_requests).await.unwrap();
    
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let mut doc_create_requests = vec![
        screate!(EntityType::from("Document"), "TestDoc".to_string(), root_id.clone()),
    ];
    store1.perform_mut(&mut doc_create_requests).await.unwrap();
    
    let doc_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = doc_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created document entity ID");
    };

    // Set field values in store1
    let mut field_requests = vec![
        swrite!(root_id.clone(), FieldType::from("Name"), Some(Value::String("TestRoot".to_string()))),
        swrite!(root_id.clone(), FieldType::from("Description"), Some(Value::String("Test root entity".to_string()))),
        swrite!(root_id.clone(), FieldType::from("Status"), Some(Value::String("Active".to_string()))),
        swrite!(root_id.clone(), FieldType::from("Children"), Some(Value::EntityList(vec![doc_id.clone()]))),
        
        swrite!(doc_id.clone(), FieldType::from("Name"), Some(Value::String("TestDoc".to_string()))),
        swrite!(doc_id.clone(), FieldType::from("Description"), Some(Value::String("Test document".to_string()))),
        swrite!(doc_id.clone(), FieldType::from("Content"), Some(Value::String("Hello, World!".to_string()))),
    ];
    store1.perform_mut(&mut field_requests).await.unwrap();

    // Take JSON snapshot from store1
    let snapshot = take_json_snapshot(&mut store1).await.unwrap();

    // Create a new empty store
    let snowflake2 = Arc::new(Snowflake::new());
    let mut store2 = AsyncStore::new(snowflake2.clone());

    // Restore the snapshot to store2
    restore_json_snapshot(&mut store2, &snapshot).await.unwrap();

    // Verify that store2 now contains the same data
    let entities = store2.find_entities(&EntityType::from("Root"), None).await.unwrap();
    assert_eq!(entities.len(), 1);
    
    let root_id_restored = &entities[0];
    
    // Check root entity fields
    let mut read_requests = vec![
        crate::sread!(root_id_restored.clone(), FieldType::from("Name")),
        crate::sread!(root_id_restored.clone(), FieldType::from("Description")),
        crate::sread!(root_id_restored.clone(), FieldType::from("Status")),
        crate::sread!(root_id_restored.clone(), FieldType::from("Children")),
    ];
    store2.perform_mut(&mut read_requests).await.unwrap();
    
    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = read_requests.get(0) {
        assert_eq!(name, "TestRoot");
    } else {
        panic!("Failed to read root name");
    }
    
    if let Some(Request::Read { value: Some(Value::String(desc)), .. }) = read_requests.get(1) {
        assert_eq!(desc, "Test root entity");
    } else {
        panic!("Failed to read root description");
    }
    
    if let Some(Request::Read { value: Some(Value::String(status)), .. }) = read_requests.get(2) {
        assert_eq!(status, "Active");
    } else {
        panic!("Failed to read root status");
    }
    
    if let Some(Request::Read { value: Some(Value::EntityList(children)), .. }) = read_requests.get(3) {
        assert_eq!(children.len(), 1);
        
        // Check the document entity
        let doc_id_restored = &children[0];
        let mut doc_read_requests = vec![
            crate::sread!(doc_id_restored.clone(), FieldType::from("Name")),
            crate::sread!(doc_id_restored.clone(), FieldType::from("Content")),
        ];
        store2.perform_mut(&mut doc_read_requests).await.unwrap();
        
        if let Some(Request::Read { value: Some(Value::String(doc_name)), .. }) = doc_read_requests.get(0) {
            assert_eq!(doc_name, "TestDoc");
        } else {
            panic!("Failed to read document name");
        }
        
        if let Some(Request::Read { value: Some(Value::String(content)), .. }) = doc_read_requests.get(1) {
            assert_eq!(content, "Hello, World!");
        } else {
            panic!("Failed to read document content");
        }
    } else {
        panic!("Failed to read root children");
    }

    println!("JSON snapshot restore test passed successfully!");
}

#[tokio::test]
async fn test_json_snapshot_path_resolution() {
    // This test ensures that normal entity references (not Children) show paths
    // while Children fields show nested entity objects
    
    let snowflake = Arc::new(Snowflake::new());
    let mut store = AsyncStore::new(snowflake.clone());

    // Define schemas
    let mut object_schema = EntitySchema::<Single>::new("Object", vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        FieldType::from("Children"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Children"),
            default_value: vec![],
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );

    let root_schema = EntitySchema::<Single>::new("Root", vec![EntityType::from("Object")]);

    let mut folder_schema = EntitySchema::<Single>::new("Folder", vec![EntityType::from("Object")]);
    folder_schema.fields.insert(
        FieldType::from("Parent"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Parent"),
            default_value: None,
            rank: 5,
            storage_scope: StorageScope::Configuration,
        },
    );
    
    let mut file_schema = EntitySchema::<Single>::new("File", vec![EntityType::from("Object")]);
    file_schema.fields.insert(
        FieldType::from("ParentFolder"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("ParentFolder"),
            default_value: None,
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );
    file_schema.fields.insert(
        FieldType::from("Parent"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Parent"),
            default_value: None,
            rank: 11,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas
    let mut schema_requests = vec![
        sschemaupdate!(object_schema),
        sschemaupdate!(root_schema),
        sschemaupdate!(folder_schema),
        sschemaupdate!(file_schema),
    ];
    store.perform_mut(&mut schema_requests).await.unwrap();

    // Create entities - start with a Root entity
    let mut root_create = vec![
        screate!(EntityType::from("Root"), "Root".to_string()),
    ];
    store.perform_mut(&mut root_create).await.unwrap();
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = root_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let mut folder_create = vec![
        screate!(EntityType::from("Folder"), "Documents".to_string(), root_id.clone()),
    ];
    store.perform_mut(&mut folder_create).await.unwrap();
    let folder_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = folder_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created folder entity ID");
    };

    let mut file_create = vec![
        screate!(EntityType::from("File"), "test.txt".to_string(), folder_id.clone()),
    ];
    store.perform_mut(&mut file_create).await.unwrap();
    let file_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = file_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created file entity ID");
    };

    // Set up relationships
    let mut setup_requests = vec![
        // Set folder as child of root (Children relationship)
        swrite!(root_id.clone(), FieldType::from("Children"), Some(Value::EntityList(vec![folder_id.clone()]))),
        
        // Set file as child of folder (Children relationship)  
        swrite!(folder_id.clone(), FieldType::from("Children"), Some(Value::EntityList(vec![file_id.clone()]))),
        
        // Set folder as parent of file (ParentFolder reference)
        swrite!(file_id.clone(), FieldType::from("ParentFolder"), Some(Value::EntityReference(Some(folder_id.clone())))),
        
        // Set up Parent chain for path resolution (used by spath! macro)
        swrite!(folder_id.clone(), FieldType::from("Parent"), Some(Value::EntityReference(Some(root_id.clone())))),
        swrite!(file_id.clone(), FieldType::from("Parent"), Some(Value::EntityReference(Some(folder_id.clone())))),
    ];
    store.perform_mut(&mut setup_requests).await.unwrap();

    // Take snapshot
    let snapshot = take_json_snapshot(&mut store).await.unwrap();
    
    println!("Generated JSON snapshot with path resolution:");
    println!("{}", serde_json::to_string_pretty(&snapshot).unwrap());

    // Verify that Children shows nested objects
    let root_children = snapshot.tree.fields.get("Children").unwrap().as_array().unwrap();
    assert_eq!(root_children.len(), 1);
    let nested_folder = &root_children[0];
    assert_eq!(nested_folder.get("entityType").unwrap().as_str().unwrap(), "Folder");
    assert_eq!(nested_folder.get("Name").unwrap().as_str().unwrap(), "Documents");

    // Find the file in the nested structure  
    let folder_children = nested_folder.get("Children").unwrap().as_array().unwrap();
    assert_eq!(folder_children.len(), 1);
    let nested_file = &folder_children[0];
    assert_eq!(nested_file.get("entityType").unwrap().as_str().unwrap(), "File");
    assert_eq!(nested_file.get("Name").unwrap().as_str().unwrap(), "test.txt");

    // TODO: Verify that ParentFolder reference shows a path (not implemented yet in current version)
    // This would require extending the value_to_json_value_with_paths macro to handle
    // non-Children entity references differently
    
    // IMPLEMENTED: Now verify that ParentFolder shows paths instead of IDs
    let parent_folder_value = nested_file.get("ParentFolder").unwrap();
    assert_eq!(parent_folder_value.as_str().unwrap(), "Root/Documents", 
        "ParentFolder should show path 'Root/Documents', got: {:?}", parent_folder_value);

    println!("Path resolution test completed successfully!");
}

#[tokio::test]
async fn test_json_snapshot_storage_scope() {
    // Test that storage scope is properly preserved in JSON snapshots
    
    let snowflake = Arc::new(Snowflake::new());
    let mut store = AsyncStore::new(snowflake.clone());

    // Define schemas with different storage scopes
    let mut object_schema = EntitySchema::<Single>::new("Object", vec![]);
    object_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Runtime,
        },
    );

    let mut root_schema = EntitySchema::<Single>::new("Root", vec![EntityType::from("Object")]);
    root_schema.fields.insert(
        FieldType::from("ConfigField"),
        FieldSchema::String {
            field_type: FieldType::from("ConfigField"),
            default_value: "config_default".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    root_schema.fields.insert(
        FieldType::from("RuntimeField"),
        FieldSchema::String {
            field_type: FieldType::from("RuntimeField"),
            default_value: "runtime_default".to_string(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        },
    );

    // Add schemas to the store
    let mut schema_requests = vec![
        sschemaupdate!(object_schema),
        sschemaupdate!(root_schema),
    ];
    store.perform_mut(&mut schema_requests).await.unwrap();

    // Create a root entity
    let mut create_requests = vec![
        screate!(EntityType::from("Root"), "TestRoot".to_string()),
    ];
    store.perform_mut(&mut create_requests).await.unwrap();

    // Take JSON snapshot
    let snapshot = take_json_snapshot(&mut store).await.unwrap();
    
    // Verify the schema in the snapshot has the correct storage scopes
    let root_schema = snapshot.schemas.iter()
        .find(|s| s.entity_type == "Root")
        .expect("Root schema should be in snapshot");
    
    // Check ConfigField has Configuration storage scope
    let config_field = root_schema.fields.iter()
        .find(|f| f.name == "ConfigField")
        .expect("ConfigField should be in schema");
    assert_eq!(config_field.storage_scope, Some("Configuration".to_string()));
    
    // Check RuntimeField has Runtime storage scope  
    let runtime_field = root_schema.fields.iter()
        .find(|f| f.name == "RuntimeField")
        .expect("RuntimeField should be in schema");
    assert_eq!(runtime_field.storage_scope, Some("Runtime".to_string()));

    // Also check the Object schema has Runtime storage scope
    let object_schema = snapshot.schemas.iter()
        .find(|s| s.entity_type == "Object")
        .expect("Object schema should be in snapshot");
    
    let name_field = object_schema.fields.iter()
        .find(|f| f.name == "Name")
        .expect("Name field should be in schema");
    assert_eq!(name_field.storage_scope, Some("Runtime".to_string()));

    // Test round-trip: convert schema to JSON and back
    let recreated_schema = root_schema.to_entity_schema().unwrap();
    
    // Verify storage scopes are preserved
    let config_field_schema = recreated_schema.fields.get(&FieldType::from("ConfigField")).unwrap();
    assert_eq!(*config_field_schema.storage_scope(), StorageScope::Configuration);
    
    let runtime_field_schema = recreated_schema.fields.get(&FieldType::from("RuntimeField")).unwrap();
    assert_eq!(*runtime_field_schema.storage_scope(), StorageScope::Runtime);

    println!("Storage scope test completed successfully!");
}
