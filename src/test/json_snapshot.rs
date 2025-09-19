
#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::StoreTrait;

#[allow(unused_imports)]
use crate::{restore_json_snapshot, screate, sschemaupdate, swrite, take_json_snapshot, EntitySchema, EntityType, FieldSchema, FieldType, Request, Single, Store, Value};


#[test]
fn test_json_snapshot_functionality() {
    // Create a new store
    let mut store = Store::new();

    // Define schemas as per the example
    let object_et = store.get_entity_type("Object").unwrap();
    let name_ft = store.get_field_type("Name").unwrap();
    let description_ft = store.get_field_type("Description").unwrap();
    let children_ft = store.get_field_type("Children").unwrap();
    
    let mut object_schema = EntitySchema::<Single>::new(object_et, vec![]);
    object_schema.fields.insert(
        name_ft,
        FieldSchema::String {
            field_type: name_ft,
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        description_ft,
        FieldSchema::String {
            field_type: description_ft,
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        children_ft,
        FieldSchema::EntityList {
            field_type: children_ft,
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Configuration,
        },
    );

    let root_et = store.get_entity_type("Root").unwrap();
    let created_entity_ft = store.get_field_type("CreatedEntity").unwrap();
    let deleted_entity_ft = store.get_field_type("DeletedEntity").unwrap();
    let schema_change_ft = store.get_field_type("SchemaChange").unwrap();
    
    let mut root_schema = EntitySchema::<Single>::new(root_et, vec![object_et]);
    root_schema.fields.insert(
        created_entity_ft,
        FieldSchema::String {
            field_type: created_entity_ft,
            default_value: "".to_string(),
            rank: 10,
            storage_scope: StorageScope::Runtime,
        },
    );
    root_schema.fields.insert(
        deleted_entity_ft,
        FieldSchema::String {
            field_type: deleted_entity_ft,
            default_value: "".to_string(),
            rank: 11,
            storage_scope: StorageScope::Runtime,
        },
    );
    root_schema.fields.insert(
        schema_change_ft,
        FieldSchema::String {
            field_type: schema_change_ft,
            default_value: "".to_string(),
            rank: 12,
            storage_scope: StorageScope::Runtime,
        },
    );

    let machine_et = store.get_entity_type("Machine").unwrap();
    let status_ft = store.get_field_type("Status").unwrap();
    
    let mut machine_schema = EntitySchema::<Single>::new(machine_et, vec![object_et]);
    machine_schema.fields.insert(
        status_ft,
        FieldSchema::Choice {
            field_type: status_ft,
            default_value: 1, // "Offline"
            rank: 10,
            storage_scope: StorageScope::Configuration,
            choices: vec!["Online".to_string(), "Offline".to_string()],
        },
    );

    let sensor_et = store.get_entity_type("Sensor").unwrap();
    let current_value_ft = store.get_field_type("CurrentValue").unwrap();
    let unit_ft = store.get_field_type("Unit").unwrap();
    let last_updated_ft = store.get_field_type("LastUpdated").unwrap();
    
    let mut sensor_schema = EntitySchema::<Single>::new(sensor_et, vec![object_et]);
    sensor_schema.fields.insert(
        current_value_ft,
        FieldSchema::Float {
            field_type: current_value_ft,
            default_value: 0.0,
            rank: 10,
            storage_scope: StorageScope::Runtime,
        },
    );
    sensor_schema.fields.insert(
        unit_ft,
        FieldSchema::String {
            field_type: unit_ft,
            default_value: "".to_string(),
            rank: 11,
            storage_scope: StorageScope::Configuration,
        },
    );
    sensor_schema.fields.insert(
        last_updated_ft,
        FieldSchema::Timestamp {
            field_type: last_updated_ft,
            default_value: time::OffsetDateTime::UNIX_EPOCH,
            rank: 12,
            storage_scope: StorageScope::Runtime,
        },
    );

    let temp_sensor_et = store.get_entity_type("TemperatureSensor").unwrap();
    let calibration_offset_ft = store.get_field_type("CalibrationOffset").unwrap();
    
    let mut temp_sensor_schema = EntitySchema::<Single>::new(temp_sensor_et, vec![sensor_et]);
    temp_sensor_schema.fields.insert(
        calibration_offset_ft,
        FieldSchema::Float {
            field_type: calibration_offset_ft,
            default_value: 0.0,
            rank: 13,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas to the store
    store.perform_mut(vec![
        sschemaupdate!(object_schema.to_string_schema(&store)),
        sschemaupdate!(root_schema.to_string_schema(&store)),
        sschemaupdate!(machine_schema.to_string_schema(&store)),
        sschemaupdate!(sensor_schema.to_string_schema(&store)),
        sschemaupdate!(temp_sensor_schema.to_string_schema(&store)),
    ]).unwrap();

    // Create entities - let the store generate IDs
    let create_requests = store.perform_mut(vec![
        Request::Create {
            entity_type: root_et,
            parent_id: None,
            name: "DataStore".to_string(),
            created_entity_id: None,
            timestamp: None,
            originator: None,
        },
    ]).unwrap();
    
    // Get the actual created root ID
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let machine_create_requests = store.perform_mut(vec![
        screate!(machine_et, "Server1".to_string(), root_id.clone()),
    ]).unwrap();
    
    // Get the actual created machine ID
    let machine_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = machine_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created machine entity ID");
    };

    let sensor_create_requests = store.perform_mut(vec![
        screate!(temp_sensor_et, "IntakeTemp".to_string(), machine_id.clone()),
    ]).unwrap();
    
    // Get the actual created sensor ID
    let sensor_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = sensor_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created sensor entity ID");
    };

    // Set field values
    store.perform_mut(vec![
        swrite!(root_id.clone(), vec![name_ft], Some(Value::String("DataStore".to_string()))),
        swrite!(root_id.clone(), vec![description_ft], Some(Value::String("Primary data store".to_string()))),
        swrite!(root_id.clone(), vec![children_ft], Some(Value::EntityList(vec![machine_id.clone()]))),
        
        swrite!(machine_id.clone(), vec![name_ft], Some(Value::String("Server1".to_string()))),
        swrite!(machine_id.clone(), vec![status_ft], Some(Value::Choice(0))), // "Online"
        swrite!(machine_id.clone(), vec![children_ft], Some(Value::EntityList(vec![sensor_id.clone()]))),
        
        swrite!(sensor_id.clone(), vec![name_ft], Some(Value::String("IntakeTemp".to_string()))),
        swrite!(sensor_id.clone(), vec![current_value_ft], Some(Value::Float(72.5))),
        swrite!(sensor_id.clone(), vec![unit_ft], Some(Value::String("C".to_string()))),
        swrite!(sensor_id.clone(), vec![calibration_offset_ft], Some(Value::Float(0.5))),
    ]).unwrap();

    // Take JSON snapshot
    let snapshot = take_json_snapshot(&mut store).unwrap();
    
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

#[test]
fn test_json_snapshot_restore() {
    // Create and populate the first store
    let mut store1 = Store::new();

    // Define schemas
    let object_et = store1.get_entity_type("Object").unwrap();
    let name_ft = store1.get_field_type("Name").unwrap();
    let description_ft = store1.get_field_type("Description").unwrap();
    let children_ft = store1.get_field_type("Children").unwrap();
    
    let mut object_schema = EntitySchema::<Single>::new(object_et, vec![]);
    object_schema.fields.insert(
        name_ft,
        FieldSchema::String {
            field_type: name_ft,
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        description_ft,
        FieldSchema::String {
            field_type: description_ft,
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        children_ft,
        FieldSchema::EntityList {
            field_type: children_ft,
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Configuration,
        },
    );

    let root_et = store1.get_entity_type("Root").unwrap();
    let status_ft = store1.get_field_type("Status").unwrap();
    let document_et = store1.get_entity_type("Document").unwrap();
    let content_ft = store1.get_field_type("Content").unwrap();
    
    let mut root_schema = EntitySchema::<Single>::new(root_et, vec![object_et]);
    root_schema.fields.insert(
        status_ft,
        FieldSchema::String {
            field_type: status_ft,
            default_value: "Active".to_string(),
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );

    let mut document_schema = EntitySchema::<Single>::new(document_et, vec![object_et]);
    document_schema.fields.insert(
        content_ft,
        FieldSchema::String {
            field_type: content_ft,
            default_value: "".to_string(),
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas to store1
    store1.perform_mut(vec![
        sschemaupdate!(object_schema.to_string_schema(&store1)),
        sschemaupdate!(root_schema.to_string_schema(&store1)),
        sschemaupdate!(document_schema.to_string_schema(&store1)),
    ]).unwrap();

    // Create entities in store1
    let create_requests = store1.perform_mut(vec![
        Request::Create {
            entity_type: root_et,
            parent_id: None,
            name: "TestRoot".to_string(),
            created_entity_id: None,
            timestamp: None,
            originator: None,
        },
    ]).unwrap();
    
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let doc_create_requests = store1.perform_mut(vec![
        screate!(document_et, "TestDoc".to_string(), root_id.clone()),
    ]).unwrap();
    
    let doc_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = doc_create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created document entity ID");
    };

    // Set field values in store1
    store1.perform_mut(vec![
        swrite!(root_id.clone(), vec![name_ft], Some(Value::String("TestRoot".to_string()))),
        swrite!(root_id.clone(), vec![description_ft], Some(Value::String("Test root entity".to_string()))),
        swrite!(root_id.clone(), vec![status_ft], Some(Value::String("Active".to_string()))),
        swrite!(root_id.clone(), vec![children_ft], Some(Value::EntityList(vec![doc_id.clone()]))),
        swrite!(doc_id.clone(), vec![name_ft], Some(Value::String("TestDoc".to_string()))),
        swrite!(doc_id.clone(), vec![description_ft], Some(Value::String("Test document".to_string()))),
        swrite!(doc_id.clone(), vec![content_ft], Some(Value::String("Hello, World!".to_string()))),
    ]).unwrap();

    // Take JSON snapshot from store1
    let snapshot = take_json_snapshot(&mut store1).unwrap();

    // Create a new empty store
    let mut store2 = Store::new();

    // Restore the snapshot to store2
    restore_json_snapshot(&mut store2, &snapshot).unwrap();

    // Verify that store2 now contains the same data
    let root_et2 = store2.get_entity_type("Root").unwrap();
    let name_ft2 = store2.get_field_type("Name").unwrap();
    let description_ft2 = store2.get_field_type("Description").unwrap();
    let status_ft2 = store2.get_field_type("Status").unwrap();
    let children_ft2 = store2.get_field_type("Children").unwrap();
    
    let entities = store2.find_entities(root_et2, None).unwrap();
    assert_eq!(entities.len(), 1);
    
    let root_id_restored = &entities[0];
    
    // Check root entity fields
    let read_requests = store2.perform_mut(vec![
        crate::sread!(root_id_restored.clone(), vec![name_ft2]),
        crate::sread!(root_id_restored.clone(), vec![description_ft2]),
        crate::sread!(root_id_restored.clone(), vec![status_ft2]),
        crate::sread!(root_id_restored.clone(), vec![children_ft2]),
    ]).unwrap();
    
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
        let content_ft2 = store2.get_field_type("Content").unwrap();
        let doc_read_requests = store2.perform_mut(vec![
            crate::sread!(doc_id_restored.clone(), vec![name_ft2]),
            crate::sread!(doc_id_restored.clone(), vec![content_ft2]),
        ]).unwrap();
        
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

#[test]
fn test_json_snapshot_path_resolution() {
    // This test ensures that normal entity references (not Children) show paths
    // while Children fields show nested entity objects
    let mut store = Store::new();

    // Define schemas
    let object_et = store.get_entity_type("Object").unwrap();
    let name_ft = store.get_field_type("Name").unwrap();
    let children_ft = store.get_field_type("Children").unwrap();
    let parent_ft = store.get_field_type("Parent").unwrap();
    let parent_folder_ft = store.get_field_type("ParentFolder").unwrap();
    
    let mut object_schema = EntitySchema::<Single>::new(object_et, vec![]);
    object_schema.fields.insert(
        name_ft,
        FieldSchema::String {
            field_type: name_ft,
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        children_ft,
        FieldSchema::EntityList {
            field_type: children_ft,
            default_value: vec![],
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );

    let root_et = store.get_entity_type("Root").unwrap();
    let folder_et = store.get_entity_type("Folder").unwrap();
    let file_et = store.get_entity_type("File").unwrap();
    
    let root_schema = EntitySchema::<Single>::new(root_et, vec![object_et]);

    let mut folder_schema = EntitySchema::<Single>::new(folder_et, vec![object_et]);
    folder_schema.fields.insert(
        parent_ft,
        FieldSchema::EntityReference {
            field_type: parent_ft,
            default_value: None,
            rank: 5,
            storage_scope: StorageScope::Configuration,
        },
    );
    
    let mut file_schema = EntitySchema::<Single>::new(file_et, vec![object_et]);
    file_schema.fields.insert(
        parent_folder_ft,
        FieldSchema::EntityReference {
            field_type: parent_folder_ft,
            default_value: None,
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );
    file_schema.fields.insert(
        parent_ft,
        FieldSchema::EntityReference {
            field_type: parent_ft,
            default_value: None,
            rank: 11,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas
    store.perform_mut(vec![
        sschemaupdate!(object_schema.to_string_schema(&store)),
        sschemaupdate!(root_schema.to_string_schema(&store)),
        sschemaupdate!(folder_schema.to_string_schema(&store)),
        sschemaupdate!(file_schema.to_string_schema(&store)),
    ]).unwrap();

    // Create entities - start with a Root entity
    let root_create = store.perform_mut(vec![
        screate!(root_et, "Root".to_string()),
    ]).unwrap();
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = root_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    let folder_create = store.perform_mut(vec![
        screate!(folder_et, "Documents".to_string(), root_id.clone()),
    ]).unwrap();
    let folder_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = folder_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created folder entity ID");
    };

    let file_create = store.perform_mut(vec![
        screate!(file_et, "test.txt".to_string(), folder_id.clone()),
    ]).unwrap();
    let file_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = file_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created file entity ID");
    };

    // Set up relationships
    store.perform_mut(vec![
        // Set folder as child of root (Children relationship)
        swrite!(root_id.clone(), vec![children_ft], Some(Value::EntityList(vec![folder_id.clone()]))),
        
        // Set file as child of folder (Children relationship)  
        swrite!(folder_id.clone(), vec![children_ft], Some(Value::EntityList(vec![file_id.clone()]))),
        
        // Set folder as parent of file (ParentFolder reference)
        swrite!(file_id.clone(), vec![parent_folder_ft], Some(Value::EntityReference(Some(folder_id.clone())))),
        
        // Set up Parent chain for path resolution (used by spath! macro)
        swrite!(folder_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(root_id.clone())))),
        swrite!(file_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(folder_id.clone())))),
    ]).unwrap();

    // Take snapshot
    let snapshot = take_json_snapshot(&mut store).unwrap();
    
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

#[test]
fn test_json_snapshot_storage_scope() {
    // Test that storage scope is properly preserved in JSON snapshots
    
    let mut store = Store::new();

    // Define schemas with different storage scopes
    let object_et = store.get_entity_type("Object").unwrap();
    let name_ft = store.get_field_type("Name").unwrap();
    let config_field_ft = store.get_field_type("ConfigField").unwrap();
    let runtime_field_ft = store.get_field_type("RuntimeField").unwrap();
    
    let mut object_schema = EntitySchema::<Single>::new(object_et, vec![]);
    object_schema.fields.insert(
        name_ft,
        FieldSchema::String {
            field_type: name_ft,
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Runtime,
        },
    );

    let root_et = store.get_entity_type("Root").unwrap();
    let mut root_schema = EntitySchema::<Single>::new(root_et, vec![object_et]);
    root_schema.fields.insert(
        config_field_ft,
        FieldSchema::String {
            field_type: config_field_ft,
            default_value: "config_default".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    root_schema.fields.insert(
        runtime_field_ft,
        FieldSchema::String {
            field_type: runtime_field_ft,
            default_value: "runtime_default".to_string(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        },
    );

    // Add schemas to the store
    store.perform_mut(vec![
        sschemaupdate!(object_schema.to_string_schema(&store)),
        sschemaupdate!(root_schema.to_string_schema(&store)),
    ]).unwrap();

    // Create a root entity
    store.perform_mut(vec![
        screate!(root_et, "TestRoot".to_string()),
    ]).unwrap();

    // Take JSON snapshot
    let snapshot = take_json_snapshot(&mut store).unwrap();
    
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
    let recreated_schema = root_schema.to_entity_schema(&store).unwrap();
    
    // Verify storage scopes are preserved
    let config_field_schema = recreated_schema.fields.get(&config_field_ft).unwrap();
    assert_eq!(*config_field_schema.storage_scope(), StorageScope::Configuration);
    
    let runtime_field_schema = recreated_schema.fields.get(&runtime_field_ft).unwrap();
    assert_eq!(*runtime_field_schema.storage_scope(), StorageScope::Runtime);

    println!("Storage scope test completed successfully!");
}

#[test]
fn test_json_snapshot_entity_list_paths() {
    // Test that EntityList fields with paths are properly handled during restore
    // This reproduces the CandidateList issue from base-topology.json
    
    let mut store = Store::new();

    // Define schemas similar to the base topology
    let object_et = store.get_entity_type("Object").unwrap();
    let name_ft = store.get_field_type("Name").unwrap();
    let parent_ft = store.get_field_type("Parent").unwrap();
    let children_ft = store.get_field_type("Children").unwrap();
    
    let mut object_schema = EntitySchema::<Single>::new(object_et, vec![]);
    object_schema.fields.insert(
        name_ft,
        FieldSchema::String {
            field_type: name_ft,
            default_value: "".to_string(),
            rank: 0,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        parent_ft,
        FieldSchema::EntityReference {
            field_type: parent_ft,
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        },
    );
    object_schema.fields.insert(
        children_ft,
        FieldSchema::EntityList {
            field_type: children_ft,
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Configuration,
        },
    );

    let root_et = store.get_entity_type("Root").unwrap();
    let machine_et = store.get_entity_type("Machine").unwrap();
    let service_et = store.get_entity_type("Service").unwrap();
    let fault_tolerance_et = store.get_entity_type("FaultTolerance").unwrap();
    let candidate_list_ft = store.get_field_type("CandidateList").unwrap();
    
    let root_schema = EntitySchema::<Single>::new(root_et, vec![object_et]);
    let machine_schema = EntitySchema::<Single>::new(machine_et, vec![object_et]);
    let service_schema = EntitySchema::<Single>::new(service_et, vec![object_et]);
    
    let mut fault_tolerance_schema = EntitySchema::<Single>::new(fault_tolerance_et, vec![object_et]);
    fault_tolerance_schema.fields.insert(
        candidate_list_ft,
        FieldSchema::EntityList {
            field_type: candidate_list_ft,
            default_value: vec![],
            rank: 10,
            storage_scope: StorageScope::Configuration,
        },
    );

    // Add schemas
    store.perform_mut(vec![
        sschemaupdate!(object_schema.to_string_schema(&store)),
        sschemaupdate!(root_schema.to_string_schema(&store)),
        sschemaupdate!(machine_schema.to_string_schema(&store)),
        sschemaupdate!(service_schema.to_string_schema(&store)),
        sschemaupdate!(fault_tolerance_schema.to_string_schema(&store)),
    ]).unwrap();

    // Create the entity structure from base-topology.json
    let create_requests = store.perform_mut(vec![
        screate!(root_et, "QOS".to_string()),
    ]).unwrap();
    let root_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        panic!("Failed to get created root entity ID");
    };

    // Create machines
    let machine_a_create = store.perform_mut(vec![screate!(machine_et, "qos-a".to_string(), root_id.clone())]).unwrap();
    let machine_a_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = machine_a_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created machine A entity ID");
    };

    let machine_b_create = store.perform_mut(vec![screate!(machine_et, "qos-b".to_string(), root_id.clone())]).unwrap();
    let machine_b_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = machine_b_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created machine B entity ID");
    };

    // Create services
    let service_a_create = store.perform_mut(vec![screate!(service_et, "qcore".to_string(), machine_a_id.clone())]).unwrap();
    let service_a_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = service_a_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created service A entity ID");
    };

    let service_b_create = store.perform_mut(vec![screate!(service_et, "qcore".to_string(), machine_b_id.clone())]).unwrap();
    let service_b_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = service_b_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created service B entity ID");
    };

    // Create fault tolerance entity
    let ft_create = store.perform_mut(vec![screate!(fault_tolerance_et, "qcore".to_string(), root_id.clone())]).unwrap();
    let ft_id = if let Some(Request::Create { created_entity_id: Some(ref id), .. }) = ft_create.first() {
        id.clone()
    } else {
        panic!("Failed to get created FaultTolerance entity ID");
    };

    // Set up the entity relationships and Parent references for path resolution
    store.perform_mut(vec![
        // Set up Parent references for path resolution
        swrite!(machine_a_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(root_id.clone())))),
        swrite!(machine_b_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(root_id.clone())))),
        swrite!(service_a_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(machine_a_id.clone())))),
        swrite!(service_b_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(machine_b_id.clone())))),
        swrite!(ft_id.clone(), vec![parent_ft], Some(Value::EntityReference(Some(root_id.clone())))),
        
        // Set up Children relationships
        swrite!(root_id.clone(), vec![children_ft], Some(Value::EntityList(vec![machine_a_id.clone(), machine_b_id.clone(), ft_id.clone()]))),
        swrite!(machine_a_id.clone(), vec![children_ft], Some(Value::EntityList(vec![service_a_id.clone()]))),
        swrite!(machine_b_id.clone(), vec![children_ft], Some(Value::EntityList(vec![service_b_id.clone()]))),
        
        // Set up CandidateList with entity references (not paths yet)
        swrite!(ft_id.clone(), vec![candidate_list_ft], Some(Value::EntityList(vec![service_a_id.clone(), service_b_id.clone()]))),
    ]).unwrap();

    // Take a snapshot
    let snapshot = take_json_snapshot(&mut store).unwrap();
    
    println!("Generated snapshot:");
    println!("{}", serde_json::to_string_pretty(&snapshot).unwrap());

    // The snapshot should now contain CandidateList with paths like ["Root/qos-a/qcore", "Root/qos-b/qcore"]
    // Let's verify this
    let ft_entity = snapshot.tree.fields.get("Children").unwrap().as_array().unwrap()
        .iter()
        .find(|child| child.get("entityType").unwrap().as_str().unwrap() == "FaultTolerance")
        .expect("FaultTolerance entity should be in children");
    
    let candidate_list = ft_entity.get("CandidateList").unwrap().as_array().unwrap();
    assert_eq!(candidate_list.len(), 2);
    assert_eq!(candidate_list[0].as_str().unwrap(), "QOS/qos-a/qcore");
    assert_eq!(candidate_list[1].as_str().unwrap(), "QOS/qos-b/qcore");

    // Now test the problematic restore operation
    // Create a new store and try to restore the snapshot
    let mut store2 = Store::new();

    // This should fail because json_value_to_value can't handle paths in EntityList
    let restore_result = restore_json_snapshot(&mut store2, &snapshot);
    
    match restore_result {
        Ok(()) => {
            println!("Restore succeeded - checking if CandidateList was set correctly");
            
            // Find the FaultTolerance entity in the restored store
            let ft_et2 = store2.get_entity_type("FaultTolerance").unwrap();
            let candidate_list_ft2 = store2.get_field_type("CandidateList").unwrap();
            let ft_entities = store2.find_entities(ft_et2, None).unwrap();
            assert_eq!(ft_entities.len(), 1);
            let restored_ft_id = &ft_entities[0];
            
            // Check if CandidateList was restored correctly
            let read_requests = store2.perform_mut(vec![
                crate::sread!(restored_ft_id.clone(), vec![candidate_list_ft2]),
            ]).unwrap();
            
            if let Some(Request::Read { value: Some(Value::EntityList(candidates)), .. }) = read_requests.get(0) {
                assert_eq!(candidates.len(), 2, "CandidateList should have 2 entities");
                println!("CandidateList restored successfully with {} candidates", candidates.len());
            } else {
                panic!("Failed to read CandidateList from restored entity");
            }
        },
        Err(e) => {
            println!("Restore failed as expected: {}", e);
            panic!("This test demonstrates the bug: EntityList paths are not properly converted during restore");
        }
    }

    println!("EntityList path test completed!");
}
