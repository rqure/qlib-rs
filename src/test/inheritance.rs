#![allow(unused_imports)]
use crate::sreq;

#[allow(unused_imports)]
use crate::*;

#[allow(unused_imports)]
use crate::data::StorageScope;

#[allow(unused_imports)]
use std::sync::Arc;

#[test]
fn test_inheritance_in_find_entities() -> Result<()> {
    let mut store = Store::new();

    // Create base and derived entity types
    // Define schemas using strings - perform_mut will intern the types
    
    // Create schemas with inheritance hierarchy:
    // Animal (base)
    //   └── Mammal (inherits from Animal)
    //       ├── Dog (inherits from Mammal)
    //       └── Cat (inherits from Mammal)
    
    // Base Animal schema with required fields
    let mut animal_schema = EntitySchema::<Single, String, String>::new("Animal".to_string(), vec![]);
    animal_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    animal_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    animal_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = sreq![sschemaupdate!(animal_schema)];
    store.perform_mut(requests)?;

    // Mammal schema (inherits from Animal)
    let mut mammal_schema = EntitySchema::<Single, String, String>::new("Mammal".to_string(), vec!["Animal".to_string()]);
    mammal_schema.fields.insert(
        "FurColor".to_string(),
        FieldSchema::String {
            field_type: "FurColor".to_string(),
            default_value: String::new(),
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = sreq![sschemaupdate!(mammal_schema)];
    store.perform_mut(requests)?;

    // Dog schema (inherits from Mammal)
    let mut dog_schema = EntitySchema::<Single, String, String>::new("Dog".to_string(), vec!["Mammal".to_string()]);
    dog_schema.fields.insert(
        "Breed".to_string(),
        FieldSchema::String {
            field_type: "Breed".to_string(),
            default_value: String::new(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = sreq![sschemaupdate!(dog_schema)];
    store.perform_mut(requests)?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single, String, String>::new("Cat".to_string(), vec!["Mammal".to_string()]);
    let requests = sreq![sschemaupdate!(cat_schema)];
    store.perform_mut(requests)?;

    // Now we can get the interned entity types
    let et_animal = store.get_entity_type("Animal")?;
    let et_mammal = store.get_entity_type("Mammal")?;
    let et_dog = store.get_entity_type("Dog")?;
    let et_cat = store.get_entity_type("Cat")?;

    // Create some entities
    let create_requests = store.perform_mut(sreq![screate!(
        et_dog,
        "Buddy".to_string()
    )])?;
    let dog1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    let create_requests = store.perform_mut(sreq![screate!(
        et_dog,
        "Rex".to_string()
    )])?;
    let dog2_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    let create_requests = store.perform_mut(sreq![screate!(
        et_cat,
        "Whiskers".to_string()
    )])?;
    let cat1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    // Test: Finding Dog entities should return only dogs
    let dogs = store.find_entities(et_dog, None)?;
    assert_eq!(dogs.len(), 2);
    assert!(dogs.contains(&dog1_id));
    assert!(dogs.contains(&dog2_id));

    // Test: Finding Mammal entities should return dogs and cats (inheritance)
    let mammals = store.find_entities(et_mammal, None)?;
    assert_eq!(mammals.len(), 3);
    assert!(mammals.contains(&dog1_id));
    assert!(mammals.contains(&dog2_id));
    assert!(mammals.contains(&cat1_id));

    // Test: Finding Animal entities should return all (full inheritance chain)
    let animals = store.find_entities(et_animal, None)?;
    assert_eq!(animals.len(), 3);
    assert!(animals.contains(&dog1_id));
    assert!(animals.contains(&dog2_id));
    assert!(animals.contains(&cat1_id));

    Ok(())
}

#[test]
fn test_inheritance_with_direct_instances() -> Result<()> {
    let mut store = Store::new();

    // Create base Animal schema with required fields
    let mut animal_schema = EntitySchema::<Single, String, String>::new("Animal".to_string(), vec![]);
    animal_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    animal_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    animal_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    store.perform_mut(sreq![sschemaupdate!(animal_schema)])?;

    // Create Mammal schema that inherits from Animal
    let mammal_schema = EntitySchema::<Single, String, String>::new("Mammal".to_string(), vec!["Animal".to_string()]);
    store.perform_mut(sreq![sschemaupdate!(mammal_schema)])?;

    // Now get the interned entity types
    let et_animal = store.get_entity_type("Animal")?;
    let et_mammal = store.get_entity_type("Mammal")?;

    // Create direct instances of both types
    let create_requests = store.perform_mut(sreq![screate!(
        et_animal,
        "Generic Animal".to_string()
    )])?;
    let animal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    let create_requests = store.perform_mut(sreq![screate!(
        et_mammal,
        "Generic Mammal".to_string()
    )])?;
    let mammal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    // Test: Finding Animal entities should return both (mammal inherits from animal)
    let animals = store.find_entities(et_animal, None)?;
    assert_eq!(animals.len(), 2);
    assert!(animals.contains(&animal1_id));
    assert!(animals.contains(&mammal1_id));

    // Test: Finding Mammal entities should return only the mammal
    let mammals = store.find_entities(et_mammal, None)?;
    assert_eq!(mammals.len(), 1);
    assert!(mammals.contains(&mammal1_id));

    Ok(())
}

#[test]
fn test_circular_inheritance_protection() -> Result<()> {
    let mut store = Store::new();

    // Create TypeA with required fields
    let mut schema_a = EntitySchema::<Single, String, String>::new("TypeA".to_string(), vec![]);
    schema_a.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    schema_a.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    schema_a.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    store.perform_mut(sreq![sschemaupdate!(schema_a)])?;

    // Create TypeB that inherits from TypeA
    let schema_b = EntitySchema::<Single, String, String>::new("TypeB".to_string(), vec!["TypeA".to_string()]);
    store.perform_mut(sreq![sschemaupdate!(schema_b)])?;

    // Try to make TypeA inherit from TypeB (should fail or be ignored)
    let circular_schema_a = EntitySchema::<Single, String, String>::new("TypeA".to_string(), vec!["TypeB".to_string()]);
    
    // This should either fail or the system should handle it gracefully
    let requests = sreq![sschemaupdate!(circular_schema_a)];
    let result = store.perform_mut(requests);
    
    // The test passes if either:
    // 1. The operation fails (returns an error)
    // 2. The operation succeeds but circular inheritance is prevented internally
    
    // Get entity types after schema creation
    let et_a = store.get_entity_type("TypeA")?;
    let et_b = store.get_entity_type("TypeB")?;

    match result {
        Ok(_) => {
            // If it succeeded, verify that circular inheritance is handled properly
            // by checking that the inheritance map doesn't create infinite loops
            store.perform_mut(sreq![screate!(
                et_b,
                "Test B".to_string()
            )])?;
            let entities_a = store.find_entities(et_a, None)?;
            
            // Should not crash or loop infinitely
            assert!(entities_a.len() >= 1);
        }
        Err(_) => {
            // Expected: circular inheritance should be rejected
            // This is fine - the system properly rejected the circular dependency
        }
    }

    Ok(())
}

#[test]
fn test_multi_inheritance() -> Result<()> {
    let mut store = Store::new();

    // Create Flyable trait (interface-like entity type) with required fields
    let mut flyable_schema = EntitySchema::<Single, String, String>::new("Flyable".to_string(), vec![]);
    flyable_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    flyable_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    flyable_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    flyable_schema.fields.insert(
        "CanFly".to_string(),
        FieldSchema::Bool {
            field_type: "CanFly".to_string(),
            default_value: true,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    flyable_schema.fields.insert(
        "WingSpan".to_string(),
        FieldSchema::Float {
            field_type: "WingSpan".to_string(),
            default_value: 0.0,
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(sreq![sschemaupdate!(flyable_schema)])?;

    // Create Mammal schema
    let mut mammal_schema = EntitySchema::<Single, String, String>::new("Mammal".to_string(), vec![]);
    mammal_schema.fields.insert(
        "FurColor".to_string(),
        FieldSchema::String {
            field_type: "FurColor".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    mammal_schema.fields.insert(
        "IsWarmBlooded".to_string(),
        FieldSchema::Bool {
            field_type: "IsWarmBlooded".to_string(),
            default_value: true,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(sreq![sschemaupdate!(mammal_schema)])?;

    // Create Bat schema that inherits from BOTH Flyable and Mammal
    let mut bat_schema = EntitySchema::<Single, String, String>::new("Bat".to_string(), vec!["Flyable".to_string(), "Mammal".to_string()]);
    bat_schema.fields.insert(
        "EcholocationRange".to_string(),
        FieldSchema::Float {
            field_type: "EcholocationRange".to_string(),
            default_value: 100.0,
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(sreq![sschemaupdate!(bat_schema)])?;

    // Now get the interned entity and field types
    let et_flyable = store.get_entity_type("Flyable")?;
    let et_mammal = store.get_entity_type("Mammal")?;
    let et_bat = store.get_entity_type("Bat")?;
    let ft_can_fly = store.get_field_type("CanFly")?;
    let ft_wing_span = store.get_field_type("WingSpan")?;
    let ft_fur_color = store.get_field_type("FurColor")?;
    let ft_is_warm_blooded = store.get_field_type("IsWarmBlooded")?;
    let ft_echolocation_range = store.get_field_type("EcholocationRange")?;

    // Create a bat entity
    let create_requests = store.perform_mut(sreq![screate!(
        et_bat,
        "Vampire Bat".to_string()
    )])?;
    let bat_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    // Verify that the bat has fields from all parent types
    let complete_schema = store.get_complete_entity_schema(et_bat)?;
    
    // Should have fields from Flyable
    assert!(complete_schema.fields.contains_key(&ft_can_fly));
    assert!(complete_schema.fields.contains_key(&ft_wing_span));
    
    // Should have fields from Mammal
    assert!(complete_schema.fields.contains_key(&ft_fur_color));
    assert!(complete_schema.fields.contains_key(&ft_is_warm_blooded));
    
    // Should have its own field
    assert!(complete_schema.fields.contains_key(&ft_echolocation_range));

    // Test inheritance lookup - searching for Flyable should find bats
    let flyable_entities = store.find_entities(et_flyable, None)?;
    assert_eq!(flyable_entities.len(), 1);
    assert!(flyable_entities.contains(&bat_id));

    // Test inheritance lookup - searching for Mammal should find bats
    let mammal_entities = store.find_entities(et_mammal, None)?;
    assert_eq!(mammal_entities.len(), 1);
    assert!(mammal_entities.contains(&bat_id));

    // Test inheritance lookup - searching for Bat should find bats
    let bat_entities = store.find_entities(et_bat, None)?;
    assert_eq!(bat_entities.len(), 1);
    assert!(bat_entities.contains(&bat_id));

    Ok(())
}