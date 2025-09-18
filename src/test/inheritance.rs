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
    let et_animal = EntityType::from("Animal");
    let et_mammal = EntityType::from("Mammal");
    let et_dog = EntityType::from("Dog");
    let et_cat = EntityType::from("Cat");
    
    // Create schemas with inheritance hierarchy:
    // Animal (base)
    //   └── Mammal (inherits from Animal)
    //       ├── Dog (inherits from Mammal)
    //       └── Cat (inherits from Mammal)
    
    // Base Animal schema
    let mut animal_schema = EntitySchema::<Single>::new(et_animal.clone(), vec![]);
    animal_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(animal_schema)];
    store.perform_mut(requests)?;

    // Mammal schema (inherits from Animal)
    let mut mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), vec![et_animal.clone()]);
    mammal_schema.fields.insert(
        FieldType::from("FurColor"),
        FieldSchema::String {
            field_type: FieldType::from("FurColor"),
            default_value: String::new(),
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(mammal_schema)];
    store.perform_mut(requests)?;

    // Dog schema (inherits from Mammal)
    let mut dog_schema = EntitySchema::<Single>::new(et_dog.clone(), vec![et_mammal.clone()]);
    dog_schema.fields.insert(
        FieldType::from("Breed"),
        FieldSchema::String {
            field_type: FieldType::from("Breed"),
            default_value: String::new(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(dog_schema)];
    store.perform_mut(requests)?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single>::new(et_cat.clone(), vec![et_mammal.clone()]);
    let requests = vec![sschemaupdate!(cat_schema)];
    store.perform_mut(requests)?;

    // Create some entities
    let create_requests = store.perform_mut(vec![screate!(
        et_dog.clone(),
        "Buddy".to_string()
    )])?;
    let dog1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let dog1_ref = dog1_id.clone();

    let create_requests = store.perform_mut(vec![screate!(
        et_dog.clone(),
        "Rex".to_string()
    )])?;
    let dog2_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let dog2_ref = dog2_id.clone();

    let create_requests = store.perform_mut(vec![screate!(
        et_cat.clone(),
        "Whiskers".to_string()
    )])?;
    let cat1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let cat1_ref = cat1_id.clone();

    // Test: Finding Dog entities should return only dogs
    let dogs = store.find_entities(&et_dog, None)?;
    assert_eq!(dogs.len(), 2);
    assert!(dogs.contains(&dog1_ref));
    assert!(dogs.contains(&dog2_ref));

    // Test: Finding Mammal entities should return dogs and cats (inheritance)
    let mammals = store.find_entities(&et_mammal, None)?;
    assert_eq!(mammals.len(), 3);
    assert!(mammals.contains(&dog1_ref));
    assert!(mammals.contains(&dog2_ref));
    assert!(mammals.contains(&cat1_ref));

    // Test: Finding Animal entities should return all (full inheritance chain)
    let animals = store.find_entities(&et_animal, None)?;
    assert_eq!(animals.len(), 3);
    assert!(animals.contains(&dog1_ref));
    assert!(animals.contains(&dog2_ref));
    assert!(animals.contains(&cat1_ref));

    Ok(())
}

#[test]
fn test_inheritance_with_direct_instances() -> Result<()> {
    let mut store = Store::new();

    let et_animal = EntityType::from("Animal");
    let et_mammal = EntityType::from("Mammal");

    // Create base Animal schema
    let animal_schema = EntitySchema::<Single>::new(et_animal.clone(), vec![]);
    store.perform_mut(vec![sschemaupdate!(animal_schema)])?;

    // Create Mammal schema that inherits from Animal
    let mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), vec![et_animal.clone()]);
    store.perform_mut(vec![sschemaupdate!(mammal_schema)])?;

    // Create direct instances of both types
    let create_requests = store.perform_mut(vec![screate!(
        et_animal.clone(),
        "Generic Animal".to_string()
    )])?;
    let animal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let animal1_ref = animal1_id.clone();

    let create_requests = store.perform_mut(vec![screate!(
        et_mammal.clone(),
        "Generic Mammal".to_string()
    )])?;
    let mammal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let mammal1_ref = mammal1_id.clone();

    // Test: Finding Animal entities should return both (mammal inherits from animal)
    let animals = store.find_entities(&et_animal, None)?;
    assert_eq!(animals.len(), 2);
    assert!(animals.contains(&animal1_ref));
    assert!(animals.contains(&mammal1_ref));

    // Test: Finding Mammal entities should return only the mammal
    let mammals = store.find_entities(&et_mammal, None)?;
    assert_eq!(mammals.len(), 1);
    assert!(mammals.contains(&mammal1_ref));

    Ok(())
}

#[test]
fn test_circular_inheritance_protection() -> Result<()> {
    let mut store = Store::new();

    let et_a = EntityType::from("TypeA");
    let et_b = EntityType::from("TypeB");

    // Create TypeA
    let schema_a: EntitySchema<Single> = EntitySchema::<Single>::new(et_a.clone(), vec![]);
    store.perform_mut(vec![sschemaupdate!(schema_a)])?;

    // Create TypeB that inherits from TypeA
    let schema_b = EntitySchema::<Single>::new(et_b.clone(), vec![et_a.clone()]);
    store.perform_mut(vec![sschemaupdate!(schema_b)])?;

    // Try to make TypeA inherit from TypeB (should fail or be ignored)
    let circular_schema_a = EntitySchema::<Single>::new(et_a.clone(), vec![et_b.clone()]);
    
    // This should either fail or the system should handle it gracefully
    let requests = vec![sschemaupdate!(circular_schema_a)];
    let result = store.perform_mut(requests);
    
    // The test passes if either:
    // 1. The operation fails (returns an error)
    // 2. The operation succeeds but circular inheritance is prevented internally
    match result {
        Ok(_) => {
            // If it succeeded, verify that circular inheritance is handled properly
            // by checking that the inheritance map doesn't create infinite loops
            store.perform_mut(vec![screate!(
                et_b.clone(),
                "Test B".to_string()
            )])?;
            let entities_a = store.find_entities(&et_a, None)?;
            
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

    // Create base types
    let et_flyable = EntityType::from("Flyable");
    let et_mammal = EntityType::from("Mammal");
    let et_bat = EntityType::from("Bat");
    
    // Create Flyable trait (interface-like entity type)
    let mut flyable_schema = EntitySchema::<Single>::new(et_flyable.clone(), vec![]);
    flyable_schema.fields.insert(
        FieldType::from("CanFly"),
        FieldSchema::Bool {
            field_type: FieldType::from("CanFly"),
            default_value: true,
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    flyable_schema.fields.insert(
        FieldType::from("WingSpan"),
        FieldSchema::Float {
            field_type: FieldType::from("WingSpan"),
            default_value: 0.0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(vec![sschemaupdate!(flyable_schema)])?;

    // Create Mammal schema
    let mut mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), vec![]);
    mammal_schema.fields.insert(
        FieldType::from("FurColor"),
        FieldSchema::String {
            field_type: FieldType::from("FurColor"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    mammal_schema.fields.insert(
        FieldType::from("IsWarmBlooded"),
        FieldSchema::Bool {
            field_type: FieldType::from("IsWarmBlooded"),
            default_value: true,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(vec![sschemaupdate!(mammal_schema)])?;

    // Create Bat schema that inherits from BOTH Flyable and Mammal
    let mut bat_schema = EntitySchema::<Single>::new(et_bat.clone(), vec![et_flyable.clone(), et_mammal.clone()]);
    bat_schema.fields.insert(
        FieldType::from("EcholocationRange"),
        FieldSchema::Float {
            field_type: FieldType::from("EcholocationRange"),
            default_value: 100.0,
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    store.perform_mut(vec![sschemaupdate!(bat_schema)])?;

    // Create a bat entity
    let create_requests = store.perform_mut(vec![screate!(
        et_bat.clone(),
        "Vampire Bat".to_string()
    )])?;
    let bat_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Verify that the bat has fields from all parent types
    let complete_schema = store.get_complete_entity_schema(&et_bat)?;
    
    // Should have fields from Flyable
    assert!(complete_schema.fields.contains_key(FieldType::from("CanFly")));
    assert!(complete_schema.fields.contains_key(FieldType::from("WingSpan")));
    
    // Should have fields from Mammal
    assert!(complete_schema.fields.contains_key(FieldType::from("FurColor")));
    assert!(complete_schema.fields.contains_key(FieldType::from("IsWarmBlooded")));
    
    // Should have its own field
    assert!(complete_schema.fields.contains_key(FieldType::from("EcholocationRange")));

    // Test inheritance lookup - searching for Flyable should find bats
    let flyable_entities = store.find_entities(&et_flyable, None)?;
    assert_eq!(flyable_entities.len(), 1);
    assert!(flyable_entities.contains(&bat_id));

    // Test inheritance lookup - searching for Mammal should find bats
    let mammal_entities = store.find_entities(&et_mammal, None)?;
    assert_eq!(mammal_entities.len(), 1);
    assert!(mammal_entities.contains(&bat_id));

    // Test inheritance lookup - searching for Bat should find bats
    let bat_entities = store.find_entities(&et_bat, None)?;
    assert_eq!(bat_entities.len(), 1);
    assert!(bat_entities.contains(&bat_id));

    Ok(())
}