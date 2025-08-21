#[allow(unused_imports)]
use crate::*;
#[allow(unused_imports)]
use std::sync::Arc;

#[tokio::test]
async fn test_inheritance_in_find_entities() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));

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
    let mut animal_schema = EntitySchema::<Single>::new(et_animal.clone(), None);
    animal_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
        }
    );
    let mut requests = vec![Request::SchemaUpdate { schema: animal_schema }];
    store.perform(&mut requests).await?;

    // Mammal schema (inherits from Animal)
    let mut mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
    mammal_schema.fields.insert(
        FieldType::from("FurColor"),
        FieldSchema::String {
            field_type: FieldType::from("FurColor"),
            default_value: String::new(),
            rank: 1,
        }
    );
    let mut requests = vec![Request::SchemaUpdate { schema: mammal_schema }];
    store.perform(&mut requests).await?;

    // Dog schema (inherits from Mammal)
    let mut dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_mammal.clone()));
    dog_schema.fields.insert(
        FieldType::from("Breed"),
        FieldSchema::String {
            field_type: FieldType::from("Breed"),
            default_value: String::new(),
            rank: 2,
        }
    );
    let mut requests = vec![Request::SchemaUpdate { schema: dog_schema }];
    store.perform(&mut requests).await?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single>::new(et_cat.clone(), Some(et_mammal.clone()));
    let mut requests = vec![Request::SchemaUpdate { schema: cat_schema }];
    store.perform(&mut requests).await?;

    // Create some entities
    let mut create_requests = vec![Request::Create {
        entity_type: et_dog.clone(),
        parent_id: None,
        name: "Buddy".to_string(),
        created_entity_id: None,
    }];
    store.perform(&mut create_requests).await?;
    let dog1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let dog1 = Entity::new(dog1_id);

    let mut create_requests = vec![Request::Create {
        entity_type: et_dog.clone(),
        parent_id: None,
        name: "Rex".to_string(),
        created_entity_id: None,
    }];
    store.perform(&mut create_requests).await?;
    let dog2_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let dog2 = Entity::new(dog2_id);

    let mut create_requests = vec![Request::Create {
        entity_type: et_cat.clone(),
        parent_id: None,
        name: "Whiskers".to_string(),
        created_entity_id: None,
    }];
    store.perform(&mut create_requests).await?;
    let cat1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let cat1 = Entity::new(cat1_id);

    // Test: Finding Dog entities should return only dogs
    let dogs = store.find_entities(&et_dog).await?;
    assert_eq!(dogs.len(), 2);
    assert!(dogs.contains(&dog1.entity_id));
    assert!(dogs.contains(&dog2.entity_id));

    // Test: Finding Mammal entities should return dogs and cats (inheritance)
    let mammals = store.find_entities(&et_mammal).await?;
    assert_eq!(mammals.len(), 3);
    assert!(mammals.contains(&dog1.entity_id));
    assert!(mammals.contains(&dog2.entity_id));
    assert!(mammals.contains(&cat1.entity_id));

    // Test: Finding Animal entities should return all (full inheritance chain)
    let animals = store.find_entities(&et_animal).await?;
    assert_eq!(animals.len(), 3);
    assert!(animals.contains(&dog1.entity_id));
    assert!(animals.contains(&dog2.entity_id));
    assert!(animals.contains(&cat1.entity_id));

    Ok(())
}

#[tokio::test]
async fn test_inheritance_with_direct_instances() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));

    let et_animal = EntityType::from("Animal");
    let et_mammal = EntityType::from("Mammal");

    // Create base Animal schema
    let animal_schema = EntitySchema::<Single>::new(et_animal.clone(), None);
    let mut requests = vec![Request::SchemaUpdate { schema: animal_schema }];
    store.perform(&mut requests).await?;

    // Create Mammal schema that inherits from Animal
    let mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
    let mut requests = vec![Request::SchemaUpdate { schema: mammal_schema }];
    store.perform(&mut requests).await?;

    // Create direct instances of both types
    let mut create_requests = vec![Request::Create {
        entity_type: et_animal.clone(),
        parent_id: None,
        name: "Generic Animal".to_string(),
        created_entity_id: None,
    }];
    store.perform(&mut create_requests).await?;
    let animal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let animal1 = Entity::new(animal1_id);

    let mut create_requests = vec![Request::Create {
        entity_type: et_mammal.clone(),
        parent_id: None,
        name: "Generic Mammal".to_string(),
        created_entity_id: None,
    }];
    store.perform(&mut create_requests).await?;
    let mammal1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };
    let mammal1 = Entity::new(mammal1_id);

    // Test: Finding Animal entities should return both (mammal inherits from animal)
    let animals = store.find_entities(&et_animal).await?;
    assert_eq!(animals.len(), 2);
    assert!(animals.contains(&animal1.entity_id));
    assert!(animals.contains(&mammal1.entity_id));

    // Test: Finding Mammal entities should return only the mammal
    let mammals = store.find_entities(&et_mammal).await?;
    assert_eq!(mammals.len(), 1);
    assert!(mammals.contains(&mammal1.entity_id));

    Ok(())
}

#[tokio::test]
async fn test_circular_inheritance_protection() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));

    let et_a = EntityType::from("TypeA");
    let et_b = EntityType::from("TypeB");

    // Create TypeA
    let schema_a = EntitySchema::<Single>::new(et_a.clone(), None);
    let mut requests = vec![Request::SchemaUpdate { schema: schema_a }];
    store.perform(&mut requests).await?;

    // Create TypeB that inherits from TypeA
    let schema_b = EntitySchema::<Single>::new(et_b.clone(), Some(et_a.clone()));
    let mut requests = vec![Request::SchemaUpdate { schema: schema_b }];
    store.perform(&mut requests).await?;

    // Try to make TypeA inherit from TypeB (should fail or be ignored)
    let circular_schema_a = EntitySchema::<Single>::new(et_a.clone(), Some(et_b.clone()));
    
    // This should either fail or the system should handle it gracefully
    let mut requests = vec![Request::SchemaUpdate { schema: circular_schema_a }];
    let result = store.perform(&mut requests).await;
    
    // The test passes if either:
    // 1. The operation fails (returns an error)
    // 2. The operation succeeds but circular inheritance is prevented internally
    match result {
        Ok(_) => {
            // If it succeeded, verify that circular inheritance is handled properly
            // by checking that the inheritance map doesn't create infinite loops
            let mut create_requests = vec![Request::Create {
                entity_type: et_b.clone(),
                parent_id: None,
                name: "Test B".to_string(),
                created_entity_id: None,
            }];
            store.perform(&mut create_requests).await?;
            let entities_a = store.find_entities(&et_a).await?;
            
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
