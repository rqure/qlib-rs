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
    store.set_entity_schema(&animal_schema).await?;

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
    store.set_entity_schema(&mammal_schema).await?;

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
    store.set_entity_schema(&dog_schema).await?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single>::new(et_cat.clone(), Some(et_mammal.clone()));
    store.set_entity_schema(&cat_schema).await?;

    // Create some entities
    let dog1 = store.create_entity(&et_dog, None, "Buddy").await?;
    let dog2 = store.create_entity(&et_dog, None, "Rex").await?;
    let cat1 = store.create_entity(&et_cat, None, "Whiskers").await?;

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
    store.set_entity_schema(&animal_schema).await?;

    // Create Mammal schema that inherits from Animal
    let mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
    store.set_entity_schema(&mammal_schema).await?;

    // Create direct instances of both types
    let animal1 = store.create_entity(&et_animal, None, "Generic Animal").await?;
    let mammal1 = store.create_entity(&et_mammal, None, "Generic Mammal").await?;

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
    store.set_entity_schema(&schema_a).await?;

    // Create TypeB that inherits from TypeA
    let schema_b = EntitySchema::<Single>::new(et_b.clone(), Some(et_a.clone()));
    store.set_entity_schema(&schema_b).await?;

    // Try to make TypeA inherit from TypeB (should fail or be ignored)
    let circular_schema_a = EntitySchema::<Single>::new(et_a.clone(), Some(et_b.clone()));
    
    // This should either fail or the system should handle it gracefully
    let result = store.set_entity_schema(&circular_schema_a).await;
    
    // The test passes if either:
    // 1. The operation fails (returns an error)
    // 2. The operation succeeds but circular inheritance is prevented internally
    match result {
        Ok(_) => {
            // If it succeeded, verify that circular inheritance is handled properly
            // by checking that the inheritance map doesn't create infinite loops
            let _entity_b = store.create_entity(&et_b, None, "Test B").await?;
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
