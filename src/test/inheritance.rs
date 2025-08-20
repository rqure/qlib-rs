#[allow(unused_imports)]
use crate::*;
#[allow(unused_imports)]
use std::sync::Arc;

#[tokio::test]
async fn test_inheritance_in_find_entities() -> Result<()> {
    let store_interface = StoreInterface::new_shared_local(Store::new(Arc::new(Snowflake::new())));
    let ctx = Context::new(store_interface.clone());
    let mut store = store_interface;

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
        ft::name(),
        FieldSchema::String {
            field_type: ft::name(),
            default_value: "".to_string(),
            rank: 0,
        }
    );
    store.set_entity_schema(&ctx, &animal_schema).await?;

    // Mammal schema (inherits from Animal)
    let mut mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
    mammal_schema.fields.insert(
        FieldType::from("FurColor"),
        FieldSchema::String {
            field_type: FieldType::from("FurColor"),
            default_value: "brown".to_string(),
            rank: 1,
        }
    );
    store.set_entity_schema(&ctx, &mammal_schema).await?;

    // Dog schema (inherits from Mammal)
    let mut dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_mammal.clone()));
    dog_schema.fields.insert(
        FieldType::from("Breed"),
        FieldSchema::String {
            field_type: FieldType::from("Breed"),
            default_value: "mixed".to_string(),
            rank: 2,
        }
    );
    store.set_entity_schema(&ctx, &dog_schema).await?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single>::new(et_cat.clone(), Some(et_mammal.clone()));
    store.set_entity_schema(&ctx, &cat_schema).await?;

    // Create some entities
    let dog1 = store.create_entity(&ctx, &et_dog, None, "Buddy").await?;
    let dog2 = store.create_entity(&ctx, &et_dog, None, "Rex").await?;
    let cat1 = store.create_entity(&ctx, &et_cat, None, "Whiskers").await?;
    let cat2 = store.create_entity(&ctx, &et_cat, None, "Mittens").await?;

    // Test 1: Find exact Dog entities
    let dog_results = store.find_entities_exact(&ctx, &et_dog, None).await?;
    assert_eq!(dog_results.total, 2);
    assert!(dog_results.items.contains(&dog1.entity_id));
    assert!(dog_results.items.contains(&dog2.entity_id));

    // Test 2: Find exact Cat entities
    let cat_results = store.find_entities_exact(&ctx, &et_cat, None).await?;
    assert_eq!(cat_results.total, 2);
    assert!(cat_results.items.contains(&cat1.entity_id));
    assert!(cat_results.items.contains(&cat2.entity_id));

    // Test 3: Find Mammal entities (should include both dogs and cats)
    let mammal_results = store.find_entities(&ctx, &et_mammal).await?;
    assert_eq!(mammal_results.len(), 4);
    assert!(mammal_results.contains(&dog1.entity_id));
    assert!(mammal_results.contains(&dog2.entity_id));
    assert!(mammal_results.contains(&cat1.entity_id));
    assert!(mammal_results.contains(&cat2.entity_id));

    // Test 4: Find Animal entities (should include all animals)
    let animal_results = store.find_entities(&ctx, &et_animal).await?;
    assert_eq!(animal_results.len(), 4);
    assert!(animal_results.contains(&dog1.entity_id));
    assert!(animal_results.contains(&dog2.entity_id));
    assert!(animal_results.contains(&cat1.entity_id));
    assert!(animal_results.contains(&cat2.entity_id));

    // Test 5: Find exact Mammal entities (should be empty since we only created Dogs and Cats)
    let exact_mammal_results = store.find_entities_exact(&ctx, &et_mammal, None).await?;
    assert_eq!(exact_mammal_results.total, 0);

    // Test 6: Find exact Animal entities (should be empty since we only created Dogs and Cats)
    let exact_animal_results = store.find_entities_exact(&ctx, &et_animal, None).await?;
    assert_eq!(exact_animal_results.total, 0);

    Ok(())
}

#[tokio::test]
async fn test_inheritance_with_direct_instances() -> Result<()> {
    let store_interface = StoreInterface::new_shared_local(Store::new(Arc::new(Snowflake::new())));
    let ctx = Context::new(store_interface.clone());
    let mut store = store_interface;

    // Test with a hierarchy where we create instances of base types too
    let et_vehicle = EntityType::from("Vehicle");
    let et_car = EntityType::from("Car");
    let et_sedan = EntityType::from("Sedan");
    
    // Vehicle schema (base)
    let vehicle_schema = EntitySchema::<Single>::new(et_vehicle.clone(), None);
    store.set_entity_schema(&ctx, &vehicle_schema).await?;

    // Car schema (inherits from Vehicle)
    let car_schema = EntitySchema::<Single>::new(et_car.clone(), Some(et_vehicle.clone()));
    store.set_entity_schema(&ctx, &car_schema).await?;

    // Sedan schema (inherits from Car)
    let sedan_schema = EntitySchema::<Single>::new(et_sedan.clone(), Some(et_car.clone()));
    store.set_entity_schema(&ctx, &sedan_schema).await?;

    // Create instances at different levels
    let vehicle1 = store.create_entity(&ctx, &et_vehicle, None, "Generic Vehicle").await?;
    let car1 = store.create_entity(&ctx, &et_car, None, "Generic Car").await?;
    let sedan1 = store.create_entity(&ctx, &et_sedan, None, "Toyota Camry").await?;
    let sedan2 = store.create_entity(&ctx, &et_sedan, None, "Honda Accord").await?;

    // Test finding all Vehicle entities (should include everything)
    let vehicle_results = store.find_entities(&ctx, &et_vehicle).await?;
    assert_eq!(vehicle_results.len(), 4);
    assert!(vehicle_results.contains(&vehicle1.entity_id));
    assert!(vehicle_results.contains(&car1.entity_id));
    assert!(vehicle_results.contains(&sedan1.entity_id));
    assert!(vehicle_results.contains(&sedan2.entity_id));

    // Test finding all Car entities (should include cars and sedans, but not generic vehicles)
    let car_results = store.find_entities(&ctx, &et_car).await?;
    assert_eq!(car_results.len(), 3);
    assert!(car_results.contains(&car1.entity_id));
    assert!(car_results.contains(&sedan1.entity_id));
    assert!(car_results.contains(&sedan2.entity_id));
    assert!(!car_results.contains(&vehicle1.entity_id));

    // Test finding exact Vehicle entities (should only include the generic vehicle)
    let exact_vehicle_results = store.find_entities_exact(&ctx, &et_vehicle, None).await?;
    assert_eq!(exact_vehicle_results.total, 1);
    assert!(exact_vehicle_results.items.contains(&vehicle1.entity_id));

    Ok(())
}

#[tokio::test]
async fn test_circular_inheritance_protection() -> Result<()> {
    let store_interface = StoreInterface::new_shared_local(Store::new(Arc::new(Snowflake::new())));
    let ctx = Context::new(store_interface.clone());
    let mut store = store_interface;

    // Try to create circular inheritance: A -> B -> C -> A
    let et_a = EntityType::from("TypeA");
    let et_b = EntityType::from("TypeB");
    let et_c = EntityType::from("TypeC");
    
    // Create initial schemas without inheritance first
    let schema_a = EntitySchema::<Single>::new(et_a.clone(), None);
    let schema_b = EntitySchema::<Single>::new(et_b.clone(), None);
    let schema_c = EntitySchema::<Single>::new(et_c.clone(), None);
    
    store.set_entity_schema(&ctx, &schema_a).await?;
    store.set_entity_schema(&ctx, &schema_b).await?;
    store.set_entity_schema(&ctx, &schema_c).await?;
    
    // Now update them to create circular inheritance: A -> B -> C -> A
    let schema_a_circular = EntitySchema::<Single>::new(et_a.clone(), Some(et_b.clone()));
    let schema_b_circular = EntitySchema::<Single>::new(et_b.clone(), Some(et_c.clone()));
    let schema_c_circular = EntitySchema::<Single>::new(et_c.clone(), Some(et_a.clone()));
    
    store.set_entity_schema(&ctx, &schema_a_circular).await?;
    store.set_entity_schema(&ctx, &schema_b_circular).await?;
    store.set_entity_schema(&ctx, &schema_c_circular).await?;
    
    // Create an entity of TypeA
    let _entity_a = store.create_entity(&ctx, &et_a, None, "EntityA").await?;
    
    // The inheritance system should not crash or enter infinite loops
    // Even with circular inheritance, find_entities should work (though results may be unexpected)
    let results_a = store.find_entities(&ctx, &et_a).await?;
    let results_b = store.find_entities(&ctx, &et_b).await?;
    let results_c = store.find_entities(&ctx, &et_c).await?;
    
    // All should return the same entity since A, B, C all inherit from each other in a cycle
    // The exact behavior is implementation-defined but should not crash
    assert!(results_a.len() > 0);
    assert!(results_b.len() > 0);
    assert!(results_c.len() > 0);
    
    Ok(())
}
