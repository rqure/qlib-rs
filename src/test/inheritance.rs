#[allow(unused_imports)]
use crate::*;
#[allow(unused_imports)]
use std::sync::Arc;

#[test]
fn test_inheritance_in_find_entities() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

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
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    store.set_entity_schema(&ctx, &animal_schema)?;

    // Mammal schema (inherits from Animal)
    let mut mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
    mammal_schema.fields.insert(
        FieldType::from("FurColor"),
        FieldSchema::String {
            field_type: FieldType::from("FurColor"),
            default_value: "brown".to_string(),
            rank: 1,
            read_permission: None,
            write_permission: None,
        }
    );
    store.set_entity_schema(&ctx, &mammal_schema)?;

    // Dog schema (inherits from Mammal)
    let mut dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_mammal.clone()));
    dog_schema.fields.insert(
        FieldType::from("Breed"),
        FieldSchema::String {
            field_type: FieldType::from("Breed"),
            default_value: "mixed".to_string(),
            rank: 2,
            read_permission: None,
            write_permission: None,
        }
    );
    store.set_entity_schema(&ctx, &dog_schema)?;

    // Cat schema (inherits from Mammal)
    let cat_schema = EntitySchema::<Single>::new(et_cat.clone(), Some(et_mammal.clone()));
    store.set_entity_schema(&ctx, &cat_schema)?;

    // Create some entities
    let dog1 = store.create_entity(&ctx, &et_dog, None, "Buddy")?;
    let dog2 = store.create_entity(&ctx, &et_dog, None, "Rex")?;
    let cat1 = store.create_entity(&ctx, &et_cat, None, "Whiskers")?;
    let cat2 = store.create_entity(&ctx, &et_cat, None, "Mittens")?;

    // Test 1: Find exact Dog entities
    let dog_results = store.find_entities_exact(&ctx, &et_dog, None)?;
    assert_eq!(dog_results.total, 2);
    assert!(dog_results.items.contains(&dog1.entity_id));
    assert!(dog_results.items.contains(&dog2.entity_id));

    // Test 2: Find exact Cat entities
    let cat_results = store.find_entities_exact(&ctx, &et_cat, None)?;
    assert_eq!(cat_results.total, 2);
    assert!(cat_results.items.contains(&cat1.entity_id));
    assert!(cat_results.items.contains(&cat2.entity_id));

    // Test 3: Find Mammal entities (should include both dogs and cats)
    let mammal_results = store.find_entities(&ctx, &et_mammal, None)?;
    assert_eq!(mammal_results.total, 4);
    assert!(mammal_results.items.contains(&dog1.entity_id));
    assert!(mammal_results.items.contains(&dog2.entity_id));
    assert!(mammal_results.items.contains(&cat1.entity_id));
    assert!(mammal_results.items.contains(&cat2.entity_id));

    // Test 4: Find Animal entities (should include all animals)
    let animal_results = store.find_entities(&ctx, &et_animal, None)?;
    assert_eq!(animal_results.total, 4);
    assert!(animal_results.items.contains(&dog1.entity_id));
    assert!(animal_results.items.contains(&dog2.entity_id));
    assert!(animal_results.items.contains(&cat1.entity_id));
    assert!(animal_results.items.contains(&cat2.entity_id));

    // Test 5: Find exact Mammal entities (should be empty since we only created Dogs and Cats)
    let exact_mammal_results = store.find_entities_exact(&ctx, &et_mammal, None)?;
    assert_eq!(exact_mammal_results.total, 0);

    // Test 6: Find exact Animal entities (should be empty since we only created Dogs and Cats)
    let exact_animal_results = store.find_entities_exact(&ctx, &et_animal, None)?;
    assert_eq!(exact_animal_results.total, 0);

    Ok(())
}

#[test]
fn test_inheritance_with_direct_instances() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    // Create hierarchy with direct instances at each level
    let et_vehicle = EntityType::from("Vehicle");
    let et_car = EntityType::from("Car");
    let et_sedan = EntityType::from("Sedan");
    
    // Vehicle schema
    let vehicle_schema = EntitySchema::<Single>::new(et_vehicle.clone(), None);
    store.set_entity_schema(&ctx, &vehicle_schema)?;

    // Car schema (inherits from Vehicle)
    let car_schema = EntitySchema::<Single>::new(et_car.clone(), Some(et_vehicle.clone()));
    store.set_entity_schema(&ctx, &car_schema)?;

    // Sedan schema (inherits from Car)
    let sedan_schema = EntitySchema::<Single>::new(et_sedan.clone(), Some(et_car.clone()));
    store.set_entity_schema(&ctx, &sedan_schema)?;

    // Create instances at each level
    let vehicle1 = store.create_entity(&ctx, &et_vehicle, None, "Generic Vehicle")?;
    let car1 = store.create_entity(&ctx, &et_car, None, "Generic Car")?;
    let sedan1 = store.create_entity(&ctx, &et_sedan, None, "Toyota Camry")?;
    let sedan2 = store.create_entity(&ctx, &et_sedan, None, "Honda Accord")?;

    // Test: Find all vehicles (should include everything)
    let vehicle_results = store.find_entities(&ctx, &et_vehicle, None)?;
    assert_eq!(vehicle_results.total, 4);
    assert!(vehicle_results.items.contains(&vehicle1.entity_id));
    assert!(vehicle_results.items.contains(&car1.entity_id));
    assert!(vehicle_results.items.contains(&sedan1.entity_id));
    assert!(vehicle_results.items.contains(&sedan2.entity_id));

    // Test: Find all cars (should include cars and sedans, but not vehicles)
    let car_results = store.find_entities(&ctx, &et_car, None)?;
    assert_eq!(car_results.total, 3);
    assert!(!car_results.items.contains(&vehicle1.entity_id));
    assert!(car_results.items.contains(&car1.entity_id));
    assert!(car_results.items.contains(&sedan1.entity_id));
    assert!(car_results.items.contains(&sedan2.entity_id));

    // Test: Find exact vehicles (should only include the direct vehicle instance)
    let exact_vehicle_results = store.find_entities_exact(&ctx, &et_vehicle, None)?;
    assert_eq!(exact_vehicle_results.total, 1);
    assert!(exact_vehicle_results.items.contains(&vehicle1.entity_id));

    Ok(())
}

#[test]
fn test_circular_inheritance_protection() -> Result<()> {
    let mut store = Store::new(Arc::new(Snowflake::new()));
    let ctx = Context {};

    // Try to create circular inheritance: A -> B -> C -> A
    let et_a = EntityType::from("TypeA");
    let et_b = EntityType::from("TypeB");
    let et_c = EntityType::from("TypeC");
    
    // Create initial schemas without inheritance first
    let schema_a = EntitySchema::<Single>::new(et_a.clone(), None);
    let schema_b = EntitySchema::<Single>::new(et_b.clone(), None);
    let schema_c = EntitySchema::<Single>::new(et_c.clone(), None);
    
    store.set_entity_schema(&ctx, &schema_a)?;
    store.set_entity_schema(&ctx, &schema_b)?;
    store.set_entity_schema(&ctx, &schema_c)?;
    
    // Now update them to create circular inheritance: A -> B -> C -> A
    let schema_a_circular = EntitySchema::<Single>::new(et_a.clone(), Some(et_b.clone()));
    let schema_b_circular = EntitySchema::<Single>::new(et_b.clone(), Some(et_c.clone()));
    let schema_c_circular = EntitySchema::<Single>::new(et_c.clone(), Some(et_a.clone()));
    
    store.set_entity_schema(&ctx, &schema_a_circular)?;
    store.set_entity_schema(&ctx, &schema_b_circular)?;
    store.set_entity_schema(&ctx, &schema_c_circular)?;
    
    // Create an entity of TypeA
    let _entity_a = store.create_entity(&ctx, &et_a, None, "EntityA")?;
    
    // The inheritance system should not crash or enter infinite loops
    // Even with circular inheritance, find_entities should work (though results may be unexpected)
    let results_a = store.find_entities(&ctx, &et_a, None)?;
    let results_b = store.find_entities(&ctx, &et_b, None)?;
    let results_c = store.find_entities(&ctx, &et_c, None)?;
    
    // All should return the same entity since A, B, C all inherit from each other in a cycle
    // The exact behavior is implementation-defined but should not crash
    assert!(results_a.total > 0);
    assert!(results_b.total > 0);
    assert!(results_c.total > 0);
    
    Ok(())
}
