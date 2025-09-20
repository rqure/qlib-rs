use crate::*;
use crate::data::StorageScope;

// Helper to create an entity schema with basic fields
fn create_entity_schema_with_name(store: &mut Store, entity_type_name: &str) -> Result<()> {
    let mut schema = EntitySchema::<Single, String, String>::new(entity_type_name.to_string(), vec![]);

    // Add default fields common to all entities
    schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: "".to_string(),
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );

    schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );

    schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: vec![],
            rank: 3,
            storage_scope: StorageScope::Configuration,
        }
    );

    let requests = vec![sschemaupdate!(schema)];
    store.perform_mut(requests)?;
    Ok(())
}

// Helper to set up a basic database structure for testing
#[allow(dead_code)]
fn setup_test_database() -> Result<Store> {
    let mut store = Store::new();

    create_entity_schema_with_name(&mut store, "Root")?;
    create_entity_schema_with_name(&mut store, "Folder")?;
    create_entity_schema_with_name(&mut store, "User")?;
    create_entity_schema_with_name(&mut store, "Role")?;

    Ok(store)
}

#[test]
fn test_create_entity_hierarchy() -> Result<()> {
    let mut store = setup_test_database()?;

    let et_root = store.get_entity_type("Root")?;
    let et_folder = store.get_entity_type("Folder")?;
    let et_user = store.get_entity_type("User")?;

    // Find root entities (should be empty initially)
    let root_entities = store.find_entities(et_root, None)?;
    assert_eq!(root_entities.len(), 0);

    // Create root entity
    let create_requests = vec![screate!(
        et_folder,
        "Security Models".to_string()
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let root_entity_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let root_entity_id_ref = root_entity_id;

    let create_requests = vec![screate!(
        et_folder,
        "Users".to_string(),
        root_entity_id_ref
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let users_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let users_folder_id_ref = users_folder_id;

    let create_requests = vec![screate!(
        et_folder,
        "Roles".to_string(),
        root_entity_id_ref
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let roles_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let roles_folder_id_ref = roles_folder_id;

    let create_requests = vec![screate!(
        et_user,
        "qei".to_string(),
        users_folder_id_ref
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let user_id_ref = user_id;

    let create_requests = vec![screate!(
        et_user,
        "admin".to_string(),
        roles_folder_id_ref
    )];
    store.perform_mut(create_requests)?;

    // Test relationships
    let ft_parent = store.get_field_type("Parent")?;
    let ft_name = store.get_field_type("Name")?;
    
    let reqs = store.perform_mut(vec![
        sread!(user_id_ref, vec![ft_parent]),
        sread!(user_id_ref, vec![ft_name]),
    ])?;

    if let Some(Request::Read { value: Some(Value::EntityReference(Some(parent_id))), .. }) = reqs.get(0) {
        assert_eq!(*parent_id, users_folder_id_ref);
    } else {
        panic!("Expected parent reference");
    }

    // Verify name
    let reqs = store.perform_mut(vec![
        sread!(users_folder_id_ref, vec![ft_name]),
    ])?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = reqs.get(0) {
        assert_eq!(name, "Users");
    } else {
        panic!("Expected folder name");
    }

    Ok(())
}

#[test]
fn test_field_operations() -> Result<()> {
    let mut store = setup_test_database()?;

    let et_folder = store.get_entity_type("Folder")?;
    let et_user = store.get_entity_type("User")?;

    let create_requests = vec![screate!(
        et_folder,
        "Users".to_string()
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let users_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    let create_requests = vec![screate!(
        et_user,
        "testuser".to_string(),
        users_folder_id
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let user_ref = user_id;

    // Test write and read operations
    let ft_name = store.get_field_type("Name")?;
    store.perform_mut(vec![
        swrite!(user_ref, vec![ft_name], sstr!("Updated User")),
    ])?;

    let reads = store.perform_mut(vec![
        sread!(user_ref, vec![ft_name]),
    ])?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = reads.get(0) {
        assert_eq!(name, "Updated User");
    } else {
        panic!("Expected updated name");
    }

    // Test field updates
    store.perform_mut(vec![
        swrite!(user_ref, vec![ft_name], sstr!("Final Name")),
    ])?;

    let verify = store.perform_mut(vec![
        sread!(user_ref, vec![ft_name]),
    ])?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = verify.get(0) {
        assert_eq!(name, "Final Name");
    } else {
        panic!("Expected final name");
    }

    Ok(())
}

#[test]
fn test_indirection_resolution() -> Result<()> {
    let mut store = setup_test_database()?;

    let et_folder = store.get_entity_type("Folder")?;
    let et_user = store.get_entity_type("User")?;

    // Create entities
    let create_requests = vec![screate!(
        et_folder,
        "Security".to_string()
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let security_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let security_folder_ref = security_folder_id;

    let create_requests = vec![screate!(
        et_folder,
        "Users".to_string(),
        security_folder_ref
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let users_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let users_folder_ref = users_folder_id;

    let create_requests = vec![screate!(
        et_user,
        "admin".to_string(),
        users_folder_ref
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let admin_user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let admin_user_ref = admin_user_id;

    // Test indirection: User->Parent->Name should resolve to "Users"
    let ft_parent = store.get_field_type("Parent")?;
    let ft_name = store.get_field_type("Name")?;
    let parent_name_field = vec![ft_parent, ft_name];

    store.perform_mut(vec![
        swrite!(admin_user_ref, vec![ft_name], sstr!("Administrator")),
    ])?;

    // Test indirection resolution
    let indirect_reads = store.perform_mut(vec![
        sread!(admin_user_ref, parent_name_field),
    ])?;

    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = indirect_reads.get(0) {
        assert_eq!(name, "Users");
    } else {
        panic!("Expected indirection to resolve to parent name");
    }

    Ok(())
}

#[test]
fn test_entity_deletion() -> Result<()> {
    let mut store = setup_test_database()?;

    let et_folder = store.get_entity_type("Folder")?;
    let et_user = store.get_entity_type("User")?;

    let create_requests = vec![screate!(
        et_folder,
        "Users".to_string()
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let users_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    let create_requests = vec![screate!(
        et_user,
        "testuser".to_string(),
        users_folder_id
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };
    let user_ref = user_id;

    // Verify entity exists
    assert!(store.entity_exists(user_ref));

    // Delete the entity
    store.perform_mut(vec![sdelete!(user_ref)])?;

    // Verify entity is gone
    assert!(!store.entity_exists(user_ref));

    // Try to read from deleted entity - the request should succeed but return no value
    let ft_name = store.get_field_type("Name")?;
    let request = vec![sread!(user_ref, vec![ft_name])];
    let result = store.perform_mut(request);
    
    // The request should fail for a deleted entity
    match result {
        Err(_) => (), // Expected error for deleted entity
        Ok(_) => panic!("Expected error when reading from deleted entity"),
    }

    Ok(())
}

#[test]
fn test_entity_listing_with_pagination() -> Result<()> {
    let mut store = setup_test_database()?;

    let et_folder = store.get_entity_type("Folder")?;
    let et_user = store.get_entity_type("User")?;

    let create_requests = vec![screate!(
        et_folder,
        "Users".to_string()
    )];
    let create_requests = store.perform_mut(create_requests)?;
    let users_folder_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        *id
    } else {
        panic!("Expected created entity ID");
    };

    // Create multiple users
    for i in 0..5 {
        let create_requests = vec![screate!(
            et_user,
            format!("user{}", i),
            users_folder_id
        )];
        store.perform_mut(create_requests)?;
    }

    let user_entities = store.find_entities(et_user, None)?;
    assert_eq!(user_entities.len(), 5);

    Ok(())
}

#[test]
fn test_cel_filtering_parameters() -> Result<()> {
    let mut store = setup_test_database()?;
    
    let et_user = store.get_entity_type("User")?;
    
    // Create some test users
    let create_requests = vec![
        screate!(et_user, "user1".to_string()),
        screate!(et_user, "user2".to_string()),
        screate!(et_user, "user3".to_string()),
    ];
    store.perform_mut(create_requests)?;
    
    // Test with None filter (should work fine)
    let all_users = store.find_entities(et_user, None)?;
    assert_eq!(all_users.len(), 3);
    
    let paginated_users = store.find_entities_paginated(et_user, None, None)?;
    assert_eq!(paginated_users.items.len(), 3);
    
    let exact_users = store.find_entities_exact(et_user, None, None)?;
    assert_eq!(exact_users.items.len(), 3);

    // Test with CEL filter
    let all_filtered = store.find_entities(et_user, Some("true".to_string()))?;
    assert_eq!(all_filtered.len(), 3); // "true" should match all entities
    
    let none_filtered = store.find_entities(et_user, Some("false".to_string()))?;
    assert_eq!(none_filtered.len(), 0); // "false" should match no entities

    Ok(())
}#[test]
fn test_find_entities_comprehensive() -> Result<()> {
    // Create a fresh store without using setup_test_database
    let mut store = Store::new();
    
    // Create a simple schema with just Name field using string types first
    let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec![]);
    user_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    user_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = vec![sschemaupdate!(user_schema)];
    store.perform_mut(requests)?;
    
    // Now we can get the interned types
    let et_user = store.get_entity_type("User")?;
    let ft_name = store.get_field_type("Name")?;
    
    // Test finding entities when none exist
    let empty_users = store.find_entities(et_user, None)?;
    assert_eq!(empty_users.len(), 0);
    
    let empty_paginated = store.find_entities_paginated(et_user, None, None)?;
    assert_eq!(empty_paginated.items.len(), 0);
    assert_eq!(empty_paginated.total, 0);
    assert!(empty_paginated.next_cursor.is_none());
    
    // Create test entities with various field values
    let create_requests = vec![
        screate!(et_user, "Alice".to_string()),
        screate!(et_user, "Bob".to_string()),
        screate!(et_user, "Charlie".to_string()),
    ];
    let create_requests = store.perform_mut(create_requests)?;
    
    // Extract created entity IDs for later use
    let alice_id = create_requests[0].entity_id().unwrap();
    
    // Verify the names were set correctly
    let name_read = vec![sread!(alice_id, vec![ft_name])];
    let name_read = store.perform_mut(name_read)?;
    if let Some(Request::Read { value: Some(Value::String(alice_name)), .. }) = name_read.get(0) {
        println!("Alice's name in store: '{}'", alice_name);
        assert_eq!(alice_name, "Alice");
    } else {
        panic!("Alice's name not found or wrong type");
    }
    
    // Test basic find_entities
    let all_users = store.find_entities(et_user, None)?;
    assert_eq!(all_users.len(), 3);
    
    // Test find_entities_exact (should be same as find_entities for non-inherited types)
    let exact_users = store.find_entities_exact(et_user, None, None)?;
    assert_eq!(exact_users.items.len(), 3);
    assert_eq!(exact_users.total, 3);
    
    // Test CEL filtering with string comparison
    let name_filtered = store.find_entities(et_user, Some("Name == \"Alice\"".to_string()))?;
    println!("Name filtered results: {:?}, expected 1", name_filtered.len());
    assert_eq!(name_filtered.len(), 1);
    if !name_filtered.is_empty() {
        assert_eq!(name_filtered[0], alice_id);
    }
    
    // Test basic boolean CEL filters
    let all_filtered = store.find_entities(et_user, Some("true".to_string()))?;
    assert_eq!(all_filtered.len(), 3); // "true" should match all entities
    
    let none_filtered = store.find_entities(et_user, Some("false".to_string()))?;
    assert_eq!(none_filtered.len(), 0); // "false" should match no entities

    Ok(())
}

#[test]
fn test_find_entities_pagination() -> Result<()> {
    let mut store = Store::new();
    
    // Create a simple schema using string types first
    let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec![]);
    user_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    user_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = vec![sschemaupdate!(user_schema)];
    store.perform_mut(requests)?;
    
    // Now get the interned types
    let et_user = store.get_entity_type("User")?;
    let ft_name = store.get_field_type("Name")?;
    
    // Create 10 test users
    for i in 0..10 {
        let create_requests = vec![screate!(
            et_user, 
            format!("User{:02}", i)
        )];
        store.perform_mut(create_requests)?;
    }
    
    // Test pagination with different page sizes
    let page_opts = PageOpts::new(3, None);
    let first_page = store.find_entities_paginated(et_user, Some(page_opts), None)?;
    assert_eq!(first_page.items.len(), 3);
    assert_eq!(first_page.total, 10);
    assert!(first_page.next_cursor.is_some());
    
    // Get second page using cursor
    let page_opts = PageOpts::new(3, first_page.next_cursor);
    let second_page = store.find_entities_paginated(et_user, Some(page_opts), None)?;
    assert_eq!(second_page.items.len(), 3);
    assert_eq!(second_page.total, 10);
    assert!(second_page.next_cursor.is_some());
    
    // Get third page
    let page_opts = PageOpts::new(3, second_page.next_cursor);
    let third_page = store.find_entities_paginated(et_user, Some(page_opts), None)?;
    assert_eq!(third_page.items.len(), 3);
    assert_eq!(third_page.total, 10);
    assert!(third_page.next_cursor.is_some());
    
    // Get fourth (final) page
    let page_opts = PageOpts::new(3, third_page.next_cursor);
    let fourth_page = store.find_entities_paginated(et_user, Some(page_opts), None)?;
    assert_eq!(fourth_page.items.len(), 1); // Only 1 item left
    assert_eq!(fourth_page.total, 10);
    assert!(fourth_page.next_cursor.is_none()); // No more pages
    
    // Test large page size (should get all items)
    let large_page = PageOpts::new(20, None);
    let all_page = store.find_entities_paginated(et_user, Some(large_page), None)?;
    assert_eq!(all_page.items.len(), 10);
    assert_eq!(all_page.total, 10);
    assert!(all_page.next_cursor.is_none());
    
    // Test zero page size (should return no results)
    let zero_page = PageOpts::new(0, None);
    let zero_result = store.find_entities_paginated(et_user, Some(zero_page), None)?;
    assert_eq!(zero_result.items.len(), 0); // Zero limit should return no items
    assert_eq!(zero_result.total, 10); // But total should still be correct
    
    // Test with invalid cursor
    let invalid_page = PageOpts::new(5, Some("invalid".to_string()));
    let invalid_result = store.find_entities_paginated(et_user, Some(invalid_page), None)?;
    assert_eq!(invalid_result.items.len(), 5); // Should start from beginning
    
    Ok(())
}

#[test]
fn test_find_entities_inheritance() -> Result<()> {
    let mut store = Store::new();
    
    // Create inheritance hierarchy: Animal -> Mammal -> Dog/Cat using string schemas first
    
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
            storage_scope: StorageScope::Runtime,
        }
    );
    animal_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: vec![],
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(animal_schema)];
    store.perform_mut(requests)?;
    
    // Create Mammal schema (inherits from Animal)
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
    let requests = vec![sschemaupdate!(mammal_schema)];
    store.perform_mut(requests)?;
    
    // Create Dog schema (inherits from Mammal)
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
    let requests = vec![sschemaupdate!(dog_schema)];
    store.perform_mut(requests)?;
    
    // Create Cat schema (inherits from Mammal)  
    let mut cat_schema = EntitySchema::<Single, String, String>::new("Cat".to_string(), vec!["Mammal".to_string()]);
    cat_schema.fields.insert(
        "IndoorOutdoor".to_string(),
        FieldSchema::String {
            field_type: "IndoorOutdoor".to_string(),
            default_value: String::new(),
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(cat_schema)];
    store.perform_mut(requests)?;
    
    // Create Bird schema (inherits from Animal)
    let mut bird_schema = EntitySchema::<Single, String, String>::new("Bird".to_string(), vec!["Animal".to_string()]);
    bird_schema.fields.insert(
        "CanFly".to_string(),
        FieldSchema::Bool {
            field_type: "CanFly".to_string(),
            default_value: true,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(bird_schema)];
    store.perform_mut(requests)?;
    
    // Now we can get the interned entity types
    let et_animal = store.get_entity_type("Animal")?;
    let et_mammal = store.get_entity_type("Mammal")?;
    let et_dog = store.get_entity_type("Dog")?;
    let et_cat = store.get_entity_type("Cat")?;
    let et_bird = store.get_entity_type("Bird")?;
    
    // And field types
    let ft_name = store.get_field_type("Name")?;
    let ft_fur_color = store.get_field_type("FurColor")?;
    let ft_breed = store.get_field_type("Breed")?;
    
    // Create test entities
    let create_requests = vec![
        screate!(et_animal, "Generic Animal".to_string()),
        screate!(et_mammal, "Generic Mammal".to_string()),
        screate!(et_dog, "Rex".to_string()),
        screate!(et_dog, "Buddy".to_string()),
        screate!(et_cat, "Whiskers".to_string()),
        screate!(et_cat, "Mittens".to_string()),
        screate!(et_bird, "Tweety".to_string()),
    ];
    store.perform_mut(create_requests)?;
    
    // Test find_entities with inheritance (includes derived types)
    let all_animals = store.find_entities(et_animal, None)?;
    assert_eq!(all_animals.len(), 7); // All entities should be included
    
    let all_mammals = store.find_entities(et_mammal, None)?;
    assert_eq!(all_mammals.len(), 5); // Mammal + dogs + cats, but not bird or base animal
    
    let all_dogs = store.find_entities(et_dog, None)?;
    assert_eq!(all_dogs.len(), 2); // Only dogs
    
    let all_cats = store.find_entities(et_cat, None)?;
    assert_eq!(all_cats.len(), 2); // Only cats
    
    let all_birds = store.find_entities(et_bird, None)?;
    assert_eq!(all_birds.len(), 1); // Only birds
    
    // Test find_entities_exact (no inheritance)
    let exact_animals = store.find_entities_exact(et_animal, None, None)?;
    assert_eq!(exact_animals.items.len(), 1); // Only the generic animal
    
    let exact_mammals = store.find_entities_exact(et_mammal, None, None)?;
    assert_eq!(exact_mammals.items.len(), 1); // Only the generic mammal
    
    let exact_dogs = store.find_entities_exact(et_dog, None, None)?;
    assert_eq!(exact_dogs.items.len(), 2); // Both dogs
    
    let exact_cats = store.find_entities_exact(et_cat, None, None)?;
    assert_eq!(exact_cats.items.len(), 2); // Both cats
    
    let exact_birds = store.find_entities_exact(et_bird, None, None)?;
    assert_eq!(exact_birds.items.len(), 1); // Only the bird
    
    // Test with filtering on inherited and non-inherited searches
    let filtered_animals = store.find_entities(et_animal, Some("Name == \"Rex\"".to_string()))?;
    assert_eq!(filtered_animals.len(), 1); // Should find Rex the dog through inheritance
    
    let filtered_exact_animals = store.find_entities_exact(et_animal, None, Some("Name == \"Rex\"".to_string()))?;
    assert_eq!(filtered_exact_animals.items.len(), 0); // Rex is not an exact Animal type
    
    Ok(())
}

#[test]
fn test_find_entities_nonexistent_types() -> Result<()> {
    let store = setup_test_database()?;
    
    let et_nonexistent = store.get_entity_type("NonExistentType").unwrap_or(EntityType(999999));
    
    // Test finding entities of a type that doesn't exist
    let empty_result = store.find_entities(et_nonexistent, None)?;
    assert_eq!(empty_result.len(), 0);
    
    let empty_paginated = store.find_entities_paginated(et_nonexistent, None, None)?;
    assert_eq!(empty_paginated.items.len(), 0);
    assert_eq!(empty_paginated.total, 0);
    assert!(empty_paginated.next_cursor.is_none());
    
    let empty_exact = store.find_entities_exact(et_nonexistent, None, None)?;
    assert_eq!(empty_exact.items.len(), 0);
    assert_eq!(empty_exact.total, 0);
    assert!(empty_exact.next_cursor.is_none());
    
    Ok(())
}

#[test]
fn test_find_entities_cel_edge_cases() -> Result<()> {
    let mut store = Store::new();
    
    // Create a simple schema using string types first
    let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec![]);
    user_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    user_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    let requests = vec![sschemaupdate!(user_schema)];
    store.perform_mut(requests)?;
    
    // Now get the interned types
    let et_user = store.get_entity_type("User")?;
    let ft_name = store.get_field_type("Name")?;
    
    // Create test users
    let create_requests = vec![
        screate!(et_user, "Alice".to_string()),
        screate!(et_user, "Bob".to_string()),
    ];
    store.perform_mut(create_requests)?;
    
    // Test CEL expression that returns non-boolean
    let non_boolean = store.find_entities(et_user, Some("42".to_string()))?;
    assert_eq!(non_boolean.len(), 0);
    
    // Test CEL expression with undefined field (should be handled gracefully)
    let undefined_field = store.find_entities(et_user, Some("UndefinedField == true".to_string()))?;
    assert_eq!(undefined_field.len(), 0);
    
    // Test basic true/false filters
    let all_match = store.find_entities(et_user, Some("true".to_string()))?;
    assert_eq!(all_match.len(), 2);
    
    let none_match = store.find_entities(et_user, Some("false".to_string()))?;
    assert_eq!(none_match.len(), 0);
    
    Ok(())
}

#[test]
fn test_complete_entity_schema_caching() -> Result<()> {
    let mut store = Store::new();
    
    // Create base entity type using string schema first
    let mut base_schema = EntitySchema::<Single, String, String>::new("BaseEntity".to_string(), vec![]);
    base_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    base_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    base_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    base_schema.fields.insert(
        "BaseField".to_string(),
        FieldSchema::String {
            field_type: "BaseField".to_string(),
            default_value: "base_default".to_string(),
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(base_schema)];
    store.perform_mut(requests)?;
    
    // Create derived entity type
    let mut derived_schema = EntitySchema::<Single, String, String>::new("DerivedEntity".to_string(), vec!["BaseEntity".to_string()]);
    derived_schema.fields.insert(
        "DerivedField".to_string(),
        FieldSchema::String {
            field_type: "DerivedField".to_string(),
            default_value: "derived_default".to_string(),
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    let requests = vec![sschemaupdate!(derived_schema)];
    store.perform_mut(requests)?;
    
    // Now get the interned types
    let et_base = store.get_entity_type("BaseEntity")?;
    let et_derived = store.get_entity_type("DerivedEntity")?;
    let ft_base_field = store.get_field_type("BaseField")?;
    let ft_derived_field = store.get_field_type("DerivedField")?;
    
    // First call to get_complete_entity_schema should populate the cache
    let complete_schema_1 = store.get_complete_entity_schema(et_derived)?;
    assert!(complete_schema_1.fields.contains_key(&ft_base_field));
    assert!(complete_schema_1.fields.contains_key(&ft_derived_field));
    
    // Second call should use the cache (no way to directly verify this without exposing cache, 
    // but this tests that the cache doesn't break functionality)
    let complete_schema_2 = store.get_complete_entity_schema(et_derived)?;
    assert_eq!(complete_schema_1.fields.len(), complete_schema_2.fields.len());
    assert!(complete_schema_2.fields.contains_key(&ft_base_field));
    assert!(complete_schema_2.fields.contains_key(&ft_derived_field));
    
    // Update the base schema - this should invalidate the cache
    let mut updated_base_schema = EntitySchema::<Single, String, String>::new("BaseEntity".to_string(), vec![]);
    updated_base_schema.fields.insert(
        "Name".to_string(),
        FieldSchema::String {
            field_type: "Name".to_string(),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    updated_base_schema.fields.insert(
        "Parent".to_string(),
        FieldSchema::EntityReference {
            field_type: "Parent".to_string(),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Configuration,
        }
    );
    updated_base_schema.fields.insert(
        "Children".to_string(),
        FieldSchema::EntityList {
            field_type: "Children".to_string(),
            default_value: Vec::new(),
            rank: 2,
            storage_scope: StorageScope::Configuration,
        }
    );
    updated_base_schema.fields.insert(
        "BaseField".to_string(),
        FieldSchema::String {
            field_type: "BaseField".to_string(),
            default_value: "updated_base_default".to_string(),
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    updated_base_schema.fields.insert(
        "NewBaseField".to_string(),
        FieldSchema::String {
            field_type: "NewBaseField".to_string(),
            default_value: "new_base_field".to_string(),
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    let requests = vec![sschemaupdate!(updated_base_schema)];
    store.perform_mut(requests)?;
    
    // Now get the new field type
    let ft_new_base_field = store.get_field_type("NewBaseField")?;
    
    // After update, cache should be invalidated and the complete schema should include the new field
    let complete_schema_3 = store.get_complete_entity_schema(et_derived)?;
    assert!(complete_schema_3.fields.contains_key(&ft_base_field));
    assert!(complete_schema_3.fields.contains_key(&ft_derived_field));
    assert!(complete_schema_3.fields.contains_key(&ft_new_base_field));
    assert_eq!(complete_schema_3.fields.len(), 6); // Name, Parent, Children, BaseField, DerivedField, NewBaseField
    
    Ok(())
}
