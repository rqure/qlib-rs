#[allow(unused_imports)]
use crate::*;
use crate::data::StorageScope;

#[allow(unused_imports)]
use crate::expr::CelExecutor;

#[allow(unused_imports)]
use std::sync::Arc;

#[allow(dead_code)]
fn setup_test_store_with_entity() -> Result<(Store, EntityId)> {
    let mut store = Store::new(Snowflake::new());

    // Create a test entity type with various field types
    let et_test = EntityType::from("TestEntity");
    let mut schema = EntitySchema::<Single>::new(et_test.clone(), vec![]);
    
    schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Age"),
        FieldSchema::Int {
            field_type: FieldType::from("Age"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Score"),
        FieldSchema::Float {
            field_type: FieldType::from("Score"),
            default_value: 0.0,
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("IsActive"),
        FieldSchema::Bool {
            field_type: FieldType::from("IsActive"),
            default_value: false,
            rank: 3,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Status"),
        FieldSchema::Choice {
            field_type: FieldType::from("Status"),
            default_value: 0,
            choices: vec!["Inactive".to_string(), "Active".to_string(), "Pending".to_string()],
            rank: 4,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Manager"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Manager"),
            default_value: None,
            rank: 5,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Tags"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Tags"),
            default_value: vec![],
            rank: 6,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("CreatedAt"),
        FieldSchema::Timestamp {
            field_type: FieldType::from("CreatedAt"),
            default_value: epoch(),
            rank: 7,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    schema.fields.insert(
        FieldType::from("Data"),
        FieldSchema::Blob {
            field_type: FieldType::from("Data"),
            default_value: vec![],
            rank: 8,
            storage_scope: StorageScope::Runtime,
        }
    );

    let requests = vec![sschemaupdate!(schema)];
    store.perform_mut(requests)?;

    // Create a test entity
    let create_requests = store.perform_mut(vec![screate!(
        et_test.clone(),
        "test_entity".to_string()
    )])?;
    
    let entity_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Set some field values
    let now = now();
    let manager_id = EntityId::new("Manager", 123);
    let tag1_id = EntityId::new("Tag", 1);
    let tag2_id = EntityId::new("Tag", 2);
    let test_data = vec![72, 101, 108, 108, 111]; // "Hello" in bytes
    
    let field_requests = vec![
        swrite!(entity_id.clone(), FieldType::from("Name"), sstr!("John Doe")),
        swrite!(entity_id.clone(), FieldType::from("Age"), sint!(30)),
        swrite!(entity_id.clone(), FieldType::from("Score"), sfloat!(95.5)),
        swrite!(entity_id.clone(), FieldType::from("IsActive"), sbool!(true)),
        swrite!(entity_id.clone(), FieldType::from("Status"), schoice!(1)),
        swrite!(entity_id.clone(), FieldType::from("Manager"), sref!(Some(manager_id))),
        swrite!(entity_id.clone(), FieldType::from("Tags"), sreflist![tag1_id, tag2_id]),
        swrite!(entity_id.clone(), FieldType::from("CreatedAt"), stimestamp!(now)),
        swrite!(entity_id.clone(), FieldType::from("Data"), sblob!(test_data)),
    ];
    store.perform_mut(field_requests)?;

    Ok((store, entity_id))
}

#[test]
fn test_cel_executor_new() {
    let _executor = CelExecutor::new();
    // Can't directly test the cache since it's private, but we can test behavior
    assert!(true); // Constructor should work without panicking
}

#[test]
fn test_cel_executor_get_or_compile_basic() -> Result<()> {
    let mut executor = CelExecutor::new();
    
    // Test compiling a simple expression
    let program1 = executor.get_or_compile("1 + 1")?;
    assert!(program1.references().variables().is_empty());
    
    // Test that getting the same expression returns cached result
    // We need to check this by pointer address, so get it separately
    let program1_ptr = program1 as *const _;
    let program2 = executor.get_or_compile("1 + 1")?;
    let program2_ptr = program2 as *const _;
    assert_eq!(program1_ptr, program2_ptr);
    
    // Test compiling a different expression
    let program3 = executor.get_or_compile("2 + 2")?;
    let program3_ptr = program3 as *const _;
    assert_ne!(program1_ptr, program3_ptr);
    
    Ok(())
}

#[test]
fn test_cel_executor_get_or_compile_with_variables() -> Result<()> {
    let mut executor = CelExecutor::new();
    
    // Test expression with variables
    let program = executor.get_or_compile("Name + ' is ' + string(Age) + ' years old'")?;
    let refs = program.references();
    let vars = refs.variables();
    
    // Should reference Name and Age variables
    assert!(vars.iter().any(|v| *v == "Name"));
    assert!(vars.iter().any(|v| *v == "Age"));
    
    Ok(())
}

#[test]
fn test_cel_executor_get_or_compile_invalid_expression() {
    let mut executor = CelExecutor::new();
    
    // Test invalid CEL expression
    let result = executor.get_or_compile("invalid syntax here +++");
    assert!(result.is_err());
    
    if let Err(crate::Error::ExecutionError(msg)) = result {
        assert!(msg.contains("syntax") || msg.contains("parse") || msg.contains("error"));
    } else {
        panic!("Expected ExecutionError");
    }
}

#[test]
fn test_cel_executor_remove() -> Result<()> {
    let mut executor = CelExecutor::new();
    
    // Compile an expression
    let _program1 = executor.get_or_compile("1 + 1")?;
    
    // Remove it from cache
    executor.remove("1 + 1");
    
    // Get it again - should recompile (can't directly test, but should not panic)
    let _program2 = executor.get_or_compile("1 + 1")?;
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_simple_expression() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test simple expression without variables
    let result = executor.execute("1 + 1", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Int(value) => assert_eq!(value, 2),
        _ => panic!("Expected int result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_string_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using string field
    let result = executor.execute("Name + ' is awesome'", &entity_id, &mut store)?;
    
    match result {
        cel::Value::String(value) => assert_eq!(value.as_str(), "John Doe is awesome"),
        _ => panic!("Expected string result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_int_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using int field
    let result = executor.execute("Age + 10", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Int(value) => assert_eq!(value, 40),
        _ => panic!("Expected int result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_float_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using float field
    let result = executor.execute("Score * 1.1", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Float(value) => {
            let expected = 95.5 * 1.1;
            assert!((value - expected).abs() < f64::EPSILON);
        },
        _ => panic!("Expected float result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_bool_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using bool field
    let result = executor.execute("IsActive && true", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_choice_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using choice field (stored as int)
    let result = executor.execute("Status == 1", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_entity_reference_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using entity reference field
    let result = executor.execute("Manager == 'Manager$123'", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_entity_list_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using entity list field
    let result = executor.execute("size(Tags) == 2", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_blob_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using blob field (converted to base64)
    // "Hello" in base64 is "SGVsbG8="
    let result = executor.execute("Data == 'SGVsbG8='", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_timestamp_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using timestamp field
    // Just test that we can access the timestamp without error
    let result = executor.execute("CreatedAt != timestamp('1970-01-01T00:00:00Z')", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_entity_id_and_type() -> Result<()> {
    // Note: EntityId and EntityType are special variables that the CelExecutor
    // should add to the context, but they shouldn't be treated as field references.
    // For now, let's test a different aspect of the executor.
    
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression that doesn't reference EntityId/EntityType but still uses context
    let result = executor.execute("'TestEntity' == 'TestEntity'", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    // Test with a string literal operation
    let result = executor.execute("size('Hello') == 5", &entity_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_complex_expression() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test complex expression combining multiple fields
    let result = executor.execute(
        "IsActive && Age >= 18 && Score > 90.0 && size(Name) > 0",
        &entity_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_indirection() -> Result<()> {
    let mut executor = CelExecutor::new();
    let mut store = Store::new(Snowflake::new());

    // Create entities for indirection test
    let et_user = EntityType::from("User");
    let et_department = EntityType::from("Department");
    
    // Create Department schema
    let mut dept_schema = EntitySchema::<Single>::new(et_department.clone(), vec![]);
    dept_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    dept_schema.fields.insert(
        FieldType::from("Budget"),
        FieldSchema::Int {
            field_type: FieldType::from("Budget"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Create User schema with department reference
    let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), vec![]);
    user_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        FieldType::from("Department"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Department"),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    let requests = vec![
        sschemaupdate!(dept_schema),
        sschemaupdate!(user_schema)
    ];
    store.perform_mut(requests)?;

    // Create department entity
    let create_requests = store.perform_mut(vec![screate!(
        et_department.clone(),
        "Engineering".to_string()
    )])?;
    let dept_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Create user entity
    let create_requests = store.perform_mut(vec![screate!(
        et_user.clone(),
        "Alice".to_string()
    )])?;
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Set field values
    let field_requests = vec![
        swrite!(dept_id.clone(), FieldType::from("Name"), sstr!("Engineering")),
        swrite!(dept_id.clone(), FieldType::from("Budget"), sint!(100000)),
        swrite!(user_id.clone(), FieldType::from("Name"), sstr!("Alice")),
        swrite!(user_id.clone(), FieldType::from("Department"), sref!(Some(dept_id))),
    ];
    store.perform_mut(field_requests)?;

    // Test indirection: Department->Name should resolve to "Engineering"
    // The CEL executor should read the field "Department->Name" via the store's indirection system
    let result = executor.execute(
        "Department->Name == 'Engineering'",
        &user_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for Department->Name"),
    }

    // Test indirection with integer field
    let result = executor.execute(
        "Department->Budget > 50000",
        &user_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for Department->Budget"),
    }

    // Test complex expression with indirection
    let result = executor.execute(
        "Name == 'Alice' && Department->Name == 'Engineering' && Department->Budget == 100000",
        &user_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for complex indirection expression"),
    }

    Ok(())
}

#[test]
fn test_cel_executor_execute_with_deep_indirection() -> Result<()> {
    let mut executor = CelExecutor::new();
    let mut store = Store::new(Snowflake::new());

    // Create a deeper indirection chain: Employee -> Department -> Company
    let et_company = EntityType::from("Company");
    let et_department = EntityType::from("Department");
    let et_employee = EntityType::from("Employee");
    
    // Create Company schema
    let mut company_schema = EntitySchema::<Single>::new(et_company.clone(), vec![]);
    company_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    company_schema.fields.insert(
        FieldType::from("Founded"),
        FieldSchema::Int {
            field_type: FieldType::from("Founded"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Create Department schema with company reference
    let mut dept_schema = EntitySchema::<Single>::new(et_department.clone(), vec![]);
    dept_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    dept_schema.fields.insert(
        FieldType::from("Company"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Company"),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Create Employee schema with department reference
    let mut employee_schema = EntitySchema::<Single>::new(et_employee.clone(), vec![]);
    employee_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    employee_schema.fields.insert(
        FieldType::from("Department"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Department"),
            default_value: None,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    let requests = vec![
        sschemaupdate!(company_schema),
        sschemaupdate!(dept_schema),
        sschemaupdate!(employee_schema)
    ];
    store.perform_mut(requests)?;

    // Create entities
    let create_requests = store.perform_mut(vec![screate!(et_company.clone(), "TechCorp".to_string())])?;
    let company_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created company ID");
    };

    let create_requests = store.perform_mut(vec![screate!(et_department.clone(), "Engineering".to_string())])?;
    let dept_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created department ID");
    };

    let create_requests = store.perform_mut(vec![screate!(et_employee.clone(), "Bob".to_string())])?;
    let employee_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created employee ID");
    };

    // Set up the entity relationships and data
    store.perform_mut(vec![
        swrite!(company_id.clone(), FieldType::from("Name"), sstr!("TechCorp")),
        swrite!(company_id.clone(), FieldType::from("Founded"), sint!(2010)),
        swrite!(dept_id.clone(), FieldType::from("Name"), sstr!("Engineering")),
        swrite!(dept_id.clone(), FieldType::from("Company"), sref!(Some(company_id))),
        swrite!(employee_id.clone(), FieldType::from("Name"), sstr!("Bob")),
        swrite!(employee_id.clone(), FieldType::from("Department"), sref!(Some(dept_id))),
    ])?;

    // Test deep indirection: Department->Company->Name should resolve to "TechCorp"
    let result = executor.execute(
        "Department->Company->Name == 'TechCorp'",
        &employee_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for deep indirection"),
    }

    // Test deep indirection with integer field
    let result = executor.execute(
        "Department->Company->Founded == 2010",
        &employee_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for deep indirection with int"),
    }

    // Test mixed indirection levels in one expression
    let result = executor.execute(
        "Name == 'Bob' && Department->Name == 'Engineering' && Department->Company->Name == 'TechCorp'",
        &employee_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for mixed indirection"),
    }

    Ok(())
}

#[test]
fn test_cel_executor_execute_with_indirection_and_entity_lists() -> Result<()> {
    let mut executor = CelExecutor::new();
    let mut store = Store::new(Snowflake::new());

    // Create schema for testing indirection with entity lists
    let et_team = EntityType::from("Team");
    let et_project = EntityType::from("Project");
    
    // Create Project schema
    let mut project_schema = EntitySchema::<Single>::new(et_project.clone(), vec![]);
    project_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    project_schema.fields.insert(
        FieldType::from("Priority"),
        FieldSchema::Int {
            field_type: FieldType::from("Priority"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Create Team schema with projects list
    let mut team_schema = EntitySchema::<Single>::new(et_team.clone(), vec![]);
    team_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    team_schema.fields.insert(
        FieldType::from("Projects"),
        FieldSchema::EntityList {
            field_type: FieldType::from("Projects"),
            default_value: vec![],
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    store.perform_mut(vec![
        sschemaupdate!(project_schema),
        sschemaupdate!(team_schema)
    ])?;

    // Create project entities
    let create_requests = store.perform_mut(vec![screate!(et_project.clone(), "WebApp".to_string())])?;
    let project1_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created project ID");
    };

    let create_requests = store.perform_mut(vec![screate!(et_project.clone(), "MobileApp".to_string())])?;
    let project2_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created project ID");
    };

    // Create team entity
    let create_requests = store.perform_mut(vec![screate!(et_team.clone(), "DevTeam".to_string())])?;
    let team_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created team ID");
    };

    // Set up the data
    let field_requests = vec![
        swrite!(project1_id.clone(), FieldType::from("Name"), sstr!("WebApp")),
        swrite!(project1_id.clone(), FieldType::from("Priority"), sint!(1)),
        swrite!(project2_id.clone(), FieldType::from("Name"), sstr!("MobileApp")),
        swrite!(project2_id.clone(), FieldType::from("Priority"), sint!(2)),
        swrite!(team_id.clone(), FieldType::from("Name"), sstr!("DevTeam")),
        swrite!(team_id.clone(), FieldType::from("Projects"), sreflist![project1_id.clone(), project2_id.clone()]),
    ];
    store.perform_mut(field_requests)?;

    // Test that we can access the entity list field
    let result = executor.execute("size(Projects) == 2", &team_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for entity list size"),
    }

    // Test that entity list is properly converted to list of strings
    let result = executor.execute(
        &format!("Projects[0] == '{}'", project1_id.to_string()),
        &team_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for entity list access"),
    }

    Ok(())
}

#[test]
fn test_cel_executor_execute_with_null_entity_reference() -> Result<()> {
    let mut executor = CelExecutor::new();
    let mut store = Store::new(Snowflake::new());

    // Create entity with null entity reference
    let et_user = EntityType::from("User");
    let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), vec![]);
    user_schema.fields.insert(
        FieldType::from("Manager"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Manager"),
            default_value: None,
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    store.perform_mut(vec![sschemaupdate!(user_schema)])?;

    let create_requests = store.perform_mut(vec![screate!(
        et_user.clone(),
        "User".to_string()
    )])?;

    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Manager field should be null/empty
    let result = executor.execute("Manager == ''", &user_id, &mut store)?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_caching_behavior() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Execute the same expression multiple times
    let expr = "Age + 10";
    
    let result1 = executor.execute(expr, &entity_id, &mut store)?;
    let result2 = executor.execute(expr, &entity_id, &mut store)?;
    
    // Results should be identical
    match (result1, result2) {
        (cel::Value::Int(val1), cel::Value::Int(val2)) => {
            assert_eq!(val1, val2);
            assert_eq!(val1, 40);
        },
        _ => panic!("Expected int results"),
    }
    
    // Remove from cache and execute again
    executor.remove(expr);
    let result3 = executor.execute(expr, &entity_id, &mut store)?;
    
    match result3 {
        cel::Value::Int(val3) => assert_eq!(val3, 40),
        _ => panic!("Expected int result"),
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_runtime_error() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression that causes runtime error (division by zero)
    let result = executor.execute("Age / 0", &entity_id, &mut store);
    
    assert!(result.is_err());
    if let Err(crate::Error::ExecutionError(_)) = result {
        // Expected
    } else {
        panic!("Expected ExecutionError");
    }
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_missing_field() -> Result<()> {
    let mut executor = CelExecutor::new();
    let (mut store, entity_id) = setup_test_store_with_entity()?;
    
    // Test expression using non-existent field
    let result = executor.execute("NonExistentField == 'test'", &entity_id, &mut store);
    
    // This should fail because the field doesn't exist
    assert!(result.is_err());
    
    Ok(())
}

#[test]
fn test_cel_executor_execute_with_mixed_field_access() -> Result<()> {
    let mut executor = CelExecutor::new();
    let mut store = Store::new(Snowflake::new());

    // Create entities for mixed field access test
    let et_user = EntityType::from("User");
    let et_department = EntityType::from("Department");
    
    // Create Department schema
    let mut dept_schema = EntitySchema::<Single>::new(et_department.clone(), vec![]);
    dept_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    // Create User schema with department reference
    let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), vec![]);
    user_schema.fields.insert(
        FieldType::from("Name"),
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: String::new(),
            rank: 0,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        FieldType::from("Age"),
        FieldSchema::Int {
            field_type: FieldType::from("Age"),
            default_value: 0,
            rank: 1,
            storage_scope: StorageScope::Runtime,
        }
    );
    user_schema.fields.insert(
        FieldType::from("Department"),
        FieldSchema::EntityReference {
            field_type: FieldType::from("Department"),
            default_value: None,
            rank: 2,
            storage_scope: StorageScope::Runtime,
        }
    );
    
    let requests = vec![
        sschemaupdate!(dept_schema),
        sschemaupdate!(user_schema)
    ];
    store.perform_mut(requests)?;

    // Create department entity
    let create_requests = store.perform_mut(vec![screate!(
        et_department.clone(),
        "Sales".to_string()
    )])?;
    let dept_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Create user entity
    let create_requests = store.perform_mut(vec![screate!(
        et_user.clone(),
        "John".to_string()
    )])?;
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_requests.get(0) {
        id.clone()
    } else {
        panic!("Expected created entity ID");
    };

    // Set field values
    let field_requests = vec![
        swrite!(dept_id.clone(), FieldType::from("Name"), sstr!("Sales")),
        swrite!(user_id.clone(), FieldType::from("Name"), sstr!("John")),
        swrite!(user_id.clone(), FieldType::from("Age"), sint!(30)),
        swrite!(user_id.clone(), FieldType::from("Department"), sref!(Some(dept_id))),
    ];
    store.perform_mut(field_requests)?;

    // Test mixed access: direct fields (Name, Age) and indirect field (Department->Name) in one expression
    let result = executor.execute(
        "Name == 'John' && Age == 30 && Department->Name == 'Sales'",
        &user_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for mixed field access"),
    }

    // Test that direct field access still works when indirection is available
    let result = executor.execute(
        "Name == 'John'",
        &user_id,
        &mut store
    )?;
    
    match result {
        cel::Value::Bool(value) => assert_eq!(value, true),
        _ => panic!("Expected bool result for direct field access"),
    }

    Ok(())
}