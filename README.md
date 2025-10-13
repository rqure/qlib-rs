# qlib-rs: A flexible database library

`qlib-rs` is a powerful database library based on an Entity-Attribute-Value (EAV) model with RESP (REdis Serialization Protocol) support. It provides both in-memory storage (`Store`) and remote database access (`StoreProxy` and `AsyncStoreProxy`) via TCP, making it suitable for applications that need flexible, relationship-focused data management.

## Quick Start

Add to your `Cargo.toml`:
```toml
[dependencies]
qlib-rs = "0.1.1"
```

### Basic Usage

```rust
use qlib_rs::*;

// Create in-memory database
let mut store = Store::new();

// Define and register a schema
let mut user_schema = EntitySchema::<Single, String, String>::new(
    "User".to_string(), 
    vec![]
);
user_schema.fields.insert("Name".to_string(), FieldSchema::String {
    field_type: "Name".to_string(),
    default_value: "".to_string(),
    rank: 1,
    storage_scope: StorageScope::Configuration,
});
store.update_schema(user_schema)?;

// Create and read entities
let user_type = store.get_entity_type("User")?;
let name_field = store.get_field_type("Name")?;

let user_id = store.create_entity(user_type, None, "John")?;

let (value, timestamp, writer_id) = store.read(user_id, &[name_field])?;
```

### Remote Access

```rust
// Connect to remote server (sync)
let proxy = StoreProxy::connect("127.0.0.1:8080")?;

// Same API as Store
let users = proxy.find_entities(user_type, None)?;

// Async version
let async_proxy = AsyncStoreProxy::connect("127.0.0.1:8080").await?;
let (value, timestamp, writer_id) = async_proxy.read(user_id, &[name_field]).await?;
```

## Core Concepts

### Data Model
- **EntityType** and **FieldType**: Type identifiers obtained via `get_entity_type("Name")` and `get_field_type("Name")`
- **Entity**: Objects identified by `EntityId`, containing fields with values
- **Value**: Data types including `Bool`, `Int`, `Float`, `String`, `EntityReference`, `EntityList`, `Blob`, `Timestamp`, `Choice`
- **Schema**: `EntitySchema` defines entity structure; `FieldSchema` defines field constraints and types

### Storage Options
- **Store**: In-memory database for single-process applications
- **StoreProxy**: Synchronous TCP-based remote database access using RESP protocol
- **AsyncStoreProxy**: Asynchronous TCP-based remote database access using RESP protocol

All implement `StoreTrait` and provide identical APIs for seamless switching between local and remote storage.

## Core Operations

### Reading and Writing Data

```rust
use qlib_rs::*;

let mut store = Store::new();
// ... set up schema ...

let user_type = store.get_entity_type("User")?;
let name_field = store.get_field_type("Name")?;
let email_field = store.get_field_type("Email")?;

// Create entity
let user_id = store.create_entity(user_type, None, "john_doe")?;

// Write field values
store.write(user_id, &[name_field], Value::String("John Doe".into()), None, None, None, None)?;
store.write(user_id, &[email_field], Value::String("john@example.com".into()), None, None, None, None)?;

// Read field values
let (name_value, timestamp, writer_id) = store.read(user_id, &[name_field])?;
if let Value::String(name) = name_value {
    println!("User name: {}", name);
}

// Delete entity
store.delete_entity(user_id)?;
```

### Numeric Field Adjustments

```rust
// Use AdjustBehavior for numeric operations
let count_field = store.get_field_type("Count")?;

// Add to field
store.write(
    entity_id, 
    &[count_field], 
    Value::Int(5), 
    None, 
    None, 
    None, 
    Some(AdjustBehavior::Add)
)?;

// Subtract from field
store.write(
    entity_id, 
    &[count_field], 
    Value::Int(3), 
    None, 
    None, 
    None, 
    Some(AdjustBehavior::Subtract)
)?;
```

## Pipeline API

For improved performance when executing multiple operations, use the Pipeline API to batch commands:

### Synchronous Pipeline

```rust
use qlib_rs::*;

let proxy = StoreProxy::connect("127.0.0.1:8080")?;
let mut pipeline = proxy.pipeline();

// Queue multiple commands
pipeline.read(user_id, &[name_field])?;
pipeline.write(user_id, &[email_field], Value::String("new@example.com".into()), None, None, None, None)?;
pipeline.create_entity(user_type, None, "new_user")?;

// Execute all commands at once
let results = pipeline.execute()?;

// Extract results in order
let (name_value, timestamp, writer_id): (Value, Timestamp, Option<EntityId>) = results.get(0)?;
let _: () = results.get(1)?;  // write returns unit
let new_user_id: EntityId = results.get(2)?;
```

### Asynchronous Pipeline

```rust
use qlib_rs::data::AsyncStoreProxy;

let async_proxy = AsyncStoreProxy::connect("127.0.0.1:8080").await?;
let mut pipeline = async_proxy.pipeline();

// Queue multiple commands
pipeline.read(user_id, &[name_field])?;
pipeline.write(user_id, &[email_field], Value::String("new@example.com".into()), None, None, None, None)?;
pipeline.create_entity(user_type, None, "new_user")?;

// Execute all commands at once
let results = pipeline.execute().await?;

// Extract results in order
let (name_value, timestamp, writer_id): (Value, Timestamp, Option<EntityId>) = results.get(0)?;
```

Pipelining significantly improves throughput by reducing network round-trips.

## Schema Management

### Defining Schemas
```rust
use qlib_rs::*;

// Define entity schema with inheritance
let mut user_schema = EntitySchema::<Single, String, String>::new(
    "User".to_string(), 
    vec!["Object".to_string()]  // Inherits from Object
);

// Add fields
user_schema.fields.insert("Email".to_string(), FieldSchema::String {
    field_type: "Email".to_string(),
    default_value: "".to_string(),
    rank: 2,
    storage_scope: StorageScope::Configuration,
});

// Register schema
store.update_schema(user_schema)?;
```

### Field Types
Available field schema types:
- `FieldSchema::String` - Text data
- `FieldSchema::Int` - Integer numbers  
- `FieldSchema::Float` - Floating point numbers
- `FieldSchema::Bool` - Boolean values
- `FieldSchema::EntityReference` - Reference to another entity
- `FieldSchema::EntityList` - List of entity references
- `FieldSchema::Blob` - Binary data
- `FieldSchema::Timestamp` - Time values
- `FieldSchema::Choice` - Enumerated string values

## Indirection

Navigate relationships in single operations using field paths:

```rust
// Read parent's name through relationship (indirection)
let (name_value, timestamp, writer_id) = store.read(
    child_id, 
    &[parent_field, name_field]
)?;

// Resolve indirection to find target entity and field
let (final_entity, final_field) = store.resolve_indirection(
    entity_id, 
    &[parent_field, name_field]
)?;

// Write through indirection
store.write(
    child_id,
    &[parent_field, name_field],
    Value::String("Updated Name".into()),
    None,
    None,
    None,
    None
)?;
```

Path utilities for navigation:
```rust
let entity_path = path(&store, entity_id)?;  // "/root/users/john"  
let entity_id = path_to_entity_id(&store, "/root/users/john")?;
```

## Querying

### Find Entities
```rust
// Get all entities of type
let all_users = store.find_entities(user_type, None)?;

// With server-side filtering
let active_users = store.find_entities(user_type, Some("status='active'"))?;

// Paginated results
let page_opts = PageOpts { page_size: 10, page_number: 1 };
let page_result = store.find_entities_paginated(
    user_type, 
    Some(&page_opts), 
    None
)?;
```

### Entity Information
```rust
// Check existence
let exists = store.entity_exists(entity_id);
let has_field = store.field_exists(entity_type, field_type);

// Get available types
let entity_types = store.get_entity_types()?;
let type_name = store.resolve_entity_type(entity_type)?;
```

## Notifications

Monitor entity changes with the notification system:

```rust
// Configure notifications
let notify_config = NotifyConfig::EntityType {
    entity_type: user_type,
    field_type: name_field,
    trigger_on_change: true,
    context: vec![vec![email_field]], // Include email in notifications
};

let queue = NotificationQueue::new();
// Register with store (implementation-specific)

// Process notifications
while let Some(notification) = queue.pop() {
    println!("Field changed: {:?} -> {:?}", 
             notification.previous, 
             notification.current);
}
```

## Entity Inheritance

Entities support inheritance for code reuse and consistency:

```rust
// Base schema
let mut person_schema = EntitySchema::<Single, String, String>::new(
    "Person".to_string(), 
    vec!["Object".to_string()]
);

// Child schema inherits Person fields
let mut employee_schema = EntitySchema::<Single, String, String>::new(
    "Employee".to_string(),
    vec!["Person".to_string()]
);

// Employee entities inherit all Person and Object fields
```

The library resolves inheritance automatically:
- `EntitySchema<Single>` - Schema as defined
- `EntitySchema<Complete>` - Fully resolved with inherited fields

## Authentication

The library includes a comprehensive authentication system supporting multiple authentication methods:

```rust
use qlib_rs::auth::*;

// Create authentication configuration
let auth_config = AuthConfig::default();

// Create a user with native authentication
let user_id = create_user(&mut store, "john_doe", AuthMethod::Native, root_entity_id)?;

// Set password for native authentication
set_user_password(&mut store, user_id, "secure_password123", &auth_config)?;

// Authenticate user
match authenticate_user(&mut store, "john_doe", "secure_password123", &auth_config) {
    Ok(user_id) => println!("Authentication successful for user: {:?}", user_id),
    Err(e) => eprintln!("Authentication failed: {}", e),
}

// Change password
change_password(&mut store, user_id, "old_password", "new_password", &auth_config)?;

// Find user by name
if let Some(user_id) = find_user_by_name(&store, "john_doe")? {
    println!("Found user: {:?}", user_id);
}
```

### Supported Authentication Methods
- **Native**: Password-based authentication with Argon2 hashing
- **LDAP**: LDAP server authentication (method enum available)
- **OpenIDConnect**: OpenID Connect authentication (method enum available)

### Security Features
- Argon2 password hashing
- Failed login attempt tracking
- Account lockout after max failed attempts
- Password complexity validation
- Account active/inactive status

## CEL Expression Evaluation

Execute Common Expression Language (CEL) expressions with access to entity fields:

```rust
use qlib_rs::expr::CelExecutor;

let mut executor = CelExecutor::new();

// Execute expression with entity context
// The executor automatically resolves field references like "Name", "Age", etc.
let result = executor.execute(
    "Name == 'John' && Age > 18",
    entity_id,
    &mut store
)?;

// CEL expressions have access to all entity fields
let result = executor.execute(
    "Status == 'active' && LastLogin > timestamp('2024-01-01T00:00:00Z')",
    entity_id,
    &mut store
)?;
```

CEL expressions are compiled and cached for efficient repeated evaluation.

## RESP Protocol

The remote access via `StoreProxy` and `AsyncStoreProxy` uses the RESP (REdis Serialization Protocol) for communication. This provides:

- **Zero-copy parsing** for maximum performance
- **Custom command support** via traits
- **CLI-friendly** string-based numeric parsing
- **Derive macro support** for automatic encoding/decoding (with `derive` feature)

### Custom Commands

Define custom RESP commands using the derive macros:

```rust
#[cfg(feature = "derive")]
use qlib_rs::{RespEncode, RespDecode};

#[derive(RespEncode, RespDecode)]
struct MyCommand {
    entity_id: u64,
    name: String,
    active: bool,
}
```

For more examples and advanced usage, see the test files in `src/test/`.