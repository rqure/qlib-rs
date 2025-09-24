# qlib-rs: A flexible database library

`qlib-rs` is a powerful database library based on an Entity-Attribute-Value (EAV) model. It supports both in-memory storage (`Store`) and remote database access (`StoreProxy`) via TCP, making it suitable for applications that need flexible, relationship-focused data management.

## Quick Start

Add to your `Cargo.toml`:
```toml
[dependencies]
qlib-rs = "0.1"
```

### Basic Usage

```rust
use qlib_rs::*;

// Create in-memory database
let mut store = Store::new();

// Define and register a schema
let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec![]);
user_schema.fields.insert("Name".to_string(), FieldSchema::String {
    field_type: "Name".to_string(),
    default_value: "".to_string(),
    rank: 1,
    storage_scope: StorageScope::Configuration,
});
store.perform_mut(sreq![sschemaupdate!(user_schema)])?;

// Create and read entities
let user_type = store.get_entity_type("User")?;
let name_field = store.get_field_type("Name")?;

let create_result = store.perform_mut(sreq![screate!(user_type, "John".to_string())])?;
let user_id = /* extract entity ID from result */;

let read_result = store.perform(sreq![sread!(user_id, sfield![name_field])])?;
```

### Remote Access

```rust
// Connect to remote server
let stream = TcpStream::connect("127.0.0.1:8080")?;
let mut proxy = StoreProxy::new(stream)?;
proxy.authenticate("username", "password")?;

// Same API as Store
let users = proxy.find_entities(user_type, None)?;
```

## Core Concepts

### Data Model
- **EntityType** and **FieldType**: Type identifiers obtained via `get_entity_type("Name")` and `get_field_type("Name")`
- **Entity**: Objects identified by `EntityId`, containing fields with values
- **Value**: Data types including `Bool`, `Int`, `Float`, `String`, `EntityReference`, `EntityList`, `Blob`, `Timestamp`, `Choice`
- **Schema**: `EntitySchema` defines entity structure; `FieldSchema` defines field constraints and types

### Storage Options
- **Store**: In-memory database for single-process applications
- **StoreProxy**: TCP-based remote database access with authentication

Both implement `StoreTrait` and provide identical APIs for seamless switching between local and remote storage.

## Request System

Operations use a batch request system with these macros:

### Request Creation
```rust
// Batch operations
let requests = sreq![
    sread!(entity_id, sfield![name_field, email_field]),
    swrite!(entity_id, sfield![status_field], sstr!("active")),
    screate!(entity_type, "New Entity".to_string()),
];

// Execute batch
let results = store.perform_mut(requests)?;
```

### Request Types
- `sread!(entity_id, fields)` - Read field values
- `swrite!(entity_id, fields, value)` - Write field values  
- `sadd!(entity_id, fields, value)` - Add to numeric fields
- `ssub!(entity_id, fields, value)` - Subtract from numeric fields
- `screate!(type, name)` - Create entities
- `sdelete!(entity_id)` - Delete entities

### Value Creation
```rust
// Value macros for type safety
let requests = sreq![
    swrite!(id, sfield![name_field], sstr!("text")),
    swrite!(id, sfield![count_field], sint!(42)),
    swrite!(id, sfield![active_field], sbool!(true)),
    swrite!(id, sfield![ref_field], sref!(Some(other_id))),
    swrite!(id, sfield![list_field], sreflist![id1, id2, id3]),
];
```

## Schema Management

### Defining Schemas
```rust
use qlib_rs::*;
use qlib_rs::data::field_schema::StorageScope;

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
store.perform_mut(sreq![sschemaupdate!(user_schema)])?;
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
// Read parent's name through relationship
let parent_name_field = sfield![parent_field, name_field];
let result = store.perform(sreq![sread!(child_id, parent_name_field)])?;

// Works with both Store and StoreProxy
let (final_entity, final_field) = store.resolve_indirection(entity_id, &[parent_field, name_field])?;
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
let active_users = store.find_entities(user_type, Some("status='active'".to_string()))?;

// Paginated results
let page_result = store.find_entities_paginated(
    user_type, 
    Some(PageOpts { page_size: 10, page_number: 1 }), 
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

For more examples and advanced usage, see the test files in `src/test/`.