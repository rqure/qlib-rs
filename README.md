# qlib-rs: A flexible in-memory and remote database library

`qlib-rs` provides a simple yet powerful database library based on an
Entity-Attribute-Value (EAV) model. It supports both in-memory (`Store`) and 
remote database access (`StoreProxy`) via TCP, making it suitable for scenarios 
where you need to manage structured but flexible data with relationships between entities.

## Getting Started

Add this to your `Cargo.toml`:

```toml
[dependencies]
qlib-rs = "0.1"
```

### Basic Usage with In-Memory Store

```rust
use qlib_rs::*;

fn main() -> Result<()> {
    let mut store = Store::new();
    
    // Create an entity schema
    let mut user_schema = EntitySchema::<Single, String, String>::new("User".to_string(), vec![]);
    user_schema.fields.insert("Name".to_string(), FieldSchema::String {
        field_type: "Name".to_string(),
        default_value: "".to_string(),
        rank: 1,
        storage_scope: crate::data::StorageScope::Configuration,
    });
    
    // Register the schema
    let requests = sreq![sschemaupdate!(user_schema)];
    store.perform_mut(requests)?;
    
    // Get types for creating entities
    let user_type = store.get_entity_type("User")?;
    let name_field = store.get_field_type("Name")?;
    
    // Create a user entity
    let create_req = sreq![screate!(user_type, "John Doe".to_string())];
    let create_result = store.perform_mut(create_req)?;
    
    // Extract the created entity ID
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = create_result.get(0) {
        *id
    } else {
        panic!("Failed to create entity");
    };
    
    // Read the user's name
    let read_req = sreq![sread!(user_id, sfield![name_field])];
    let read_result = store.perform(read_req)?;
    
    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = read_result.get(0) {
        println!("User name: {}", name);
    }
    
    Ok(())
}
```

### Remote Database Access with StoreProxy

```rust
use qlib_rs::*;
use std::net::TcpStream;

fn main() -> Result<()> {
    // Connect to remote qlib database server
    let stream = TcpStream::connect("127.0.0.1:8080")?;
    let mut proxy = StoreProxy::new(stream)?;
    
    // Authenticate with the server
    proxy.authenticate("username", "password")?;
    
    // Use the same API as with Store
    let user_type = proxy.get_entity_type("User")?;
    let name_field = proxy.get_field_type("Name")?;
    
    // Create and manipulate entities remotely
    let create_req = sreq![screate!(user_type, "Remote User".to_string())];
    let create_result = proxy.perform_mut(create_req)?;
    
    Ok(())
}
```

## Core Concepts

The database is built around a few key concepts:

*   **EntityType**: A type identifier for entities (e.g., "User", "Folder"). 
    Obtained via `store.get_entity_type("TypeName")` or created when defining schemas.

*   **FieldType**: A type identifier for fields within entities (e.g., "Name", "Email").
    Obtained via `store.get_field_type("FieldName")` or created when defining schemas.

*   **Entity**: An `Entity` is a unique object in the database, identified by an
    `EntityId`. Each entity has a type and a unique ID. Entities are lightweight 
    containers for fields.

*   **Field**: A `Field` is a piece of data associated with an entity. It's
    defined by a `FieldType` and holds a `Value`.

*   **Value**: The `Value` enum represents the actual data stored in a field. It
    can be a primitive type (`Bool`, `Int`, `Float`, `String`), a timestamp,
    binary data, or references to other entities (`EntityReference`, `EntityList`).

*   **EntitySchema**: Defines the structure for a given `EntityType`, specifying 
    which fields an entity of that type can have and supporting inheritance.

*   **FieldSchema**: Describes individual fields within an entity schema, 
    defining data type, default values, rank, and other constraints.

## The `Store` and `StoreProxy`

`qlib-rs` provides two main interfaces for database operations:

### `Store` - In-Memory Database

The `Store` is the core in-memory database that holds all entities, schemas, and 
their associated fields locally. It's perfect for:
- Single-process applications
- Local data management
- Testing and development
- Maximum performance with direct memory access

```rust
let mut store = Store::new();
let requests = sreq![sread!(entity_id, sfield![field_type])];
let result = store.perform(requests)?;
```

### `StoreProxy` - Remote Database Access

The `StoreProxy` provides the same API as `Store` but operates over TCP connections
to remote `qlib-rs` database servers. It's ideal for:
- Distributed applications
- Client-server architectures
- Remote data access
- Shared database access across multiple processes

```rust
let stream = TcpStream::connect("127.0.0.1:8080")?;
let mut proxy = StoreProxy::new(stream)?;
proxy.authenticate("username", "password")?;

// Same API as Store
let requests = sreq![sread!(entity_id, sfield![field_type])];
let result = proxy.perform(requests)?;
```

Both `Store` and `StoreProxy` implement the `StoreTrait`, meaning you can write 
generic code that works with either:

```rust
fn read_entity_name<T: StoreTrait>(store: &T, entity_id: EntityId, name_field: FieldType) -> Result<String> {
    let requests = sreq![sread!(entity_id, sfield![name_field])];
    let result = store.perform(requests)?;
    
    if let Some(Request::Read { value: Some(Value::String(name)), .. }) = result.get(0) {
        Ok(name.clone())
    } else {
        Err(Error::FieldTypeNotFound(entity_id, name_field))
    }
}
```

### Request Batching

Operations are batched into `Requests` and processed by the `perform` and `perform_mut` 
methods. This allows for efficient bulk operations:

```rust
// Batch multiple operations for efficiency
let requests = sreq![
    sread!(user_id, sfield![name_field]),
    sread!(user_id, sfield![email_field]),
    swrite!(user_id, sfield![status_field], sstr!("active")),
];
let results = store.perform_mut(requests)?;
```

## Macros for Easy Database Operations

`qlib-rs` provides a rich set of macros to make database operations more concise and readable:

### Request Creation Macros

#### `sreq!` - Create Request Batches
Creates a collection of requests for batch operations:
```rust
let requests = sreq![
    sread!(entity_id, sfield![field1]),
    swrite!(entity_id, sfield![field2], sstr!("value")),
    screate!(entity_type, "New Entity".to_string()),
];
```

#### `sread!` - Read Requests
Creates read requests to fetch field values:
```rust
// Read single field
let read_req = sread!(entity_id, sfield![name_field]);

// Read multiple fields
let read_req = sread!(entity_id, sfield![name_field, email_field, status_field]);
```

#### `swrite!` - Write Requests
Creates write requests to set field values:
```rust
// Basic write
let write_req = swrite!(entity_id, sfield![name_field], sstr!("John Doe"));

// Write without value (clear field)
let clear_req = swrite!(entity_id, sfield![field_type]);
```

#### `sadd!` and `ssub!` - Arithmetic Operations
Perform addition and subtraction on numeric fields:
```rust
// Add to counter
let add_req = sadd!(entity_id, sfield![counter_field], sint!(5));

// Subtract from balance
let sub_req = ssub!(entity_id, sfield![balance_field], sint!(10));
```

#### `screate!` - Entity Creation
Creates new entities:
```rust
// Create entity without parent
let create_req = screate!(entity_type, "Entity Name".to_string());

// Create entity with parent
let create_req = screate!(entity_type, "Child Entity".to_string(), parent_id);
```

#### `sdelete!` - Entity Deletion
Deletes entities:
```rust
let delete_req = sdelete!(entity_id);
```

#### `sschemaupdate!` - Schema Updates
Updates entity schemas:
```rust
let schema_req = sschemaupdate!(entity_schema);
```

### Value Creation Macros

#### `sfield!` - Field Type Lists
Creates lists of field types for read/write operations:
```rust
// Single field
let fields = sfield![name_field];

// Multiple fields
let fields = sfield![name_field, email_field, status_field];
```

#### Value Type Macros
Create typed values for write operations:
```rust
// Boolean values
let bool_val = sbool!(true);
let bool_val = sbool!(false);

// Numeric values
let int_val = sint!(42);
let float_val = sfloat!(3.14);

// String values
let str_val = sstr!("Hello, World!");

// Entity references
let ref_val = sref!(Some(referenced_entity_id));
let null_ref = sref!(None);

// Entity lists
let empty_list = sreflist![];
let entity_list = sreflist![entity1, entity2, entity3];

// Timestamps
let time_val = stimestamp!(now());

// Binary data
let blob_val = sblob!(vec![0u8, 1, 2, 3]);

// Choice values
let choice_val = schoice!("option_a".to_string());
```

### Complete Example with Macros
```rust
use qlib_rs::*;

fn macro_example() -> Result<()> {
    let mut store = Store::new();
    
    // Setup entity types and field types
    let user_type = store.get_entity_type("User")?;
    let name_field = store.get_field_type("Name")?;
    let age_field = store.get_field_type("Age")?;
    let email_field = store.get_field_type("Email")?;
    
    // Create a user and read back data in one batch
    let batch_requests = sreq![
        screate!(user_type, "Jane Doe".to_string()),
        // Note: In real usage, you'd get the entity_id from the create response
        // This is just for demonstration
    ];
    
    let results = store.perform_mut(batch_requests)?;
    
    // Extract created entity ID
    let user_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = results.get(0) {
        *id
    } else {
        return Err(Error::InvalidRequest("Failed to create entity".to_string()));
    };
    
    // Write and read data
    let data_operations = sreq![
        swrite!(user_id, sfield![name_field], sstr!("Jane Smith")),
        swrite!(user_id, sfield![age_field], sint!(28)),
        swrite!(user_id, sfield![email_field], sstr!("jane@example.com")),
        sread!(user_id, sfield![name_field, age_field, email_field]),
    ];
    
    let results = store.perform_mut(data_operations)?;
    
    // Process results
    for request in results.iter() {
        match request {
            Request::Read { value: Some(value), .. } => {
                println!("Read value: {:?}", value);
            }
            Request::Write { .. } => {
                println!("Write completed");
            }
            _ => {}
        }
    }
    
    Ok(())
}
```

## Entity and Field Schema Management

### EntityType and FieldType Management

Before working with entities, you need to define and register entity types and field types:

```rust
// Get or create entity types
let user_type = store.get_entity_type("User")?;
let folder_type = store.get_entity_type("Folder")?;

// Get string representation
let user_type_name = store.resolve_entity_type(user_type)?; // Returns "User"

// Get or create field types
let name_field = store.get_field_type("Name")?;
let email_field = store.get_field_type("Email")?;
let parent_field = store.get_field_type("Parent")?;

// Get string representation
let name_field_name = store.resolve_field_type(name_field)?; // Returns "Name"
```

### EntitySchema Definition

Schemas define the structure and constraints for entity types:

```rust
use qlib_rs::*;
use qlib_rs::data::StorageScope;

// Create a new entity schema
let mut user_schema = EntitySchema::<Single, String, String>::new(
    "User".to_string(),
    vec![] // No inheritance for this example
);

// Add fields to the schema
user_schema.fields.insert("Name".to_string(), FieldSchema::String {
    field_type: "Name".to_string(),
    default_value: "Unnamed User".to_string(),
    rank: 1,
    storage_scope: StorageScope::Configuration,
});

user_schema.fields.insert("Email".to_string(), FieldSchema::String {
    field_type: "Email".to_string(),
    default_value: "".to_string(),
    rank: 2,
    storage_scope: StorageScope::Configuration,
});

user_schema.fields.insert("Age".to_string(), FieldSchema::Int {
    field_type: "Age".to_string(),
    default_value: 0,
    rank: 3,
    storage_scope: StorageScope::Configuration,
});

user_schema.fields.insert("IsActive".to_string(), FieldSchema::Bool {
    field_type: "IsActive".to_string(),
    default_value: true,
    rank: 4,
    storage_scope: StorageScope::Configuration,
});

// Relationships
user_schema.fields.insert("Manager".to_string(), FieldSchema::EntityReference {
    field_type: "Manager".to_string(),
    default_value: None,
    rank: 5,
    storage_scope: StorageScope::Configuration,
});

user_schema.fields.insert("DirectReports".to_string(), FieldSchema::EntityList {
    field_type: "DirectReports".to_string(),
    default_value: vec![],
    rank: 6,
    storage_scope: StorageScope::Configuration,
});

// Register the schema
let requests = sreq![sschemaupdate!(user_schema)];
store.perform_mut(requests)?;
```

### FieldSchema Types

Different field types support different data:

```rust
// String fields
FieldSchema::String {
    field_type: "Name".to_string(),
    default_value: "Default Name".to_string(),
    rank: 1,
    storage_scope: StorageScope::Configuration,
}

// Integer fields
FieldSchema::Int {
    field_type: "Count".to_string(),
    default_value: 0,
    rank: 2,
    storage_scope: StorageScope::Configuration,
}

// Float fields
FieldSchema::Float {
    field_type: "Rating".to_string(),
    default_value: 0.0,
    rank: 3,
    storage_scope: StorageScope::Configuration,
}

// Boolean fields
FieldSchema::Bool {
    field_type: "IsEnabled".to_string(),
    default_value: false,
    rank: 4,
    storage_scope: StorageScope::Configuration,
}

// Entity reference (one-to-one/many-to-one)
FieldSchema::EntityReference {
    field_type: "Parent".to_string(),
    default_value: None,
    rank: 5,
    storage_scope: StorageScope::Configuration,
}

// Entity list (one-to-many)
FieldSchema::EntityList {
    field_type: "Children".to_string(),
    default_value: vec![],
    rank: 6,
    storage_scope: StorageScope::Configuration,
}

// Binary data
FieldSchema::Blob {
    field_type: "Avatar".to_string(),
    default_value: vec![],
    rank: 7,
    storage_scope: StorageScope::Configuration,
}

// Timestamp
FieldSchema::Timestamp {
    field_type: "CreatedAt".to_string(),
    default_value: epoch(),
    rank: 8,
    storage_scope: StorageScope::Configuration,
}

// Choice field (enumerated values)
FieldSchema::Choice {
    field_type: "Status".to_string(),
    default_value: "active".to_string(),
    rank: 9,
    storage_scope: StorageScope::Configuration,
}
```

### Working with Schemas

```rust
// Get entity schema
let schema = store.get_entity_schema(user_type)?;

// Check if field exists in schema
let has_email = store.field_exists(user_type, email_field);

// Get specific field schema
let email_schema = store.get_field_schema(user_type, email_field)?;

// Update field schema
let new_email_schema = FieldSchema::String {
    field_type: "Email".to_string(),
    default_value: "user@domain.com".to_string(), // Changed default
    rank: 2,
    storage_scope: StorageScope::Configuration,
};
store.set_field_schema(user_type, email_field, new_email_schema)?;
```

## Indirection

Indirection is a powerful feature that allows you to traverse relationships
between entities in a single read or write request, without needing to perform
multiple queries. An indirection path is a string composed of field names and
list indices, separated by `->`.

### How Indirection Works

Consider a hierarchy: `Root -> Folder -> User`. To get the email of
a user named "admin" inside a "Users" folder, you can use indirection paths.

Let's break down an example path: `Parent->Children->0->Name`
1.  `Parent`: This resolves to the `Parent` field of the starting entity. This
    field is expected to be an `EntityReference`. The store follows this
    reference to the parent entity.
2.  `Children`: Now on the parent entity, it looks for the `Children` field. This
    is expected to be an `EntityList`.
3.  `0`: This is an index into the `EntityList` from the previous step. It
    selects the first entity in the list.
4.  `Name`: Finally, it resolves the `Name` field on the entity selected by the
    index.

This allows for complex data retrieval in a concise and efficient manner. If any
part of the path fails to resolve (e.g., an empty reference, an index out of
bounds), the operation will fail with a `BadIndirection` error.

### Indirection with Both Store and StoreProxy

Indirection works with both `Store` and `StoreProxy`:

```rust
// Direct indirection resolution
let (final_entity_id, final_field) = store.resolve_indirection(
    starting_entity_id, 
    &[parent_field, children_field, name_field]
)?;

// Works the same with StoreProxy
let (final_entity_id, final_field) = proxy.resolve_indirection(
    starting_entity_id,
    &[parent_field, children_field, name_field]
)?;
```

### Path-Based Navigation

The library provides path utilities for easier navigation:

```rust
use qlib_rs::{path, path_to_entity_id};

// Get path from entity to root
let entity_path = path(&store, entity_id)?;  // Returns "/root/users/john"

// Find entity by path
let found_entity = path_to_entity_id(&store, "/root/users/john")?;
```

### Reading with Indirection

Use indirection in read requests to traverse relationships:

```rust
// Read a field through indirection
let indirect_field_types = sfield![parent_field, name_field]; // Parent->Name
let read_req = sread!(child_entity_id, indirect_field_types);
let result = store.perform(read_req)?;

// This reads the Name field of the parent entity
if let Some(Request::Read { value: Some(Value::String(parent_name)), .. }) = result.get(0) {
    println!("Parent name: {}", parent_name);
}
```

### Writing with Indirection

You can also write through indirection:

```rust
// Write to a field through indirection
let indirect_field_types = sfield![parent_field, status_field]; // Parent->Status
let write_req = swrite!(child_entity_id, indirect_field_types, sstr!("updated"));
store.perform_mut(sreq![write_req])?;

// This updates the Status field of the parent entity
```

### Complex Indirection Examples

```rust
// Navigate through multiple levels: Parent->Parent->Name (grandparent's name)
let grandparent_name_field = sfield![parent_field, parent_field, name_field];
let read_req = sread!(entity_id, grandparent_name_field);

// Access first child's email: Children->0->Email
let first_child_email = sfield![children_field]; // Note: array indexing happens during resolution
// You would need to use the indirection resolution system for array access
```

## Notifications System

`qlib-rs` provides a comprehensive notification system that allows you to monitor changes to entities and fields in real-time. This is useful for reactive applications, caching, and maintaining data consistency.

### Notification Configuration

You can configure notifications at two levels:

#### Entity-Specific Notifications
Monitor specific entities for field changes:

```rust
use qlib_rs::*;

// Create notification config for a specific entity
let notify_config = NotifyConfig::EntityId {
    entity_id: user_id,
    field_type: name_field,
    trigger_on_change: true,  // Only notify when value actually changes
    context: vec![
        vec![email_field],     // Include email in notification context
        vec![parent_field, name_field], // Include parent's name via indirection
    ],
};

// Hash the config for efficient lookup
let config_hash = hash_notify_config(&notify_config);
```

#### Entity Type Notifications
Monitor all entities of a specific type:

```rust
let notify_config = NotifyConfig::EntityType {
    entity_type: user_type,
    field_type: status_field,
    trigger_on_change: false,  // Notify on all writes, even if value doesn't change
    context: vec![
        vec![name_field],       // Include name in context
        vec![last_login_field], // Include last login time
    ],
};
```

### Notification Queue

Use `NotificationQueue` to receive notifications:

```rust
let notification_queue = NotificationQueue::new();

// Register the queue with the store (implementation depends on your setup)
// This is typically done in the store configuration

// Process notifications
while let Some(notification) = notification_queue.pop() {
    println!("Received notification:");
    println!("  Current value: {:?}", notification.current);
    println!("  Previous value: {:?}", notification.previous);
    println!("  Config hash: {}", notification.config_hash);
    
    // Process context fields
    for (context_field, context_value) in notification.context.iter() {
        println!("  Context {:?}: {:?}", context_field, context_value);
    }
}
```

### Notification Structure

Each `Notification` contains:
- `current`: A `Request::Read` with the new field value and metadata
- `previous`: A `Request::Read` with the old field value and metadata  
- `context`: A map of context fields to their values (via indirection)
- `config_hash`: Hash identifying which notification config triggered this

### Practical Example

```rust
use qlib_rs::*;

fn setup_user_notifications(store: &mut Store) -> Result<NotificationQueue> {
    let user_type = store.get_entity_type("User")?;
    let name_field = store.get_field_type("Name")?;
    let email_field = store.get_field_type("Email")?;
    let status_field = store.get_field_type("Status")?;
    
    let notification_queue = NotificationQueue::new();
    
    // Monitor name changes for all users
    let name_config = NotifyConfig::EntityType {
        entity_type: user_type,
        field_type: name_field,
        trigger_on_change: true,
        context: vec![
            vec![email_field],  // Include email for context
            vec![status_field], // Include status for context
        ],
    };
    
    // Configure notifications (this would typically integrate with store internals)
    // let config_hash = hash_notify_config(&name_config);
    // store.add_notification_config(name_config, notification_queue.clone())?;
    
    Ok(notification_queue)
}

fn process_user_notifications(queue: &NotificationQueue) {
    while let Some(notification) = queue.pop() {
        // Extract the changed name
        if let Request::Read { value: Some(Value::String(new_name)), .. } = &notification.current {
            if let Request::Read { value: Some(Value::String(old_name)), .. } = &notification.previous {
                println!("User name changed from '{}' to '{}'", old_name, new_name);
                
                // Check context for additional information
                for (context_path, context_request) in &notification.context {
                    match context_request {
                        Request::Read { value: Some(Value::String(email)), .. } 
                            if context_path.len() == 1 => {
                            println!("  User email: {}", email);
                        }
                        Request::Read { value: Some(Value::String(status)), .. } => {
                            println!("  User status: {}", status);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
```

### Notification Best Practices

1. **Use `trigger_on_change: true`** when you only care about actual value changes
2. **Include relevant context** to avoid additional queries when processing notifications
3. **Use indirection in context** to gather related data efficiently
4. **Hash configs for performance** when managing many notification subscriptions
5. **Process notifications asynchronously** to avoid blocking database operations

## Querying Entities

Both `Store` and `StoreProxy` provide powerful querying capabilities to find entities based on type and filters.

### Basic Entity Finding

```rust
// Find all entities of a specific type
let user_type = store.get_entity_type("User")?;
let all_users = store.find_entities(user_type, None)?;
println!("Found {} users", all_users.len());

// Find entities with a filter (implementation-dependent)
let active_users = store.find_entities(user_type, Some("status = 'active'".to_string()))?;
```

### Paginated Queries

For large datasets, use pagination:

```rust
use qlib_rs::PageOpts;

let page_opts = PageOpts {
    page_size: 10,
    page_number: 1,
};

// Paginated search (approximate results, good for UI)
let page_result = store.find_entities_paginated(user_type, Some(page_opts), None)?;
println!("Page {} of {}, {} total entities", 
         page_result.page_number, 
         page_result.total_pages, 
         page_result.total_count);

for entity_id in page_result.entities {
    println!("  User ID: {:?}", entity_id);
}

// Exact search (precise results, may be slower)
let exact_result = store.find_entities_exact(user_type, Some(page_opts), None)?;
```

### Querying with StoreProxy

StoreProxy provides the same querying interface:

```rust
// Remote querying works identically
let stream = TcpStream::connect("127.0.0.1:8080")?;
let mut proxy = StoreProxy::new(stream)?;
proxy.authenticate("username", "password")?;

let user_type = proxy.get_entity_type("User")?;
let remote_users = proxy.find_entities(user_type, None)?;

// Pagination also works remotely
let remote_page = proxy.find_entities_paginated(user_type, Some(page_opts), None)?;
```

### Entity Type Discovery

```rust
// Get all available entity types
let all_types = store.get_entity_types()?;
for entity_type in all_types {
    let type_name = store.resolve_entity_type(entity_type)?;
    println!("Available type: {}", type_name);
}
```

### Checking Entity Existence

```rust
// Check if specific entity exists
let exists = store.entity_exists(entity_id);
if exists {
    println!("Entity {:?} exists", entity_id);
}

// Check if entity has specific field
let has_email = store.field_exists(user_type, email_field);
```

### Practical Querying Example

```rust
use qlib_rs::*;

fn find_active_users_by_department(store: &impl StoreTrait, department: &str) -> Result<Vec<EntityId>> {
    let user_type = store.get_entity_type("User")?;
    let department_field = store.get_field_type("Department")?;
    let status_field = store.get_field_type("Status")?;
    
    // Get all users (in real implementation, you might filter server-side)
    let all_users = store.find_entities(user_type, None)?;
    let mut matching_users = Vec::new();
    
    // Filter users by department and status
    for user_id in all_users {
        let read_req = sreq![sread!(user_id, sfield![department_field, status_field])];
        let result = store.perform(read_req)?;
        
        if result.len() >= 2 {
            let dept_match = matches!(result[0], Request::Read { 
                value: Some(Value::String(ref d)), .. 
            } if d == department);
            
            let status_match = matches!(result[1], Request::Read { 
                value: Some(Value::String(ref s)), .. 
            } if s == "active");
            
            if dept_match && status_match {
                matching_users.push(user_id);
            }
        }
    }
    
    Ok(matching_users)
}
```

## Entity Inheritance

The `qlib-rs` library supports an entity inheritance model similar to object-oriented programming. This allows you to define entity types that inherit fields and behavior from parent entity types.

### How Inheritance Works

1. **Entity Schema Definition**:
   When defining an entity schema, you can specify a parent entity type using the `inherit` field:

   ```rust
   let mut schema = EntitySchema::<Single, String, String>::new(
       "User".to_string(), 
       vec!["Object".to_string()]  // Inherits from Object
   );
   ```

2. **Field Inheritance**:
   Child entity types automatically inherit all fields from their parent entity types. For example, if the "Object" type has "Name", "Parent", and "Children" fields, any entity type inheriting from "Object" will also have these fields.

3. **Field Override**:
   Child entity types can override fields defined by their parent types by defining fields with the same field type:

   ```rust
   // Parent "Object" has a default Name field, but User can override it with different properties
   let name_schema = FieldSchema::String {
       field_type: "Name".to_string(),
       default_value: "New User".to_string(),  // Override default value
       rank: 0,
       storage_scope: StorageScope::Configuration,
   };
   ```

4. **Multi-level Inheritance**:
   The system supports multiple levels of inheritance. For example, "Employee" could inherit from "User", which inherits from "Object".

5. **Complete Schema Resolution**:
   When working with entities, the library automatically resolves the complete schema by combining all inherited fields:

   ```rust
   // This returns a schema with all inherited fields
   let complete_schema = store.get_complete_entity_schema(entity_type)?;
   ```

### Base Type

By convention, all entity types should inherit from the "Object" base type, which provides common fields:

- `Name`: String type for naming the entity
- `Parent`: EntityReference type to establish hierarchy
- `Children`: EntityList type to track child entities

### Benefits of Inheritance

- **Code Reuse**: Define common fields once and reuse them across multiple entity types
- **Consistency**: Ensure consistent field structure across related entity types
- **Schema Evolution**: Easily evolve your data model by adding fields to parent types

### Example Usage

```rust
use qlib_rs::*;
use qlib_rs::data::StorageScope;

// Define a base Person type inheriting from Object
let mut person_schema = EntitySchema::<Single, String, String>::new(
    "Person".to_string(), 
    vec!["Object".to_string()]
);

// Add Person-specific fields
person_schema.fields.insert("Age".to_string(), FieldSchema::Int {
    field_type: "Age".to_string(),
    default_value: 0,
    rank: 3,
    storage_scope: StorageScope::Configuration,
});

// Register the schema
let requests = sreq![sschemaupdate!(person_schema)];
store.perform_mut(requests)?;

// Define an Employee type inheriting from Person
let mut employee_schema = EntitySchema::<Single, String, String>::new(
    "Employee".to_string(), 
    vec!["Person".to_string()]
);

// Add Employee-specific fields
employee_schema.fields.insert("Department".to_string(), FieldSchema::String {
    field_type: "Department".to_string(),
    default_value: "".to_string(),
    rank: 4,
    storage_scope: StorageScope::Configuration,
});

// Register the schema
let requests = sreq![sschemaupdate!(employee_schema)];
store.perform_mut(requests)?;

// Now Employee entities will have:
// - Name, Parent, Children (from Object)
// - Age (from Person)  
// - Department (from Employee)
```

### Implementation Details

The library manages two versions of entity schemas:

1. `EntitySchema<Single>`: Represents the schema as defined, without resolving inheritance
2. `EntitySchema<Complete>`: Represents the fully resolved schema with all inherited fields

When querying or manipulating entities, the library uses the complete schema to ensure all inherited fields are available.

## Database Structure

`qlib-rs` uses a structure similar to an Entity-Attribute-Value (EAV) model.
Instead of rigid tables, data is stored in a more flexible way:

*   `schemas`: A map from `EntityType` to `EntitySchema`, defining the data model.
*   `entities`: A map from `EntityType` to a list of all `EntityId`s of that type.
*   `fields`: The core data storage. It's a map where the key is an `EntityId`
    and the value is another map from `FieldType` to the actual `Field` data
    (which includes the `Value`).

Relationships between entities are a key feature. They are not enforced by foreign
keys but are managed through special `Value` types:
*   `Value::EntityReference`: Represents a one-to-one or many-to-one relationship.
    For example, a "User" entity might have a "Manager" field of this type.
*   `Value::EntityList`: Represents a one-to-many relationship. For example, a
    "Folder" entity would have a "Children" field of this type to list all the
    entities it contains.

The library provides helpers like `create_entity` which automatically manage
bidirectional parent-child relationships.

## Complete Working Example

Here's a comprehensive example that demonstrates most features of qlib-rs:

```rust
use qlib_rs::*;
use qlib_rs::data::StorageScope;
use std::net::TcpStream;

fn main() -> Result<()> {
    // Option 1: Local in-memory database
    let mut store = Store::new();
    demo_with_store(&mut store)?;
    
    // Option 2: Remote database via StoreProxy
    // let stream = TcpStream::connect("127.0.0.1:8080")?;
    // let mut proxy = StoreProxy::new(stream)?;
    // proxy.authenticate("username", "password")?;
    // demo_with_store(&mut proxy)?;
    
    Ok(())
}

fn demo_with_store<T: StoreTrait>(store: &mut T) -> Result<()> 
where T: StoreTrait + Clone
{
    // 1. Define schemas with inheritance
    setup_schemas(store)?;
    
    // 2. Create entities
    let company_id = create_company(store)?;
    let dept_id = create_department(store, company_id)?;
    let manager_id = create_employee(store, dept_id, "Alice Manager", "Engineering", true)?;
    let dev_id = create_employee(store, dept_id, "Bob Developer", "Engineering", false)?;
    
    // 3. Establish relationships
    establish_relationships(store, manager_id, dev_id)?;
    
    // 4. Query and display data
    display_company_structure(store, company_id)?;
    
    // 5. Demonstrate notifications (conceptual)
    // setup_notifications(store)?;
    
    Ok(())
}

fn setup_schemas<T: StoreTrait>(store: &mut T) -> Result<()> {
    // Base Object schema
    let mut object_schema = EntitySchema::<Single, String, String>::new("Object".to_string(), vec![]);
    object_schema.fields.insert("Name".to_string(), FieldSchema::String {
        field_type: "Name".to_string(),
        default_value: "".to_string(),
        rank: 1,
        storage_scope: StorageScope::Configuration,
    });
    object_schema.fields.insert("Parent".to_string(), FieldSchema::EntityReference {
        field_type: "Parent".to_string(),
        default_value: None,
        rank: 2,
        storage_scope: StorageScope::Configuration,
    });
    object_schema.fields.insert("Children".to_string(), FieldSchema::EntityList {
        field_type: "Children".to_string(),
        default_value: vec![],
        rank: 3,
        storage_scope: StorageScope::Configuration,
    });
    
    // Company schema
    let mut company_schema = EntitySchema::<Single, String, String>::new(
        "Company".to_string(), 
        vec!["Object".to_string()]
    );
    company_schema.fields.insert("Industry".to_string(), FieldSchema::String {
        field_type: "Industry".to_string(),
        default_value: "Technology".to_string(),
        rank: 4,
        storage_scope: StorageScope::Configuration,
    });
    
    // Department schema  
    let mut dept_schema = EntitySchema::<Single, String, String>::new(
        "Department".to_string(),
        vec!["Object".to_string()]
    );
    dept_schema.fields.insert("Budget".to_string(), FieldSchema::Float {
        field_type: "Budget".to_string(),
        default_value: 0.0,
        rank: 4,
        storage_scope: StorageScope::Configuration,
    });
    
    // Employee schema
    let mut employee_schema = EntitySchema::<Single, String, String>::new(
        "Employee".to_string(),
        vec!["Object".to_string()]
    );
    employee_schema.fields.insert("Department".to_string(), FieldSchema::String {
        field_type: "Department".to_string(),
        default_value: "".to_string(),
        rank: 4,
        storage_scope: StorageScope::Configuration,
    });
    employee_schema.fields.insert("IsManager".to_string(), FieldSchema::Bool {
        field_type: "IsManager".to_string(),
        default_value: false,
        rank: 5,
        storage_scope: StorageScope::Configuration,
    });
    employee_schema.fields.insert("Manager".to_string(), FieldSchema::EntityReference {
        field_type: "Manager".to_string(),
        default_value: None,
        rank: 6,
        storage_scope: StorageScope::Configuration,
    });
    
    // Register all schemas
    let schema_requests = sreq![
        sschemaupdate!(object_schema),
        sschemaupdate!(company_schema),
        sschemaupdate!(dept_schema),
        sschemaupdate!(employee_schema),
    ];
    store.perform_mut(schema_requests)?;
    
    Ok(())
}

fn create_company<T: StoreTrait>(store: &mut T) -> Result<EntityId> {
    let company_type = store.get_entity_type("Company")?;
    let name_field = store.get_field_type("Name")?;
    let industry_field = store.get_field_type("Industry")?;
    
    let requests = sreq![
        screate!(company_type, "TechCorp Inc.".to_string()),
    ];
    let results = store.perform_mut(requests)?;
    
    let company_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = results.get(0) {
        *id
    } else {
        return Err(Error::InvalidRequest("Failed to create company".to_string()));
    };
    
    // Set company details
    let detail_requests = sreq![
        swrite!(company_id, sfield![name_field], sstr!("TechCorp Inc.")),
        swrite!(company_id, sfield![industry_field], sstr!("Software Development")),
    ];
    store.perform_mut(detail_requests)?;
    
    Ok(company_id)
}

fn create_department<T: StoreTrait>(store: &mut T, parent_id: EntityId) -> Result<EntityId> {
    let dept_type = store.get_entity_type("Department")?;
    let name_field = store.get_field_type("Name")?;
    let budget_field = store.get_field_type("Budget")?;
    
    let requests = sreq![
        screate!(dept_type, "Engineering".to_string(), parent_id),
    ];
    let results = store.perform_mut(requests)?;
    
    let dept_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = results.get(0) {
        *id
    } else {
        return Err(Error::InvalidRequest("Failed to create department".to_string()));
    };
    
    // Set department details
    let detail_requests = sreq![
        swrite!(dept_id, sfield![name_field], sstr!("Engineering")),
        swrite!(dept_id, sfield![budget_field], sfloat!(500000.0)),
    ];
    store.perform_mut(detail_requests)?;
    
    Ok(dept_id)
}

fn create_employee<T: StoreTrait>(
    store: &mut T, 
    dept_id: EntityId, 
    name: &str, 
    department: &str, 
    is_manager: bool
) -> Result<EntityId> {
    let employee_type = store.get_entity_type("Employee")?;
    let name_field = store.get_field_type("Name")?;
    let dept_field = store.get_field_type("Department")?;
    let manager_flag_field = store.get_field_type("IsManager")?;
    
    let requests = sreq![
        screate!(employee_type, name.to_string(), dept_id),
    ];
    let results = store.perform_mut(requests)?;
    
    let employee_id = if let Some(Request::Create { created_entity_id: Some(id), .. }) = results.get(0) {
        *id
    } else {
        return Err(Error::InvalidRequest("Failed to create employee".to_string()));
    };
    
    // Set employee details
    let detail_requests = sreq![
        swrite!(employee_id, sfield![name_field], sstr!(name)),
        swrite!(employee_id, sfield![dept_field], sstr!(department)),
        swrite!(employee_id, sfield![manager_flag_field], sbool!(is_manager)),
    ];
    store.perform_mut(detail_requests)?;
    
    Ok(employee_id)
}

fn establish_relationships<T: StoreTrait>(store: &mut T, manager_id: EntityId, employee_id: EntityId) -> Result<()> {
    let manager_field = store.get_field_type("Manager")?;
    
    // Set manager relationship
    let requests = sreq![
        swrite!(employee_id, sfield![manager_field], sref!(Some(manager_id))),
    ];
    store.perform_mut(requests)?;
    
    Ok(())
}

fn display_company_structure<T: StoreTrait>(store: &T, company_id: EntityId) -> Result<()> {
    let name_field = store.get_field_type("Name")?;
    let children_field = store.get_field_type("Children")?;
    let manager_field = store.get_field_type("Manager")?;
    
    // Read company info
    let company_read = sreq![sread!(company_id, sfield![name_field, children_field])];
    let company_result = store.perform(company_read)?;
    
    println!("=== Company Structure ===");
    if let Some(Request::Read { value: Some(Value::String(company_name)), .. }) = company_result.get(0) {
        println!("Company: {}", company_name);
    }
    
    // Display departments and employees using indirection
    if let Some(Request::Read { value: Some(Value::EntityList(departments)), .. }) = company_result.get(1) {
        for dept_id in departments {
            display_department(store, *dept_id)?;
        }
    }
    
    Ok(())
}

fn display_department<T: StoreTrait>(store: &T, dept_id: EntityId) -> Result<()> {
    let name_field = store.get_field_type("Name")?;
    let children_field = store.get_field_type("Children")?;
    let budget_field = store.get_field_type("Budget")?;
    
    let dept_read = sreq![sread!(dept_id, sfield![name_field, budget_field, children_field])];
    let dept_result = store.perform(dept_read)?;
    
    if let (
        Some(Request::Read { value: Some(Value::String(dept_name)), .. }),
        Some(Request::Read { value: Some(Value::Float(budget)), .. }),
        Some(Request::Read { value: Some(Value::EntityList(employees)), .. })
    ) = (dept_result.get(0), dept_result.get(1), dept_result.get(2)) {
        println!("  Department: {} (Budget: ${:.2})", dept_name, budget);
        
        for employee_id in employees {
            display_employee(store, *employee_id)?;
        }
    }
    
    Ok(())
}

fn display_employee<T: StoreTrait>(store: &T, employee_id: EntityId) -> Result<()> {
    let name_field = store.get_field_type("Name")?;
    let is_manager_field = store.get_field_type("IsManager")?;
    let manager_field = store.get_field_type("Manager")?;
    
    let employee_read = sreq![sread!(employee_id, sfield![name_field, is_manager_field, manager_field])];
    let employee_result = store.perform(employee_read)?;
    
    if let Some(Request::Read { value: Some(Value::String(emp_name)), .. }) = employee_result.get(0) {
        let is_mgr = matches!(employee_result.get(1), Some(Request::Read { value: Some(Value::Bool(true)), .. }));
        let role = if is_mgr { "Manager" } else { "Employee" };
        
        println!("    {}: {}", role, emp_name);
        
        // Show manager relationship
        if let Some(Request::Read { value: Some(Value::EntityReference(Some(mgr_id))), .. }) = employee_result.get(2) {
            let mgr_read = sreq![sread!(*mgr_id, sfield![name_field])];
            if let Ok(mgr_result) = store.perform(mgr_read) {
                if let Some(Request::Read { value: Some(Value::String(mgr_name)), .. }) = mgr_result.get(0) {
                    println!("      Reports to: {}", mgr_name);
                }
            }
        }
    }
    
    Ok(())
}
```

This example demonstrates:
- Schema definition with inheritance
- Entity creation with relationships
- Complex querying and data traversal  
- Working with both Store and StoreProxy (commented)
- Practical usage patterns

For more examples and advanced usage, check the `src/test/` directory in the repository.