# qlib-rs: A flexible in-memory database library

`qlib-rs` provides a simple yet powerful in-memory database based on an
Entity-Attribute-Value (EAV) model. It's designed for scenarios where you need to
manage structured but flexible data, with a focus on relationships between
entities.

## Core Concepts

The database is built around a few key concepts:

*   **Entity**: An `Entity` is a unique object in the database, identified by an
    `EntityId`. Each entity has a type (e.g., "User", "Folder") and a unique
    ID. Entities are lightweight; they are primarily containers for fields.

*   **Field**: A `Field` is a piece of data associated with an entity. It's
    defined by a `FieldType` (e.g., "Name", "Email") and holds a `Value`.

*   **Value**: The `Value` enum represents the actual data stored in a field. It
    can be a primitive type (`Bool`, `Int`, `Float`, `String`), a timestamp,
    binary data, or a reference to other entities (`EntityReference`,
    `EntityList`).

*   **Schema**: A `Schema` defines the structure for a given `EntityType`. The
    `EntitySchema` specifies which fields an entity of that type can have. Each
    field is further described by a `FieldSchema`, which defines its data type
    (via a `default_value`), rank, and other constraints.

## The `Store`

The `Store` is the central component of `qlib-rs`. It's the in-memory database
that holds all entities, schemas, and their associated fields. All interactions
with the database, such as creating entities, reading fields, or writing values,
are performed through the `Store`.

Operations are batched into a `Vec<Request>` and processed by the `Store::perform`
method. A `Request` can be either a `Read` or a `Write`.

### Database Structure

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

## Indirection

Indirection is a powerful feature that allows you to traverse relationships
between entities in a single read or write request, without needing to perform
multiple queries. An indirection path is a string composed of field names and
list indices, separated by `->`.

For example, consider a hierarchy: `Root -> Folder -> User`. To get the email of
a user named "admin" inside a "Users" folder, you might use an indirection path
like: `"Children->0->Email"`.

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

# Entity Inheritance in qlib-rs

The `qlib-rs` library supports an entity inheritance model similar to object-oriented programming. This allows you to define entity types that inherit fields and behavior from parent entity types.

## How Inheritance Works

1. **Entity Schema Definition**:
   When defining an entity schema, you can specify a parent entity type using the `inherit` field:

   ```rust
   let mut schema = EntitySchema::<Single>::new("User".into(), Some("Object".into()));
   ```

2. **Field Inheritance**:
   Child entity types automatically inherit all fields from their parent entity types. For example, if the "Object" type has "Name", "Parent", and "Children" fields, any entity type inheriting from "Object" will also have these fields.

3. **Field Override**:
   Child entity types can override fields defined by their parent types by defining fields with the same field type:

   ```rust
   // Parent "Object" has a default Name field, but User can override it with different properties
   let name_schema = FieldSchema {
       field_type: "Name".into(),
       default_value: Value::String("New User".into()),  // Override default value
       rank: 0,
       read_permission: None,
       write_permission: None,
       choices: None,
   };
   ```

4. **Multi-level Inheritance**:
   The system supports multiple levels of inheritance. For example, "Employee" could inherit from "User", which inherits from "Object".

5. **Complete Schema Resolution**:
   When working with entities, the library automatically resolves the complete schema by combining all inherited fields:

   ```rust
   // This returns a schema with all inherited fields
   let complete_schema = store.get_complete_entity_schema(&ctx, &entity_type)?;
   ```

## Base Type

By convention, all entity types should inherit from the "Object" base type, which provides common fields:

- `Name`: String type for naming the entity
- `Parent`: EntityReference type to establish hierarchy
- `Children`: EntityList type to track child entities

## Benefits of Inheritance

- **Code Reuse**: Define common fields once and reuse them across multiple entity types
- **Consistency**: Ensure consistent field structure across related entity types
- **Schema Evolution**: Easily evolve your data model by adding fields to parent types

## Example Usage

```rust
// Define a base Person type inheriting from Object
let mut person_schema = EntitySchema::<Single>::new("Person".into(), Some("Object".into()));

// Add Person-specific fields
person_schema.fields.insert("Age".into(), FieldSchema {
    entity_type: "Person".into(),
    field_type: "Age".into(),
    default_value: Value::Int(0),
    rank: 3,
    read_permission: None,
    write_permission: None,
    choices: None,
});

// Register the schema
store.set_entity_schema(&ctx, &person_schema)?;

// Define an Employee type inheriting from Person
let mut employee_schema = EntitySchema::<Single>::new("Employee".into(), Some("Person".into()));

// Add Employee-specific fields
employee_schema.fields.insert("Department".into(), FieldSchema {
    entity_type: "Employee".into(),
    field_type: "Department".into(),
    default_value: Value::String("".into()),
    rank: 4,
    read_permission: None,
    write_permission: None,
    choices: None,
});

// Register the schema
store.set_entity_schema(&ctx, &employee_schema)?;

// Now Employee entities will have:
// - Name, Parent, Children (from Object)
// - Age (from Person)
// - Department (from Employee)
```

## Implementation Details

The library manages two versions of entity schemas:

1. `EntitySchema<Single>`: Represents the schema as defined, without resolving inheritance
2. `EntitySchema<Complete>`: Represents the fully resolved schema with all inherited fields

When querying or manipulating entities, the library uses the complete schema to ensure all inherited fields are available.