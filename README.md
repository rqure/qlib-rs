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