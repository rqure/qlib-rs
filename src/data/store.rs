use std::{collections::HashMap, error, mem::discriminant};
use serde::{Deserialize, Serialize};

use crate::{
    Entity, EntityId, EntitySchema, Field, FieldSchema, Request, Result, Snowflake, Value,
    data::{EntityType, FieldType, Shared, Timestamp, now, request::WriteOption},
    sread, sref, sreflist, sstr, swrite,
};

pub const INDIRECTION_DELIMITER: &str = "->";

#[derive(Debug, Clone)]
pub struct EntityExists(EntityId);
impl error::Error for EntityExists {}
impl std::fmt::Display for EntityExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity already exists: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EntityTypeNotFound(EntityType);
impl error::Error for EntityTypeNotFound {}
impl std::fmt::Display for EntityTypeNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown entity type: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EntityNotFound(EntityId);
impl error::Error for EntityNotFound {}
impl std::fmt::Display for EntityNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity not found: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct FieldNotFound(EntityId, FieldType);
impl error::Error for FieldNotFound {}
impl std::fmt::Display for FieldNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Field not found for entity {}: {}", self.0, self.1)
    }
}

#[derive(Debug, Clone)]
pub struct ValueTypeMismatch(EntityId, FieldType, Value, Value);
impl error::Error for ValueTypeMismatch {}
impl std::fmt::Display for ValueTypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value type mismatch for entity {} field {}: expected {:?}, got {:?}",
            self.0, self.1, self.2, self.3
        )
    }
}

#[derive(Debug, Clone)]
pub enum BadIndirectionReason {
    NegativeIndex(i64),
    ArrayIndexOutOfBounds(usize, usize),
    EmptyEntityReference,
    InvalidEntityId(EntityId),
    UnexpectedValueType(FieldType, String),
    ExpectedIndexAfterEntityList(FieldType),
    FailedToResolveField(FieldType, String),
}

#[derive(Debug, Clone)]
pub struct BadIndirection {
    entity_id: EntityId,
    field_type: FieldType,
    reason: BadIndirectionReason,
}

impl BadIndirection {
    pub fn new(entity_id: EntityId, field_type: FieldType, reason: BadIndirectionReason) -> Self {
        BadIndirection {
            entity_id,
            field_type,
            reason,
        }
    }
}

impl error::Error for BadIndirection {}

impl std::fmt::Display for BadIndirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bad indirection for entity {}, field {}: ",
            self.entity_id, self.field_type
        )?;
        match &self.reason {
            BadIndirectionReason::NegativeIndex(index) => {
                write!(f, "negative index: {}", index)
            }
            BadIndirectionReason::ArrayIndexOutOfBounds(index, size) => {
                write!(f, "array index out of bounds: {} >= {}", index, size)
            }
            BadIndirectionReason::EmptyEntityReference => {
                write!(f, "empty entity reference")
            }
            BadIndirectionReason::InvalidEntityId(id) => {
                write!(f, "invalid entity id: {}", id)
            }
            BadIndirectionReason::UnexpectedValueType(field, value) => {
                write!(f, "unexpected value type for field {}: {}", field, value)
            }
            BadIndirectionReason::ExpectedIndexAfterEntityList(field) => {
                write!(f, "expected index after EntityList, got: {}", field)
            }
            BadIndirectionReason::FailedToResolveField(field, error) => {
                write!(f, "failed to resolve field {}: {}", field, error)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {}

/// Pagination options for retrieving lists of items
#[derive(Debug, Clone)]
pub struct PageOpts {
    /// The maximum number of items to return
    pub limit: usize,
    /// The starting point for pagination
    pub cursor: Option<String>,
}

impl Default for PageOpts {
    fn default() -> Self {
        PageOpts {
            limit: 100,
            cursor: None,
        }
    }
}

impl PageOpts {
    pub fn new(limit: usize, cursor: Option<String>) -> Self {
        PageOpts { limit, cursor }
    }
}

/// Result of a paginated query
#[derive(Debug, Clone)]
pub struct PageResult<T> {
    /// The items returned in this page
    pub items: Vec<T>,
    /// The total number of items available
    pub total: usize,
    /// Cursor for retrieving the next page, if available
    pub next_cursor: Option<String>,
}

impl<T> PageResult<T> {
    pub fn new(items: Vec<T>, total: usize, next_cursor: Option<String>) -> Self {
        PageResult {
            items,
            total,
            next_cursor,
        }
    }
}

pub struct MapStore {
    schema: HashMap<EntityType, EntitySchema>,
    entity: HashMap<EntityType, Vec<EntityId>>,
    types: Vec<EntityType>,
    field: HashMap<EntityId, HashMap<FieldType, Field>>,
    snowflake: Snowflake,
}

impl MapStore {
    pub fn new() -> Self {
        MapStore {
            schema: HashMap::new(),
            entity: HashMap::new(),
            types: Vec::new(),
            field: HashMap::new(),
            snowflake: Snowflake::new(),
        }
    }

    pub async fn create_entity(
        &mut self,
        ctx: &Context,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        if !self.schema.contains_key(&entity_type) {
            return Err(EntityTypeNotFound(entity_type.clone()).into());
        }

        if let Some(parent) = &parent_id {
            if !self.entity_exists(ctx, &parent).await {
                return Err(EntityNotFound(parent.clone()).into());
            }
        }

        let entity_id = EntityId::new(&entity_type, self.snowflake.generate());
        if self.field.contains_key(&entity_id) {
            return Err(EntityExists(entity_id).into());
        }

        {
            let entities = self
                .entity
                .entry(entity_type.clone())
                .or_insert_with(Vec::new);
            entities.push(entity_id.clone());
        }

        {
            self.field
                .entry(entity_id.clone())
                .or_insert_with(HashMap::new);
        }

        {
            let mut writes = self
                .schema
                .get(&entity_type)
                .map(|s| &s.fields)
                .into_iter()
                .flat_map(|fields| fields.iter())
                .map(|(field_type, _)| match field_type.as_str() {
                    "Name" => {
                        swrite!(entity_id, field_type, sstr!(name))
                    }
                    "Parent" => match &parent_id {
                        Some(parent) => swrite!(entity_id, field_type, sref!(Some(parent.clone()))),
                        None => swrite!(entity_id, field_type),
                    },
                    _ => {
                        swrite!(entity_id, field_type)
                    }
                })
                .collect::<Vec<Request>>();

            if let Some(parent) = &parent_id {
                let mut children = Field::new(parent, "Children");
                self.perform(ctx, &mut vec![children.read_request().await])
                    .await?;

                children.get_value().await.and_then(|value| {
                    if let Value::EntityList(mut entities) = value {
                        entities.push(entity_id.clone());

                        writes.push(swrite!(
                            parent,
                            "Children",
                            Some(Value::EntityList(entities))
                        ));
                    }

                    Some(())
                });
            }

            self.perform(ctx, &mut writes).await?;
        }

        Ok(Entity::new(entity_id))
    }

    pub async fn get_entity_schema(
        &self,
        _: &Context,
        entity_type: &EntityType,
    ) -> Result<&EntitySchema> {
        self.schema
            .get(entity_type)
            .ok_or_else(|| EntityTypeNotFound(entity_type.clone()).into())
    }

    /// Set or update the schema for an entity type
    pub async fn set_entity_schema(
        &mut self,
        _: &Context,
        entity_schema: &EntitySchema,
    ) -> Result<()> {
        let entity_type = entity_schema.entity_type.clone();
        self.schema.insert(entity_type.clone(), entity_schema.clone());
        
        // Make sure the entity type is tracked in our types list
        if !self.entity.contains_key(&entity_type) {
            self.entity.insert(entity_type.clone(), Vec::new());
        }
        
        // Update the types list if needed
        if !self.types.contains(&entity_type) {
            self.types.push(entity_type);
        }
        
        Ok(())
    }

    /// Get the schema for a specific field
    pub async fn get_field_schema(
        &self,
        _: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<&FieldSchema> {
        // First get the entity schema
        let entity_schema = self.get_entity_schema(&Context {}, entity_type).await?;
        
        // Then get the field schema
        entity_schema
            .fields
            .get(field_type)
            .ok_or_else(|| FieldNotFound(EntityId::new(entity_type, 0), field_type.clone()).into())
    }

    /// Set or update the schema for a specific field
    pub async fn set_field_schema(
        &mut self,
        _: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: &FieldSchema,
    ) -> Result<()> {
        // Make sure the entity type exists
        if !self.schema.contains_key(entity_type) {
            // Create a new entity schema
            let entity_schema = EntitySchema::new(entity_type.clone());
            self.schema.insert(entity_type.clone(), entity_schema);
            
            // Make sure the entity type is tracked
            if !self.entity.contains_key(entity_type) {
                self.entity.insert(entity_type.clone(), Vec::new());
            }
            
            // Update types list if needed
            if !self.types.contains(entity_type) {
                self.types.push(entity_type.clone());
            }
        }
        
        // Get the entity schema and update it
        let entity_schema = self.schema.get_mut(entity_type).unwrap();
        entity_schema.fields.insert(field_type.clone(), field_schema.clone());
        
        Ok(())
    }

    pub async fn entity_exists(&self, _: &Context, entity_id: &EntityId) -> bool {
        self.field.contains_key(entity_id)
    }

    pub async fn field_exists(
        &self,
        _: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        self.schema
            .get(entity_type)
            .map(|schema| schema.fields.contains_key(field_type))
            .unwrap_or(false)
    }

    pub async fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
        for request in requests {
            match request {
                Request::Read {
                    entity_id,
                    field_type,
                    value,
                    write_time,
                    writer_id,
                } => {
                    let indir = Box::pin(resolve_indirection(self, entity_id, field_type)).await?;
                    self.read(
                        ctx, 
                        &indir.0, 
                        &indir.1,
                        value,
                        write_time,
                        writer_id
                    ).await?;
                }
                Request::Write {
                    entity_id,
                    field_type,
                    value,
                    write_time,
                    writer_id,
                    write_option,
                } => {
                    let indir = Box::pin(resolve_indirection(self, entity_id, field_type)).await?;
                    self.write(
                        ctx,
                        &indir.0,
                        &indir.1,
                        value,
                        write_time,
                        writer_id,
                        write_option,
                    ).await?;
                }
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        _: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Shared<Option<Value>>,
        write_time: &Shared<Option<Timestamp>>,
        writer_id: &Shared<Option<EntityId>>,
    ) -> Result<()> {
        let field = self
            .field
            .get(entity_id)
            .and_then(|fields| fields.get(field_type));

        if let Some(field) = field {
            value.set(field.get_value().await).await;
            write_time.set(field.get_write_time().await).await;
            writer_id.set(field.get_writer_id().await).await;
        } else {
            return Err(FieldNotFound(entity_id.clone(), field_type.clone()).into());
        }

        Ok(())
    }

    async fn write(
        &mut self,
        _: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Option<Value>,
        write_time: &Option<Timestamp>,
        writer_id: &Option<EntityId>,
        write_option: &WriteOption,
    ) -> Result<()> {
        let field_schema = self
            .schema
            .get(entity_id.get_type())
            .and_then(|schema| schema.fields.get(field_type))
            .ok_or_else(|| FieldNotFound(entity_id.clone(), field_type.clone()))?;

        let fields = self
            .field
            .entry(entity_id.clone())
            .or_insert_with(HashMap::new);

        let field = fields
            .entry(field_type.clone())
            .or_insert_with(|| Field::new(entity_id, field_type));

        let mut new_value: Option<Value> = value.clone();
        // Check that the value being written is the same type as the field schema
        // If the value is None, use the default value from the schema
        if let Some(value) = value {
            if discriminant(value) != discriminant(&field_schema.default_value) {
                return Err(ValueTypeMismatch(
                    entity_id.clone(),
                    field_type.clone(),
                    field_schema.default_value.clone(),
                    value.clone(),
                )
                .into());
            }
        } else {
            new_value = Some(field_schema.default_value.clone().into());
        }

        match write_option {
            WriteOption::Normal => {
                // Normal write, overwrite existing value
                field.set_value(new_value).await;
                if let Some(write_time) = write_time {
                    field.set_write_time(Some(write_time.clone())).await;
                } else {
                    field.set_write_time(Some(now())).await;
                }
                if let Some(writer_id) = writer_id {
                    field.set_writer_id(Some(writer_id.clone())).await;
                } else {
                    field.set_writer_id(None).await;
                }
            }
            WriteOption::Changes => {
                // Changes write, only update if the value is different
                if field.get_value().await != new_value {
                    field.set_value(new_value).await;
                    if let Some(write_time) = write_time {
                        field.set_write_time(Some(write_time.clone())).await;
                    } else {
                        field.set_write_time(Some(now())).await;
                    }
                    if let Some(writer_id) = writer_id {
                        field.set_writer_id(Some(writer_id.clone())).await;
                    } else {
                        field.set_writer_id(None).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Deletes an entity and all its fields
    /// Returns an error if the entity doesn't exist
    pub async fn delete_entity(&mut self, _: &Context, entity_id: &EntityId) -> Result<()> {
        // Check if the entity exists
        if !self.field.contains_key(entity_id) {
            return Err(EntityNotFound(entity_id.clone()).into());
        }

        // Remove fields
        self.field.remove(entity_id);

        // Remove from entity type list
        if let Some(entities) = self.entity.get_mut(entity_id.get_type()) {
            entities.retain(|id| id != entity_id);
        }

        Ok(())
    }

    /// Find entities of a specific type with pagination
    pub async fn find_entities(
        &self,
        _: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.unwrap_or_default();
        
        // Check if entity type exists
        if !self.entity.contains_key(entity_type) {
            return Ok(PageResult {
                items: Vec::new(),
                total: 0,
                next_cursor: None,
            });
        }

        let all_entities = self.entity.get(entity_type).unwrap();
        let total = all_entities.len();

        // Find the starting index based on cursor
        let start_idx = if let Some(cursor) = &opts.cursor {
            match cursor.parse::<usize>() {
                Ok(idx) => idx,
                Err(_) => 0,
            }
        } else {
            0
        };

        // Get the slice of entities for this page
        let end_idx = std::cmp::min(start_idx + opts.limit, total);
        let items: Vec<EntityId> = if start_idx < total {
            all_entities[start_idx..end_idx].to_vec()
        } else {
            Vec::new()
        };

        // Calculate the next cursor
        let next_cursor = if end_idx < total {
            Some(end_idx.to_string())
        } else {
            None
        };

        Ok(PageResult {
            items,
            total,
            next_cursor,
        })
    }

    /// Get all entity types with pagination
    pub async fn get_entity_types(
        &self,
        _: &Context,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let opts = page_opts.unwrap_or_default();
        
        // Collect all types from schema
        let all_types: Vec<EntityType> = self.schema.keys().cloned().collect();
        let total = all_types.len();

        // Find the starting index based on cursor
        let start_idx = if let Some(cursor) = &opts.cursor {
            match cursor.parse::<usize>() {
                Ok(idx) => idx,
                Err(_) => 0,
            }
        } else {
            0
        };

        // Get the slice of types for this page
        let end_idx = std::cmp::min(start_idx + opts.limit, total);
        let items: Vec<EntityType> = if start_idx < total {
            all_types[start_idx..end_idx].to_vec()
        } else {
            Vec::new()
        };

        // Calculate the next cursor
        let next_cursor = if end_idx < total {
            Some(end_idx.to_string())
        } else {
            None
        };

        Ok(PageResult {
            items,
            total,
            next_cursor,
        })
    }
}

pub async fn resolve_indirection(
    store: &mut MapStore,
    entity_id: &EntityId,
    field_type: &FieldType,
) -> Result<(EntityId, FieldType)> {
    let fields = field_type
        .split(INDIRECTION_DELIMITER)
        .collect::<Vec<&str>>();

    if fields.len() == 1 {
        return Ok((entity_id.clone(), field_type.clone()));
    }

    let mut current_entity_id = entity_id.clone();

    for i in 0..fields.len() - 1 {
        let field = fields[i];

        // Handle array index navigation (for EntityList fields)
        if i > 0 && field.parse::<i64>().is_ok() {
            let index = field.parse::<i64>().unwrap();
            if index < 0 {
                return Err(BadIndirection::new(
                    current_entity_id.clone(),
                    field_type.clone(),
                    BadIndirectionReason::NegativeIndex(index),
                )
                .into());
            }

            // The previous field should have been an EntityList
            let prev_field = fields[i - 1];

            let mut request = vec![sread!(current_entity_id, prev_field)];

            let context = &Context {};
            store.perform(&context, &mut request).await?;

            if let Request::Read { value, .. } = &request[0] {
                let value_lock = value.get().await;

                if let Some(Value::EntityList(entities)) = &*value_lock {
                    let index_usize = index as usize;
                    if index_usize >= entities.len() {
                        return Err(BadIndirection::new(
                            current_entity_id.clone(),
                            field_type.clone(),
                            BadIndirectionReason::ArrayIndexOutOfBounds(
                                index_usize,
                                entities.len(),
                            ),
                        )
                        .into());
                    }

                    current_entity_id = entities[index_usize].clone();
                } else {
                    return Err(BadIndirection::new(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::UnexpectedValueType(
                            prev_field.to_string(),
                            format!("{:?}", value_lock),
                        ),
                    )
                    .into());
                }
            }

            continue;
        }

        // Normal field resolution
        let mut request = vec![
            sread!(current_entity_id, field),
        ];

        let context = &Context {};
        if let Err(e) = store.perform(&context, &mut request).await {
            return Err(BadIndirection::new(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::FailedToResolveField(field.to_string(), e.to_string()),
            )
            .into());
        }

        if let Request::Read { value, .. } = &request[0] {
            let value_lock = value.get().await;

            if let Some(Value::EntityReference(reference)) = &*value_lock {
                if reference.is_none() {
                    return Err(BadIndirection::new(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::EmptyEntityReference,
                    )
                    .into());
                }

                continue;
            }

            if let Some(Value::EntityList(_)) = &*value_lock {
                // If next segment is not an index, this is an error
                if i + 1 >= fields.len() - 1 || fields[i + 1].parse::<i64>().is_err() {
                    return Err(BadIndirection::new(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::ExpectedIndexAfterEntityList(
                            fields[i + 1].to_string(),
                        ),
                    )
                    .into());
                }
                // The index will be processed in the next iteration
                continue;
            }

            return Err(BadIndirection::new(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::UnexpectedValueType(
                    field.to_string(),
                    format!("{:?}", value_lock),
                ),
            )
            .into());
        }
    }

    Ok((current_entity_id, fields[fields.len() - 1].to_string()))
}
