use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error, mem::discriminant, sync::Arc};

use crate::{
    data::{
        entity_schema::Complete, now, request::PushCondition, EntityType, FieldType, Timestamp
    },
    sadd, sread, sref, sreflist, sstr, ssub, swrite, AdjustBehavior, Entity, EntityId,
    EntitySchema, Field, FieldSchema, Request, Result, Single, Snowflake, Value,
};

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
pub struct UnsupportAdjustBehavior(EntityId, FieldType, AdjustBehavior);
impl error::Error for UnsupportAdjustBehavior {}
impl std::fmt::Display for UnsupportAdjustBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Unsupported adjust behavior for entity {} field {}: {:?}",
            self.0, self.1, self.2
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

/// Represents a complete snapshot of the store at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    schemas: HashMap<EntityType, EntitySchema<Single>>,
    entities: HashMap<EntityType, Vec<EntityId>>,
    types: Vec<EntityType>,
    fields: HashMap<EntityId, HashMap<FieldType, Field>>,
}

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

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Store {
    schemas: HashMap<EntityType, EntitySchema<Single>>,
    entities: HashMap<EntityType, Vec<EntityId>>,
    types: Vec<EntityType>,
    fields: HashMap<EntityId, HashMap<FieldType, Field>>,
    
    /// Maps parent types to all their derived types (including direct and indirect children)
    /// This allows fast lookup of all entity types that inherit from a given parent type
    #[serde(skip)]
    inheritance_map: HashMap<EntityType, Vec<EntityType>>,

    #[serde(skip)]
    snowflake: Arc<Snowflake>,
}

impl Store {
    pub fn new(snowflake: Arc<Snowflake>) -> Self {
        Store {
            schemas: HashMap::new(),
            entities: HashMap::new(),
            types: Vec::new(),
            fields: HashMap::new(),
            inheritance_map: HashMap::new(),
            snowflake,
        }
    }

    pub fn create_entity(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        if !self.schemas.contains_key(&entity_type) {
            return Err(EntityTypeNotFound(entity_type.clone()).into());
        }

        if let Some(parent) = &parent_id {
            if !self.entity_exists(ctx, &parent) {
                return Err(EntityNotFound(parent.clone()).into());
            }
        }

        let entity_id = EntityId::new(entity_type.clone(), self.snowflake.generate());
        if self.fields.contains_key(&entity_id) {
            return Err(EntityExists(entity_id).into());
        }

        {
            let entities = self
                .entities
                .entry(entity_type.clone())
                .or_insert_with(Vec::new);
            entities.push(entity_id.clone());
        }

        {
            self.fields
                .entry(entity_id.clone())
                .or_insert_with(HashMap::new);
        }

        {
            let mut writes = self
                .schemas
                .get(entity_type)
                .map(|s| &s.fields)
                .into_iter()
                .flat_map(|fields| fields.iter())
                .map(|(field_type, _)| match field_type.as_ref() {
                    "Name" => {
                        swrite!(entity_id.clone(), field_type.clone(), sstr!(name))
                    }
                    "Parent" => match &parent_id {
                        Some(parent) => swrite!(
                            entity_id.clone(),
                            field_type.clone(),
                            sref!(Some(parent.clone()))
                        ),
                        None => swrite!(entity_id.clone(), field_type.clone()),
                    },
                    _ => {
                        // Write the field with its default value
                        swrite!(entity_id.clone(), field_type.clone())
                    }
                })
                .collect::<Vec<Request>>();

            // If we have a parent, add it to the parent's children list
            if let Some(parent) = &parent_id {
                writes.push(sadd!(
                    parent.clone(),
                    "Children".into(),
                    sreflist![entity_id.clone()]
                ));
            }

            self.perform(ctx, &mut writes)?;
        }

        Ok(Entity::new(entity_id))
    }

    pub fn get_entity_schema(
        &self,
        _: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        self.schemas
            .get(entity_type)
            .cloned()
            .ok_or_else(|| EntityTypeNotFound(entity_type.clone()).into())
    }

    pub fn get_complete_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let mut schema = EntitySchema::<Complete>::from(self.get_entity_schema(ctx, entity_type)?);
        let mut visited_types = std::collections::HashSet::new();
        visited_types.insert(entity_type.clone());

        loop {
            if let Some(inherit_type) = &schema.inherit.clone() {
                // Check for circular inheritance
                if visited_types.contains(inherit_type) {
                    // Circular inheritance detected, break the loop
                    schema.inherit = None;
                    break;
                }
                
                if let Some(inherit_schema) = self.schemas.get(inherit_type) {
                    visited_types.insert(inherit_type.clone());
                    
                    // Merge inherited fields into the current schema
                    for (field_type, field_schema) in &inherit_schema.fields {
                        schema
                            .fields
                            .entry(field_type.clone())
                            .or_insert_with(|| field_schema.clone());
                    }
                    // Move up the inheritance chain
                    schema.inherit = inherit_schema.inherit.clone();
                } else {
                    return Err(EntityTypeNotFound(inherit_type.clone()).into());
                }
            } else {
                break;
            }
        }

        Ok(schema)
    }

    /// Set or update the schema for an entity type
    pub fn set_entity_schema(
        &mut self,
        ctx: &Context,
        entity_schema: &EntitySchema<Single>,
    ) -> Result<()> {
        // Get a copy of the existing schema if it exists
        // We'll use this to see if any fields have been added or removed
        let complete_old_schema = self
            .get_complete_entity_schema(ctx, &entity_schema.entity_type)
            .unwrap_or_else(|_| EntitySchema::<Complete>::new(entity_schema.entity_type.clone()));

        self.schemas
            .insert(entity_schema.entity_type.clone(), entity_schema.clone());

        if !self.entities.contains_key(&entity_schema.entity_type) {
            self.entities
                .insert(entity_schema.entity_type.clone(), Vec::new());
        }

        if !self.types.contains(&entity_schema.entity_type) {
            self.types.push(entity_schema.entity_type.clone());
        }

        // Get the complete schema for the entity type
        let complete_new_schema =
            self.get_complete_entity_schema(ctx, &entity_schema.entity_type)?;

        for removed_field in complete_old_schema.diff(&complete_new_schema) {
            // If the field was removed, we need to remove it from all entities
            for entity_id in self
                .entities
                .get(&entity_schema.entity_type)
                .unwrap_or(&Vec::new())
            {
                if let Some(fields) = self.fields.get_mut(entity_id) {
                    fields.remove(&removed_field.field_type());
                }
            }
        }

        for added_field in complete_new_schema.diff(&complete_old_schema) {
            // If the field was added, we need to add it to all entities
            for entity_id in self
                .entities
                .get(&entity_schema.entity_type)
                .unwrap_or(&Vec::new())
            {
                let fields = self
                    .fields
                    .entry(entity_id.clone())
                    .or_insert_with(HashMap::new);
                fields.insert(
                    added_field.field_type().clone(),
                    Field {
                        field_type: added_field.field_type().clone(),
                        value: added_field.default_value(),
                        write_time: now(),
                        writer_id: None,
                    },
                );
            }
        }

        // Rebuild inheritance map after schema changes
        self.rebuild_inheritance_map();

        Ok(())
    }

    /// Get the schema for a specific field
    pub fn get_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        self
            .get_entity_schema(ctx, entity_type)?
            .fields
            .get(field_type)
            .cloned()
            .ok_or_else(|| {
                FieldNotFound(EntityId::new(entity_type.clone(), 0), field_type.clone()).into()
            })
    }

    /// Set or update the schema for a specific field
    pub fn set_field_schema(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self
            .get_entity_schema(ctx, entity_type)?;

        entity_schema.fields.insert(field_type.clone(), field_schema);

        self.set_entity_schema(ctx, &entity_schema)
    }

    pub fn entity_exists(&self, _: &Context, entity_id: &EntityId) -> bool {
        self.fields.contains_key(entity_id)
    }

    pub fn field_exists(
        &self,
        _: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        self.schemas
            .get(entity_type)
            .map(|schema| schema.fields.contains_key(field_type))
            .unwrap_or(false)
    }

    pub fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
        for request in requests.iter_mut() {
            match request {
                Request::Read {
                    entity_id,
                    field_type,
                    value,
                    write_time,
                    writer_id,
                } => {
                    let indir: (EntityId, FieldType) =
                        resolve_indirection(ctx, self, entity_id, field_type)?;
                    self.read(ctx, &indir.0, &indir.1, value, write_time, writer_id)?;
                }
                Request::Write {
                    entity_id,
                    field_type,
                    value,
                    write_time,
                    writer_id,
                    push_condition,
                    adjust_behavior,
                } => {
                    let indir = resolve_indirection(ctx, self, entity_id, field_type)?;
                    self.write(
                        ctx,
                        &indir.0,
                        &indir.1,
                        value,
                        write_time,
                        writer_id,
                        push_condition,
                        adjust_behavior,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn read(
        &self,
        _: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &mut Option<Value>,
        write_time: &mut Option<Timestamp>,
        writer_id: &mut Option<EntityId>,
    ) -> Result<()> {
        let field = self
            .fields
            .get(&entity_id)
            .and_then(|fields| fields.get(field_type));

        if let Some(field) = field {
            *value = Some(field.value.clone());
            *write_time = Some(field.write_time.clone());
            *writer_id = field.writer_id.clone();
        } else {
            return Err(FieldNotFound(entity_id.clone(), field_type.clone()).into());
        }

        Ok(())
    }

    fn write(
        &mut self,
        ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Option<Value>,
        write_time: &Option<Timestamp>,
        writer_id: &Option<EntityId>,
        write_option: &PushCondition,
        adjust_behavior: &AdjustBehavior,
    ) -> Result<()> {
        let entity_schema = self.get_complete_entity_schema(ctx, entity_id.get_type())?;
        let field_schema = entity_schema.fields.get(field_type)
            .ok_or_else(|| FieldNotFound(entity_id.clone(), field_type.clone()))?;

        let fields = self
            .fields
            .entry(entity_id.clone())
            .or_insert_with(HashMap::new);

        let field = fields.entry(field_type.clone()).or_insert_with(|| Field {
            field_type: field_type.clone(),
            value: field_schema.default_value(),
            write_time: now(),
            writer_id: None,
        });

        let mut new_value = field_schema.default_value();
        // Check that the value being written is the same type as the field schema
        // If the value is None, use the default value from the schema
        if let Some(value) = value {
            if discriminant(value) != discriminant(&field_schema.default_value()) {
                return Err(ValueTypeMismatch(
                    entity_id.clone(),
                    field_type.clone(),
                    field_schema.default_value(),
                    value.clone(),
                )
                .into());
            }

            new_value = value.clone();
        }

        let old_value = &field.value;
        match adjust_behavior {
            AdjustBehavior::Add => match old_value {
                Value::Int(old_int) => {
                    new_value = Value::Int(old_int + new_value.as_int().unwrap_or(0));
                }
                Value::Float(old_float) => {
                    new_value = Value::Float(old_float + new_value.as_float().unwrap_or(0.0));
                }
                Value::EntityList(old_list) => {
                    new_value = Value::EntityList(
                        old_list
                            .iter()
                            .chain(new_value.as_entity_list().unwrap_or(&Vec::new()).iter())
                            .cloned()
                            .collect(),
                    );
                }
                Value::String(old_string) => {
                    new_value = Value::String(format!(
                        "{}{}",
                        old_string,
                        new_value.as_string().cloned().unwrap_or_default()
                    ));
                }
                Value::Blob(old_file) => {
                    new_value = Value::Blob(
                        old_file
                            .iter()
                            .chain(
                                new_value
                                    .as_blob()
                                    .map_or(&Vec::new(), |f| &f)
                                    .iter(),
                            )
                            .cloned()
                            .collect(),
                    );
                }
                _ => {
                    return Err(UnsupportAdjustBehavior(
                        entity_id.clone(),
                        field_type.clone(),
                        adjust_behavior.clone(),
                    )
                    .into());
                }
            },
            AdjustBehavior::Subtract => match old_value {
                Value::Int(old_int) => {
                    new_value = Value::Int(old_int - new_value.as_int().unwrap_or(0));
                }
                Value::Float(old_float) => {
                    new_value = Value::Float(old_float - new_value.as_float().unwrap_or(0.0));
                }
                Value::EntityList(old_list) => {
                    let new_list = new_value.as_entity_list().cloned().unwrap_or_default();
                    new_value = Value::EntityList(
                        old_list
                            .into_iter()
                            .filter(|item| !new_list.contains(item))
                            .cloned()
                            .collect(),
                    );
                }
                _ => {
                    return Err(UnsupportAdjustBehavior(
                        entity_id.clone(),
                        field_type.clone(),
                        adjust_behavior.clone(),
                    )
                    .into());
                }
            },
            _ => {
                // No adjustment needed
            }
        }

        match write_option {
            PushCondition::Always => {
                field.value = new_value;

                if let Some(write_time) = write_time {
                    field.write_time = *write_time;
                } else {
                    field.write_time = now();
                }
                if let Some(writer_id) = writer_id {
                    field.writer_id = Some(writer_id.clone());
                } else {
                    field.writer_id = None;
                }
            }
            PushCondition::Changes => {
                // Changes write, only update if the value is different
                if field.value != new_value {
                    field.value = new_value;
                    if let Some(write_time) = write_time {
                        field.write_time = *write_time;
                    } else {
                        field.write_time = now();
                    }
                    if let Some(writer_id) = writer_id {
                        field.writer_id = Some(writer_id.clone());
                    } else {
                        field.writer_id = None;
                    }
                }
            }
        }

        Ok(())
    }

    /// Deletes an entity and all its fields
    /// Returns an error if the entity doesn't exist
    pub fn delete_entity(&mut self, ctx: &Context, entity_id: &EntityId) -> Result<()> {
        // Check if the entity exists
        {
            if !self.fields.contains_key(entity_id) {
                return Err(EntityNotFound(entity_id.clone()).into());
            }
        }

        // Remove all childrens
        {
            let mut reqs = vec![sread!(entity_id.clone(), "Children".into())];
            self.perform(ctx, &mut reqs)?;
            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityList(children)) = value {
                    for child in children {
                        self.delete_entity(ctx, child)?;
                    }
                } else {
                    return Err(BadIndirection::new(
                        entity_id.clone(),
                        "Children".into(),
                        BadIndirectionReason::UnexpectedValueType(
                            "Children".into(),
                            format!("{:?}", value),
                        ),
                    )
                    .into());
                }
            }
        }

        // Remove from parent's children list
        {
            self.perform(
                ctx,
                &mut vec![ssub!(
                    entity_id.clone(),
                    "Parent->Children".into(),
                    sreflist![entity_id.clone()]
                )],
            )?;
        }

        // Remove fields
        {
            self.fields.remove(entity_id);
        }

        // Remove from entity type list
        {
            if let Some(entities) = self.entities.get_mut(entity_id.get_type()) {
                entities.retain(|id| id != entity_id);
            }
        }

        Ok(())
    }

    /// Find entities of a specific type with pagination
    /// 
    /// This method supports inheritance - when searching for a parent type,
    /// it will include entities of all derived types as well.
    /// 
    /// For example, if you have the hierarchy:
    /// - Animal (base type)
    ///   - Mammal (inherits from Animal)
    ///     - Dog (inherits from Mammal)
    ///     - Cat (inherits from Mammal)
    /// 
    /// Then calling `find_entities` with `EntityType::from("Animal")` will return
    /// all Dog, Cat, Mammal, and Animal entities.
    /// 
    /// Calling `find_entities` with `EntityType::from("Mammal")` will return
    /// all Dog, Cat, and Mammal entities (but not Animal entities).
    /// 
    /// If you need to find entities of an exact type without inheritance,
    /// use `find_entities_exact` instead.
    pub fn find_entities(
        &self,
        _: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.unwrap_or_default();

        // Get all entity types that match the requested type (including derived types)
        let types_to_search = self.inheritance_map
            .get(entity_type)
            .cloned()
            .unwrap_or_else(|| {
                // If not in inheritance map, just check the exact type
                if self.entities.contains_key(entity_type) {
                    vec![entity_type.clone()]
                } else {
                    Vec::new()
                }
            });

        // Collect all entities from all matching types
        let mut all_entities = Vec::new();
        for et in &types_to_search {
            if let Some(entities) = self.entities.get(et) {
                all_entities.extend(entities.iter().cloned());
            }
        }

        let total = all_entities.len();

        if total == 0 {
            return Ok(PageResult {
                items: Vec::new(),
                total: 0,
                next_cursor: None,
            });
        }

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

    /// Find entities of exactly the specified type (no inheritance)
    /// 
    /// This method only returns entities of the exact type, not derived types.
    /// This is useful when you need to distinguish between parent and child
    /// types in an inheritance hierarchy.
    /// 
    /// For example, if you have the hierarchy:
    /// - Animal (base type)
    ///   - Dog (inherits from Animal)
    /// 
    /// Then calling `find_entities_exact` with `EntityType::from("Animal")` will
    /// only return entities that were created with the "Animal" type, not Dog entities.
    pub fn find_entities_exact(
        &self,
        _: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.unwrap_or_default();

        // Check if entity type exists
        if !self.entities.contains_key(entity_type) {
            return Ok(PageResult {
                items: Vec::new(),
                total: 0,
                next_cursor: None,
            });
        }

        let all_entities = self.entities.get(entity_type).unwrap();
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
    pub fn get_entity_types(
        &self,
        _: &Context,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let opts = page_opts.unwrap_or_default();

        // Collect all types from schema
        let all_types: Vec<EntityType> = self.schemas.keys().cloned().collect();
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

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self, _: &Context) -> Snapshot {
        Snapshot {
            schemas: self.schemas.clone(),
            entities: self.entities.clone(),
            types: self.types.clone(),
            fields: self.fields.clone(),
        }
    }

    /// Restore the store state from a snapshot
    pub fn restore_snapshot(&mut self, _: &Context, snapshot: Snapshot) {
        self.schemas = snapshot.schemas;
        self.entities = snapshot.entities;
        self.types = snapshot.types;
        self.fields = snapshot.fields;
    }

    /// Rebuild the inheritance map for fast lookup of derived types
    /// This should be called whenever schemas are added or updated
    fn rebuild_inheritance_map(&mut self) {
        self.inheritance_map.clear();
        
        // For each entity type, find all types that inherit from it
        for entity_type in &self.types {
            let mut derived_types = Vec::new();
            
            // Check all other types to see if they inherit from this type
            for other_type in &self.types {
                if self.inherits_from(other_type, entity_type) {
                    derived_types.push(other_type.clone());
                }
            }
            
            // Include the type itself in the list
            derived_types.push(entity_type.clone());
            
            self.inheritance_map.insert(entity_type.clone(), derived_types);
        }
    }
    
    /// Check if derived_type inherits from base_type (directly or indirectly)
    /// This method guards against circular inheritance by limiting the depth of inheritance traversal
    fn inherits_from(&self, derived_type: &EntityType, base_type: &EntityType) -> bool {
        if derived_type == base_type {
            return false; // A type doesn't inherit from itself
        }
        
        let mut current_type = derived_type;
        let mut depth = 0;
        const MAX_INHERITANCE_DEPTH: usize = 100; // Prevent infinite loops
        
        while depth < MAX_INHERITANCE_DEPTH {
            if let Some(schema) = self.schemas.get(current_type) {
                if let Some(inherit_type) = &schema.inherit {
                    if inherit_type == base_type {
                        return true;
                    }
                    current_type = inherit_type;
                    depth += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        
        false
    }
}

pub fn resolve_indirection(
    ctx: &Context,
    store: &mut Store,
    entity_id: &EntityId,
    field_type: &FieldType,
) -> Result<(EntityId, FieldType)> {
    let fields = field_type.indirect_fields();

    if fields.len() == 1 {
        return Ok((entity_id.clone(), field_type.clone()));
    }

    let mut current_entity_id = entity_id.clone();

    for i in 0..fields.len() - 1 {
        let field = &fields[i];

        // Handle array index navigation (for EntityList fields)
        if i > 0 && field.0.parse::<i64>().is_ok() {
            let index = field.0.parse::<i64>().unwrap();
            if index < 0 {
                return Err(BadIndirection::new(
                    current_entity_id.clone(),
                    field_type.clone(),
                    BadIndirectionReason::NegativeIndex(index),
                )
                .into());
            }

            // The previous field should have been an EntityList
            let prev_field = &fields[i - 1];

            let mut reqs = vec![sread!(current_entity_id.clone(), prev_field.clone())];
            store.perform(ctx, &mut reqs)?;

            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityList(entities)) = value {
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
                            prev_field.clone(),
                            format!("{:?}", value),
                        ),
                    )
                    .into());
                }
            }

            continue;
        }

        // Normal field resolution
        let mut reqs = vec![sread!(current_entity_id.clone(), field.clone())];

        if let Err(e) = store.perform(ctx, &mut reqs) {
            return Err(BadIndirection::new(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
            )
            .into());
        }

        if let Request::Read { value, .. } = &reqs[0] {
            if let Some(Value::EntityReference(reference)) = value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid
                        if !store.entity_exists(ctx, ref_id) {
                            return Err(BadIndirection::new(
                                current_entity_id.clone(),
                                field_type.clone(),
                                BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            )
                            .into());
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(BadIndirection::new(
                            current_entity_id.clone(),
                            field_type.clone(),
                            BadIndirectionReason::EmptyEntityReference,
                        )
                        .into());
                    }
                }

                continue;
            }

            if let Some(Value::EntityList(_)) = value {
                // If next segment is not an index, this is an error
                if i + 1 >= fields.len() - 1 || fields[i + 1].0.parse::<i64>().is_err() {
                    return Err(BadIndirection::new(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::ExpectedIndexAfterEntityList(fields[i + 1].clone()),
                    )
                    .into());
                }
                // The index will be processed in the next iteration
                continue;
            }

            return Err(BadIndirection::new(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::UnexpectedValueType(field.clone(), format!("{:?}", value)),
            )
            .into());
        }
    }

    Ok((current_entity_id, fields.last().cloned().ok_or_else(|| {
        BadIndirection::new(
            entity_id.clone(),
            field_type.clone(),
            BadIndirectionReason::UnexpectedValueType(
                "".into(),
                "Empty field path".to_string(),
            ),
        )
    })?))
}
