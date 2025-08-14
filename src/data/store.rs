use serde::{Deserialize, Serialize};
use std::{collections::HashMap, mem::discriminant, sync::Arc};

use crate::{
    data::{
        entity_schema::Complete, now, request::PushCondition, EntityType, FieldType, Notification,
        NotifyConfig, NotificationSender, hash_notify_config, Timestamp, INDIRECTION_DELIMITER,
    }, resolve_indirection, sadd, sread, sref, sreflist, sstr, ssub, swrite, AdjustBehavior, BadIndirectionReason, Context, Entity, EntityId, EntitySchema, Error, Field, FieldSchema, PageOpts, PageResult, Request, Result, Single, Snapshot, Snowflake, Value
};

#[derive(Serialize, Deserialize, Default)]
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

    /// Notification senders indexed by entity ID and field type
    /// Each config can have multiple senders
    #[serde(skip)]
    id_notifications:
        HashMap<EntityId, HashMap<FieldType, HashMap<NotifyConfig, Vec<NotificationSender>>>>,

    /// Notification senders indexed by entity type and field type
    /// Each config can have multiple senders
    #[serde(skip)]
    type_notifications:
        HashMap<EntityType, HashMap<FieldType, HashMap<NotifyConfig, Vec<NotificationSender>>>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("schemas", &self.schemas)
            .field("entities", &self.entities)
            .field("types", &self.types)
            .field("fields", &self.fields)
            .field("inheritance_map", &self.inheritance_map)
            .field(
                "entity_notifications",
                &format_args!("{} entity notifications", self.id_notifications.len()),
            )
            .field(
                "type_notifications",
                &format_args!("{} type notifications", self.type_notifications.len()),
            )
            .finish()
    }
}

impl Store {
    pub async fn create_entity(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        if !self.schemas.contains_key(&entity_type) {
            return Err(Error::EntityTypeNotFound(entity_type.clone()));
        }

        if let Some(parent) = &parent_id {
            if !self.entity_exists(ctx, &parent).await {
                return Err(Error::EntityNotFound(parent.clone()));
            }
        }

        let entity_id = EntityId::new(entity_type.clone(), self.snowflake.generate());
        if self.fields.contains_key(&entity_id) {
            return Err(Error::EntityAlreadyExists(entity_id));
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
            let complete_schema = self.get_complete_entity_schema(ctx, entity_type).await?;
            let mut writes = complete_schema
                .fields
                .iter()
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

            self.perform(ctx, &mut writes).await?;
        }

        Ok(Entity::new(entity_id))
    }

    pub async fn get_entity_schema(
        &self,
        _: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        self.schemas
            .get(entity_type)
            .cloned()
            .ok_or_else(|| Error::EntityTypeNotFound(entity_type.clone()))
    }

    pub async fn get_complete_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let mut schema = EntitySchema::<Complete>::from(self.get_entity_schema(ctx, entity_type).await?);
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
                    return Err(Error::EntityTypeNotFound(inherit_type.clone()));
                }
            } else {
                break;
            }
        }

        Ok(schema)
    }

    /// Set or update the schema for an entity type
    pub async fn set_entity_schema(
        &mut self,
        ctx: &Context,
        entity_schema: &EntitySchema<Single>,
    ) -> Result<()> {
        // Get a copy of the existing schema if it exists
        // We'll use this to see if any fields have been added or removed
        let complete_old_schema = self
            .get_complete_entity_schema(ctx, &entity_schema.entity_type)
            .await
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
            self.get_complete_entity_schema(ctx, &entity_schema.entity_type)
            .await?;

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
    pub async fn get_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        self.get_entity_schema(ctx, entity_type)
            .await?
            .fields
            .get(field_type)
            .cloned()
            .ok_or_else(|| {
                Error::FieldNotFound(EntityId::new(entity_type.clone(), 0), field_type.clone())
            })
    }

    /// Set or update the schema for a specific field
    pub async fn set_field_schema(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(ctx, entity_type).await?;

        entity_schema
            .fields
            .insert(field_type.clone(), field_schema);

        self.set_entity_schema(ctx, &entity_schema).await
    }

    pub async fn entity_exists(&self, _: &Context, entity_id: &EntityId) -> bool {
        self.fields.contains_key(entity_id)
    }

    pub async fn field_exists(
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

    pub async fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
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
                        Box::pin(resolve_indirection(ctx, &mut store_type, entity_id, field_type)).await?;
                    self.read(ctx, &indir.0, &indir.1, value, write_time, writer_id).await?;
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
                    let indir = Box::pin(resolve_indirection(ctx, &mut store_type, entity_id, field_type)).await?;
                    self.write(
                        ctx,
                        &indir.0,
                        &indir.1,
                        value,
                        write_time,
                        writer_id,
                        push_condition,
                        adjust_behavior,
                    ).await?;
                }
            }
        }
        Ok(())
    }

    /// Deletes an entity and all its fields
    /// Returns an error if the entity doesn't exist
    pub async fn delete_entity(&mut self, ctx: &Context, entity_id: &EntityId) -> Result<()> {
        // Check if the entity exists
        {
            if !self.fields.contains_key(entity_id) {
                return Err(Error::EntityNotFound(entity_id.clone()));
            }
        }

        // Remove all childrens
        {
            let mut reqs = vec![sread!(entity_id.clone(), "Children".into())];
            self.perform(ctx, &mut reqs).await?;
            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityList(children)) = value {
                    for child in children {
                        Box::pin(self.delete_entity(ctx, child)).await?;
                    }
                } else {
                    return Err(Error::BadIndirection(
                        entity_id.clone(),
                        "Children".into(),
                        BadIndirectionReason::UnexpectedValueType(
                            "Children".into(),
                            format!("{:?}", value),
                        ),
                    ));
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
            ).await?;
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
    pub async fn find_entities_paginated(
        &self,
        _: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.unwrap_or_default();

        // Get all entity types that match the requested type (including derived types)
        let types_to_search = self
            .inheritance_map
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
    pub async fn find_entities_exact(
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

    pub async fn find_entities(
        &self,
        ctx: &Context,
        entity_type: &EntityType
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self.find_entities_paginated(ctx, entity_type, page_opts.clone()).await?;
            if page_result.items.is_empty() {
                break;
            }

            let length = page_result.items.len();
            result.extend(page_result.items);
            if page_result.next_cursor.is_none() {
                break;
            }

            page_opts = Some(PageOpts::new(length, page_result.next_cursor));
        }

        Ok(result)
    }

    /// Get all entity types with pagination
    pub async fn get_entity_types(
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

    /// Register a notification configuration with a provided sender
    /// The sender will be added to the list of senders for this notification config
    /// Returns an error if the field_type contains indirection (context fields can be indirect)
    pub async fn register_notification(
        &mut self,
        _ctx: &Context,
        config: NotifyConfig,
        sender: NotificationSender,
    ) -> Result<()> {
        // Validate that the main field_type is not indirect
        let field_type = match &config {
            NotifyConfig::EntityId { field_type, .. } => field_type,
            NotifyConfig::EntityType { field_type, .. } => field_type,
        };

        if field_type.as_ref().contains(INDIRECTION_DELIMITER) {
            return Err(Error::InvalidNotifyConfig(
                "Cannot register notifications on indirect fields".to_string(),
            ));
        }

        // Add sender to the list for this notification config
        match &config {
            NotifyConfig::EntityId {
                entity_id,
                field_type,
                ..
            } => {
                let senders = self
                    .id_notifications
                    .entry(entity_id.clone())
                    .or_insert_with(HashMap::new)
                    .entry(field_type.clone())
                    .or_insert_with(HashMap::new)
                    .entry(config.clone())
                    .or_insert_with(Vec::new);
                senders.push(sender);
            }
            NotifyConfig::EntityType {
                entity_type,
                field_type,
                ..
            } => {
                let senders = self
                    .type_notifications
                    .entry(EntityType::from(entity_type.clone()))
                    .or_insert_with(HashMap::new)
                    .entry(field_type.clone())
                    .or_insert_with(HashMap::new)
                    .entry(config.clone())
                    .or_insert_with(Vec::new);
                senders.push(sender);
            }
        }

        Ok(())
    }

    /// Unregister a notification by removing a specific sender
    /// Returns true if the sender was found and removed
    pub async fn unregister_notification(&mut self, target_config: &NotifyConfig, target_sender: &NotificationSender) -> bool {
        let mut removed_any = false;
        
        match target_config {
            NotifyConfig::EntityId {
                entity_id,
                field_type,
                ..
            } => {
                if let Some(field_map) = self.id_notifications.get_mut(entity_id) {
                    if let Some(sender_map) = field_map.get_mut(field_type) {
                        if let Some(senders) = sender_map.get_mut(target_config) {
                            // Find and remove the specific sender
                            let original_len = senders.len();
                            senders.retain(|sender| !std::ptr::eq(sender, target_sender));
                            removed_any = senders.len() != original_len;
                            
                            // Clean up empty entries
                            if senders.is_empty() {
                                sender_map.remove(target_config);
                            }
                        }

                        // Clean up empty maps
                        if sender_map.is_empty() {
                            field_map.remove(field_type);
                        }

                        if field_map.is_empty() {
                            self.id_notifications.remove(entity_id);
                        }
                    }
                }
            }
            NotifyConfig::EntityType {
                entity_type,
                field_type,
                ..
            } => {
                let entity_type_key = EntityType::from(entity_type.clone());
                if let Some(field_map) = self.type_notifications.get_mut(&entity_type_key) {
                    if let Some(sender_map) = field_map.get_mut(field_type) {
                        if let Some(senders) = sender_map.get_mut(target_config) {
                            // Find and remove the specific sender
                            let original_len = senders.len();
                            senders.retain(|sender| !std::ptr::eq(sender, target_sender));
                            removed_any = senders.len() != original_len;
                            
                            // Clean up empty entries
                            if senders.is_empty() {
                                sender_map.remove(target_config);
                            }
                        }

                        // Clean up empty maps
                        if sender_map.is_empty() {
                            field_map.remove(field_type);
                        }
                        if field_map.is_empty() {
                            self.type_notifications.remove(&entity_type_key);
                        }
                    }
                }
            }
        }

        removed_any
    }
    
    pub fn new(snowflake: Arc<Snowflake>) -> Self {
        Store {
            schemas: HashMap::new(),
            entities: HashMap::new(),
            types: Vec::new(),
            fields: HashMap::new(),
            inheritance_map: HashMap::new(),
            snowflake,
            id_notifications: HashMap::new(),
            type_notifications: HashMap::new(),
        }
    }

    async fn read(
        &mut self,
        _ctx: &Context,
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
            return Err(Error::FieldNotFound(entity_id.clone(), field_type.clone()).into());
        }

        Ok(())
    }

    async fn write(
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
        let entity_schema = self.get_complete_entity_schema(ctx, entity_id.get_type()).await?;
        let field_schema = entity_schema
            .fields
            .get(field_type)
            .ok_or_else(|| Error::FieldNotFound(entity_id.clone(), field_type.clone()))?;

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

        let old_value = field.value.clone();
        let mut new_value = field_schema.default_value();
        // Check that the value being written is the same type as the field schema
        // If the value is None, use the default value from the schema
        if let Some(value) = value {
            if discriminant(value) != discriminant(&field_schema.default_value()) {
                return Err(Error::ValueTypeMismatch(
                    entity_id.clone(),
                    field_type.clone(),
                    field_schema.default_value(),
                    value.clone(),
                ));
            }

            new_value = value.clone();
        }

        match adjust_behavior {
            AdjustBehavior::Add => match &old_value {
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
                            .chain(new_value.as_blob().map_or(&Vec::new(), |f| &f).iter())
                            .cloned()
                            .collect(),
                    );
                }
                _ => {
                    return Err(Error::UnsupportedAdjustBehavior(
                        entity_id.clone(),
                        field_type.clone(),
                        adjust_behavior.clone(),
                    ));
                }
            },
            AdjustBehavior::Subtract => match &old_value {
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
                            .iter()
                            .filter(|item| !new_list.contains(item))
                            .cloned()
                            .collect(),
                    );
                }
                _ => {
                    return Err(Error::UnsupportedAdjustBehavior(
                        entity_id.clone(),
                        field_type.clone(),
                        adjust_behavior.clone(),
                    ));
                }
            },
            _ => {
                // No adjustment needed
            }
        }

        // Store values for notification before updating the field
        let notification_new_value = new_value.clone();
        let notification_old_value = old_value.clone();

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

        // Trigger notifications after a write operation
        self.trigger_notifications(
            ctx,
            entity_id,
            field_type,
            &notification_new_value,
            &notification_old_value,
        );

        Ok(())
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

            self.inheritance_map
                .insert(entity_type.clone(), derived_types);
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

    /// Get all parent types in the inheritance chain for a given entity type
    /// Returns a vector of parent types from most specific to most general
    /// For example, if Sedan -> Car -> Vehicle -> Object, returns [Car, Vehicle, Object]
    fn get_parent_types(&self, entity_type: &EntityType) -> Vec<EntityType> {
        let mut parent_types = Vec::new();
        let mut current_type = entity_type;
        let mut depth = 0;
        const MAX_INHERITANCE_DEPTH: usize = 100; // Prevent infinite loops

        while depth < MAX_INHERITANCE_DEPTH {
            if let Some(schema) = self.schemas.get(current_type) {
                if let Some(inherit_type) = &schema.inherit {
                    parent_types.push(inherit_type.clone());
                    current_type = inherit_type;
                    depth += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        parent_types
    }

    /// Build context fields using the perform method to handle indirection
    async fn build_context_fields(
        &mut self,
        ctx: &Context,
        entity_id: &EntityId,
        context_fields: &[FieldType],
    ) -> std::collections::BTreeMap<FieldType, Option<Value>> {
        let mut context_map = std::collections::BTreeMap::new();

        for context_field in context_fields {
            // Use perform to handle indirection properly
            let mut requests = vec![sread!(entity_id.clone(), context_field.clone())];

            if let Ok(()) = self.perform(ctx, &mut requests).await {
                if let Request::Read { value, .. } = &requests[0] {
                    context_map.insert(context_field.clone(), value.clone());
                }
            } else {
                context_map.insert(context_field.clone(), None);
            }
        }

        context_map
    }
    /// Trigger notifications for a write operation
    async fn trigger_notifications(
        &mut self,
        ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        current_value: &Value,
        previous_value: &Value,
    ) {
        // Collect notifications that need to be triggered to avoid borrowing conflicts
        let mut notifications_to_trigger = Vec::new();

        // Check entity-specific notifications with O(1) lookup by entity_id and field_type
        if let Some(field_map) = self.id_notifications.get(entity_id) {
            if let Some(sender_map) = field_map.get(field_type) {
                for (config, _) in sender_map {
                    if let NotifyConfig::EntityId {
                        trigger_on_change,
                        context,
                        ..
                    } = config
                    {
                        let should_notify = if *trigger_on_change {
                            current_value != previous_value
                        } else {
                            true // Always trigger on write
                        };

                        if should_notify {
                            notifications_to_trigger.push((config.clone(), context.clone()));
                        }
                    }
                }
            }
        }

        // Check entity type notifications with O(1) lookup by entity_type and field_type
        // Also check parent entity types for inheritance support
        let entity_type = entity_id.get_type();
        let mut types_to_check = vec![entity_type.clone()];
        types_to_check.extend(self.get_parent_types(entity_type));

        for entity_type_to_check in types_to_check {
            if let Some(field_map) = self.type_notifications.get(&entity_type_to_check) {
                if let Some(sender_map) = field_map.get(field_type) {
                for (config, _) in sender_map {
                        if let NotifyConfig::EntityType {
                            trigger_on_change,
                            context,
                            ..
                        } = config
                        {
                            let should_notify = if *trigger_on_change {
                                current_value != previous_value
                            } else {
                                true // Always trigger on write
                            };

                            if should_notify {
                                notifications_to_trigger.push((config.clone(), context.clone()));
                            }
                        }
                    }
                }
            }
        }

        // Now trigger the collected notifications
        for (config, context) in notifications_to_trigger {
            let context_fields = self.build_context_fields(ctx, entity_id, &context).await;
            let config_hash = hash_notify_config(&config);

            let notification = Notification {
                entity_id: entity_id.clone(),
                field_type: field_type.clone(),
                current_value: current_value.clone(),
                previous_value: previous_value.clone(),
                context: context_fields,
                config_hash,
            };

            // Find the senders and send the notification through each channel
            match &config {
                NotifyConfig::EntityId {
                    field_type: config_field_type,
                    ..
                } => {
                    if let Some(field_map) = self.id_notifications.get(entity_id) {
                        if let Some(sender_map) = field_map.get(config_field_type) {
                            if let Some(senders) = sender_map.get(&config) {
                                // Send to all senders for this config
                                for sender in senders {
                                    // Ignore send errors (receiver may have been dropped)
                                    let _ = sender.send(notification.clone());
                                }
                            }
                        }
                    }
                }
                NotifyConfig::EntityType {
                    entity_type: config_entity_type,
                    field_type: config_field_type,
                    ..
                } => {
                    if let Some(field_map) = self.type_notifications.get(config_entity_type) {
                        if let Some(sender_map) = field_map.get(config_field_type) {
                            if let Some(senders) = sender_map.get(&config) {
                                // Send to all senders for this config
                                for sender in senders {
                                    // Ignore send errors (receiver may have been dropped)
                                    let _ = sender.send(notification.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
