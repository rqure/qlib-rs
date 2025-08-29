use serde::{Deserialize, Serialize};
use std::{collections::HashMap, mem::discriminant, sync::Arc};
use async_trait::async_trait;

use crate::{
    data::{
        entity_schema::Complete, hash_notify_config, indirection::resolve_indirection, now, request::PushCondition, EntityType, FieldType, Notification, NotificationSender, NotifyConfig, StoreTrait, Timestamp, INDIRECTION_DELIMITER
    }, sread, AdjustBehavior, Entity, EntityId, EntitySchema, Error, Field, FieldSchema, PageOpts, PageResult, Request, Result, Single, Snapshot, Snowflake, Value
};

#[derive(Serialize, Deserialize)]
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

    #[serde(skip, default = "Store::default_write_channel")]
    pub write_channel: (tokio::sync::mpsc::UnboundedSender<Vec<Request>>, Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<Request>>>>),

    /// Flag to temporarily disable notifications (e.g., during WAL replay)
    #[serde(skip)]
    notifications_disabled: bool,
}

#[derive(Serialize, Deserialize)]
pub struct AsyncStore {
    inner: Store
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
    fn default_write_channel() -> (tokio::sync::mpsc::UnboundedSender<Vec<Request>>, Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<Request>>>>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (sender, Arc::new(tokio::sync::Mutex::new(receiver)))
    }

    /// Internal entity creation that doesn't use perform to avoid recursion
    pub fn create_entity_internal(
        &mut self,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        if !self.schemas.contains_key(&entity_type) {
            return Err(Error::EntityTypeNotFound(entity_type.clone()));
        }

        if let Some(parent) = &parent_id {
            if !self.entity_exists(&parent) {
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

        // Get the schema before accessing fields to avoid borrow issues
        let complete_schema = self.get_complete_entity_schema(entity_type)?;

        {
            let entity_fields = self.fields
                .entry(entity_id.clone())
                .or_insert_with(HashMap::new);
            
            // Directly set fields in the entity's field map
            for (field_type, field_schema) in complete_schema.fields.iter() {
                let value = match field_type.as_ref() {
                    "Name" => Value::String(name.to_string()),
                    "Parent" => match &parent_id {
                        Some(parent) => Value::EntityReference(Some(parent.clone())),
                        None => field_schema.default_value(),
                    },
                    _ => field_schema.default_value(),
                };

                entity_fields.insert(field_type.clone(), Field {
                    field_type: field_type.clone(),
                    value,
                    write_time: now(),
                    writer_id: None,
                });
            }
        }

        // If we have a parent, add it to the parent's children list
        if let Some(parent) = &parent_id {
            if let Some(parent_fields) = self.fields.get_mut(parent) {
                let children_field = parent_fields
                    .entry("Children".into())
                    .or_insert_with(|| Field {
                        field_type: "Children".into(),
                        value: Value::EntityList(Vec::new()),
                        write_time: now(),
                        writer_id: None,
                    });

                if let Value::EntityList(children) = &mut children_field.value {
                    children.push(entity_id.clone());
                    children_field.write_time = now();
                }
            }
        }

        Ok(Entity::new(entity_id))
    }

    pub fn get_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        self.schemas
            .get(entity_type)
            .cloned()
            .ok_or_else(|| Error::EntityTypeNotFound(entity_type.clone()))
    }

    pub fn get_complete_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let mut schema = EntitySchema::<Complete>::from(self.get_entity_schema(entity_type)?);
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

    /// Get the schema for a specific field
    pub fn get_field_schema(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        self.get_entity_schema(entity_type)?
            .fields
            .get(field_type)
            .cloned()
            .ok_or_else(|| {
                Error::FieldNotFound(EntityId::new(entity_type.clone(), 0), field_type.clone())
            })
    }

    /// Set or update the schema for a specific field
    pub fn set_field_schema(
        &mut self,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(entity_type)?;

        entity_schema
            .fields
            .insert(field_type.clone(), field_schema);

        let mut requests = vec![Request::SchemaUpdate { schema: entity_schema, originator: None }];
        self.perform(&mut requests)
    }

    pub fn entity_exists(&self, entity_id: &EntityId) -> bool {
        self.fields.contains_key(entity_id)
    }

    pub fn field_exists(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        self.schemas
            .get(entity_type)
            .map(|schema| schema.fields.contains_key(field_type))
            .unwrap_or(false)
    }

    pub fn perform(&mut self, requests: &mut Vec<Request>) -> Result<()> {
        let mut write_requests = Vec::new();
        
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
                        resolve_indirection(self, entity_id, field_type)?;
                    self.read(&indir.0, &indir.1, value, write_time, writer_id)?;
                }
                Request::Write {
                    entity_id,
                    field_type,
                    value,
                    write_time,
                    writer_id,
                    push_condition,
                    adjust_behavior,
                    ..
                } => {
                    let indir = resolve_indirection(self, entity_id, field_type)?;
                    self.write(
                        &indir.0,
                        &indir.1,
                        value,
                        write_time,
                        writer_id,
                        push_condition,
                        adjust_behavior,
                    )?;

                    write_requests.push(request.clone());
                }
                Request::Create {
                    entity_type,
                    parent_id,
                    name,
                    created_entity_id,
                    ..
                } => {
                    let entity = self.create_entity_internal(entity_type, parent_id.clone(), name)?;
                    *created_entity_id = Some(entity.entity_id);

                    write_requests.push(request.clone());
                }
                Request::Delete {
                    entity_id,
                    ..
                } => {
                    self.delete_entity_internal(entity_id)?;

                    write_requests.push(request.clone());
                }
                Request::SchemaUpdate {
                    schema,
                    ..
                } => {
                    // Get a copy of the existing schema if it exists
                    // We'll use this to see if any fields have been added or removed
                    let complete_old_schema = self
                        .get_complete_entity_schema(&schema.entity_type)
                        .unwrap_or_else(|_| EntitySchema::<Complete>::new(schema.entity_type.clone()));

                    self.schemas
                        .insert(schema.entity_type.clone(), schema.clone());

                    if !self.entities.contains_key(&schema.entity_type) {
                        self.entities
                            .insert(schema.entity_type.clone(), Vec::new());
                    }

                    if !self.types.contains(&schema.entity_type) {
                        self.types.push(schema.entity_type.clone());
                    }

                    // Get the complete schema for the entity type
                    let complete_new_schema =
                        self.get_complete_entity_schema(&schema.entity_type)?;

                    for removed_field in complete_old_schema.diff(&complete_new_schema) {
                        // If the field was removed, we need to remove it from all entities
                        for entity_id in self
                            .entities
                            .get(&schema.entity_type)
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
                            .get(&schema.entity_type)
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

                    write_requests.push(request.clone());
                }
                Request::Snapshot {
                    ..
                } => {
                    // Snapshot requests are mainly for WAL marking purposes
                    // The actual snapshot logic is handled elsewhere
                    // We just log this event and include it in write requests for WAL persistence
                    write_requests.push(request.clone());
                }
            }
        }
        
        // Send all write requests as a batch to maintain atomicity
        if !write_requests.is_empty() {
            let _ = self.write_channel.0.send(write_requests);
        }
        
        Ok(())
    }

    /// Internal entity deletion that doesn't use perform to avoid recursion
    fn delete_entity_internal(&mut self, entity_id: &EntityId) -> Result<()> {
        // Check if the entity exists
        if !self.fields.contains_key(entity_id) {
            return Err(Error::EntityNotFound(entity_id.clone()));
        }

        // Remove all children first (recursively)
        if let Some(entity_fields) = self.fields.get(entity_id) {
            if let Some(children_field) = entity_fields.get(&"Children".into()) {
                if let Value::EntityList(children) = &children_field.value {
                    let children_to_delete = children.clone(); // Clone to avoid borrow issues
                    for child in children_to_delete {
                        self.delete_entity_internal(&child)?;
                    }
                }
            }
        }

        // Remove from parent's children list
        let parent_id = if let Some(entity_fields) = self.fields.get(entity_id) {
            if let Some(parent_field) = entity_fields.get(&"Parent".into()) {
                if let Value::EntityReference(Some(parent_id)) = &parent_field.value {
                    Some(parent_id.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(parent_id) = parent_id {
            if let Some(parent_fields) = self.fields.get_mut(&parent_id) {
                if let Some(children_field) = parent_fields.get_mut(&"Children".into()) {
                    if let Value::EntityList(children) = &mut children_field.value {
                        children.retain(|id| id != entity_id);
                        children_field.write_time = now();
                    }
                }
            }
        }

        // Remove fields
        self.fields.remove(entity_id);

        // Remove from entity type list
        if let Some(entities) = self.entities.get_mut(entity_id.get_type()) {
            entities.retain(|id| id != entity_id);
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
    pub fn find_entities_paginated(
        &self,
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
    pub fn find_entities_exact(
        &self,
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

    pub fn find_entities(
        &self,
        entity_type: &EntityType
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self.find_entities_paginated(entity_type, page_opts.clone())?;
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

    pub fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .get_entity_types_paginated(page_opts)?;
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
    pub fn get_entity_types_paginated(
        &self,
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
    pub fn register_notification(
        &mut self,
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
    pub fn unregister_notification(&mut self, target_config: &NotifyConfig, target_sender: &NotificationSender) -> bool {
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
            write_channel: {
                let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
                (sender, Arc::new(tokio::sync::Mutex::new(receiver)))
            },
            notifications_disabled: false,
        }
    }

    /// Get a reference to the schemas map
    pub fn get_schemas(&self) -> &HashMap<EntityType, EntitySchema<Single>> {
        &self.schemas
    }

    /// Get a reference to the fields map
    pub fn get_fields(&self) -> &HashMap<EntityId, HashMap<FieldType, Field>> {
        &self.fields
    }

    /// Get a reference to the snowflake generator
    pub fn get_snowflake(&self) -> &Arc<Snowflake> {
        &self.snowflake
    }

    /// Get a clone of the write channel receiver for external consumption
    pub fn get_write_channel_receiver(&self) -> Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<Request>>>> {
        self.write_channel.1.clone()
    }

    /// Disable notifications temporarily (e.g., during WAL replay)
    pub fn disable_notifications(&mut self) {
        self.notifications_disabled = true;
    }

    /// Re-enable notifications
    pub fn enable_notifications(&mut self) {
        self.notifications_disabled = false;
    }

    /// Check if notifications are currently disabled
    pub fn are_notifications_disabled(&self) -> bool {
        self.notifications_disabled
    }

    fn read(
        &mut self,
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

    fn write(
        &mut self,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Option<Value>,
        write_time: &Option<Timestamp>,
        writer_id: &Option<EntityId>,
        write_option: &PushCondition,
        adjust_behavior: &AdjustBehavior,
    ) -> Result<()> {
        let entity_schema = self.get_complete_entity_schema( entity_id.get_type())?;
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
                // Only update if the incoming write is newer or if no write_time is specified (local write)
                let incoming_time = write_time.unwrap_or_else(|| now());
                if write_time.is_none() || incoming_time >= field.write_time {
                    field.value = new_value;
                    field.write_time = incoming_time;
                    if let Some(writer_id) = writer_id {
                        field.writer_id = Some(writer_id.clone());
                    } else {
                        field.writer_id = None;
                    }

                    // Trigger notifications after a write operation
                    let current_request = Request::Read {
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: Some(notification_new_value.clone()),
                        write_time: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    };
                    let previous_request = Request::Read {
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: Some(notification_old_value.clone()),
                        write_time: Some(field.write_time), // Use the time before the write
                        writer_id: field.writer_id.clone(),
                    };
                    
                    self.trigger_notifications(
                        entity_id,
                        field_type,
                        current_request,
                        previous_request,
                    );
                } else {
                    // Incoming write is older, ignore it
                    return Ok(());
                }
            }
            PushCondition::Changes => {
                // Changes write, only update if the value is different AND the write is newer
                let incoming_time = write_time.unwrap_or_else(|| now());
                if (write_time.is_none() || incoming_time >= field.write_time) && field.value != new_value {
                    field.value = new_value;
                    field.write_time = incoming_time;
                    if let Some(writer_id) = writer_id {
                        field.writer_id = Some(writer_id.clone());
                    } else {
                        field.writer_id = None;
                    }
                    
                    // Trigger notifications after a write operation
                    let current_request = Request::Read {
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: Some(notification_new_value.clone()),
                        write_time: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    };
                    let previous_request = Request::Read {
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: Some(notification_old_value.clone()),
                        write_time: Some(field.write_time), // Use the time before the write
                        writer_id: field.writer_id.clone(),
                    };
                    
                    self.trigger_notifications(
                        entity_id,
                        field_type,
                        current_request,
                        previous_request,
                    );
                } else if write_time.is_some() && incoming_time < field.write_time {
                    // Incoming write is older, ignore it
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self) -> Snapshot {
        Snapshot {
            schemas: self.schemas.clone(),
            entities: self.entities.clone(),
            types: self.types.clone(),
            fields: self.fields.clone(),
        }
    }

    /// Restore the store state from a snapshot
    pub fn restore_snapshot(&mut self, snapshot: Snapshot) {
        self.schemas = snapshot.schemas;
        self.entities = snapshot.entities;
        self.types = snapshot.types;
        self.fields = snapshot.fields;
        // Rebuild inheritance map after restoring
        self.rebuild_inheritance_map();
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
    fn build_context_fields(
        &mut self,
        entity_id: &EntityId,
        context_fields: &[FieldType],
    ) -> std::collections::BTreeMap<FieldType, Request> {
        let mut context_map = std::collections::BTreeMap::new();

        for context_field in context_fields {
            // Use perform to handle indirection properly
            let mut requests = vec![sread!(entity_id.clone(), context_field.clone())];

            let _ = self.perform(&mut requests); // Include both successful and failed reads
            context_map.insert(context_field.clone(), requests.into_iter().next().unwrap());
        }

        context_map
    }
    
    /// Trigger notifications for a write operation
    fn trigger_notifications(
        &mut self,
        entity_id: &EntityId,
        field_type: &FieldType,
        current_request: Request,
        previous_request: Request,
    ) {
        // Skip notifications if they are disabled
        if self.notifications_disabled {
            return;
        }

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
                            // Compare values from the requests
                            if let (Request::Read { value: Some(current_val), .. }, Request::Read { value: Some(previous_val), .. }) = (&current_request, &previous_request) {
                                current_val != previous_val
                            } else {
                                true // Always notify if we can't compare values
                            }
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
                                // Compare values from the requests
                                if let (Request::Read { value: Some(current_val), .. }, Request::Read { value: Some(previous_val), .. }) = (&current_request, &previous_request) {
                                    current_val != previous_val
                                } else {
                                    true // Always notify if we can't compare values
                                }
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
            let context_fields = self.build_context_fields(entity_id, &context);
            let config_hash = hash_notify_config(&config);

            let notification = Notification {
                current: current_request.clone(),
                previous: previous_request.clone(),
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

#[async_trait]
impl StoreTrait for AsyncStore {
    async fn get_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Single>> {
        self.inner.get_entity_schema(entity_type)
    }

    async fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Complete>> {
        self.inner.get_complete_entity_schema(entity_type)
    }

    async fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<FieldSchema> {
        self.inner.get_field_schema(entity_type, field_type)
    }

    async fn set_field_schema(&mut self, entity_type: &EntityType, field_type: &FieldType, schema: FieldSchema) -> Result<()> {
        self.inner.set_field_schema(entity_type, field_type, schema)
    }

    async fn entity_exists(&self, entity_id: &EntityId) -> bool {
        self.inner.entity_exists(entity_id)
    }

    async fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        self.inner.field_exists(entity_type, field_type)
    }

    async fn perform(&mut self, requests: &mut Vec<Request>) -> Result<()> {
        self.inner.perform(requests)
    }

    async fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>) -> Result<PageResult<EntityId>> {
        self.inner.find_entities_paginated(entity_type, page_opts)
    }

    async fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>) -> Result<PageResult<EntityId>> {
        self.inner.find_entities_exact(entity_type, page_opts)
    }

    async fn find_entities(&self, entity_type: &EntityType) -> Result<Vec<EntityId>> {
        self.inner.find_entities(entity_type)
    }

    async fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        self.inner.get_entity_types()
    }

    async fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>> {
        self.inner.get_entity_types_paginated(page_opts)
    }

    async fn register_notification(&mut self, config: NotifyConfig, sender: NotificationSender) -> Result<()> {
        self.inner.register_notification(config, sender)
    }

    async fn unregister_notification(&mut self, config: &NotifyConfig, sender: &NotificationSender) -> bool {
        self.inner.unregister_notification(config, sender)
    }
}

impl AsyncStore {
    pub async fn new(store: Store) -> Self {
        Self {
            inner: store,
        }
    }

    pub async fn inner(&self) -> &Store {
        &self.inner
    }

    pub async fn inner_mut(&mut self) -> &mut Store {
        &mut self.inner
    }
}