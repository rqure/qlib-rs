use itertools::Itertools;
use rustc_hash::FxHashMap;
use sorted_vec::SortedVec;
use std::{
    collections::VecDeque,
    mem::discriminant,
    sync::{Arc, Mutex},
};

use crate::{
    data::{
        entity_schema::Complete, hash_notify_config,
        interner::Interner, now, EntityType, FieldType, Notification,
        NotificationQueue, NotifyConfig, NotifyInfo, StoreTrait, Timestamp,
    }, et::ET, expr::CelExecutor, ft::FT, AdjustBehavior, EntityId, EntitySchema, Error, Field, FieldSchema, PageOpts, PageResult, PushCondition, Result, Single, Snapshot, Value, WriteInfo
};

pub struct Store {
    schemas: FxHashMap<EntityType, EntitySchema<Single>>,
    entities: FxHashMap<EntityType, SortedVec<EntityId>>,
    fields: FxHashMap<(EntityId, FieldType), Field>,

    entity_type_interner: Interner,
    field_type_interner: Interner,
    pub et: Option<ET>,
    pub ft: Option<FT>,

    /// Maps parent types to all their derived types (including direct and indirect children)
    /// This allows fast lookup of all entity types that inherit from a given parent type
    inheritance_map: FxHashMap<EntityType, Vec<EntityType>>,

    /// Cache for complete entity schemas to avoid rebuilding inheritance chains repeatedly
    /// This cache is invalidated whenever schemas are updated or inheritance map is rebuilt
    complete_entity_schema_cache: FxHashMap<EntityType, EntitySchema<Complete>>,

    /// Cached CEL executor for filter expressions
    /// This is wrapped in Arc<Mutex<>> for thread-safe access with interior mutability
    cel_executor_cache: Arc<Mutex<CelExecutor>>,

    /// Notification senders indexed by entity ID and field type
    /// Each config can have multiple senders
    id_notifications:
        FxHashMap<EntityId, FxHashMap<FieldType, FxHashMap<NotifyConfig, Vec<NotificationQueue>>>>,

    /// Notification senders indexed by entity type and field type
    /// Each config can have multiple senders
    type_notifications: FxHashMap<
        EntityType,
        FxHashMap<FieldType, FxHashMap<NotifyConfig, Vec<NotificationQueue>>>,
    >,

    pub write_queue: VecDeque<WriteInfo>,

    /// Flag to temporarily disable notifications (e.g., during WAL replay)
    notifications_disabled: bool,

    /// Default writer id for operations that don't specify one
    pub default_writer_id: Option<EntityId>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("schemas", &self.schemas)
            .field("entities", &self.entities)
            .field("fields", &self.fields)
            .field("inheritance_map", &self.inheritance_map)
            .field(
                "complete_entity_schema_cache",
                &format_args!("{} cached schemas", self.complete_entity_schema_cache.len()),
            )
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
    pub fn new() -> Self {
        Store {
            schemas: FxHashMap::default(),
            entities: FxHashMap::default(),
            fields: FxHashMap::default(),
            entity_type_interner: Interner::new(),
            field_type_interner: Interner::new(),
            et: None,
            ft: None,
            inheritance_map: FxHashMap::default(),
            complete_entity_schema_cache: FxHashMap::default(),
            id_notifications: FxHashMap::default(),
            type_notifications: FxHashMap::default(),
            write_queue: VecDeque::new(),
            notifications_disabled: false,
            default_writer_id: None,
            cel_executor_cache: Arc::new(Mutex::new(CelExecutor::new())),
        }
    }

    /// Internal entity creation that doesn't use perform to avoid recursion
    fn create_entity_internal(
        &mut self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        created_entity_id: &mut Option<EntityId>,
        name: &str,
    ) -> Result<()> {
        if !self.schemas.contains_key(&entity_type) {
            return Err(Error::EntityTypeNotFound(entity_type.clone()));
        }

        if let Some(parent) = parent_id.clone() {
            if !self.entity_exists(parent) {
                return Err(Error::EntityNotFound(parent));
            }
        }

        let entity_id = {
            if let Some(id) = created_entity_id {
                id.clone()
            } else {
                let last_id = self
                    .entities
                    .get(&entity_type)
                    .and_then(|v| v.last())
                    .cloned()
                    .unwrap_or(EntityId::new(entity_type.clone(), 0));
                let entity_id = EntityId::new(entity_type.clone(), last_id.extract_id() + 1);
                *created_entity_id = Some(entity_id);
                entity_id
            }
        };
        if self.fields.keys().any(|(eid, _)| eid == &entity_id) {
            return Err(Error::EntityAlreadyExists(entity_id));
        }

        {
            let entities = self
                .entities
                .entry(entity_type.clone())
                .or_insert_with(SortedVec::new);
            entities.push(entity_id);
        }

        // Get the schema before accessing fields to avoid borrow issues
        // The cache should be populated by rebuild_complete_entity_schema_cache()
        let complete_schema = self.get_complete_entity_schema(entity_type)?;
        let ft = self.ft.as_ref().unwrap();
        
        // Clone the fields we need to avoid borrowing conflicts
        let schema_fields: Vec<(FieldType, FieldSchema)> = complete_schema.fields.iter()
            .map(|(ft, fs)| (*ft, fs.clone()))
            .collect();

        // Directly set fields in the entity's field map
        for (field_type, field_schema) in schema_fields {
            let value = {
                if field_type == ft.name.unwrap() {
                    Value::String(name.to_string().into())
                } else if field_type == ft.parent.unwrap() {
                    match &parent_id {
                        Some(parent) => Value::EntityReference(Some(*parent)),
                        None => field_schema.default_value(),
                    }
                } else {
                    field_schema.default_value()
                }
            };

            let field_key = (entity_id, field_type);
            self.fields.insert(
                field_key,
                Field {
                    field_type: field_type,
                    value,
                    write_time: now(),
                    writer_id: None,
                },
            );
        }

        // If we have a parent, add it to the parent's children list
        if let Some(parent) = &parent_id {
            let children_field_key = (*parent, ft.children.unwrap());
            if let Some(children_field) = self.fields.get_mut(&children_field_key) {
                if let Value::EntityList(children) = &mut children_field.value {
                    children.push(entity_id);
                    children_field.write_time = now();
                }
            } else {
                // Create the Children field if it doesn't exist
                self.fields.insert(
                    children_field_key,
                    Field {
                        field_type: ft.children.unwrap(),
                        value: Value::EntityList(vec![entity_id]),
                        write_time: now(),
                        writer_id: None,
                    },
                );
            }
        }

        Ok(())
    }

    pub fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        self.schemas
            .get(&entity_type)
            .cloned()
            .ok_or_else(|| Error::EntityTypeNotFound(entity_type.clone()))
    }

    pub fn get_complete_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<&EntitySchema<Complete>> {
        // Check cache first
        if let Some(cached_schema) = self.complete_entity_schema_cache.get(&entity_type) {
            return Ok(cached_schema);
        }

        // If not in cache, we need to build it, but since we need to return a reference,
        // we can't build it here. The cache must be pre-populated.
        Err(Error::EntityTypeNotFound(entity_type))
    }

    /// Internal method to build a complete entity schema from inheritance hierarchy
    /// This method should only be called when the cache is empty or being rebuilt
    fn build_complete_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let mut schema =
            EntitySchema::<Complete>::from(self.get_entity_schema(entity_type.clone())?);
        let mut visited_types = std::collections::HashSet::new();
        visited_types.insert(entity_type.clone());

        // Collect all fields from inheritance hierarchy with proper rank ordering
        let mut all_fields: Vec<(FieldType, FieldSchema)> = Vec::new();

        // Add fields from the base schema first
        for (field_type, field_schema) in &schema.fields {
            all_fields.push((*field_type, field_schema.clone()));
        }

        // Use a queue to process inheritance in breadth-first manner
        let mut inherit_queue: std::collections::VecDeque<EntityType> =
            schema.inherit.clone().into_iter().collect();

        while let Some(inherit_type) = inherit_queue.pop_front() {
            // Check for circular inheritance
            if visited_types.contains(&inherit_type) {
                // Circular inheritance detected, skip this type
                continue;
            }

            if let Some(inherit_schema) = self.schemas.get(&inherit_type) {
                visited_types.insert(inherit_type.clone());

                // Add inherited fields if they don't already exist in the derived schema
                // Fields in the derived schema take precedence over inherited fields
                for (field_type, field_schema) in &inherit_schema.fields {
                    if !all_fields
                        .iter()
                        .any(|(existing_field_type, _)| existing_field_type == field_type)
                    {
                        all_fields.push((*field_type, field_schema.clone()));
                    }
                }

                // Add parent types to the queue for further processing
                for parent in &inherit_schema.inherit {
                    if !visited_types.contains(parent) {
                        inherit_queue.push_back(*parent);
                    }
                }
            } else {
                return Err(Error::EntityTypeNotFound(inherit_type.clone()));
            }
        }

        // Sort all fields by rank to ensure proper ordering
        all_fields.sort_by_key(|(_, field_schema)| field_schema.rank());

        // Rebuild the schema with properly ordered fields
        schema.fields.clear();
        for (field_type, field_schema) in all_fields {
            schema.fields.insert(field_type, field_schema);
        }

        Ok(schema)
    }

    /// Get the schema for a specific field
    pub fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema> {
        self.get_entity_schema(entity_type.clone())?
            .fields
            .get(&field_type)
            .cloned()
            .ok_or_else(|| {
                Error::FieldTypeNotFound(EntityId::new(entity_type.clone(), 0), field_type)
            })
    }

    /// Set or update the schema for a specific field
    pub fn set_field_schema(
        &mut self,
        entity_type: EntityType,
        field_type: FieldType,
        field_schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(entity_type)?;

        entity_schema.fields.insert(field_type, field_schema);

        self.update_schema(entity_schema.to_string_schema(self))
    }

    pub fn entity_exists(&self, entity_id: EntityId) -> bool {
        let ft = self.ft.as_ref().unwrap();
        self.fields.contains_key(&(entity_id, ft.name.unwrap()))
    }

    pub fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        self.schemas
            .get(&entity_type)
            .map(|schema| schema.fields.contains_key(&field_type))
            .unwrap_or(false)
    }

    /// Internal entity deletion that doesn't use perform to avoid recursion
    fn delete_entity_internal(&mut self, entity_id: EntityId) -> Result<()> {
        // Check if the entity exists
        if !self.fields.keys().any(|(eid, _)| *eid == entity_id) {
            return Err(Error::EntityNotFound(entity_id));
        }

        // Remove all children first (recursively)
        let children_field_key = {
            let ft = self.ft.as_ref().unwrap();
            (entity_id, ft.children.unwrap())
        };
        if let Some(children_field) = self.fields.get(&children_field_key) {
            if let Value::EntityList(children) = &children_field.value {
                let children_to_delete = children.clone(); // Clone to avoid borrow issues
                for child in children_to_delete {
                    self.delete_entity_internal(child)?;
                }
            }
        }

        // Remove from parent's children list
        let parent_field_key = {
            let ft = self.ft.as_ref().unwrap();
            (entity_id, ft.parent.unwrap())
        };
        let parent_id = if let Some(parent_field) = self.fields.get(&parent_field_key) {
            if let Value::EntityReference(Some(parent_id)) = &parent_field.value {
                Some(parent_id.clone())
            } else {
                None
            }
        } else {
            None
        };

        if let Some(parent_id) = parent_id {
            let parent_children_key = {
                let ft = self.ft.as_ref().unwrap();
                (parent_id, ft.children.unwrap())
            };
            if let Some(children_field) = self.fields.get_mut(&parent_children_key) {
                if let Value::EntityList(children) = &mut children_field.value {
                    children.retain(|id| *id != entity_id);
                    children_field.write_time = now();
                }
            }
        }

        // Remove fields
        self.fields.retain(|(eid, _), _| *eid != entity_id);

        // Remove from entity type list
        if let Some(entities) = self.entities.get_mut(&entity_id.extract_type()) {
            entities.retain(|id| *id != entity_id);
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
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.cloned().unwrap_or_default();

        // Get all entity types that match the requested type (including derived types)
        // Use reference to avoid cloning the entire vector
        let types_to_search = self
            .inheritance_map
            .get(&entity_type)
            .map(|v| v.as_slice())
            .unwrap_or_else(|| {
                // If not in inheritance map, just check the exact type
                if self.entities.contains_key(&entity_type) {
                    std::slice::from_ref(&entity_type)
                } else {
                    &[]
                }
            });

        // Early return if no types to search
        if types_to_search.is_empty() {
            return Ok(PageResult {
                items: Vec::new(),
                total: 0,
                next_cursor: None,
            });
        }

        // Get cursor value
        let start_idx = opts.cursor.unwrap_or(0);

        if let Some(filter_expr) = filter {
            // Optimized path for filtered queries - lazy evaluation with early termination
            self.find_entities_paginated_filtered(types_to_search, &opts, start_idx, &filter_expr)
        } else {
            // Optimized path for unfiltered queries - direct iteration without collecting all
            self.find_entities_paginated_unfiltered(types_to_search, &opts, start_idx)
        }
    }

    /// Fast path for unfiltered paginated queries
    fn find_entities_paginated_unfiltered(
        &self,
        types_to_search: &[EntityType],
        opts: &PageOpts,
        start_idx: usize,
    ) -> Result<PageResult<EntityId>> {
        // Calculate total count without allocating all entities
        let total: usize = types_to_search
            .iter()
            .map(|et| self.entities.get(et).map_or(0, |entities| entities.len()))
            .sum();

        if total == 0 || start_idx >= total {
            return Ok(PageResult {
                items: Vec::new(),
                total,
                next_cursor: None,
            });
        }

        // Collect only the entities we need for this page
        let mut items = Vec::with_capacity(opts.limit);
        let mut current_idx = 0;
        let end_idx = std::cmp::min(start_idx + opts.limit, total);

        'outer: for et in types_to_search {
            if let Some(entities) = self.entities.get(et) {
                for entity_id in entities {
                    if current_idx >= start_idx && items.len() < opts.limit {
                        items.push(*entity_id);
                        if items.len() >= opts.limit {
                            break 'outer;
                        }
                    }
                    current_idx += 1;
                    if current_idx >= end_idx {
                        break 'outer;
                    }
                }
            }
        }

        let next_cursor = if end_idx < total {
            Some(end_idx)
        } else {
            None
        };

        Ok(PageResult {
            items,
            total,
            next_cursor,
        })
    }

    /// Optimized path for filtered paginated queries
    fn find_entities_paginated_filtered(
        &self,
        types_to_search: &[EntityType],
        opts: &PageOpts,
        start_idx: usize,
        filter_expr: &str,
    ) -> Result<PageResult<EntityId>> {
        let mut page_items = Vec::with_capacity(opts.limit);
        let mut current_filtered_idx = 0;
        let mut total_filtered = 0;
        let end_target = start_idx + opts.limit;

        // Early termination flags
        let mut page_complete = false;

        // Iterate through all entities with optimized early termination
        for et in types_to_search {
            if let Some(entities) = self.entities.get(et) {
                for entity_id in entities {
                    // Apply filter using cached executor
                    let passes_filter = {
                        let mut executor = self.cel_executor_cache.lock().unwrap();
                        match executor.execute(filter_expr, *entity_id, self) {
                            Ok(cel::Value::Bool(true)) => true,
                            _ => false, // Skip for false, non-boolean, or error results
                        }
                    };

                    if passes_filter {
                        total_filtered += 1;

                        // Collect items for the current page
                        if current_filtered_idx >= start_idx && page_items.len() < opts.limit {
                            page_items.push(*entity_id);

                            // Check if we've filled the page
                            if page_items.len() >= opts.limit {
                                page_complete = true;
                                // For filtered queries, we still need the total count
                                // so we can't exit early
                            }
                        }

                        current_filtered_idx += 1;
                    }
                }
            }
        }

        let next_cursor = if page_complete && total_filtered > end_target {
            Some(end_target)
        } else {
            None
        };

        Ok(PageResult {
            items: page_items,
            total: total_filtered,
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
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        let opts = page_opts.cloned().unwrap_or_default();

        // Check if entity type exists - early return if not
        let entities = match self.entities.get(&entity_type) {
            Some(entities) => entities,
            None => {
                return Ok(PageResult {
                    items: Vec::new(),
                    total: 0,
                    next_cursor: None,
                });
            }
        };

        // Get cursor value
        let start_idx = opts.cursor.unwrap_or(0);

        if let Some(filter_expr) = filter {
            // Optimized filtered path - only evaluate what we need
            self.find_entities_exact_filtered(entities, &opts, start_idx, &filter_expr)
        } else {
            // Optimized unfiltered path - direct slicing without cloning all
            self.find_entities_exact_unfiltered(entities, &opts, start_idx)
        }
    }

    /// Fast path for exact unfiltered queries
    fn find_entities_exact_unfiltered(
        &self,
        entities: &[EntityId],
        opts: &PageOpts,
        start_idx: usize,
    ) -> Result<PageResult<EntityId>> {
        let total = entities.len();

        if total == 0 || start_idx >= total {
            return Ok(PageResult {
                items: Vec::new(),
                total,
                next_cursor: None,
            });
        }

        let end_idx = std::cmp::min(start_idx + opts.limit, total);
        let items = entities[start_idx..end_idx].to_vec();

        let next_cursor = if end_idx < total {
            Some(end_idx)
        } else {
            None
        };

        Ok(PageResult {
            items,
            total,
            next_cursor,
        })
    }

    /// Optimized path for exact filtered queries
    fn find_entities_exact_filtered(
        &self,
        entities: &[EntityId],
        opts: &PageOpts,
        start_idx: usize,
        filter_expr: &str,
    ) -> Result<PageResult<EntityId>> {
        let mut page_items = Vec::with_capacity(opts.limit);
        let mut current_filtered_idx = 0;
        let mut total_filtered = 0;

        // Process entities in order, collecting only what we need
        for entity_id in entities {
            // Apply filter using cached executor
            let passes_filter = {
                let mut executor = self.cel_executor_cache.lock().unwrap();
                match executor.execute(filter_expr, *entity_id, self) {
                    Ok(cel::Value::Bool(true)) => true,
                    _ => false, // Skip for false, non-boolean, or error results
                }
            };

            if passes_filter {
                total_filtered += 1;

                // Collect for current page if within range
                if current_filtered_idx >= start_idx && page_items.len() < opts.limit {
                    page_items.push(*entity_id);
                }

                current_filtered_idx += 1;
            }
        }

        let next_cursor = if start_idx + opts.limit < total_filtered {
            Some(start_idx + opts.limit)
        } else {
            None
        };

        Ok(PageResult {
            items: page_items,
            total: total_filtered,
            next_cursor,
        })
    }

    pub fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<&str>,
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self.find_entities_paginated(
                entity_type.clone(),
                page_opts.as_ref(),
                filter.as_deref(),
            )?;
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
            let page_result = self.get_entity_types_paginated(page_opts.as_ref())?;
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
        page_opts: Option<&PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let opts = page_opts.cloned().unwrap_or_default();

        // Collect all types from schema
        let all_types: Vec<EntityType> = self.schemas.keys().cloned().collect();
        let total = all_types.len();

        // Get the starting index based on cursor
        let start_idx = opts.cursor.unwrap_or(0);

        // Get the slice of types for this page
        let end_idx = std::cmp::min(start_idx + opts.limit, total);
        let items: Vec<EntityType> = if start_idx < total {
            all_types[start_idx..end_idx].to_vec()
        } else {
            Vec::new()
        };

        // Calculate the next cursor
        let next_cursor = if end_idx < total {
            Some(end_idx)
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
        sender: NotificationQueue,
    ) -> Result<()> {
        // Add sender to the list for this notification config
        match &config {
            NotifyConfig::EntityId {
                entity_id,
                field_type,
                ..
            } => {
                let senders = self
                    .id_notifications
                    .entry(*entity_id)
                    .or_insert_with(FxHashMap::default)
                    .entry(*field_type)
                    .or_insert_with(FxHashMap::default)
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
                    .entry(*entity_type)
                    .or_insert_with(FxHashMap::default)
                    .entry(*field_type)
                    .or_insert_with(FxHashMap::default)
                    .entry(config.clone())
                    .or_insert_with(Vec::new);
                senders.push(sender);
            }
        }

        Ok(())
    }

    /// Unregister a notification by removing a specific sender
    /// Returns true if the sender was found and removed
    pub fn unregister_notification(
        &mut self,
        target_config: &NotifyConfig,
        target_sender: &NotificationQueue,
    ) -> bool {
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
                let entity_type_key = *entity_type;
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

    /// Get a reference to the fields map (converts to nested structure for compatibility)
    fn get_fields(&self) -> FxHashMap<EntityId, FxHashMap<FieldType, Field>> {
        let mut nested_fields = FxHashMap::default();
        for ((entity_id, field_type), field) in &self.fields {
            nested_fields
                .entry(*entity_id)
                .or_insert_with(FxHashMap::default)
                .insert(*field_type, field.clone());
        }
        nested_fields
    }

    /// Disable notifications temporarily (e.g., during WAL replay)
    pub fn disable_notifications(&mut self) {
        self.notifications_disabled = true;
    }

    /// Re-enable notifications
    pub fn enable_notifications(&mut self) {
        self.notifications_disabled = false;
    }

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self) -> Snapshot {
        Snapshot::new(
            self.schemas.clone(),
            self.entities.clone(),
            self.entity_type_interner.clone(),
            self.field_type_interner.clone(),
            self.get_fields(),
        )
    }

    /// Restore the store state from a snapshot
    pub fn restore_snapshot(&mut self, snapshot: Snapshot) {
        self.schemas = snapshot.schemas;
        self.entities = snapshot.entities;
        self.entity_type_interner = snapshot.entity_type_interner;
        self.field_type_interner = snapshot.field_type_interner;

        // Re-initialize ET and FT after restoring snapshot data
        self.et = Some(ET::new(self));
        self.ft = Some(FT::new(self));

        // Convert nested fields structure to flattened structure
        self.fields.clear();
        for (entity_id, entity_fields) in snapshot.fields {
            for (field_type, field) in entity_fields {
                self.fields.insert((entity_id, field_type), field);
            }
        }

        // Clear the cache since schema structure may have changed
        self.complete_entity_schema_cache.clear();

        // Rebuild inheritance map after restoring (this will also rebuild the cache)
        self.rebuild_inheritance_map();
    }

    /// Rebuild the inheritance map for fast lookup of derived types
    /// This should be called whenever schemas are added or updated
    fn rebuild_inheritance_map(&mut self) {
        self.inheritance_map.clear();
        // Clear the complete entity schema cache since inheritance relationships may have changed
        self.complete_entity_schema_cache.clear();

        // For each entity type, find all types that inherit from it
        for entity_type in self
            .entity_type_interner
            .ids()
            .map(|id| EntityType(id as u32))
        {
            let mut derived_types = Vec::new();

            // Check all other types to see if they inherit from this type
            for other_type in self
                .entity_type_interner
                .ids()
                .map(|id| EntityType(id as u32))
            {
                if self.inherits_from(other_type.clone(), entity_type.clone()) {
                    derived_types.push(other_type.clone());
                }
            }

            // Include the type itself in the list
            derived_types.push(entity_type.clone());

            self.inheritance_map
                .insert(entity_type.clone(), derived_types);
        }

        // Rebuild the complete entity schema cache
        self.rebuild_complete_entity_schema_cache();
    }

    /// Rebuild the complete entity schema cache for all entity types
    /// This should be called after inheritance map changes or schema updates
    fn rebuild_complete_entity_schema_cache(&mut self) {
        self.complete_entity_schema_cache.clear();

        // Build complete schemas for all entity types
        for entity_type in self
            .entity_type_interner
            .ids()
            .map(|id| EntityType(id as u32))
        {
            if let Ok(complete_schema) = self.build_complete_entity_schema(entity_type.clone()) {
                self.complete_entity_schema_cache
                    .insert(entity_type.clone(), complete_schema);
            }
        }
    }

    /// Check if derived_type inherits from base_type (directly or indirectly)
    /// This method guards against circular inheritance by limiting the depth of inheritance traversal
    fn inherits_from(&self, derived_type: EntityType, base_type: EntityType) -> bool {
        if derived_type == base_type {
            return false; // A type doesn't inherit from itself
        }

        let mut types_to_check = std::collections::VecDeque::new();
        let mut visited = std::collections::HashSet::new();
        types_to_check.push_back(derived_type.clone());
        visited.insert(derived_type.clone());

        while let Some(current_type) = types_to_check.pop_front() {
            if let Some(schema) = self.schemas.get(&current_type) {
                for inherit_type in &schema.inherit {
                    if *inherit_type == base_type {
                        return true;
                    }

                    // Add to queue if not already visited to prevent infinite loops
                    if !visited.contains(inherit_type) {
                        types_to_check.push_back(inherit_type.clone());
                        visited.insert(inherit_type.clone());
                    }
                }
            }
        }

        false
    }

    /// Get all parent types in the inheritance chain for a given entity type
    /// Returns a vector of parent types (all ancestors in the inheritance hierarchy)
    /// For multi-inheritance, this includes all paths through the inheritance graph
    fn get_parent_types(&self, entity_type: EntityType) -> Vec<EntityType> {
        let mut parent_types = Vec::new();
        let mut types_to_check = std::collections::VecDeque::new();
        let mut visited = std::collections::HashSet::new();

        types_to_check.push_back(entity_type.clone());
        visited.insert(entity_type.clone());

        while let Some(current_type) = types_to_check.pop_front() {
            if let Some(schema) = self.schemas.get(&current_type) {
                for inherit_type in &schema.inherit {
                    if !visited.contains(inherit_type) {
                        parent_types.push(inherit_type.clone());
                        types_to_check.push_back(inherit_type.clone());
                        visited.insert(inherit_type.clone());
                    }
                }
            }
        }

        parent_types
    }

    /// Build context fields using direct read method to handle indirection
    fn build_context_fields(
        &mut self,
        entity_id: EntityId,
        context_fields: &[Vec<FieldType>],
    ) -> std::collections::BTreeMap<Vec<FieldType>, NotifyInfo> {
        let mut context_map = std::collections::BTreeMap::new();

        for context_field in context_fields {
            // Use direct read method to handle indirection properly
            if let Ok((value, timestamp, writer_id)) = self.read(entity_id, context_field) {
                let notify_info = NotifyInfo {
                    entity_id,
                    field_path: context_field.clone().into_iter().collect(),
                    value: Some(value),
                    timestamp: Some(timestamp),
                    writer_id,
                };
                context_map.insert(context_field.clone(), notify_info);
            } else {
                // If read fails, insert a NotifyInfo with None values
                let notify_info = NotifyInfo {
                    entity_id,
                    field_path: context_field.clone().into_iter().collect(),
                    value: None,
                    timestamp: None,
                    writer_id: None,
                };
                context_map.insert(context_field.clone(), notify_info);
            }
        }

        context_map
    }

    /// Resolve indirection for field lookups with direct field access for performance
    /// This is an optimized version that bypasses the perform() method for faster lookups
    pub fn resolve_indirection(
        &self,
        entity_id: EntityId,
        fields: &[FieldType],
    ) -> Result<(EntityId, FieldType)> {
        use crate::{BadIndirectionReason, Error, Value};

        if fields.len() == 1 {
            return Ok((entity_id, fields[0].clone()));
        }

        let mut current_entity_id = entity_id;

        for (i, field) in fields.iter().enumerate() {
            // If this is the last field in the path, we're done - return the current entity and field
            if i == fields.len() - 1 {
                break;
            }

            // Direct field lookup using self.fields for performance
            let field_key = (current_entity_id, field.clone());
            let field_value = match self.fields.get(&field_key) {
                Some(field) => &field.value,
                None => {
                    return Err(Error::BadIndirection(
                        current_entity_id,
                        fields.to_vec(),
                        BadIndirectionReason::FailedToResolveField(
                            field.clone(),
                            "Field not found".to_string(),
                        ),
                    ));
                }
            };

            // For intermediate fields, they must be EntityReferences
            if let Value::EntityReference(reference) = field_value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid using direct entity existence check
                        if !self.entity_exists(ref_id.clone()) {
                            return Err(Error::BadIndirection(
                                current_entity_id,
                                fields.to_vec(),
                                BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            ));
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(Error::BadIndirection(
                            current_entity_id,
                            fields.to_vec(),
                            BadIndirectionReason::EmptyEntityReference,
                        ));
                    }
                }
            } else {
                return Err(Error::BadIndirection(
                    current_entity_id,
                    fields.to_vec(),
                    BadIndirectionReason::UnexpectedValueType(
                        field.clone(),
                        format!("{:?}", field_value),
                    ),
                ));
            }
        }

        Ok((
            current_entity_id,
            fields.last().cloned().ok_or_else(|| {
                Error::BadIndirection(
                    entity_id,
                    fields.to_vec(),
                    BadIndirectionReason::UnexpectedValueType(
                        FieldType(0),
                        "Empty field path".to_string(),
                    ),
                )
            })?,
        ))
    }

    /// Trigger notifications for a write operation
    fn trigger_notifications(
        &mut self,
        entity_id: EntityId,
        field_type: FieldType,
        current_info: NotifyInfo,
        previous_info: NotifyInfo,
    ) {
        // Skip notifications if they are disabled
        if self.notifications_disabled {
            return;
        }

        // Collect notifications that need to be triggered to avoid borrowing conflicts
        let mut notifications_to_trigger = Vec::new();

        // Check entity-specific notifications with O(1) lookup by entity_id and field_type
        if let Some(field_map) = self.id_notifications.get(&entity_id) {
            if let Some(sender_map) = field_map.get(&field_type) {
                for (config, _) in sender_map {
                    if let NotifyConfig::EntityId {
                        trigger_on_change,
                        context,
                        ..
                    } = config
                    {
                        let should_notify = if *trigger_on_change {
                            // Compare values from the infos
                            if let (Some(current_val), Some(previous_val)) = (&current_info.value, &previous_info.value) {
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
        let entity_type = entity_id.extract_type();
        let mut types_to_check = vec![entity_type.clone()];
        types_to_check.extend(self.get_parent_types(entity_type));

        for entity_type_to_check in types_to_check {
            if let Some(field_map) = self.type_notifications.get(&entity_type_to_check) {
                if let Some(sender_map) = field_map.get(&field_type) {
                    for (config, _) in sender_map {
                        if let NotifyConfig::EntityType {
                            trigger_on_change,
                            context,
                            ..
                        } = config
                        {
                            let should_notify = if *trigger_on_change {
                                // Compare values from the infos
                                if let (Some(current_val), Some(previous_val)) = (&current_info.value, &previous_info.value) {
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
                current: current_info.clone(),
                previous: previous_info.clone(),
                context: context_fields,
                config_hash,
            };

            // Find the senders and send the notification through each channel
            match &config {
                NotifyConfig::EntityId {
                    field_type: config_field_type,
                    ..
                } => {
                    if let Some(field_map) = self.id_notifications.get_mut(&entity_id) {
                        if let Some(queue_map) = field_map.get_mut(config_field_type) {
                            if let Some(queues) = queue_map.get_mut(&config) {
                                for queue in queues.iter() {
                                    queue.push(notification.clone());
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
                    if let Some(field_map) = self.type_notifications.get_mut(config_entity_type) {
                        if let Some(queue_map) = field_map.get_mut(config_field_type) {
                            if let Some(queues) = queue_map.get_mut(&config) {
                                // Send to all senders for this config
                                for queue in queues.iter() {
                                    queue.push(notification.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl StoreTrait for Store {
    fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        if let Some(id) = self.entity_type_interner.get(name) {
            Ok(EntityType(id as u32))
        } else {
            Err(Error::EntityTypeStrNotFound(name.to_string()))
        }
    }

    fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        if let Some(entity_type_str) = self.entity_type_interner.resolve(entity_type.0 as u64) {
            Ok(entity_type_str.clone())
        } else {
            Err(Error::EntityTypeNotFound(entity_type))
        }
    }

    fn get_field_type(&self, name: &str) -> Result<FieldType> {
        if let Some(id) = self.field_type_interner.get(name) {
            Ok(FieldType(id))
        } else {
            Err(Error::FieldTypeStrNotFound(name.to_string()))
        }
    }

    fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        if let Some(field_type_str) = self.field_type_interner.resolve(field_type.0 as u64) {
            Ok(field_type_str.clone())
        } else {
            Err(Error::FieldTypeNotFound(EntityId(0), field_type))
        }
    }

    fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        self.get_entity_schema(entity_type)
    }

    fn get_complete_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<&EntitySchema<Complete>> {
        self.get_complete_entity_schema(entity_type)
    }

    fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema> {
        self.get_field_schema(entity_type, field_type)
    }

    fn set_field_schema(
        &mut self,
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
    ) -> Result<()> {
        self.set_field_schema(entity_type, field_type, schema)
    }

    fn entity_exists(&self, entity_id: EntityId) -> bool {
        self.entity_exists(entity_id)
    }

    fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        self.field_exists(entity_type, field_type)
    }

    fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        self.resolve_indirection(entity_id, fields)
    }

    fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let (resolved_entity_id, resolved_field_type) = self.resolve_indirection(entity_id, field_path)?;
        let field_key = (resolved_entity_id, resolved_field_type);
        
        if let Some(field) = self.fields.get(&field_key) {
            Ok((field.value.clone(), field.write_time, field.writer_id))
        } else {
            Err(Error::FieldTypeNotFound(resolved_entity_id, resolved_field_type))
        }
    }

    fn write(&mut self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
        let (entity_id, field_type) = self.resolve_indirection(entity_id, field_path)?;
        let push_condition = push_condition.unwrap_or(PushCondition::Always);
        let adjust_behavior = adjust_behavior.unwrap_or(AdjustBehavior::Set);

        // Get the schema from cache (should be populated by rebuild_complete_entity_schema_cache())
        let entity_schema = self.get_complete_entity_schema(entity_id.extract_type())?;
        let default_value = {
            let field_schema = entity_schema
                .fields
                .get(&field_type)
                .ok_or_else(|| Error::FieldTypeNotFound(entity_id, field_type))?;
            field_schema.default_value()
        };

        let field = self
            .fields
            .entry((entity_id, field_type))
            .or_insert_with(|| Field {
                field_type: field_type,
                value: default_value.clone(),
                write_time: now(),
                writer_id: None,
            });

        let old_value = field.value.clone();
        // Check that the value being written is the same type as the field schema
        // If the value is None, use the default value from the schema
        if discriminant(&value) != discriminant(&default_value) {
            return Err(Error::ValueTypeMismatch(
                entity_id,
                field_type,
                default_value,
                value.clone(),
            ));
        }

        let mut new_value = value.clone();

        match adjust_behavior {
            AdjustBehavior::Add => match &old_value {
                Value::Int(old_int) => {
                    new_value = Value::Int(old_int + new_value.as_int().unwrap_or(0));
                }
                Value::Float(old_float) => {
                    new_value = Value::Float(old_float + new_value.as_float().unwrap_or(0.0));
                }
                Value::EntityReference(old_ref) => {
                    if old_ref.is_some() {
                        // prefer the old value if old value exists
                        new_value = old_value.clone();
                    }
                    // otherwise just use the new value (which could be None or Some)
                }
                Value::EntityList(old_list) => {
                    new_value = Value::EntityList(
                        old_list
                            .iter()
                            .chain(new_value.as_entity_list().unwrap_or(&Vec::new()).iter())
                            .unique()
                            .cloned()
                            .collect(),
                    );
                }
                Value::String(old_string) => {
                    new_value = Value::String(format!(
                        "{}{}",
                        old_string,
                        new_value.as_string().unwrap_or_default()
                    ).into());
                }
                Value::Blob(old_file) => {
                    let combined_vec: Vec<u8> = old_file
                        .iter()
                        .chain(new_value.as_blob().unwrap_or(&[]).iter())
                        .cloned()
                        .collect();
                    new_value = Value::Blob(combined_vec.into());
                }
                _ => {
                    return Err(Error::UnsupportedAdjustBehavior(
                        entity_id,
                        field_type,
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
                Value::EntityReference(old_ref) => {
                    if let Some(old_id) = old_ref {
                        if let Some(new_id) = new_value.as_entity_reference().unwrap_or(&None) {
                            if old_id == new_id {
                                // If the new value matches the old value, set to None
                                new_value = Value::EntityReference(None);
                            } else {
                                // Otherwise, keep the old value
                                new_value = old_value.clone();
                            }
                        }
                    }
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
                        entity_id,
                        field_type,
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

        match push_condition {
            PushCondition::Always => {
                // Only update if the incoming write is newer or if no write_time is specified (local write)
                let incoming_time = write_time.unwrap_or_else(|| now());
                if write_time.is_none() || incoming_time >= field.write_time {
                    field.value = new_value;
                    field.write_time = incoming_time;
                    if let Some(writer_id) = writer_id {
                        field.writer_id = Some(writer_id.clone());
                    } else {
                        field.writer_id = self.default_writer_id.clone();
                    }

                    // Trigger notifications after a write operation
                    let current_info = NotifyInfo {
                        entity_id,
                        field_path: crate::sfield![field_type],
                        value: Some(notification_new_value.clone()),
                        timestamp: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    };
                    let previous_info = NotifyInfo {
                        entity_id,
                        field_path: crate::sfield![field_type],
                        value: Some(notification_old_value.clone()),
                        timestamp: Some(field.write_time), // Use the time before the write
                        writer_id: field.writer_id.clone(),
                    };

                    self.write_queue.push_back(WriteInfo::FieldUpdate {
                        entity_id,
                        field_type,
                        value: Some(notification_new_value.clone()),
                        push_condition,
                        adjust_behavior,
                        write_time: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    });

                    self.trigger_notifications(
                        entity_id,
                        field_type,
                        current_info,
                        previous_info,
                    );

                } else {
                    // Incoming write is older, ignore it
                    return Ok(());
                }
            }
            PushCondition::Changes => {
                // Changes write, only update if the value is different AND the write is newer
                let incoming_time = write_time.unwrap_or_else(|| now());
                if (write_time.is_none() || incoming_time >= field.write_time)
                    && field.value != new_value
                {
                    field.value = new_value;
                    field.write_time = incoming_time;
                    if let Some(writer_id) = writer_id {
                        field.writer_id = Some(writer_id.clone());
                    } else {
                        field.writer_id = self.default_writer_id.clone();
                    }

                    // Trigger notifications after a write operation
                    let current_info = NotifyInfo {
                        entity_id: entity_id,
                        field_path: crate::sfield![field_type],
                        value: Some(notification_new_value.clone()),
                        timestamp: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    };
                    let previous_info = NotifyInfo {
                        entity_id: entity_id,
                        field_path: crate::sfield![field_type],
                        value: Some(notification_old_value.clone()),
                        timestamp: Some(field.write_time), // Use the time before the write
                        writer_id: field.writer_id.clone(),
                    };

                    self.write_queue.push_back(WriteInfo::FieldUpdate {
                        entity_id,
                        field_type,
                        value: Some(notification_new_value.clone()),
                        push_condition,
                        adjust_behavior,
                        write_time: Some(field.write_time),
                        writer_id: field.writer_id.clone(),
                    });

                    self.trigger_notifications(
                        entity_id,
                        field_type,
                        current_info,
                        previous_info,
                    );
                } else if write_time.is_some() && incoming_time < field.write_time {
                    // Incoming write is older, ignore it
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let mut created_entity_id = None;
        self.create_entity_internal(entity_type, parent_id, &mut created_entity_id, name)?;
        let created_entity_id = created_entity_id.ok_or_else(|| Error::InvalidRequest("Failed to create entity".to_string()))?;

        self.write_queue.push_back(WriteInfo::CreateEntity {
            entity_type,
            parent_id,
            name: name.to_string(),
            created_entity_id,
            timestamp: now(),
        });

        Ok(created_entity_id)
    }

    fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        self.delete_entity_internal(entity_id)?;

        self.write_queue.push_back(WriteInfo::DeleteEntity {
            entity_id,
            timestamp: now(),
        });

        Ok(())
    }

    fn update_schema(&mut self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        // Validate whether inherited entity types exist or not:
        for parent in schema.inherit.iter() {
            self.entity_type_interner
                .get(parent.as_str())
                .ok_or_else(|| Error::EntityTypeStrNotFound(parent.clone()))?;
        }

        // Get or create the entity type if it doesn't exist
        let entity_type = EntityType(
            self.entity_type_interner
                .intern(schema.entity_type.as_str()) as u32,
        );

        // Intern all field types before converting the schema
        for field_name in schema.fields.keys() {
            self.field_type_interner.intern(field_name.as_str());
        }

        let schema = EntitySchema::<Single>::from_string_schema(schema.clone(), self);

        // Get a copy of the existing schema if it exists
        let complete_old_schema = self
            .get_complete_entity_schema(entity_type.clone())
            .map(|schema| schema.clone())
            .unwrap_or_else(|_| EntitySchema::<Complete>::new(entity_type.clone()));

        self.schemas.insert(entity_type.clone(), schema.clone());

        if !self.entities.contains_key(&entity_type) {
            self.entities.insert(entity_type.clone(), SortedVec::new());
        }

        // Clear the complete entity schema cache since a schema was updated
        self.complete_entity_schema_cache.clear();

        // Get the complete schema for the entity type (will rebuild since cache is cleared)
        let complete_new_schema =
            self.build_complete_entity_schema(schema.entity_type.clone())?;

        for removed_field in complete_old_schema.diff(&complete_new_schema) {
            // If the field was removed, we need to remove it from all entities
            for entity_id in self
                .entities
                .get(&schema.entity_type)
                .unwrap_or(&SortedVec::new())
            {
                let field_key = (*entity_id, removed_field.field_type().clone());
                self.fields.remove(&field_key);
            }
        }

        for added_field in complete_new_schema.diff(&complete_old_schema) {
            // If the field was added, we need to add it to all entities
            for entity_id in self
                .entities
                .get(&schema.entity_type)
                .unwrap_or(&SortedVec::new())
            {
                let field_key = (*entity_id, added_field.field_type().clone());
                self.fields.insert(
                    field_key,
                    Field {
                        field_type: added_field.field_type().clone(),
                        value: added_field.default_value(),
                        write_time: now(),
                        writer_id: None,
                    },
                );
            }
        }

        self.et = Some(ET::new(self));
        self.ft = Some(FT::new(self));

        // Rebuild inheritance map after schema changes
        self.rebuild_inheritance_map();

        self.write_queue.push_back(WriteInfo::SchemaUpdate {
            schema,
            timestamp: now(),
        });

        Ok(())
    }

    fn take_snapshot(&self) -> crate::data::Snapshot {
        self.take_snapshot()
    }

    fn find_entities_paginated(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        self.find_entities_paginated(entity_type, page_opts, filter)
    }

    fn find_entities_exact(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        self.find_entities_exact(entity_type, page_opts, filter)
    }

    fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<&str>,
    ) -> Result<Vec<EntityId>> {
        self.find_entities(entity_type, filter)
    }

    fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        self.get_entity_types()
    }

    fn get_entity_types_paginated(
        &self,
        page_opts: Option<&PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        self.get_entity_types_paginated(page_opts)
    }

    fn register_notification(
        &mut self,
        config: NotifyConfig,
        sender: NotificationQueue,
    ) -> Result<()> {
        self.register_notification(config, sender)
    }

    fn unregister_notification(
        &mut self,
        config: &NotifyConfig,
        sender: &NotificationQueue,
    ) -> bool {
        self.unregister_notification(config, sender)
    }
}
