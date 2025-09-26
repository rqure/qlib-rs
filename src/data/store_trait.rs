use std::collections::HashMap;

use crate::{
    data::request::Requests, Complete, EntityId, EntitySchema, EntityType, FieldSchema, FieldType,
    IndirectFieldType, NotificationQueue, NotifyConfig, PageOpts, PageResult, Request, Result,
    Single,
};

/// Async trait defining the common interface for store implementations
/// This allows different store implementations to be used interchangeably
pub trait StoreTrait {
    fn get_entity_type(&self, name: &str) -> Result<EntityType>;

    fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String>;

    fn get_field_type(&self, name: &str) -> Result<FieldType>;

    fn resolve_field_type(&self, field_type: FieldType) -> Result<String>;

    /// Get the schema for a specific entity type
    fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>>;

    /// Get the complete schema for a specific entity type (including inherited fields)
    fn get_complete_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<&EntitySchema<Complete>>;

    /// Get the schema for a specific field
    fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema>;

    /// Set or update the schema for a specific field
    fn set_field_schema(
        &mut self,
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
    ) -> Result<()>;

    /// Check if an entity exists
    fn entity_exists(&self, entity_id: EntityId) -> bool;

    /// Check if a field type exists for an entity type
    fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool;

    /// Resolve indirection for field lookups
    fn resolve_indirection(
        &self,
        entity_id: EntityId,
        fields: &[FieldType],
    ) -> Result<(EntityId, FieldType)>;

    /// Perform a batch of requests
    fn perform(&self, requests: Requests) -> Result<Requests>;
    fn perform_mut(&mut self, requests: Requests) -> Result<Requests>;

    fn perform_map(&self, requests: Requests) -> Result<HashMap<IndirectFieldType, Request>> {
        let updated_requests = self.perform(requests)?;

        let mut result_map = HashMap::new();
        for request in updated_requests.read().iter() {
            if let Some(field_type) = request.field_type() {
                result_map.insert(field_type.clone(), request.clone());
            }
        }

        Ok(result_map)
    }

    /// Find entities of a specific type with pagination (includes inherited types)
    fn find_entities_paginated(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>>;

    /// Find entities of exactly the specified type (no inheritance) with pagination
    fn find_entities_exact(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>>;

    /// Find all entities of a specific type (includes inherited types)
    fn find_entities(&self, entity_type: EntityType, filter: Option<&str>)
        -> Result<Vec<EntityId>>;

    /// Get all entity types
    fn get_entity_types(&self) -> Result<Vec<EntityType>>;

    /// Get all entity types with pagination
    fn get_entity_types_paginated(
        &self,
        page_opts: Option<&PageOpts>,
    ) -> Result<PageResult<EntityType>>;

    /// Register a notification with a provided sender
    fn register_notification(
        &mut self,
        config: NotifyConfig,
        sender: NotificationQueue,
    ) -> Result<()>;

    /// Unregister a notification by removing a specific sender
    fn unregister_notification(
        &mut self,
        config: &NotifyConfig,
        sender: &NotificationQueue,
    ) -> bool;
}
