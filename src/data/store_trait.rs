use async_trait::async_trait;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationSender, NotifyConfig,
    PageOpts, PageResult, Request, Result, Single, Complete,
};

/// Async trait defining the common interface for store implementations
/// This allows StoreWrapper to work with both Store and StoreProxy
#[async_trait]
pub trait StoreTrait: Send + Sync {
    /// Get the schema for a specific entity type
    async fn get_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Single>>;

    /// Get the complete schema for a specific entity type (including inherited fields)
    async fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Complete>>;

    /// Get the schema for a specific field
    async fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<FieldSchema>;

    /// Set or update the schema for a specific field
    async fn set_field_schema(&mut self, entity_type: &EntityType, field_type: &FieldType, schema: FieldSchema) -> Result<()>;

    /// Check if an entity exists
    async fn entity_exists(&self, entity_id: &EntityId) -> bool;

    /// Check if a field type exists for an entity type
    async fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool;

    /// Perform a batch of requests
    async fn perform(&mut self, requests: &mut Vec<Request>) -> Result<()>;

    /// Find entities of a specific type with pagination (includes inherited types)
    async fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>) -> Result<PageResult<EntityId>>;

    /// Find entities of exactly the specified type (no inheritance) with pagination
    async fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>) -> Result<PageResult<EntityId>>;

    /// Find all entities of a specific type (includes inherited types)
    async fn find_entities(&self, entity_type: &EntityType) -> Result<Vec<EntityId>>;

    /// Get all entity types
    async fn get_entity_types(&self) -> Result<Vec<EntityType>>;

    /// Get all entity types with pagination
    async fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>>;

    /// Register a notification with a provided sender
    async fn register_notification(&mut self, config: NotifyConfig, sender: NotificationSender) -> Result<()>;

    /// Unregister a notification by removing a specific sender
    async fn unregister_notification(&mut self, config: &NotifyConfig, sender: &NotificationSender) -> bool;
}