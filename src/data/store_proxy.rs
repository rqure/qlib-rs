use serde::{Deserialize, Serialize};

use crate::{
    Context, Entity, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, 
    NotificationCallback, NotifyConfig, NotifyToken, PageOpts, PageResult, Request, 
    Single, Complete, Notification,
};

/// WebSocket message types for Store proxy communication
/// These messages are compatible with the qcore-rs WebSocketMessage format
#[derive(Debug, Serialize, Deserialize)]
pub enum StoreMessage {
    // Store operations
    CreateEntity {
        id: String,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
    },
    CreateEntityResponse {
        id: String,
        response: std::result::Result<Entity, String>,
    },
    
    DeleteEntity {
        id: String,
        entity_id: EntityId,
    },
    DeleteEntityResponse {
        id: String,
        response: std::result::Result<(), String>,
    },
    
    SetEntitySchema {
        id: String,
        schema: EntitySchema<Single>,
    },
    SetEntitySchemaResponse {
        id: String,
        response: std::result::Result<(), String>,
    },
    
    GetEntitySchema {
        id: String,
        entity_type: EntityType,
    },
    GetEntitySchemaResponse {
        id: String,
        response: std::result::Result<Option<EntitySchema<Single>>, String>,
    },
    
    GetCompleteEntitySchema {
        id: String,
        entity_type: EntityType,
    },
    GetCompleteEntitySchemaResponse {
        id: String,
        response: std::result::Result<EntitySchema<Complete>, String>,
    },
    
    SetFieldSchema {
        id: String,
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
    },
    SetFieldSchemaResponse {
        id: String,
        response: std::result::Result<(), String>,
    },
    
    GetFieldSchema {
        id: String,
        entity_type: EntityType,
        field_type: FieldType,
    },
    GetFieldSchemaResponse {
        id: String,
        response: std::result::Result<Option<FieldSchema>, String>,
    },
    
    EntityExists {
        id: String,
        entity_id: EntityId,
    },
    EntityExistsResponse {
        id: String,
        response: bool,
    },
    
    FieldExists {
        id: String,
        entity_id: EntityId,
        field_type: FieldType,
    },
    FieldExistsResponse {
        id: String,
        response: bool,
    },
    
    Perform {
        id: String,
        requests: Vec<Request>,
    },
    PerformResponse {
        id: String,
        response: std::result::Result<Vec<Request>, String>,
    },
    
    FindEntities {
        id: String,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    },
    FindEntitiesResponse {
        id: String,
        response: std::result::Result<PageResult<EntityId>, String>,
    },
    
    FindEntitiesExact {
        id: String,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    },
    FindEntitiesExactResponse {
        id: String,
        response: std::result::Result<PageResult<EntityId>, String>,
    },
    
    GetEntityTypes {
        id: String,
        parent_type: Option<EntityType>,
        page_opts: Option<PageOpts>,
    },
    GetEntityTypesResponse {
        id: String,
        response: std::result::Result<PageResult<EntityType>, String>,
    },
    
    TakeSnapshot {
        id: String,
    },
    TakeSnapshotResponse {
        id: String,
        response: crate::data::store::Snapshot,
    },
    
    RestoreSnapshot {
        id: String,
        snapshot: crate::data::store::Snapshot,
    },
    RestoreSnapshotResponse {
        id: String,
        response: std::result::Result<(), String>,
    },
    
    // Notification support
    RegisterNotification {
        id: String,
        config: NotifyConfig,
    },
    RegisterNotificationResponse {
        id: String,
        response: std::result::Result<NotifyToken, String>,
    },
    
    UnregisterNotification {
        id: String,
        token: NotifyToken,
    },
    UnregisterNotificationResponse {
        id: String,
        response: bool,
    },
    
    GetNotificationConfigs {
        id: String,
    },
    GetNotificationConfigsResponse {
        id: String,
        response: Vec<(NotifyToken, NotifyConfig)>,
    },
    
    // Notification delivery
    Notification {
        notification: Notification,
    },
    
    // Connection management
    Error {
        id: String,
        error: String,
    },
}

/// Extract the message ID from a StoreMessage
pub fn extract_message_id(message: &StoreMessage) -> Option<String> {
    match message {
        StoreMessage::CreateEntity { id, .. } => Some(id.clone()),
        StoreMessage::CreateEntityResponse { id, .. } => Some(id.clone()),
        StoreMessage::DeleteEntity { id, .. } => Some(id.clone()),
        StoreMessage::DeleteEntityResponse { id, .. } => Some(id.clone()),
        StoreMessage::SetEntitySchema { id, .. } => Some(id.clone()),
        StoreMessage::SetEntitySchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetEntitySchema { id, .. } => Some(id.clone()),
        StoreMessage::GetEntitySchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetCompleteEntitySchema { id, .. } => Some(id.clone()),
        StoreMessage::GetCompleteEntitySchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::SetFieldSchema { id, .. } => Some(id.clone()),
        StoreMessage::SetFieldSchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetFieldSchema { id, .. } => Some(id.clone()),
        StoreMessage::GetFieldSchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::EntityExists { id, .. } => Some(id.clone()),
        StoreMessage::EntityExistsResponse { id, .. } => Some(id.clone()),
        StoreMessage::FieldExists { id, .. } => Some(id.clone()),
        StoreMessage::FieldExistsResponse { id, .. } => Some(id.clone()),
        StoreMessage::Perform { id, .. } => Some(id.clone()),
        StoreMessage::PerformResponse { id, .. } => Some(id.clone()),
        StoreMessage::FindEntities { id, .. } => Some(id.clone()),
        StoreMessage::FindEntitiesResponse { id, .. } => Some(id.clone()),
        StoreMessage::FindEntitiesExact { id, .. } => Some(id.clone()),
        StoreMessage::FindEntitiesExactResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetEntityTypes { id, .. } => Some(id.clone()),
        StoreMessage::GetEntityTypesResponse { id, .. } => Some(id.clone()),
        StoreMessage::TakeSnapshot { id, .. } => Some(id.clone()),
        StoreMessage::TakeSnapshotResponse { id, .. } => Some(id.clone()),
        StoreMessage::RestoreSnapshot { id, .. } => Some(id.clone()),
        StoreMessage::RestoreSnapshotResponse { id, .. } => Some(id.clone()),
        StoreMessage::RegisterNotification { id, .. } => Some(id.clone()),
        StoreMessage::RegisterNotificationResponse { id, .. } => Some(id.clone()),
        StoreMessage::UnregisterNotification { id, .. } => Some(id.clone()),
        StoreMessage::UnregisterNotificationResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetNotificationConfigs { id, .. } => Some(id.clone()),
        StoreMessage::GetNotificationConfigsResponse { id, .. } => Some(id.clone()),
        StoreMessage::Error { id, .. } => Some(id.clone()),
        StoreMessage::Notification { .. } => None, // Notifications don't have request IDs
    }
}

/// A trait that defines the interface for a Store proxy
/// This allows different implementations (WebSocket, HTTP, etc.) while keeping the same interface
pub trait StoreProxy {
    type Error;
    
    /// Create a new entity
    fn create_entity(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> impl std::future::Future<Output = std::result::Result<Entity, Self::Error>> + Send;

    /// Delete an entity
    fn delete_entity(
        &self, 
        ctx: &Context, 
        entity_id: &EntityId
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

    /// Set entity schema
    fn set_entity_schema(
        &self,
        ctx: &Context,
        schema: &EntitySchema<Single>,
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

    /// Get entity schema
    fn get_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> impl std::future::Future<Output = std::result::Result<Option<EntitySchema<Single>>, Self::Error>> + Send;

    /// Get complete entity schema
    fn get_complete_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> impl std::future::Future<Output = std::result::Result<EntitySchema<Complete>, Self::Error>> + Send;

    /// Set field schema
    fn set_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        schema: FieldSchema,
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

    /// Get field schema
    fn get_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> impl std::future::Future<Output = std::result::Result<Option<FieldSchema>, Self::Error>> + Send;

    /// Check if entity exists
    fn entity_exists(
        &self, 
        ctx: &Context, 
        entity_id: &EntityId
    ) -> impl std::future::Future<Output = bool> + Send;

    /// Check if field exists
    fn field_exists(
        &self,
        ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
    ) -> impl std::future::Future<Output = bool> + Send;

    /// Perform requests
    fn perform(
        &self, 
        ctx: &Context, 
        requests: &mut Vec<Request>
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

    /// Find entities
    fn find_entities(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    ) -> impl std::future::Future<Output = std::result::Result<PageResult<EntityId>, Self::Error>> + Send;

    /// Find entities exact
    fn find_entities_exact(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    ) -> impl std::future::Future<Output = std::result::Result<PageResult<EntityId>, Self::Error>> + Send;

    /// Get entity types
    fn get_entity_types(
        &self,
        ctx: &Context,
        parent_type: Option<EntityType>,
        page_opts: Option<PageOpts>,
    ) -> impl std::future::Future<Output = std::result::Result<PageResult<EntityType>, Self::Error>> + Send;

    /// Take snapshot
    fn take_snapshot(
        &self, 
        ctx: &Context
    ) -> impl std::future::Future<Output = crate::data::store::Snapshot> + Send;

    /// Restore snapshot
    fn restore_snapshot(
        &self, 
        ctx: &Context, 
        snapshot: crate::data::store::Snapshot
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

    /// Register notification
    fn register_notification(
        &self,
        ctx: &Context,
        config: NotifyConfig,
        callback: NotificationCallback,
    ) -> impl std::future::Future<Output = std::result::Result<NotifyToken, Self::Error>> + Send;

    /// Unregister notification by token
    fn unregister_notification_by_token(
        &self, 
        token: &NotifyToken
    ) -> impl std::future::Future<Output = bool> + Send;

    /// Get notification configs
    fn get_notification_configs(
        &self, 
        ctx: &Context
    ) -> impl std::future::Future<Output = Vec<(NotifyToken, NotifyConfig)>> + Send;
}


