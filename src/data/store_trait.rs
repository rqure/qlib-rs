use std::collections::HashMap;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationQueue, NotifyConfig,
    PageOpts, PageResult, Request, Result, Single, Complete,
};

use crate::protocol::{FastStoreMessage, FastStoreMessageType};

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
    fn get_complete_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Complete>>;

    /// Get the schema for a specific field
    fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema>;

    /// Set or update the schema for a specific field
    fn set_field_schema(&mut self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()>;

    /// Check if an entity exists
    fn entity_exists(&self, entity_id: EntityId) -> bool;

    /// Check if a field type exists for an entity type
    fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool;

    /// Perform a batch of requests
    fn perform(&self, requests: Vec<Request>) -> Result<Vec<Request>>;
    fn perform_mut(&mut self, requests: Vec<Request>) -> Result<Vec<Request>>;

    fn perform_map(&self, requests: Vec<Request>) -> Result<HashMap<Vec<FieldType>, Request>> {
        let updated_requests = self.perform(requests)?;

        let mut result_map = HashMap::new();
        for request in updated_requests.iter() {
            if let Some(field_type) = request.field_type() {
                result_map.insert(field_type.clone(), request.clone());
            }
        }

        Ok(result_map)
    }

    /// Find entities of a specific type with pagination (includes inherited types)
    fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>>;

    /// Find entities of exactly the specified type (no inheritance) with pagination
    fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>>;

    /// Find all entities of a specific type (includes inherited types)
    fn find_entities(&self, entity_type: EntityType, filter: Option<String>) -> Result<Vec<EntityId>>;

    /// Get all entity types
    fn get_entity_types(&self) -> Result<Vec<EntityType>>;

    /// Get all entity types with pagination
    fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>>;

    /// Register a notification with a provided sender
    fn register_notification(&mut self, config: NotifyConfig, sender: NotificationQueue) -> Result<()>;

    /// Unregister a notification by removing a specific sender
    fn unregister_notification(&mut self, config: &NotifyConfig, sender: &NotificationQueue) -> bool;

    /// Process a FastStoreMessage directly without bincode deserialization
    /// This provides true zero-copy performance for simple operations
    fn process_fast_message(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        match &fast_message.message {
            FastStoreMessageType::EntityExists { entity_id } => {
                let response = self.entity_exists(*entity_id);
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::EntityExistsResponse { response },
                }))
            },
            FastStoreMessageType::FieldExists { entity_type, field_type } => {
                let response = self.field_exists(*entity_type, *field_type);
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::FieldExistsResponse { response },
                }))
            },
            FastStoreMessageType::GetEntityType { name } => {
                let response = self.get_entity_type(name)
                    .map_err(|e| format!("{:?}", e));
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::GetEntityTypeResponse { response },
                }))
            },
            FastStoreMessageType::ResolveEntityType { entity_type } => {
                let response = self.resolve_entity_type(*entity_type)
                    .map_err(|e| format!("{:?}", e));
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::ResolveEntityTypeResponse { response },
                }))
            },
            FastStoreMessageType::GetFieldType { name } => {
                let response = self.get_field_type(name)
                    .map_err(|e| format!("{:?}", e));
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::GetFieldTypeResponse { response },
                }))
            },
            FastStoreMessageType::ResolveFieldType { field_type } => {
                let response = self.resolve_field_type(*field_type)
                    .map_err(|e| format!("{:?}", e));
                Ok(Some(FastStoreMessage {
                    id: fast_message.id.clone(),
                    message: FastStoreMessageType::ResolveFieldTypeResponse { response },
                }))
            },
            // For request messages that don't need responses, return None
            FastStoreMessageType::AuthenticateResponse { .. } |
            FastStoreMessageType::EntityExistsResponse { .. } |
            FastStoreMessageType::FieldExistsResponse { .. } |
            FastStoreMessageType::GetEntityTypeResponse { .. } |
            FastStoreMessageType::ResolveEntityTypeResponse { .. } |
            FastStoreMessageType::GetFieldTypeResponse { .. } |
            FastStoreMessageType::ResolveFieldTypeResponse { .. } => {
                // These are response messages, no further processing needed
                Ok(None)
            },
            // For operations that need mutable access or are complex, fall back to StoreMessage conversion
            FastStoreMessageType::Authenticate { .. } |
            FastStoreMessageType::ComplexOperation { .. } => {
                // These require special handling or mutable access
                Ok(None)
            },
        }
    }

    /// Process a FastStoreMessage directly with mutable access
    fn process_fast_message_mut(&mut self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Handle authentication which requires mutable access
        if let FastStoreMessageType::Authenticate { subject_name: _, credential: _ } = &fast_message.message {
            // This would typically involve checking credentials against a database
            // For now, we'll create a simple success response
            // In a real implementation, this would authenticate against a user store
            let auth_result = crate::protocol::FastAuthenticationResult {
                subject_id: EntityId::new(EntityType(1), 1), // Dummy authenticated user
                subject_type: "User".to_string(),
            };
            
            return Ok(Some(FastStoreMessage {
                id: fast_message.id.clone(),
                message: FastStoreMessageType::AuthenticateResponse { 
                    response: Ok(auth_result) 
                },
            }));
        }

        // For other operations, delegate to the immutable version
        self.process_fast_message(fast_message)
    }
}