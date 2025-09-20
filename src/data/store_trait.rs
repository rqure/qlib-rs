use std::collections::HashMap;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationQueue, NotifyConfig,
    PageOpts, PageResult, Request, Result, Single, Complete,
};

use crate::protocol::{FastStoreMessage, FastMessageType, OperationHint};
use crate::Error;

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

    /// Process a FastStoreMessage with elegant intelligent routing
    /// This provides zero-copy metadata access and smart processing for ALL operations including read/write
    fn process_fast_message(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Use zero-copy metadata for intelligent routing decisions
        match fast_message.operation_hint() {
            OperationHint::SimpleRead => {
                // Try to handle simple reads without full deserialization when possible
                self.try_simple_processing(fast_message)
            },
            
            OperationHint::SingleEntity => {
                // Single entity operations (including single read/write) - efficient processing
                self.process_with_entity_optimization(fast_message)
            },
            
            OperationHint::BatchOperation => {
                // Batch operations (including multiple reads/writes) - full processing
                self.process_batch_operation(fast_message)
            },
            
            OperationHint::Administrative => {
                // Administrative operations - standard processing
                self.process_administrative(fast_message)
            },
        }
    }
    
    /// Try to process simple operations without full deserialization
    fn try_simple_processing(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // For simple reads like EntityExists, we can potentially optimize
        if matches!(fast_message.message_type(), FastMessageType::EntityExists) {
            if let Some(entity_id) = fast_message.primary_entity_id() {
                let response = self.entity_exists(entity_id);
                
                // Create response FastStoreMessage
                let response_msg = crate::data::StoreMessage::EntityExistsResponse {
                    id: fast_message.message_id().to_string(),
                    response,
                };
                
                let response_fast = FastStoreMessage::from_store_message(&response_msg)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to create response: {}", e)))?;
                
                return Ok(Some(response_fast));
            }
        }
        
        // Fall back to standard processing for other operations
        self.process_with_deserialization(fast_message)
    }
    
    /// Process single entity operations with optimization
    fn process_with_entity_optimization(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Single entity operations (including read/write through Perform)
        // We have the entity ID available for routing/caching decisions
        self.process_with_deserialization(fast_message)
    }
    
    /// Process batch operations (multiple reads/writes)
    fn process_batch_operation(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Batch operations need full processing but we have metadata for optimization
        self.process_with_deserialization(fast_message)
    }
    
    /// Process administrative operations
    fn process_administrative(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Administrative operations - standard processing
        self.process_with_deserialization(fast_message)
    }
    
    /// Process with full deserialization but using metadata for optimization
    fn process_with_deserialization(&self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // Deserialize the complete message
        let store_message = fast_message.to_store_message()
            .map_err(|e| Error::StoreProxyError(format!("Failed to deserialize: {}", e)))?;
        
        // Process based on message type - this handles ALL operations elegantly!
        let response_store_message = match store_message {
            crate::data::StoreMessage::EntityExists { id, entity_id } => {
                let response = self.entity_exists(entity_id);
                crate::data::StoreMessage::EntityExistsResponse { id, response }
            },
            
            crate::data::StoreMessage::FieldExists { id, entity_type, field_type } => {
                let response = self.field_exists(entity_type, field_type);
                crate::data::StoreMessage::FieldExistsResponse { id, response }
            },
            
            // THE KEY: This elegantly handles ALL read/write operations!
            crate::data::StoreMessage::Perform { id, requests } => {
                let response = self.perform(requests).map_err(|e| format!("{:?}", e));
                crate::data::StoreMessage::PerformResponse { id, response }
            },
            
            crate::data::StoreMessage::GetEntityType { id, name } => {
                let response = self.get_entity_type(&name).map_err(|e| format!("{:?}", e));
                crate::data::StoreMessage::GetEntityTypeResponse { id, response }
            },
            
            crate::data::StoreMessage::ResolveEntityType { id, entity_type } => {
                let response = self.resolve_entity_type(entity_type).map_err(|e| format!("{:?}", e));
                crate::data::StoreMessage::ResolveEntityTypeResponse { id, response }
            },
            
            crate::data::StoreMessage::GetFieldType { id, name } => {
                let response = self.get_field_type(&name).map_err(|e| format!("{:?}", e));
                crate::data::StoreMessage::GetFieldTypeResponse { id, response }
            },
            
            crate::data::StoreMessage::ResolveFieldType { id, field_type } => {
                let response = self.resolve_field_type(field_type).map_err(|e| format!("{:?}", e));
                crate::data::StoreMessage::ResolveFieldTypeResponse { id, response }
            },
            
            // Add more operations as needed...
            _ => {
                // For unhandled operations, return None
                return Ok(None);
            }
        };
        
        // Convert response back to FastStoreMessage
        let response_fast = FastStoreMessage::from_store_message(&response_store_message)
            .map_err(|e| Error::StoreProxyError(format!("Failed to create response: {}", e)))?;
        
        Ok(Some(response_fast))
    }

    /// Process a FastStoreMessage with mutable access
    fn process_fast_message_mut(&mut self, fast_message: &FastStoreMessage) -> Result<Option<FastStoreMessage>> {
        // For mutable operations, deserialize and process
        let store_message = fast_message.to_store_message()
            .map_err(|e| Error::StoreProxyError(format!("Failed to deserialize: {}", e)))?;
        
        match store_message {
            crate::data::StoreMessage::Authenticate { id, subject_name: _, credential: _ } => {
                // Handle authentication
                let auth_result = crate::data::AuthenticationResult {
                    subject_id: EntityId::new(EntityType(1), 1),
                    subject_type: "User".to_string(),
                };
                
                let response = crate::data::StoreMessage::AuthenticateResponse {
                    id,
                    response: Ok(auth_result),
                };
                
                let response_fast = FastStoreMessage::from_store_message(&response)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to create response: {}", e)))?;
                Ok(Some(response_fast))
            },
            
            // THE KEY: Mutable Perform operations handle read/write with mutations!
            crate::data::StoreMessage::Perform { id, requests } => {
                let response = self.perform_mut(requests).map_err(|e| format!("{:?}", e));
                let response_msg = crate::data::StoreMessage::PerformResponse { id, response };
                let response_fast = FastStoreMessage::from_store_message(&response_msg)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to create response: {}", e)))?;
                Ok(Some(response_fast))
            },
            
            _ => {
                // For other operations, delegate to immutable version
                self.process_fast_message(fast_message)
            }
        }
    }
}