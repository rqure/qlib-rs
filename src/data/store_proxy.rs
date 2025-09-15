use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::net::TcpStream;
use std::io::{Read, Write};
use uuid::Uuid;
use anyhow;

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, Notification, NotificationQueue, NotifyConfig, hash_notify_config, PageOpts, PageResult, Request, Result, Single
};
use crate::data::StoreTrait;
use crate::protocol::{MessageBuffer, encode_store_message};

/// Result of authentication attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationResult {
    /// The authenticated subject ID
    pub subject_id: EntityId,
    /// Subject type (User or Service)
    pub subject_type: String,
}

/// TCP message types for Store proxy communication
/// These messages are compatible with the qcore-rs protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreMessage {
    // Authentication messages - MUST be first message from client
    Authenticate {
        id: String,
        subject_name: String,
        credential: String, // Password for users, secret key for services
    },
    AuthenticateResponse {
        id: String,
        response: std::result::Result<AuthenticationResult, String>,
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
        entity_type: EntityType,
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
        page_opts: Option<PageOpts>,
        filter: Option<String>,
    },
    FindEntitiesResponse {
        id: String,
        response: std::result::Result<PageResult<EntityId>, String>,
    },

    FindEntitiesExact {
        id: String,
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
    },
    FindEntitiesExactResponse {
        id: String,
        response: std::result::Result<PageResult<EntityId>, String>,
    },

    GetEntityTypes {
        id: String,
        page_opts: Option<PageOpts>,
    },
    GetEntityTypesResponse {
        id: String,
        response: std::result::Result<PageResult<EntityType>, String>,
    },

    // Notification support
    RegisterNotification {
        id: String,
        config: NotifyConfig,
    },
    RegisterNotificationResponse {
        id: String,
        response: std::result::Result<(), String>,
    },

    UnregisterNotification {
        id: String,
        config: NotifyConfig,
    },
    UnregisterNotificationResponse {
        id: String,
        response: bool,
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
        StoreMessage::Authenticate { id, .. } => Some(id.clone()),
        StoreMessage::AuthenticateResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetEntitySchema { id, .. } => Some(id.clone()),
        StoreMessage::GetEntitySchemaResponse { id, .. } => Some(id.clone()),
        StoreMessage::GetCompleteEntitySchema { id, .. } => Some(id.clone()),
        StoreMessage::GetCompleteEntitySchemaResponse { id, .. } => Some(id.clone()),
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
        StoreMessage::RegisterNotification { id, .. } => Some(id.clone()),
        StoreMessage::RegisterNotificationResponse { id, .. } => Some(id.clone()),
        StoreMessage::UnregisterNotification { id, .. } => Some(id.clone()),
        StoreMessage::UnregisterNotificationResponse { id, .. } => Some(id.clone()),
        StoreMessage::Error { id, .. } => Some(id.clone()),
        StoreMessage::Notification { .. } => None, // Notifications don't have request IDs
    }
}

/// TCP connection with message buffering
#[derive(Debug)]
pub struct TcpConnection {
    stream: TcpStream,
    message_buffer: MessageBuffer,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            message_buffer: MessageBuffer::new(),
        }
    }
    
    pub fn send_message(&mut self, message: &StoreMessage) -> anyhow::Result<()> {
        let encoded = encode_store_message(message)?;
        self.stream.write_all(&encoded)?;
        self.stream.flush()?;
        Ok(())
    }
    
    pub fn try_receive_message(&mut self) -> anyhow::Result<Option<StoreMessage>> {
        // Try to read more data
        let mut buffer = [0u8; 8192];
        match self.stream.read(&mut buffer) {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                self.message_buffer.add_data(&buffer[..bytes_read]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available right now
            }
            Err(e) => return Err(anyhow::anyhow!("TCP read error: {}", e)),
        }
        
        // Try to decode a message
        self.message_buffer.try_decode_store_message()
    }
}

#[derive(Debug)]
pub struct StoreProxy {
    tcp_connection: Rc<RefCell<TcpConnection>>,
    pending_requests: Rc<RefCell<HashMap<String, serde_json::Value>>>,
    // Map from config hash to list of notification senders
    notification_configs: Rc<RefCell<HashMap<u64, Vec<NotificationQueue>>>>,
    // Authentication state - set once during connection
    authenticated_subject: Option<EntityId>,
}

impl StoreProxy {
    /// Connect to TCP server and authenticate immediately
    /// If authentication fails, the connection will be closed by the server
    pub fn connect_and_authenticate(
        address: &str,
        subject_name: &str,
        credential: &str,
    ) -> Result<Self> {
        // Connect to TCP server
        let stream = TcpStream::connect(address)
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect to {}: {}", address, e)))?;
        
        // Set to non-blocking for message handling
        stream.set_nonblocking(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set non-blocking: {}", e)))?;

        let mut tcp_connection = TcpConnection::new(stream);

        // Send authentication message immediately
        let auth_request = StoreMessage::Authenticate {
            id: Uuid::new_v4().to_string(),
            subject_name: subject_name.to_string(),
            credential: credential.to_string(),
        };

        tcp_connection.send_message(&auth_request)
            .map_err(|e| Error::StoreProxyError(format!("Failed to send auth message: {}", e)))?;

        // Wait for authentication response with timeout
        let auth_start = std::time::Instant::now();
        let auth_timeout = std::time::Duration::from_secs(5); // 5 second timeout
        
        let auth_result = loop {
            if auth_start.elapsed() > auth_timeout {
                return Err(Error::StoreProxyError("Authentication timeout".to_string()));
            }
            
            match tcp_connection.try_receive_message() {
                Ok(Some(message)) => break message,
                Ok(None) => {
                    // No message yet, continue waiting
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(Error::StoreProxyError(format!("TCP error during auth: {}", e))),
            }
        };

        let authenticated_subject = match auth_result {
            StoreMessage::AuthenticateResponse { response, .. } => {
                match response {
                    Ok(auth_result) => auth_result.subject_id,
                    Err(error) => return Err(Error::StoreProxyError(format!("Authentication failed: {}", error))),
                }
            }
            _ => return Err(Error::StoreProxyError("Unexpected response to authentication".to_string())),
        };

        // Create single-threaded collections
        let tcp_connection = Rc::new(RefCell::new(tcp_connection));
        let pending_requests = Rc::new(RefCell::new(HashMap::new()));
        let notification_configs = Rc::new(RefCell::new(HashMap::new()));

        Ok(StoreProxy {
            tcp_connection,
            pending_requests,
            notification_configs,
            authenticated_subject: Some(authenticated_subject),
        })
    }

    /// Handle incoming messages from the TCP connection
    /// This is called synchronously to process messages as they arrive
    fn handle_incoming_message(
        &self,
        store_message: &StoreMessage,
    ) -> std::result::Result<(), String> {
        match store_message {
            StoreMessage::Notification { notification } => {
                // Handle notification
                let config_hash = notification.config_hash;
                let configs = self.notification_configs.borrow();
                if let Some(senders) = configs.get(&config_hash) {
                    for sender in senders {
                        sender.push(notification.clone());
                    }
                }
            }
            _ => {
                // Handle response to pending request
                if let Some(id) = extract_message_id(store_message) {
                    let response_json = serde_json::to_value(store_message).unwrap_or_default();
                    self.pending_requests.borrow_mut().insert(id, response_json);
                }
            }
        }
        Ok(())
    }

    /// Send a request and wait for response
    fn send_request(&self, request: StoreMessage) -> Result<StoreMessage> {
        let id = extract_message_id(&request)
            .ok_or_else(|| Error::StoreProxyError("Request missing ID".to_string()))?;

        // Send message
        self.tcp_connection.borrow_mut().send_message(&request)
            .map_err(|e| Error::StoreProxyError(format!("Failed to send message: {}", e)))?;

        // Wait for response by polling the TCP connection until we get our response
        loop {
            // Check if we already have the response
            if let Some(response_json) = self.pending_requests.borrow_mut().remove(&id) {
                let response: StoreMessage = serde_json::from_value(response_json)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to parse response: {}", e)))?;
                return Ok(response);
            }

            // Read next message from TCP connection
            match self.tcp_connection.borrow_mut().try_receive_message() {
                Ok(Some(message)) => {
                    if let Err(_) = self.handle_incoming_message(&message) {
                        // Handle error if needed
                    }
                }
                Ok(None) => {
                    // No message available, continue loop
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(e) => {
                    return Err(Error::StoreProxyError(format!("TCP error: {}", e)));
                }
            }
        }
    }

    /// Get the authenticated subject ID
    pub fn get_authenticated_subject(&self) -> Option<&EntityId> {
        self.authenticated_subject.as_ref()
    }
    /// Check if entity exists
    pub fn get_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        let request = StoreMessage::GetEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::GetEntitySchemaResponse { response, .. } => {
                response
                    .and_then(|s| {
                        if let Some(s) = s {
                            Ok(s)
                        } else {
                            Err("Schema not found".to_string())
                        }
                    })
                    .map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let request = StoreMessage::GetCompleteEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::GetCompleteEntitySchemaResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Set field schema
    pub fn set_field_schema(
        &mut self,
        entity_type: &EntityType,
        field_type: &FieldType,
        schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(entity_type)?;
        entity_schema
            .fields
            .insert(field_type.clone(), schema);

                let requests = vec![Request::SchemaUpdate { schema: entity_schema, timestamp: None, originator: None }];
        self.perform(requests).map(|_| ())
    }

    /// Get field schema
    pub fn get_field_schema(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        let request = StoreMessage::GetFieldSchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::GetFieldSchemaResponse { response, .. } => {
                response
                    .and_then(|s| {
                        if let Some(s) = s {
                            Ok(s)
                        } else {
                            Err("Field schema not found".to_string())
                        }
                    })
                    .map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: &EntityId) -> bool {
        let request = StoreMessage::EntityExists {
            id: Uuid::new_v4().to_string(),
            entity_id: entity_id.clone(),
        };

        if let Ok(response) = self.send_request(request) {
            match response {
                StoreMessage::EntityExistsResponse { response, .. } => response,
                _ => false, // If we get an unexpected response, assume entity does not exist
            }
        } else {
            false
        }
    }

    /// Check if field exists
    pub fn field_exists(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        let request = StoreMessage::FieldExists {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
        };

        if let Ok(response) = self.send_request(request) {
            match response {
                StoreMessage::FieldExistsResponse { response, .. } => response,
                _ => false, // If we get an unexpected response, assume field does not exist
            }
        } else {
            false
        }
    }

    /// Perform requests
    pub fn perform(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        let request = StoreMessage::Perform {
            id: Uuid::new_v4().to_string(),
            requests: requests.clone(),
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::PerformResponse { response, .. } => match response {
                Ok(updated_requests) => {
                    Ok(updated_requests)
                }
                Err(e) => Err(Error::StoreProxyError(e)),
            },
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Find entities
    pub fn find_entities_paginated(
        &self,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntities {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            page_opts,
            filter,
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::FindEntitiesResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Find entities exact
    pub fn find_entities_exact(
        &self,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntitiesExact {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            page_opts,
            filter,
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::FindEntitiesExactResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    pub fn find_entities(
        &self,
        entity_type: &EntityType,
        filter: Option<String>,
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .find_entities_paginated(entity_type, page_opts.clone(), filter.clone())
                ?;
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
                .get_entity_types_paginated(page_opts)
                ?;
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

    /// Get entity types
    pub fn get_entity_types_paginated(
        &self,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let request = StoreMessage::GetEntityTypes {
            id: Uuid::new_v4().to_string(),
            page_opts,
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::GetEntityTypesResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Register notification with provided sender
    /// Note: For proxy, this registers the notification on the remote server
    /// and stores the sender locally to forward notifications
    pub fn register_notification(
        &mut self,
        config: NotifyConfig,
        sender: NotificationQueue,
    ) -> Result<()> {
        let request = StoreMessage::RegisterNotification {
            id: Uuid::new_v4().to_string(),
            config: config.clone(),
        };

        let response: StoreMessage = self.send_request(request)?;
        match response {
            StoreMessage::RegisterNotificationResponse { response, .. } => {
                match response {
                    Ok(_) => {
                        // Store the sender locally so we can forward notifications
                        let config_hash = hash_notify_config(&config);
                        let mut configs = self.notification_configs.borrow_mut();
                        configs.entry(config_hash).or_insert_with(Vec::new).push(sender);
                        Ok(())
                    }
                    Err(e) => Err(Error::StoreProxyError(e)),
                }
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Unregister a notification by removing a specific sender
    /// Note: This will remove ALL notifications matching the config for proxy
   pub fn unregister_notification(&mut self, target_config: &NotifyConfig, sender: &NotificationQueue) -> bool {
        // First remove the sender from local mapping
        let config_hash = hash_notify_config(target_config);
        let mut configs = self.notification_configs.borrow_mut();
        let mut removed_locally = false;
        
        if let Some(senders) = configs.get_mut(&config_hash) {
            // Find and remove the specific sender by comparing addresses
            if let Some(pos) = senders.iter().position(|s| std::ptr::eq(s, sender)) {
                senders.remove(pos);
                removed_locally = true;
                
                // If no more senders for this config, remove the entry
                if senders.is_empty() {
                    configs.remove(&config_hash);
                }
            }
        }
        drop(configs);

        // If we removed a sender locally, also unregister from remote
        if removed_locally {
            let request = StoreMessage::UnregisterNotification {
                id: Uuid::new_v4().to_string(),
                config: target_config.clone(),
            };

            if let Ok(response) = self.send_request(request) {
                match response {
                    StoreMessage::UnregisterNotificationResponse { response, .. } => response,
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

}

impl StoreTrait for StoreProxy {
    fn get_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Single>> {
        self.get_entity_schema(entity_type)
    }

    fn get_complete_entity_schema(&self, entity_type: &EntityType) -> Result<EntitySchema<Complete>> {
        self.get_complete_entity_schema(entity_type)
    }

    fn get_field_schema(&self, entity_type: &EntityType, field_type: &FieldType) -> Result<FieldSchema> {
        self.get_field_schema(entity_type, field_type)
    }

    fn set_field_schema(&mut self, entity_type: &EntityType, field_type: &FieldType, schema: FieldSchema) -> Result<()> {
        self.set_field_schema(entity_type, field_type, schema)
    }

    fn entity_exists(&self, entity_id: &EntityId) -> bool {
        self.entity_exists(entity_id)
    }

    fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        self.field_exists(entity_type, field_type)
    }

    fn perform(&self, requests: Vec<Request>) -> Result<Vec<Request>> {
        self.perform(requests)
    }

    fn perform_mut(&mut self, requests: Vec<Request>) -> Result<Vec<Request>> {
        self.perform(requests)
    }

    fn find_entities_paginated(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        self.find_entities_paginated(entity_type, page_opts, filter)
    }

    fn find_entities_exact(&self, entity_type: &EntityType, page_opts: Option<PageOpts>, filter: Option<String>) -> Result<PageResult<EntityId>> {
        self.find_entities_exact(entity_type, page_opts, filter)
    }

    fn find_entities(&self, entity_type: &EntityType, filter: Option<String>) -> Result<Vec<EntityId>> {
        self.find_entities(entity_type, filter)
    }

    fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        self.get_entity_types()
    }

    fn get_entity_types_paginated(&self, page_opts: Option<PageOpts>) -> Result<PageResult<EntityType>> {
        self.get_entity_types_paginated(page_opts)
    }

    fn register_notification(&mut self, config: NotifyConfig, sender: NotificationQueue) -> Result<()> {
        self.register_notification(config, sender)
    }

    fn unregister_notification(&mut self, config: &NotifyConfig, sender: &NotificationQueue) -> bool {
        self.unregister_notification(config, sender)
    }
}
