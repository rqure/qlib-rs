use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, Notification, NotificationSender, NotifyConfig, hash_notify_config, PageOpts, PageResult, Request, Result, Single
};

/// Result of authentication attempt
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthenticationResult {
    /// The authenticated subject ID
    pub subject_id: EntityId,
    /// Subject type (User or Service)
    pub subject_type: String,
}

/// WebSocket message types for Store proxy communication
/// These messages are compatible with the qcore-rs WebSocketMessage format
#[derive(Debug, Serialize, Deserialize)]
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
    },
    FindEntitiesResponse {
        id: String,
        response: std::result::Result<PageResult<EntityId>, String>,
    },

    FindEntitiesExact {
        id: String,
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
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

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug)]
pub struct StoreProxy {
    sender: Arc<Mutex<futures_util::stream::SplitSink<WsStream, Message>>>,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    // Map from config hash to list of notification senders
    notification_configs: Arc<RwLock<HashMap<u64, Vec<NotificationSender>>>>,
    // Authentication state - set once during connection
    authenticated_subject: Option<EntityId>,
}

impl StoreProxy {
    /// Connect to WebSocket server and authenticate immediately
    /// If authentication fails, the connection will be closed by the server
    pub async fn connect_and_authenticate(
        url: &str,
        subject_name: &str,
        credential: &str,
    ) -> Result<Self> {
        // Connect to WebSocket
        let (ws_stream, _) = connect_async(url).await
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect: {}", e)))?;

        let (sender, mut receiver) = ws_stream.split();
        let sender = Arc::new(Mutex::new(sender));
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));
        let notification_configs = Arc::new(RwLock::new(HashMap::new()));

        // Send authentication message immediately
        let auth_request = StoreMessage::Authenticate {
            id: Uuid::new_v4().to_string(),
            subject_name: subject_name.to_string(),
            credential: credential.to_string(),
        };

        let auth_message = serde_json::to_string(&auth_request)
            .map_err(|e| Error::StoreProxyError(format!("Failed to serialize auth request: {}", e)))?;

        sender.lock().await.send(Message::Text(auth_message)).await
            .map_err(|e| Error::StoreProxyError(format!("Failed to send auth message: {}", e)))?;

        // Wait for authentication response
        let auth_response = receiver.next().await
            .ok_or_else(|| Error::StoreProxyError("Connection closed during authentication".to_string()))?
            .map_err(|e| Error::StoreProxyError(format!("WebSocket error during auth: {}", e)))?;

        let auth_response_text = match auth_response {
            Message::Text(text) => text,
            Message::Close(_) => return Err(Error::StoreProxyError("Authentication failed - connection closed".to_string())),
            _ => return Err(Error::StoreProxyError("Unexpected message type during authentication".to_string())),
        };

        let auth_result: StoreMessage = serde_json::from_str(&auth_response_text)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse auth response: {}", e)))?;

        let authenticated_subject = match auth_result {
            StoreMessage::AuthenticateResponse { response, .. } => {
                match response {
                    Ok(auth_result) => auth_result.subject_id,
                    Err(error) => return Err(Error::StoreProxyError(format!("Authentication failed: {}", error))),
                }
            }
            _ => return Err(Error::StoreProxyError("Unexpected response to authentication".to_string())),
        };

        // Start background task to handle incoming messages
        let pending_requests_clone = pending_requests.clone();
        let notification_configs_clone = notification_configs.clone();
        tokio::spawn(async move {
            StoreProxy::handle_incoming_messages(receiver, pending_requests_clone, notification_configs_clone).await;
        });

        Ok(StoreProxy {
            sender,
            pending_requests,
            notification_configs,
            authenticated_subject: Some(authenticated_subject),
        })
    }

    /// Handle incoming messages from the WebSocket
    async fn handle_incoming_messages(
        mut receiver: futures_util::stream::SplitStream<WsStream>,
        pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
        notification_configs: Arc<RwLock<HashMap<u64, Vec<NotificationSender>>>>,
    ) {
        while let Some(message) = receiver.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if let Ok(store_message) = serde_json::from_str::<StoreMessage>(&text) {
                        match store_message {
                            StoreMessage::Notification { notification } => {
                                // Handle notification
                                let config_hash = notification.config_hash;
                                let configs = notification_configs.read().await;
                                if let Some(senders) = configs.get(&config_hash) {
                                    for sender in senders {
                                        let _ = sender.send(notification.clone());
                                    }
                                }
                            }
                            _ => {
                                // Handle response to pending request
                                if let Some(id) = extract_message_id(&store_message) {
                                    let mut pending = pending_requests.lock().await;
                                    if let Some(sender) = pending.remove(&id) {
                                        let response_json = serde_json::to_value(store_message).unwrap_or_default();
                                        let _ = sender.send(response_json);
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    break;
                }
                Err(_) => {
                    break;
                }
                _ => {} // Ignore other message types
            }
        }
    }

    /// Send a request and wait for response
    async fn send_request(&self, request: StoreMessage) -> Result<StoreMessage> {
        let id = extract_message_id(&request)
            .ok_or_else(|| Error::StoreProxyError("Request missing ID".to_string()))?;

        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(id.clone(), tx);

        let message_text = serde_json::to_string(&request)
            .map_err(|e| Error::StoreProxyError(format!("Failed to serialize request: {}", e)))?;

        self.sender.lock().await.send(Message::Text(message_text)).await
            .map_err(|e| Error::StoreProxyError(format!("Failed to send message: {}", e)))?;

        let response_json = rx.await
            .map_err(|_| Error::StoreProxyError("Request timeout or connection closed".to_string()))?;

        let response: StoreMessage = serde_json::from_value(response_json)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse response: {}", e)))?;

        Ok(response)
    }

    /// Get the authenticated subject ID
    pub fn get_authenticated_subject(&self) -> Option<&EntityId> {
        self.authenticated_subject.as_ref()
    }
    /// Check if entity exists
    pub async fn get_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        let request = StoreMessage::GetEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
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
    pub async fn get_complete_entity_schema(
        &self,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let request = StoreMessage::GetCompleteEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetCompleteEntitySchemaResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Set field schema
    pub async fn set_field_schema(
        &mut self,
        entity_type: &EntityType,
        field_type: &FieldType,
        schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(entity_type).await?;
        entity_schema
            .fields
            .insert(field_type.clone(), schema);

        let mut requests = vec![Request::SchemaUpdate { schema: entity_schema, originator: None }];
        self.perform(&mut requests).await
    }

    /// Get field schema
    pub async fn get_field_schema(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        let request = StoreMessage::GetFieldSchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
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
    pub async fn entity_exists(&self, entity_id: &EntityId) -> bool {
        let request = StoreMessage::EntityExists {
            id: Uuid::new_v4().to_string(),
            entity_id: entity_id.clone(),
        };

        if let Ok(response) = self.send_request(request).await {
            match response {
                StoreMessage::EntityExistsResponse { response, .. } => response,
                _ => false, // If we get an unexpected response, assume entity does not exist
            }
        } else {
            false
        }
    }

    /// Check if field exists
    pub async fn field_exists(
        &self,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        let request = StoreMessage::FieldExists {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
        };

        if let Ok(response) = self.send_request(request).await {
            match response {
                StoreMessage::FieldExistsResponse { response, .. } => response,
                _ => false, // If we get an unexpected response, assume field does not exist
            }
        } else {
            false
        }
    }

    /// Perform requests
    pub async fn perform(&mut self, requests: &mut Vec<Request>) -> Result<()> {
        let request = StoreMessage::Perform {
            id: Uuid::new_v4().to_string(),
            requests: requests.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::PerformResponse { response, .. } => match response {
                Ok(updated_requests) => {
                    *requests = updated_requests;
                    Ok(())
                }
                Err(e) => Err(Error::StoreProxyError(e)),
            },
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Find entities
    pub async fn find_entities_paginated(
        &self,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntities {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::FindEntitiesResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Find entities exact
    pub async fn find_entities_exact(
        &self,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntitiesExact {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::FindEntitiesExactResponse { response, .. } => {
                response.map_err(|e| Error::StoreProxyError(e))
            }
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    pub async fn find_entities(
        &self,
        entity_type: &EntityType,
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .find_entities_paginated(entity_type, page_opts.clone())
                .await?;
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

    pub async fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .get_entity_types_paginated(page_opts)
                .await?;
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
    pub async fn get_entity_types_paginated(
        &self,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let request = StoreMessage::GetEntityTypes {
            id: Uuid::new_v4().to_string(),
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
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
    pub async fn register_notification(
        &mut self,
        config: NotifyConfig,
        sender: NotificationSender,
    ) -> Result<()> {
        let request = StoreMessage::RegisterNotification {
            id: Uuid::new_v4().to_string(),
            config: config.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::RegisterNotificationResponse { response, .. } => {
                match response {
                    Ok(_) => {
                        // Store the sender locally so we can forward notifications
                        let config_hash = hash_notify_config(&config);
                        let mut configs = self.notification_configs.write().await;
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
   pub async fn unregister_notification(&mut self, target_config: &NotifyConfig, sender: &NotificationSender) -> bool {
        // First remove the sender from local mapping
        let config_hash = hash_notify_config(target_config);
        let mut configs = self.notification_configs.write().await;
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

            if let Ok(response) = self.send_request(request).await {
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
