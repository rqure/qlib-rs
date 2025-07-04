use serde::{Deserialize, Serialize};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, oneshot, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::{
    Context, Entity, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, 
    NotifyConfig, NotifyToken, PageOpts, PageResult, Request, 
    Single, Complete, Notification, Snapshot,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
    
    // Script execution
    ExecuteScript {
        id: String,
        script: String,
    },
    ExecuteScriptResponse {
        id: String,
        response: std::result::Result<bool, String>,
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
        StoreMessage::ExecuteScript { id, .. } => Some(id.clone()),
        StoreMessage::ExecuteScriptResponse { id, .. } => Some(id.clone()),
        StoreMessage::Error { id, .. } => Some(id.clone()),
        StoreMessage::Notification { .. } => None, // Notifications don't have request IDs
    }
}

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone)]
pub struct StoreProxyError(String);

impl std::fmt::Display for StoreProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreProxy error: {}", self.0)
    }
}

impl std::error::Error for StoreProxyError {}

pub struct StoreProxy {
    sender: Arc<Mutex<futures_util::stream::SplitSink<WsStream, Message>>>,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    notification_sender: Arc<RwLock<Option<mpsc::UnboundedSender<Notification>>>>,
}

impl StoreProxy {
    /// Connect to a qcore-rs WebSocket server
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(url).await
            .map_err(|e| StoreProxyError(format!("Failed to connect to WebSocket: {}", e)))?;

        let (sink, mut stream) = ws_stream.split();
        let sender = Arc::new(Mutex::new(sink));
        let pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>> = Arc::new(Mutex::new(HashMap::new()));
        let notification_sender: Arc<RwLock<Option<mpsc::UnboundedSender<Notification>>>> = Arc::new(RwLock::new(None));

        // Spawn task to handle incoming messages
        let pending_requests_clone = pending_requests.clone();
        let notification_sender_clone = notification_sender.clone();
        
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(store_msg) = serde_json::from_str::<StoreMessage>(&text) {
                            match store_msg {
                                StoreMessage::Notification { notification } => {
                                    // Handle notification
                                    if let Some(sender) = notification_sender_clone.read().await.as_ref() {
                                        let _ = sender.send(notification);
                                    }
                                }
                                _ => {
                                    // Handle response messages
                                    if let Some(id) = extract_message_id(&store_msg) {
                                        if let Some(sender) = pending_requests_clone.lock().await.remove(&id) {
                                            let response_json = serde_json::to_value(&store_msg)
                                                .unwrap_or_else(|_| serde_json::Value::Null);
                                            let _ = sender.send(response_json);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(e) => {
                        log::error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {} // Ignore other message types
                }
            }
        });

        Ok(StoreProxy {
            sender,
            pending_requests,
            notification_sender,
        })
    }

    /// Send a request and wait for response
    async fn send_request<T>(&self, request: StoreMessage) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let id = extract_message_id(&request)
            .ok_or_else(|| StoreProxyError("Request missing ID".to_string()))?;

        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(id.clone(), tx);

        let message_text = serde_json::to_string(&request)
            .map_err(|e| StoreProxyError(format!("Failed to serialize request: {}", e)))?;

        self.sender.lock().await.send(Message::Text(message_text)).await
            .map_err(|e| StoreProxyError(format!("Failed to send message: {}", e)))?;

        let response = rx.await
            .map_err(|_| StoreProxyError("Request cancelled".to_string()))?;

        serde_json::from_value(response)
            .map_err(|e| StoreProxyError(format!("Failed to deserialize response: {}", e)).into())
    }

    /// Create a new entity
    pub async fn create_entity(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        let request = StoreMessage::CreateEntity {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            parent_id,
            name: name.to_string(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::CreateEntityResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Delete an entity
    pub async fn delete_entity(&self, _ctx: &Context, entity_id: &EntityId) -> Result<()> {
        let request = StoreMessage::DeleteEntity {
            id: Uuid::new_v4().to_string(),
            entity_id: entity_id.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::DeleteEntityResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Set entity schema
    pub async fn set_entity_schema(
        &self,
        _ctx: &Context,
        schema: &EntitySchema<Single>,
    ) -> Result<()> {
        let request = StoreMessage::SetEntitySchema {
            id: Uuid::new_v4().to_string(),
            schema: schema.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::SetEntitySchemaResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Get entity schema
    pub async fn get_entity_schema(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<Option<EntitySchema<Single>>> {
        let request = StoreMessage::GetEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetEntitySchemaResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Get complete entity schema
    pub async fn get_complete_entity_schema(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let request = StoreMessage::GetCompleteEntitySchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetCompleteEntitySchemaResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Set field schema
    pub async fn set_field_schema(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        schema: &FieldSchema,
    ) -> Result<()> {
        let request = StoreMessage::SetFieldSchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
            schema: schema.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::SetFieldSchemaResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Get field schema
    pub async fn get_field_schema(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<Option<FieldSchema>> {
        let request = StoreMessage::GetFieldSchema {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            field_type: field_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetFieldSchemaResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Check if entity exists
    pub async fn entity_exists(&self, _ctx: &Context, entity_id: &EntityId) -> Result<bool> {
        let request = StoreMessage::EntityExists {
            id: Uuid::new_v4().to_string(),
            entity_id: entity_id.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::EntityExistsResponse { response, .. } => Ok(response),
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Check if field exists
    pub async fn field_exists(
        &self,
        _ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
    ) -> Result<bool> {
        let request = StoreMessage::FieldExists {
            id: Uuid::new_v4().to_string(),
            entity_id: entity_id.clone(),
            field_type: field_type.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::FieldExistsResponse { response, .. } => Ok(response),
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Perform requests
    pub async fn perform(&self, _ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
        let request = StoreMessage::Perform {
            id: Uuid::new_v4().to_string(),
            requests: requests.clone(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::PerformResponse { response, .. } => {
                match response {
                    Ok(updated_requests) => {
                        *requests = updated_requests;
                        Ok(())
                    }
                    Err(e) => Err(StoreProxyError(e).into()),
                }
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Find entities
    pub async fn find_entities(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntities {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            parent_id,
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::FindEntitiesResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Find entities exact
    pub async fn find_entities_exact(
        &self,
        _ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        let request = StoreMessage::FindEntitiesExact {
            id: Uuid::new_v4().to_string(),
            entity_type: entity_type.clone(),
            parent_id,
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::FindEntitiesExactResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Get entity types
    pub async fn get_entity_types(
        &self,
        _ctx: &Context,
        parent_type: Option<EntityType>,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let request = StoreMessage::GetEntityTypes {
            id: Uuid::new_v4().to_string(),
            parent_type,
            page_opts,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetEntityTypesResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Take snapshot
    pub async fn take_snapshot(&self, _ctx: &Context) -> Result<Snapshot> {
        let request = StoreMessage::TakeSnapshot {
            id: Uuid::new_v4().to_string(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::TakeSnapshotResponse { response, .. } => Ok(response),
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Restore snapshot
    pub async fn restore_snapshot(&self, _ctx: &Context, snapshot: Snapshot) -> Result<()> {
        let request = StoreMessage::RestoreSnapshot {
            id: Uuid::new_v4().to_string(),
            snapshot,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::RestoreSnapshotResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Register notification
    pub async fn register_notification(
        &self,
        _ctx: &Context,
        config: NotifyConfig,
    ) -> Result<NotifyToken> {
        let request = StoreMessage::RegisterNotification {
            id: Uuid::new_v4().to_string(),
            config,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::RegisterNotificationResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Unregister notification
    pub async fn unregister_notification(&self, _ctx: &Context, token: NotifyToken) -> Result<bool> {
        let request = StoreMessage::UnregisterNotification {
            id: Uuid::new_v4().to_string(),
            token,
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::UnregisterNotificationResponse { response, .. } => Ok(response),
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Get notification configs
    pub async fn get_notification_configs(
        &self,
        _ctx: &Context,
    ) -> Result<Vec<(NotifyToken, NotifyConfig)>> {
        let request = StoreMessage::GetNotificationConfigs {
            id: Uuid::new_v4().to_string(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::GetNotificationConfigsResponse { response, .. } => Ok(response),
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }

    /// Subscribe to notifications
    /// Returns a receiver that will get notification events
    pub async fn subscribe_notifications(&self) -> mpsc::UnboundedReceiver<Notification> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.notification_sender.write().await = Some(tx);
        rx
    }

    /// Stop notification subscription
    pub async fn unsubscribe_notifications(&self) {
        *self.notification_sender.write().await = None;
    }

    /// Execute a script on the cluster
    pub async fn execute_script(&self, _ctx: &Context, script: &str) -> Result<bool> {
        let request = StoreMessage::ExecuteScript {
            id: Uuid::new_v4().to_string(),
            script: script.to_string(),
        };

        let response: StoreMessage = self.send_request(request).await?;
        match response {
            StoreMessage::ExecuteScriptResponse { response, .. } => {
                response.map_err(|e| StoreProxyError(e).into())
            }
            _ => Err(StoreProxyError("Unexpected response type".to_string()).into()),
        }
    }
}
