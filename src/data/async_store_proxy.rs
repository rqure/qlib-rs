use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use anyhow::{Result as AnyhowResult};

use crate::{
    EntityId, EntityType, FieldType, PageOpts, PageResult, Request, Requests, Result, Error
};
use crate::data::StoreMessage;
use crate::protocol::{MessageBuffer, encode_store_message};

/// Internal message for communication with the connection task
#[derive(Debug)]
enum ConnectionMessage {
    Send {
        message: StoreMessage,
        response_sender: oneshot::Sender<StoreMessage>,
    },
    Shutdown,
}

/// Async TCP connection handler
struct ConnectionHandler {
    stream: TcpStream,
    message_buffer: MessageBuffer,
    pending_requests: HashMap<u64, oneshot::Sender<StoreMessage>>,
    receiver: mpsc::UnboundedReceiver<ConnectionMessage>,
}

impl ConnectionHandler {
    fn new(stream: TcpStream, receiver: mpsc::UnboundedReceiver<ConnectionMessage>) -> Self {
        Self {
            stream,
            message_buffer: MessageBuffer::new(),
            pending_requests: HashMap::new(),
            receiver,
        }
    }

    async fn run(mut self) -> AnyhowResult<()> {
        let mut buffer = [0u8; 8192];
        
        loop {
            tokio::select! {
                // Handle incoming messages from the network
                result = self.stream.read(&mut buffer) => {
                    match result {
                        Ok(0) => {
                            // Connection closed
                            break;
                        }
                        Ok(n) => {
                            self.message_buffer.add_data(&buffer[..n]);
                            
                            // Process any complete messages
                            while let Some(message) = self.message_buffer.try_decode()? {
                                self.handle_incoming_message(message).await;
                            }
                        }
                        Err(e) => {
                            eprintln!("Read error: {}", e);
                            break;
                        }
                    }
                }
                
                // Handle outgoing messages from the application
                msg = self.receiver.recv() => {
                    match msg {
                        Some(ConnectionMessage::Send { message, response_sender }) => {
                            if let Err(e) = self.handle_send_message(message, response_sender).await {
                                eprintln!("Send error: {}", e);
                                break;
                            }
                        }
                        Some(ConnectionMessage::Shutdown) | None => {
                            break;
                        }
                    }
                }
            }
        }

        // Clean up any pending requests
        for (_, sender) in self.pending_requests.drain() {
            let _ = sender.send(StoreMessage::Error {
                id: 0,
                error: "Connection closed".to_string(),
            });
        }

        Ok(())
    }

    async fn handle_send_message(
        &mut self,
        message: StoreMessage,
        response_sender: oneshot::Sender<StoreMessage>,
    ) -> AnyhowResult<()> {
        // Encode and send the message
        let encoded = encode_store_message(&message)?;
        self.stream.write_all(&encoded).await?;
        self.stream.flush().await?;

        // Store the response sender if this message expects a response
        if let Some(id) = extract_message_id(&message) {
            self.pending_requests.insert(id, response_sender);
        }

        Ok(())
    }

    async fn handle_incoming_message(&mut self, message: crate::protocol::ProtocolMessage) {
        match message {
            crate::protocol::ProtocolMessage::Store(store_message) => {
                if let Some(id) = extract_message_id(&store_message) {
                    if let Some(sender) = self.pending_requests.remove(&id) {
                        let _ = sender.send(store_message);
                    }
                }
                // Handle notifications if needed (not implemented for simplicity)
            }
            _ => {
                // Ignore non-store messages
            }
        }
    }
}

/// Extract the message ID from a StoreMessage
fn extract_message_id(message: &StoreMessage) -> Option<u64> {
    match message {
        StoreMessage::Perform { id, .. } => Some(*id),
        StoreMessage::PerformResponse { id, .. } => Some(*id),
        StoreMessage::RegisterNotification { id, .. } => Some(*id),
        StoreMessage::RegisterNotificationResponse { id, .. } => Some(*id),
        StoreMessage::UnregisterNotification { id, .. } => Some(*id),
        StoreMessage::UnregisterNotificationResponse { id, .. } => Some(*id),
        StoreMessage::Error { id, .. } => Some(*id),
        StoreMessage::Notification { .. } => None,
    }
}

/// Async Store Proxy for high-performance operations
#[derive(Clone)]
pub struct AsyncStoreProxy {
    sender: mpsc::UnboundedSender<ConnectionMessage>,
    next_id: Arc<Mutex<u64>>,
}

impl AsyncStoreProxy {
    /// Connect to TCP server
    pub async fn connect(address: &str) -> Result<Self> {
        // Connect to TCP server
        let stream = TcpStream::connect(address).await
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect to {}: {}", address, e)))?;

        // Optimize TCP socket for low latency
        stream.set_nodelay(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set TCP_NODELAY: {}", e)))?;

        // Create communication channel
        let (sender, receiver) = mpsc::unbounded_channel();
        
        // Start the connection handler task
        let handler = ConnectionHandler::new(stream, receiver);
        tokio::spawn(async move {
            if let Err(e) = handler.run().await {
                eprintln!("Connection handler error: {}", e);
            }
        });

        Ok(Self {
            sender,
            next_id: Arc::new(Mutex::new(1)), // Start from 1
        })
    }

    async fn next_id(&self) -> u64 {
        let mut id = self.next_id.lock().await;
        let current = *id;
        *id += 1;
        current
    }

    async fn send_request(&self, request: StoreMessage) -> Result<StoreMessage> {
        let (response_sender, response_receiver) = oneshot::channel();
        
        self.sender
            .send(ConnectionMessage::Send {
                message: request,
                response_sender,
            })
            .map_err(|_| Error::StoreProxyError("Connection closed".to_string()))?;

        response_receiver.await
            .map_err(|_| Error::StoreProxyError("Response channel closed".to_string()))
    }

    /// Get entity type by name
    pub async fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let request = Request::GetEntityType {
            name: name.to_string(),
            entity_type: None,
        };
        
        let requests = Requests::new(vec![request]);
        let response = self.perform(requests).await?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetEntityType { entity_type, .. } => {
                    entity_type.clone().ok_or_else(|| Error::StoreProxyError("Entity type not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Get field type by name
    pub async fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let request = Request::GetFieldType {
            name: name.to_string(),
            field_type: None,
        };
        
        let requests = Requests::new(vec![request]);
        let response = self.perform(requests).await?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetFieldType { field_type, .. } => {
                    field_type.clone().ok_or_else(|| Error::StoreProxyError("Field type not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Check if entity exists
    pub async fn entity_exists(&self, entity_id: EntityId) -> bool {
        let request = Request::EntityExists {
            entity_id,
            exists: None,
        };
        
        let requests = Requests::new(vec![request]);
        if let Ok(response) = self.perform(requests).await {
            if let Some(req) = response.first() {
                match req {
                    Request::EntityExists { exists, .. } => exists.unwrap_or(false),
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Perform requests asynchronously
    pub async fn perform(&self, requests: Requests) -> Result<Requests> {
        let request = StoreMessage::Perform {
            id: self.next_id().await,
            requests,
        };

        let response = self.send_request(request).await?;
        match response {
            StoreMessage::PerformResponse { response, .. } => match response {
                Ok(updated_requests) => Ok(updated_requests),
                Err(e) => Err(Error::StoreProxyError(e)),
            },
            _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
        }
    }

    /// Find entities asynchronously
    pub async fn find_entities_paginated(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        let request = Request::FindEntities {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            result: None,
        };
        
        let requests = Requests::new(vec![request]);
        let response = self.perform(requests).await?;
        
        if let Some(req) = response.first() {
            match req {
                Request::FindEntities { result, .. } => {
                    result.clone().ok_or_else(|| Error::StoreProxyError("Find entities result not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Find all entities matching criteria
    pub async fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<&str>,
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .find_entities_paginated(entity_type.clone(), page_opts.as_ref(), filter)
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

    /// Shutdown the connection
    pub async fn shutdown(&self) {
        let _ = self.sender.send(ConnectionMessage::Shutdown);
    }
}