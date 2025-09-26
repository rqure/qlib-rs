use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::io::{Read, Write};
use anyhow;
use mio::{Poll, Token, Interest, Events};

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, Notification, NotificationQueue, NotifyConfig, hash_notify_config, PageOpts, PageResult, Request, Requests, Result, Single, sreq
};
use crate::data::StoreTrait;
use crate::qresp::{QrespMessageBuffer, encode_store_message};

/// Result of authentication attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationResult {
    /// The authenticated subject ID
    pub subject_id: EntityId,
}

/// TCP message types for Store proxy communication
/// These messages are compatible with the qcore-rs protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreMessage {
    // Authentication messages - MUST be first message from client
    Authenticate {
        id: u64,
        subject_name: String,
        credential: String, // Password for users, secret key for services
    },
    AuthenticateResponse {
        id: u64,
        response: std::result::Result<AuthenticationResult, String>,
    },

    // Perform operation using Request enum
    Perform {
        id: u64,
        requests: Requests,
    },
    PerformResponse {
        id: u64,
        response: std::result::Result<Requests, String>,
    },

    // Notification support - these need to remain separate due to NotificationQueue limitations
    RegisterNotification {
        id: u64,
        config: NotifyConfig,
    },
    RegisterNotificationResponse {
        id: u64,
        response: std::result::Result<(), String>,
    },

    UnregisterNotification {
        id: u64,
        config: NotifyConfig,
    },
    UnregisterNotificationResponse {
        id: u64,
        response: bool,
    },

    // Notification delivery
    Notification {
        notification: Notification,
    },

    // Connection management
    Error {
        id: u64,
        error: String,
    },
}

/// Extract the message ID from a StoreMessage
pub fn extract_message_id(message: &StoreMessage) -> Option<u64> {
    match message {
        StoreMessage::Authenticate { id, .. } => Some(*id),
        StoreMessage::AuthenticateResponse { id, .. } => Some(*id),
        StoreMessage::Perform { id, .. } => Some(*id),
        StoreMessage::PerformResponse { id, .. } => Some(*id),
        StoreMessage::RegisterNotification { id, .. } => Some(*id),
        StoreMessage::RegisterNotificationResponse { id, .. } => Some(*id),
        StoreMessage::UnregisterNotification { id, .. } => Some(*id),
        StoreMessage::UnregisterNotificationResponse { id, .. } => Some(*id),
        StoreMessage::Error { id, .. } => Some(*id),
        StoreMessage::Notification { .. } => None, // Notifications don't have request IDs
    }
}

/// TCP connection with message buffering
#[derive(Debug)]
pub struct TcpConnection {
    stream: mio::net::TcpStream,
    message_buffer: QrespMessageBuffer,
    poll: Poll,
    token: Token,
}

impl TcpConnection {
    pub fn new(stream: std::net::TcpStream) -> anyhow::Result<Self> {
        // Convert std::net::TcpStream to mio::net::TcpStream
        let mut stream = mio::net::TcpStream::from_std(stream);
        
        // Create a poll instance
        let poll = Poll::new()?;
        let token = Token(0);
        
        // Register the stream with the poll instance
        poll.registry().register(&mut stream, token, Interest::READABLE)?;
        
        Ok(Self {
            stream,
            message_buffer: QrespMessageBuffer::new(),
            poll,
            token,
        })
    }
    
    pub fn send_message(&mut self, message: &StoreMessage) -> anyhow::Result<()> {
        let encoded = encode_store_message(message)
            .map_err(|e| anyhow::anyhow!("QRESP encode failed: {}", e))?;
        self.stream.write_all(&encoded)?;
        self.stream.flush()?;
        Ok(())
    }
    
    /// Wait for the socket to be ready for reading, with a timeout
    pub fn wait_for_readable(&mut self, timeout: Option<std::time::Duration>) -> anyhow::Result<bool> {
        let mut events = Events::with_capacity(1);
        self.poll.poll(&mut events, timeout)?;
        
        // Check if our token has events
        for event in events.iter() {
            if event.token() == self.token && event.is_readable() {
                return Ok(true);
            }
        }
        
        Ok(false) // Timeout or no readable event
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
        self.message_buffer
            .try_decode_store_message()
            .map_err(|e| anyhow::anyhow!("QRESP decode failed: {}", e))
    }
}

#[derive(Debug)]
pub struct StoreProxy {
    tcp_connection: Rc<RefCell<TcpConnection>>,
    pending_requests: Rc<RefCell<HashMap<u64, StoreMessage>>>,
    // Map from config hash to list of notification senders
    notification_configs: Rc<RefCell<HashMap<u64, Vec<NotificationQueue>>>>,
    // Authentication state - set once during connection
    authenticated_subject: Option<EntityId>,
    // Counter for generating strictly increasing IDs
    next_id: Rc<RefCell<u64>>,
}

impl StoreProxy {
    /// Generate the next strictly increasing ID
    fn next_id(&self) -> u64 {
        let mut id = self.next_id.borrow_mut();
        let current = *id;
        *id += 1;
        current
    }

    /// Connect to TCP server and authenticate immediately
    /// If authentication fails, the connection will be closed by the server
    pub fn connect_and_authenticate(
        address: &str,
        subject_name: &str,
        credential: &str,
    ) -> Result<Self> {
        // Connect to TCP server
        let stream = std::net::TcpStream::connect(address)
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect to {}: {}", address, e)))?;
        
        // Optimize TCP socket for low latency
        stream.set_nodelay(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set TCP_NODELAY: {}", e)))?;
        
        // Set to non-blocking for message handling
        stream.set_nonblocking(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set non-blocking: {}", e)))?;

        let mut tcp_connection = TcpConnection::new(stream)
            .map_err(|e| Error::StoreProxyError(format!("Failed to create TCP connection: {}", e)))?;

        // Send authentication message immediately
        let auth_request = StoreMessage::Authenticate {
            id: 1, // First message always has ID 1
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
                    // No message yet, wait for data to be ready
                    let remaining_timeout = auth_timeout.saturating_sub(auth_start.elapsed());
                    if remaining_timeout.is_zero() {
                        return Err(Error::StoreProxyError("Authentication timeout".to_string()));
                    }
                    
                    match tcp_connection.wait_for_readable(Some(std::time::Duration::from_millis(10))) {
                        Ok(true) => continue, // Data is ready, try reading again
                        Ok(false) => continue, // Timeout, check overall timeout and try again
                        Err(e) => return Err(Error::StoreProxyError(format!("Poll error during auth: {}", e))),
                    }
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
        let next_id = Rc::new(RefCell::new(1u64)); // Start from 1

        Ok(StoreProxy {
            tcp_connection,
            pending_requests,
            notification_configs,
            authenticated_subject: Some(authenticated_subject),
            next_id,
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
                // Handle response to pending request - store the message directly
                if let Some(id) = extract_message_id(store_message) {
                    self.pending_requests.borrow_mut().insert(id, store_message.clone());
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
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_message(&request)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send message: {}", e)))?;
        }

        // Wait for response by polling the TCP connection until we get our response
        loop {
            // Check if we already have the response
            {
                let mut pending = self.pending_requests.borrow_mut();
                if let Some(response) = pending.remove(&id) {
                    drop(pending); // Explicitly drop the borrow
                    return Ok(response);
                }
            }

            // Read next message from TCP connection
            let message_result = {
                let mut conn = self.tcp_connection.borrow_mut();
                conn.try_receive_message()
            };

            match message_result {
                Ok(Some(message)) => {
                    if let Err(_) = self.handle_incoming_message(&message) {
                        // Handle error if needed
                    }
                }
                Ok(None) => {
                    // No message available, wait for data to be ready using mio
                    let wait_result = {
                        let mut conn = self.tcp_connection.borrow_mut();
                        conn.wait_for_readable(Some(std::time::Duration::from_millis(10)))
                    };
                    
                    match wait_result {
                        Ok(true) => {
                            // Data is ready, continue loop to try reading again
                            continue;
                        }
                        Ok(false) => {
                            // Timeout, continue loop to check for pending responses
                            continue;
                        }
                        Err(e) => {
                            return Err(Error::StoreProxyError(format!("Poll error: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::StoreProxyError(format!("TCP error: {}", e)));
                }
            }
        }
    }

    /// Get the authenticated subject ID
    pub fn get_authenticated_subject(&self) -> Option<EntityId> {
        self.authenticated_subject.clone()
    }

    /// Get entity type by name
    pub fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let request = Request::GetEntityType {
            name: name.to_string(),
            entity_type: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
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

    /// Resolve entity type to name
    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let request = Request::ResolveEntityType {
            entity_type,
            name: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::ResolveEntityType { name, .. } => {
                    name.clone().ok_or_else(|| Error::StoreProxyError("Entity type name not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Get field type by name
    pub fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let request = Request::GetFieldType {
            name: name.to_string(),
            field_type: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
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

    /// Resolve field type to name
    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let request = Request::ResolveFieldType {
            field_type,
            name: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::ResolveFieldType { name, .. } => {
                    name.clone().ok_or_else(|| Error::StoreProxyError("Field type name not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Get entity schema
    pub fn get_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<EntitySchema<Single>> {
        let request = Request::GetEntitySchema {
            entity_type,
            schema: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetEntitySchema { schema, .. } => {
                    schema.clone().ok_or_else(|| Error::StoreProxyError("Entity schema not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(
        &self,
        entity_type: EntityType,
    ) -> Result<EntitySchema<Complete>> {
        let request = Request::GetCompleteEntitySchema {
            entity_type,
            schema: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetCompleteEntitySchema { schema, .. } => {
                    schema.clone().ok_or_else(|| Error::StoreProxyError("Complete entity schema not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Set field schema
    pub fn set_field_schema(
        &mut self,
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
    ) -> Result<()> {
        let mut entity_schema = self.get_entity_schema(entity_type)?;
        entity_schema
            .fields
            .insert(field_type, schema);

        let string_schema = entity_schema.to_string_schema(self);
        let requests = sreq![Request::SchemaUpdate { 
            schema: string_schema, 
            timestamp: None,
        }];
        self.perform(requests).map(|_| ())
    }

    /// Get field schema
    pub fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema> {
        let request = Request::GetFieldSchema {
            entity_type,
            field_type,
            schema: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetFieldSchema { schema, .. } => {
                    schema.clone().ok_or_else(|| Error::StoreProxyError("Field schema not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: EntityId) -> bool {
        let request = Request::EntityExists {
            entity_id,
            exists: None,
        };
        
        let requests = sreq![request];
        if let Ok(response) = self.perform(requests) {
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

    /// Check if field exists
    pub fn field_exists(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> bool {
        let request = Request::FieldExists {
            entity_type,
            field_type,
            exists: None,
        };
        
        let requests = sreq![request];
        if let Ok(response) = self.perform(requests) {
            if let Some(req) = response.first() {
                match req {
                    Request::FieldExists { exists, .. } => exists.unwrap_or(false),
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Perform requests
    pub fn perform(&self, requests: Requests) -> Result<Requests> {
        let request = StoreMessage::Perform {
            id: self.next_id(),
            requests,
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
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
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

    /// Find entities exact
    pub fn find_entities_exact(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        let request = Request::FindEntitiesExact {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            result: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::FindEntitiesExact { result, .. } => {
                    result.clone().ok_or_else(|| Error::StoreProxyError("Find entities exact result not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    pub fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<&str>,
    ) -> Result<Vec<EntityId>> {
        let mut result = Vec::new();
        let mut page_opts: Option<PageOpts> = None;

        loop {
            let page_result = self
                .find_entities_paginated(entity_type.clone(), page_opts.as_ref(), filter)?;
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
                .get_entity_types_paginated(page_opts.as_ref())
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
        page_opts: Option<&PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        let request = Request::GetEntityTypes {
            page_opts: page_opts.cloned(),
            result: None,
        };
        
        let requests = sreq![request];
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::GetEntityTypes { result, .. } => {
                    result.clone().ok_or_else(|| Error::StoreProxyError("Get entity types result not found".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
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
            id: self.next_id(),
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
                id: self.next_id(),
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
    fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        self.get_entity_type(name)
    }

    fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        self.resolve_entity_type(entity_type)
    }

    fn get_field_type(&self, name: &str) -> Result<FieldType> {
        self.get_field_type(name)
    }

    fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        self.resolve_field_type(field_type)
    }

    fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        self.get_entity_schema(entity_type)
    }

    fn get_complete_entity_schema(&self, _entity_type: EntityType) -> Result<&EntitySchema<Complete>> {
        // StoreProxy cannot return a reference since it gets data over network
        // This is a limitation of the proxy pattern with the reference-based API
        unimplemented!("StoreProxy cannot return references to remote data")
    }

    fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema> {
        self.get_field_schema(entity_type, field_type)
    }

    fn set_field_schema(&mut self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()> {
        self.set_field_schema(entity_type, field_type, schema)
    }

    fn entity_exists(&self, entity_id: EntityId) -> bool {
        self.entity_exists(entity_id)
    }

    fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        self.field_exists(entity_type, field_type)
    }

    fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        // For StoreProxy, we need to use the old approach via perform() since we don't have direct field access
        crate::data::indirection::resolve_indirection_via_trait(self, entity_id, fields)
    }

    fn perform(&self, requests: Requests) -> Result<Requests> {
        self.perform(requests)
    }

    fn perform_mut(&mut self, requests: Requests) -> Result<Requests> {
        self.perform(requests)
    }

    fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        self.find_entities_paginated(entity_type, page_opts, filter)
    }

    fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        self.find_entities_exact(entity_type, page_opts, filter)
    }

    fn find_entities(&self, entity_type: EntityType, filter: Option<&str>) -> Result<Vec<EntityId>> {
        self.find_entities(entity_type, filter)
    }

    fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        self.get_entity_types()
    }

    fn get_entity_types_paginated(&self, page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        self.get_entity_types_paginated(page_opts)
    }

    fn register_notification(&mut self, config: NotifyConfig, sender: NotificationQueue) -> Result<()> {
        self.register_notification(config, sender)
    }

    fn unregister_notification(&mut self, config: &NotifyConfig, sender: &NotificationQueue) -> bool {
        self.unregister_notification(config, sender)
    }
}
