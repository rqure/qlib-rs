use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::Duration;

use anyhow;
use bytes::Bytes;
use bincode;
use mio::{Events, Interest, Poll, Token};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, Notification, NotificationQueue, NotifyConfig, hash_notify_config, PageOpts, PageResult, Request, Requests, Result, Single, sreq, Value, Timestamp, now, PushCondition, AdjustBehavior
};
use crate::data::StoreTrait;
use crate::protocol::{MessageBuffer, QuspCommand, QuspFrame, QuspResponse, encode_command};

const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn encode_utf8(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

fn encode_bincode<T: Serialize>(value: &T) -> Result<Bytes> {
    bincode::serialize(value)
        .map(Bytes::from)
        .map_err(|e| Error::StoreProxyError(format!("Failed to serialize payload: {}", e)))
}

fn decode_bincode<T: DeserializeOwned>(value: &Bytes) -> Result<T> {
    bincode::deserialize(value.as_ref())
        .map_err(|e| Error::StoreProxyError(format!("Failed to deserialize payload: {}", e)))
}

fn response_to_bytes(response: QuspResponse) -> Result<Bytes> {
    match response {
        QuspResponse::Bulk(data) | QuspResponse::Simple(data) => Ok(data),
        QuspResponse::Null => Err(Error::StoreProxyError("Expected payload but received NULL".into())),
        QuspResponse::Integer(_) => Err(Error::StoreProxyError("Expected payload but received integer".into())),
        QuspResponse::Array(_) => Err(Error::StoreProxyError("Expected payload but received array".into())),
        QuspResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
    }
}

fn response_to_string(response: QuspResponse) -> Result<String> {
    let bytes = response_to_bytes(response)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| Error::StoreProxyError(format!("Invalid UTF-8 payload: {}", e)))
}

fn response_to_bincode<T: DeserializeOwned>(response: QuspResponse) -> Result<T> {
    let bytes = response_to_bytes(response)?;
    decode_bincode(&bytes)
}

fn response_to_bool(response: QuspResponse) -> Result<bool> {
    match response {
        QuspResponse::Integer(value) => Ok(value != 0),
        QuspResponse::Simple(data) => Ok(data.as_ref() == b"1" || data.as_ref() == b"true"),
        QuspResponse::Bulk(data) => Ok(data.as_ref() == b"1" || data.as_ref() == b"true"),
        QuspResponse::Null => Ok(false),
        QuspResponse::Array(_) => Err(Error::StoreProxyError("Expected boolean but received array".into())),
        QuspResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
    }
}

fn expect_ok(response: QuspResponse) -> Result<()> {
    match response {
        QuspResponse::Simple(data) if data.as_ref() == b"OK" => Ok(()),
        QuspResponse::Null => Ok(()),
        QuspResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
        QuspResponse::Array(_) => Err(Error::StoreProxyError("Unexpected array response".into())),
        other => Err(Error::StoreProxyError(format!(
            "Unexpected response (expected OK): {:?}",
            other
        ))),
    }
}


/// TCP connection with message buffering
#[derive(Debug)]
pub struct TcpConnection {
    stream: mio::net::TcpStream,
    message_buffer: MessageBuffer,
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
            message_buffer: MessageBuffer::new(),
            poll,
            token,
        })
    }
    
    pub fn send_command(&mut self, command: &QuspCommand) -> anyhow::Result<()> {
        let encoded = encode_command(command)?;
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
    
    pub fn try_receive_frame(&mut self) -> anyhow::Result<Option<QuspFrame>> {
        if let Some(frame) = self.message_buffer.try_decode()? {
            return Ok(Some(frame));
        }

        // Try to read more data
        let mut buffer = [0u8; 8192];
        match self.stream.read(&mut buffer) {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                self.message_buffer.add_data(&buffer[..bytes_read]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available right now
                return Ok(None);
            }
            Err(e) => return Err(anyhow::anyhow!("TCP read error: {}", e)),
        }

        // Try to decode a message
        self.message_buffer.try_decode()
    }
}

#[derive(Debug)]
pub struct StoreProxy {
    tcp_connection: Rc<RefCell<TcpConnection>>,
    // Map from config hash to list of notification senders
    notification_configs: Rc<RefCell<HashMap<u64, Vec<NotificationQueue>>>>,
}

impl StoreProxy {
    /// Connect to TCP server
    pub fn connect(address: &str) -> Result<Self> {
        // Connect to TCP server
        let stream = std::net::TcpStream::connect(address)
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect to {}: {}", address, e)))?;
        
        // Optimize TCP socket for low latency
        stream.set_nodelay(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set TCP_NODELAY: {}", e)))?;
        
        // Set to non-blocking for message handling
        stream.set_nonblocking(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set non-blocking: {}", e)))?;

        let tcp_connection = TcpConnection::new(stream)
            .map_err(|e| Error::StoreProxyError(format!("Failed to create TCP connection: {}", e)))?;

        // Create single-threaded collections
        let tcp_connection = Rc::new(RefCell::new(tcp_connection));
        let notification_configs = Rc::new(RefCell::new(HashMap::new()));

        Ok(StoreProxy {
            tcp_connection,
            notification_configs,
        })
    }

    fn handle_incoming_command(&self, command: QuspCommand) -> Result<()> {
        let name = command
            .uppercase_name()
            .map_err(|e| Error::StoreProxyError(format!("Invalid command name: {}", e)))?;
        match name.as_str() {
            "NOTIFY" => {
                let payload = command
                    .args
                    .get(0)
                    .ok_or_else(|| Error::StoreProxyError("Notification missing payload".into()))?;
                let notification: Notification = decode_bincode(payload)?;
                if let Some(queues) = self
                    .notification_configs
                    .borrow()
                    .get(&notification.config_hash)
                {
                    for queue in queues {
                        queue.push(notification.clone());
                    }
                }
                Ok(())
            }
            other => Err(Error::StoreProxyError(format!(
                "Unexpected server command: {}",
                other
            ))),
        }
    }

    fn poll_for_frame(&self) -> Result<Option<QuspFrame>> {
        let result = {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.try_receive_frame()
        };

        result.map_err(|e| Error::StoreProxyError(format!("TCP error: {}", e)))
    }

    fn send_command(&self, name: &str, args: Vec<Bytes>) -> Result<QuspResponse> {
        let command = QuspCommand::new(name.to_ascii_uppercase().into_bytes(), args);

        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_command(&command)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send {} command: {}", name, e)))?;
        }

        loop {
            match self.poll_for_frame()? {
                Some(QuspFrame::Response(response)) => {
                    return match response {
                        QuspResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
                        other => Ok(other),
                    }
                }
                Some(QuspFrame::Command(command)) => {
                    self.handle_incoming_command(command)?;
                }
                None => {
                    let readable = {
                        let mut conn = self.tcp_connection.borrow_mut();
                        conn.wait_for_readable(Some(READ_POLL_INTERVAL))
                    }
                    .map_err(|e| Error::StoreProxyError(format!("Poll error: {}", e)))?;

                    if !readable {
                        continue;
                    }
                }
            }
        }
    }

    fn send_command_ok(&self, name: &str, args: Vec<Bytes>) -> Result<()> {
        let response = self.send_command(name, args)?;
        expect_ok(response)
    }

    fn send_command_bincode<T: DeserializeOwned>(&self, name: &str, args: Vec<Bytes>) -> Result<T> {
        let response = self.send_command(name, args)?;
        response_to_bincode(response)
    }

    fn send_command_string(&self, name: &str, args: Vec<Bytes>) -> Result<String> {
        let response = self.send_command(name, args)?;
        response_to_string(response)
    }

    fn send_command_bool(&self, name: &str, args: Vec<Bytes>) -> Result<bool> {
        let response = self.send_command(name, args)?;
        response_to_bool(response)
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
        let id = self.next_id();
        let payload = serialize_requests(&requests)?;
        let response = self.send_request(QuspMessage::Perform { id, payload })?;

        match response {
            QuspMessage::PerformOk { payload, .. } => deserialize_requests(&payload),
            QuspMessage::PerformErr { message, .. } => {
                Err(Error::StoreProxyError(bytes_to_string(&message)))
            }
            QuspMessage::Error { message, .. } => {
                Err(Error::StoreProxyError(bytes_to_string(&message)))
            }
            other => Err(Error::StoreProxyError(format!(
                "Unexpected response type: {:?}",
                other
            ))),
        }
    }

    pub fn perform_mut(&mut self, requests: Requests) -> Result<Requests> {
        StoreProxy::perform(self, requests)
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
        let payload = serialize_notify_config(&config)?;
        match self.send_request(QuspMessage::Register {
            id: self.next_id(),
            config: payload,
        })? {
            QuspMessage::RegisterOk { .. } => {
                let config_hash = hash_notify_config(&config);
                let mut configs = self.notification_configs.borrow_mut();
                configs.entry(config_hash).or_insert_with(Vec::new).push(sender);
                Ok(())
            }
            QuspMessage::RegisterErr { message, .. } => {
                Err(Error::StoreProxyError(bytes_to_string(&message)))
            }
            QuspMessage::Error { message, .. } => {
                Err(Error::StoreProxyError(bytes_to_string(&message)))
            }
            other => Err(Error::StoreProxyError(format!(
                "Unexpected response type: {:?}",
                other
            ))),
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
                let payload = match serialize_notify_config(target_config) {
                    Ok(bytes) => bytes,
                    Err(_) => return false,
                };

                match self.send_request(QuspMessage::Unregister {
                    id: self.next_id(),
                    config: payload,
                }) {
                    Ok(QuspMessage::UnregisterOk { removed, .. }) => removed,
                    Ok(QuspMessage::UnregisterErr { .. }) => false,
                    Ok(QuspMessage::Error { .. }) => false,
                    Ok(_) => false,
                    Err(_) => false,
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

    fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let request = Request::Read {
            entity_id,
            field_types: field_path.iter().cloned().collect(),
            value: None,
            write_time: None,
            writer_id: None,
        };
        
        let requests = Requests::new(vec![request]);
        let response = self.perform(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::Read { value, write_time, writer_id, .. } => {
                    if let (Some(val), Some(time)) = (value, write_time) {
                        Ok((val.clone(), time.clone(), writer_id.clone()))
                    } else {
                        Err(Error::StoreProxyError("Read result incomplete".to_string()))
                    }
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    fn write(&mut self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>) -> Result<()> {
        let request = Request::Write {
            entity_id,
            field_types: field_path.iter().cloned().collect(),
            value: Some(value),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: Some(now()),
            writer_id,
            write_processed: false,
        };
        
        let requests = Requests::new(vec![request]);
        let _response = self.perform_mut(requests)?;
        Ok(())
    }

    fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let request = Request::Create {
            entity_type,
            parent_id,
            name: name.to_string(),
            created_entity_id: None,
            timestamp: Some(now()),
        };
        
        let requests = Requests::new(vec![request]);
        let response = self.perform_mut(requests)?;
        
        if let Some(req) = response.first() {
            match req {
                Request::Create { created_entity_id, .. } => {
                    created_entity_id.ok_or_else(|| Error::StoreProxyError("Entity creation failed".to_string()))
                }
                _ => Err(Error::StoreProxyError("Unexpected response type".to_string())),
            }
        } else {
            Err(Error::StoreProxyError("No response received".to_string()))
        }
    }

    fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        let request = Request::Delete {
            entity_id,
            timestamp: Some(now()),
        };
        
        let requests = Requests::new(vec![request]);
        let _response = self.perform_mut(requests)?;
        Ok(())
    }

    fn update_schema(&mut self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        let request = Request::SchemaUpdate {
            schema,
            timestamp: Some(now()),
        };
        
        let requests = Requests::new(vec![request]);
        let _response = self.perform_mut(requests)?;
        Ok(())
    }

    fn take_snapshot(&self) -> crate::data::Snapshot {
        // For StoreProxy, snapshots are not directly supported
        // This would need to be implemented via perform if needed
        unimplemented!("Snapshots not supported in StoreProxy")
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
