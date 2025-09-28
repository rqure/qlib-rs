use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use bincode;
use mio::{Events, Interest, Poll, Token};
use serde::{de::DeserializeOwned};

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, Notification, NotificationQueue, NotifyConfig, hash_notify_config, PageOpts, PageResult, Single, Value, Timestamp, PushCondition, AdjustBehavior
};
use crate::data::StoreTrait;
use crate::protocol::{MessageBuffer, QuspCommand, QuspFrame, QuspResponse, encode_command};

const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn decode_bincode<T: DeserializeOwned>(value: &Bytes) -> Result<T> {
    bincode::deserialize(value.as_ref())
        .map_err(|e| Error::StoreProxyError(format!("Failed to deserialize payload: {}", e)))
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
        let encoded = encode_command(command);
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
            .name_uppercase()
            .map_err(|e| anyhow!("Failed to get command name: {}", e))?;
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

    /// Get entity type by name
    pub fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let args = vec![Bytes::copy_from_slice(name.as_bytes())];
        let response = self.send_command("GET_ENTITY_TYPE", args)?;
        crate::protocol::parse_entity_type_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse entity type response: {}", e)))
    }

    /// Resolve entity type to name
    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let args = vec![Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes())];
        let response = self.send_command("RESOLVE_ENTITY_TYPE", args)?;
        crate::protocol::parse_string_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse string response: {}", e)))
    }

    /// Get field type by name
    pub fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let args = vec![Bytes::copy_from_slice(name.as_bytes())];
        let response = self.send_command("GET_FIELD_TYPE", args)?;
        crate::protocol::parse_field_type_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse field type response: {}", e)))
    }

    /// Resolve field type to name
    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let args = vec![Bytes::copy_from_slice(&field_type.0.to_string().as_bytes())];
        let response = self.send_command("RESOLVE_FIELD_TYPE", args)?;
        crate::protocol::parse_string_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse string response: {}", e)))
    }

    /// Get entity schema
    pub fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        let args = vec![Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes())];
        let response = self.send_command("GET_ENTITY_SCHEMA", args)?;
        crate::protocol::parse_entity_schema_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse entity schema response: {}", e)))
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        let args = vec![Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes())];
        let response = self.send_command("GET_COMPLETE_ENTITY_SCHEMA", args)?;
        crate::protocol::parse_complete_entity_schema_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse complete entity schema response: {}", e)))
    }

    /// Set field schema
    pub fn set_field_schema(
        &mut self,
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
    ) -> Result<()> {
        let encoded_schema = crate::protocol::encode_field_schema(&schema)
            .map_err(|e| Error::StoreProxyError(format!("Failed to encode schema: {}", e)))?;
        
        let args = vec![
            Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes()),
            Bytes::copy_from_slice(&field_type.0.to_string().as_bytes()),
            Bytes::copy_from_slice(&encoded_schema),
        ];
        self.send_command_ok("SET_FIELD_SCHEMA", args)
    }

    /// Get field schema
    pub fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema> {
        let args = vec![
            Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes()),
            Bytes::copy_from_slice(&field_type.0.to_string().as_bytes()),
        ];
        let response = self.send_command("GET_FIELD_SCHEMA", args)?;
        crate::protocol::parse_field_schema_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse field schema response: {}", e)))
    }

    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: EntityId) -> bool {
        let args = vec![Bytes::copy_from_slice(&entity_id.0.to_string().as_bytes())];
        match self.send_command("ENTITY_EXISTS", args) {
            Ok(response) => crate::protocol::parse_bool_response(response).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Check if field exists
    pub fn field_exists(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> bool {
        let args = vec![
            Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes()),
            Bytes::copy_from_slice(&field_type.0.to_string().as_bytes()),
        ];
        match self.send_command("FIELD_EXISTS", args) {
            Ok(response) => crate::protocol::parse_bool_response(response).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Resolve indirection
    pub fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        let field_path_str = crate::protocol::field_path_to_string(fields);
        let args = vec![
            Bytes::copy_from_slice(&entity_id.0.to_string().as_bytes()),
            Bytes::copy_from_slice(field_path_str.as_bytes()),
        ];
        let response = self.send_command("RESOLVE_INDIRECTION", args)?;
        crate::protocol::parse_indirection_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse indirection response: {}", e)))
    }

    /// Read a field value
    pub fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let field_path_str = crate::protocol::field_path_to_string(field_path);
        let args = vec![
            Bytes::copy_from_slice(&entity_id.0.to_string().as_bytes()),
            Bytes::copy_from_slice(field_path_str.as_bytes()),
        ];
        let response = self.send_command("READ", args)?;
        crate::protocol::parse_read_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse read response: {}", e)))
    }

    /// Write a field value
    pub fn write(&mut self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
        let encoded_value = crate::protocol::encode_value(&value);
        let field_path_str = crate::protocol::field_path_to_string(field_path);
        let args = vec![
            Bytes::copy_from_slice(&entity_id.0.to_string().as_bytes()),
            Bytes::copy_from_slice(field_path_str.as_bytes()),
            Bytes::copy_from_slice(&encoded_value),
            Bytes::copy_from_slice(&crate::protocol::encode_optional_entity_id(writer_id).as_bytes()),
            Bytes::copy_from_slice(&crate::protocol::encode_optional_timestamp(write_time).as_bytes()),
            Bytes::copy_from_slice(&crate::protocol::encode_push_condition(push_condition).as_bytes()),
            Bytes::copy_from_slice(&crate::protocol::encode_adjust_behavior(adjust_behavior).as_bytes()),
        ];
        self.send_command_ok("WRITE", args)
    }

    /// Create a new entity
    pub fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let args = vec![
            Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes()),
            Bytes::copy_from_slice(&crate::protocol::encode_optional_entity_id(parent_id).as_bytes()),
            Bytes::copy_from_slice(name.as_bytes()),
        ];
        let response = self.send_command("CREATE_ENTITY", args)?;
        crate::protocol::parse_entity_id_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse entity ID response: {}", e)))
    }

    /// Delete an entity
    pub fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        let args = vec![Bytes::copy_from_slice(&entity_id.0.to_string().as_bytes())];
        self.send_command_ok("DELETE_ENTITY", args)
    }

    /// Update entity schema
    pub fn update_schema(&mut self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        let encoded_schema = crate::protocol::encode_entity_schema_string(&schema)
            .map_err(|e| Error::StoreProxyError(format!("Failed to encode entity schema: {}", e)))?;
        let args = vec![Bytes::copy_from_slice(&encoded_schema)];
        self.send_command_ok("UPDATE_SCHEMA", args)
    }

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self) -> crate::data::Snapshot {
        let response = self.send_command("TAKE_SNAPSHOT", vec![]).unwrap();
        crate::protocol::parse_snapshot_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse snapshot response: {}", e)))
            .unwrap()
    }

    /// Find entities with pagination (includes inherited types)
    pub fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let mut args = vec![Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes())];
        
        if let Some(opts) = page_opts {
            let encoded_opts = crate::protocol::encode_page_opts(opts);
            args.push(Bytes::copy_from_slice(&encoded_opts));
        } else {
            args.push(Bytes::copy_from_slice(b"null"));
        }
        
        if let Some(f) = filter {
            args.push(Bytes::copy_from_slice(f.as_bytes()));
        } else {
            args.push(Bytes::copy_from_slice(b"null"));
        }
        
        let response = self.send_command("FIND_ENTITIES_PAGINATED", args)?;
        crate::protocol::parse_page_result_entity_ids_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse page result response: {}", e)))
    }

    /// Find entities exactly of the specified type (no inheritance) with pagination
    pub fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let mut args = vec![Bytes::copy_from_slice(&entity_type.0.to_string().as_bytes())];
        
        if let Some(opts) = page_opts {
            let encoded_opts = crate::protocol::encode_page_opts(opts);
            args.push(Bytes::copy_from_slice(&encoded_opts));
        } else {
            args.push(Bytes::copy_from_slice(b"null"));
        }
        
        if let Some(f) = filter {
            args.push(Bytes::copy_from_slice(f.as_bytes()));
        } else {
            args.push(Bytes::copy_from_slice(b"null"));
        }
        
        let response = self.send_command("FIND_ENTITIES_EXACT", args)?;
        crate::protocol::parse_page_result_entity_ids_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse page result response: {}", e)))
    }

    /// Get all entity types with pagination
    pub fn get_entity_types_paginated(&self, page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        let mut args = vec![];
        
        if let Some(opts) = page_opts {
            let encoded_opts = crate::protocol::encode_page_opts(opts);
            args.push(Bytes::copy_from_slice(&encoded_opts));
        } else {
            args.push(Bytes::copy_from_slice(b"null"));
        }
        
        let response = self.send_command("GET_ENTITY_TYPES_PAGINATED", args)?;
        crate::protocol::parse_page_result_entity_types_response(response)
            .map_err(|e| Error::StoreProxyError(format!("Failed to parse page result response: {}", e)))
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



    /// Register notification with provided sender
    /// Note: For proxy, this registers the notification on the remote server
    /// and stores the sender locally to forward notifications
    pub fn register_notification(
        &mut self,
        config: NotifyConfig,
        sender: NotificationQueue,
    ) -> Result<()> {
        let encoded_config = crate::protocol::encode_notify_config(&config)
            .map_err(|e| Error::StoreProxyError(format!("Failed to encode notify config: {}", e)))?;
        let args = vec![Bytes::copy_from_slice(&encoded_config)];
        self.send_command_ok("REGISTER_NOTIFICATION", args)?;
        
        let config_hash = hash_notify_config(&config);
        let mut configs = self.notification_configs.borrow_mut();
        configs.entry(config_hash).or_insert_with(Vec::new).push(sender);
        Ok(())
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
            let encoded_config = match crate::protocol::encode_notify_config(target_config) {
                Ok(bytes) => bytes,
                Err(_) => return false,
            };
            let args = vec![Bytes::copy_from_slice(&encoded_config)];
            match self.send_command_ok("UNREGISTER_NOTIFICATION", args) {
                Ok(_) => true,
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
        self.resolve_indirection(entity_id, fields)
    }

    fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        self.read(entity_id, field_path)
    }

    fn write(&mut self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
        self.write(entity_id, field_path, value, writer_id, write_time, push_condition, adjust_behavior)
    }

    fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        self.create_entity(entity_type, parent_id, name)
    }

    fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        self.delete_entity(entity_id)
    }

    fn update_schema(&mut self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        self.update_schema(schema)
    }

    fn take_snapshot(&self) -> crate::data::Snapshot {
        self.take_snapshot()
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
