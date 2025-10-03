use std::cell::RefCell;
use std::io::{Read, Write};
use std::time::Duration;

use crossbeam::channel::Sender;
use mio::{Events, Interest, Poll, Token};
use ahash::AHashMap;

use crate::data::resp::{BooleanResponse, CreateEntityCommand, CreateEntityResponse, DeleteEntityCommand, EntityExistsCommand, EntityListResponse, EntityTypeListResponse, FieldExistsCommand, FieldSchemaResponse, FindEntitiesCommand, FindEntitiesExactCommand, FindEntitiesPaginatedCommand, GetEntitySchemaCommand, GetEntityTypeCommand, GetEntityTypesCommand, GetEntityTypesPaginatedCommand, GetFieldSchemaCommand, GetFieldTypeCommand, IntegerResponse, NotificationCommand, PaginatedEntityResponse, PaginatedEntityTypeResponse, ReadCommand, ReadResponse, RegisterNotificationCommand, ResolveEntityTypeCommand, ResolveFieldTypeCommand, ResolveIndirectionCommand, ResolveIndirectionResponse, RespCommand, RespDecode, RespFromBytes, RespToBytes, RespValue, SetFieldSchemaCommand, SnapshotResponse, StringResponse, TakeSnapshotCommand, UnregisterNotificationCommand, UpdateSchemaCommand, WriteCommand};
use crate::{
    AdjustBehavior, Complete, EntityId, EntitySchema, EntitySchemaResp, EntityType, Error, FieldSchema, FieldType, Notification, NotifyConfig, PageOpts, PageResult, PushCondition, Result, Single, Timestamp, Value
};
use crate::data::StoreTrait;

const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Expect an OK response from RESP
fn expect_ok(resp_value: RespValue) -> Result<()> {
    match resp_value {
        RespValue::SimpleString(s) if s == "OK" => Ok(()),
        RespValue::Error(msg) => Err(Error::StoreProxyError(format!("Server error: {}", msg))),
        _ => Err(Error::StoreProxyError("Expected OK response".to_string())),
    }
}

/// TCP connection for RESP protocol
#[derive(Debug)]
pub struct TcpConnection {
    stream: mio::net::TcpStream,
    poll: Poll,
    token: Token,
    pub(crate) read_buffer: Vec<u8>,
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
            poll,
            token,
            read_buffer: Vec::new(),
        })
    }
    
    pub fn send_bytes(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(data)?;
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
    
    pub fn read_bytes(&mut self) -> anyhow::Result<()> {
        let mut buffer = [0u8; 65536];
        match self.stream.read(&mut buffer) {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                self.read_buffer.extend_from_slice(&buffer[..bytes_read]);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(()) // No data available
            }
            Err(e) => Err(anyhow::anyhow!("TCP read error: {}", e)),
        }
    }
}

#[derive(Debug)]
pub struct StoreProxy {
    pub(crate) tcp_connection: RefCell<TcpConnection>,
    /// Mapping from config_hash to (NotifyConfig, list of notification senders)
    notification_senders: RefCell<AHashMap<u64, (NotifyConfig, Vec<Sender<Notification>>)>>,
}

impl StoreProxy {
    /// Create a new pipeline for batching commands
    pub fn pipeline(&self) -> crate::data::pipeline::Pipeline {
        crate::data::pipeline::Pipeline::new(self)
    }

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

        Ok(StoreProxy {
            tcp_connection: RefCell::new(tcp_connection),
            notification_senders: RefCell::new(AHashMap::new()),
        })
    }

    fn send_command_get_response<C, R>(&self, command: &C) -> Result<R>
    where
        C: RespCommand<'static>,
        R: for<'a> RespDecode<'a>,
    {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        
        self.tcp_connection.borrow_mut().send_bytes(&encoded_bytes)
            .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;

        loop {
            // Try to parse and get the number of bytes consumed
            let consumed_opt = {
                let conn = self.tcp_connection.borrow();
                match RespValue::from_bytes(&conn.read_buffer) {
                    Ok((resp_value, remaining)) => {
                        match R::decode(resp_value.clone()) {
                            Ok(response_struct) => {
                                let consumed = conn.read_buffer.len() - remaining.len();
                                Ok(Some((consumed, Some(response_struct))))
                            }
                            Err(e) => {
                                if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                                    let consumed = conn.read_buffer.len() - remaining.len();
                                    self.handle_notification(notification);
                                    Ok(Some((consumed, None)))
                                } else {
                                    Err(Error::StoreProxyError(format!("Failed to decode response: {}", e)))
                                }
                            },
                        }
                    }
                    Err(_) => Ok(None)
                }
            }?;
            
            if let Some((consumed, response_struct)) = consumed_opt {
                // Remove only the consumed bytes, keeping the remaining unparsed data
                self.tcp_connection.borrow_mut().read_buffer.drain(..consumed);
                if let Some(response_struct) = response_struct {
                    return Ok(response_struct);
                } else {
                    // Continue loop to see if we can get the actual response
                    // without having to read more data
                    continue; 
                }
            }
            
            // Need more data
            let readable = self.tcp_connection.borrow_mut()
                .wait_for_readable(Some(READ_POLL_INTERVAL))
                .map_err(|e| Error::StoreProxyError(format!("Poll error: {}", e)))?;
            if readable {
                self.tcp_connection.borrow_mut().read_bytes()
                    .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
            }
        }
    }

    fn send_command_ok<C>(&self, command: &C) -> Result<()>
    where
        C: RespCommand<'static>,
    {
        // We need to get the raw RESP value and check if it's OK
        // Use a custom inline check instead of trying to decode RespValue itself
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        
        self.tcp_connection.borrow_mut().send_bytes(&encoded_bytes)
            .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;

        loop {
            // Try to parse and get the number of bytes consumed
            let consumed_opt = {
                let conn = self.tcp_connection.borrow();
                match RespValue::from_bytes(&conn.read_buffer) {
                    Ok((resp_value, remaining)) => {
                        let consumed = conn.read_buffer.len() - remaining.len();
                        // First, try to decode as notification
                        if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                            self.handle_notification(notification);
                            Ok(Some((consumed, None))) // None means notification handled, continue
                        } else {
                            // Not a notification, check if OK
                            let result = expect_ok(resp_value);
                            Ok(Some((consumed, Some(result))))
                        }
                    }
                    Err(_) => Ok(None)
                }
            }?;
            
            if let Some((consumed, maybe_result)) = consumed_opt {
                // Remove only the consumed bytes, keeping the remaining unparsed data
                self.tcp_connection.borrow_mut().read_buffer.drain(..consumed);
                if let Some(result) = maybe_result {
                    return result;
                } else {
                    // Notification handled, continue loop
                    continue;
                }
            }
            
            // Need more data
            let readable = self.tcp_connection.borrow_mut()
                .wait_for_readable(Some(READ_POLL_INTERVAL))
                .map_err(|e| Error::StoreProxyError(format!("Poll error: {}", e)))?;
            if readable {
                self.tcp_connection.borrow_mut().read_bytes()
                    .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
            }
        }
    }

    /// Get entity type by name
    pub fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let command = GetEntityTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<GetEntityTypeCommand, IntegerResponse>(&command)?;
        Ok(EntityType(integer_response.value as u32))
    }

    /// Resolve entity type to name
    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let command = ResolveEntityTypeCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<ResolveEntityTypeCommand, StringResponse>(&command)?;
        Ok(string_response.value)
    }

    /// Get field type by name
    pub fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let command = GetFieldTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<GetFieldTypeCommand, IntegerResponse>(&command)?;
        Ok(FieldType(integer_response.value as u64))
    }

    /// Resolve field type to name
    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let command = ResolveFieldTypeCommand {
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<ResolveFieldTypeCommand, StringResponse>(&command)?;
        Ok(string_response.value)
    }

    /// Get entity schema
    pub fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        let command = GetEntitySchemaCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        
        let schema_resp = self.send_command_get_response::<GetEntitySchemaCommand, EntitySchemaResp>(&command)?;
        
        // Convert EntitySchemaResp back to EntitySchema<Single, String, String>
        let schema_string = schema_resp.to_entity_schema(self)?;
        
        // Convert from string-based schema to typed schema
        let typed_schema = EntitySchema::from_string_schema(schema_string, self);
        
        Ok(typed_schema)
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(&self, _entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        // TODO: Implement RESP command for complete entity schema
        unimplemented!("Complete entity schema not yet implemented with RESP")
    }

    /// Set field schema
    pub fn set_field_schema(&self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()> {
        // Convert FieldSchema to FieldSchemaResp
        let field_type_str = self.resolve_field_type(field_type.clone())?;
        let schema_resp = crate::data::entity_schema::FieldSchemaResp {
            field_type: field_type_str,
            rank: schema.rank(),
            default_value: schema.default_value(),
        };

        let command = SetFieldSchemaCommand {
            entity_type,
            field_type,
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        
        self.send_command_ok(&command)
    }

    /// Get field schema
    pub fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema> {
        let command = GetFieldSchemaCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        let field_schema_response = self.send_command_get_response::<GetFieldSchemaCommand, FieldSchemaResponse>(&command)?;
        
        // Convert FieldSchemaResp back to FieldSchema<FieldType>
        let field_schema_string = field_schema_response.schema.to_field_schema();
        
        // Convert from string-based field schema to typed field schema
        let typed_field_schema = FieldSchema::from_string_schema(field_schema_string, self);
        
        Ok(typed_field_schema)
    }

    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: EntityId) -> bool {
        let command = EntityExistsCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<EntityExistsCommand, BooleanResponse>(&command) {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Check if field exists
    pub fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        let command = FieldExistsCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<FieldExistsCommand, BooleanResponse>(&command) {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Resolve indirection
    pub fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        let command = ResolveIndirectionCommand {
            entity_id,
            fields: fields.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let resolve_response = self.send_command_get_response::<ResolveIndirectionCommand, ResolveIndirectionResponse>(&command)?;
        Ok((resolve_response.entity_id, resolve_response.field_type))
    }

    /// Read a field value
    pub fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let read_response = self.send_command_get_response::<ReadCommand, ReadResponse>(&command)?;
        Ok((read_response.value, read_response.timestamp, read_response.writer_id))
    }

    /// Write a field value
    #[allow(unused_variables)]
    pub fn write(&self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
        let command = WriteCommand {
            entity_id,
            field_path: field_path.to_vec(),
            value,
            writer_id,
            write_time,
            push_condition,
            adjust_behavior,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    /// Create a new entity
    pub fn create_entity(&self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let create_response = self.send_command_get_response::<CreateEntityCommand, CreateEntityResponse>(&command)?;
        Ok(create_response.entity_id)
    }

    /// Delete an entity
    pub fn delete_entity(&self, entity_id: EntityId) -> Result<()> {
        let command = DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    /// Update entity schema
    pub fn update_schema(&self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        // Convert EntitySchema to EntitySchemaResp
        let fields_resp: Vec<crate::data::entity_schema::FieldSchemaResp> = schema
            .fields
            .into_iter()
            .map(|(field_type_str, field_schema)| {
                crate::data::entity_schema::FieldSchemaResp {
                    field_type: field_type_str,
                    rank: field_schema.rank(),
                    default_value: field_schema.default_value(),
                }
            })
            .collect();

        let schema_resp = crate::data::entity_schema::EntitySchemaResp {
            entity_type: schema.entity_type,
            inherit: schema.inherit,
            fields: fields_resp,
        };
        
        let command = UpdateSchemaCommand {
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self) -> crate::data::Snapshot {
        let command = TakeSnapshotCommand {
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<TakeSnapshotCommand, SnapshotResponse>(&command) {
            Ok(snapshot_response) => {
                // Deserialize the JSON data back to Snapshot
                match serde_json::from_str::<crate::data::Snapshot>(&snapshot_response.data) {
                    Ok(snapshot) => snapshot,
                    Err(_) => {
                        // Return empty snapshot on deserialization error
                        crate::data::Snapshot::default()
                    }
                }
            }
            Err(_) => {
                // Return empty snapshot on command error
                crate::data::Snapshot::default()
            }
        }
    }

    /// Find entities with pagination (includes inherited types)
    pub fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let command = FindEntitiesPaginatedCommand {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<FindEntitiesPaginatedCommand, PaginatedEntityResponse>(&command)?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    /// Find entities exactly of the specified type (no inheritance) with pagination
    pub fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let command = FindEntitiesExactCommand {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<FindEntitiesExactCommand, PaginatedEntityResponse>(&command)?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    /// Get all entity types with pagination
    pub fn get_entity_types_paginated(&self, page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        let command = GetEntityTypesPaginatedCommand {
            page_opts: page_opts.cloned(),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<GetEntityTypesPaginatedCommand, PaginatedEntityTypeResponse>(&command)?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    pub fn find_entities(&self, entity_type: EntityType, filter: Option<&str>) -> Result<Vec<EntityId>> {
        let command = FindEntitiesCommand {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let entity_list_response = self.send_command_get_response::<FindEntitiesCommand, EntityListResponse>(&command)?;
        Ok(entity_list_response.entities)
    }

    pub fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        let command = GetEntityTypesCommand {
            _marker: std::marker::PhantomData,
        };
        
        let entity_type_list_response = self.send_command_get_response::<GetEntityTypesCommand, EntityTypeListResponse>(&command)?;
        Ok(entity_type_list_response.entity_types)
    }



    /// Handle a notification command received from the server
    pub(crate) fn handle_notification(&self, notification_cmd: NotificationCommand) {
        // Deserialize the notification from JSON
        let notification: Notification = match serde_json::from_str(&notification_cmd.notification_data) {
            Ok(n) => n,
            Err(_e) => {
                // Silently ignore deserialization errors - they shouldn't happen in normal operation
                return;
            }
        };

        // Get the config_hash from the notification to find matching senders
        let config_hash = notification.config_hash;
        
        // Send to all registered senders for this config
        let senders = self.notification_senders.borrow();
        if let Some((_config, sender_list)) = senders.get(&config_hash) {
            for sender in sender_list {
                // Ignore send errors (receiver might have been dropped)
                let _ = sender.try_send(notification.clone());
            }
        }
    }

    /// Process notifications for up to some time
    /// This checks if any notification commands were received from the server
    pub fn process_notifications(&self) -> Result<()> {
        loop {
            // Try to parse and get the number of bytes consumed
            let consumed_opt = {
                let conn = self.tcp_connection.borrow();
                match RespValue::from_bytes(&conn.read_buffer) {
                    Ok((resp_value, remaining)) => {
                        let consumed = conn.read_buffer.len() - remaining.len();
                        match NotificationCommand::decode(resp_value) {
                            Ok(response_struct) => {
                                Ok(Some((consumed, Some(response_struct))))
                            }
                            Err(_) => {
                                // This should not happen since we only expect notifications here
                                // but if it does, ignore the message because it did not follow
                                // the protocol
                                Ok(Some((consumed, None)))
                            },
                        }
                    }
                    Err(_) => Ok(None)
                }
            }?;
            
            if let Some((consumed, response_struct)) = consumed_opt {
                // Remove only the consumed bytes, keeping the remaining unparsed data
                self.tcp_connection.borrow_mut().read_buffer.drain(..consumed);
                if let Some(response_struct) = response_struct {
                    self.handle_notification(response_struct);
                    
                    // Continue loop to see if we can get the more notifications
                    // without having to read more data
                    continue;
                }
            } else {
                // Read more data
                let readable = self.tcp_connection.borrow_mut()
                    .wait_for_readable(Some(READ_POLL_INTERVAL))
                    .map_err(|e| Error::StoreProxyError(format!("Poll error: {}", e)))?;
                if readable {
                    self.tcp_connection.borrow_mut().read_bytes()
                        .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
                }
                
                break;
            }
        }

        Ok(())
    }

    /// Register notification with provided sender
    /// Note: For proxy, this registers the notification on the remote server
    /// and stores the sender locally to forward notifications
    pub fn register_notification(
        &self,
        config: NotifyConfig,
        sender: Sender<Notification>,
    ) -> Result<()> {
        // Calculate config hash
        let config_hash = crate::data::notifications::hash_notify_config(&config);
        
        // Add sender to our local mapping
        let mut senders = self.notification_senders.borrow_mut();
        let entry = senders.entry(config_hash).or_insert_with(|| (config.clone(), Vec::new()));
        entry.1.push(sender);
        
        let command = RegisterNotificationCommand {
            config,
            _marker: std::marker::PhantomData,
        };
        
        // Register on the server
        self.send_command_ok(&command)
    }

    /// Unregister a notification by removing a specific sender
    pub fn unregister_notification(&self, target_config: &NotifyConfig, sender: &Sender<Notification>) -> bool {
        // Calculate config hash
        let config_hash = crate::data::notifications::hash_notify_config(target_config);
        
        // Remove sender from our local mapping
        let mut senders = self.notification_senders.borrow_mut();
        let should_unregister_on_server = if let Some((_config, sender_list)) = senders.get_mut(&config_hash) {
            // Remove this specific sender by comparing channels
            sender_list.retain(|s| !s.same_channel(sender));
            
            // If no more senders for this config, remove the config entry and unregister on server
            if sender_list.is_empty() {
                senders.remove(&config_hash);
                true
            } else {
                false
            }
        } else {
            // Config not found, nothing to do
            false
        };
        
        // Drop the borrow before calling send_command_ok
        drop(senders);
        
        // Only unregister on server if we removed the last sender for this config
        if should_unregister_on_server {
            let command = UnregisterNotificationCommand {
                config: target_config.clone(),
                _marker: std::marker::PhantomData,
            };
            
            match self.send_command_ok(&command) {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            true
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
        // Convert FieldSchema to FieldSchemaResp
        let field_type_str = Self::resolve_field_type(self, field_type.clone())?;
        let schema_resp = crate::data::entity_schema::FieldSchemaResp {
            field_type: field_type_str,
            rank: schema.rank(),
            default_value: schema.default_value(),
        };

        let command = SetFieldSchemaCommand {
            entity_type,
            field_type,
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        
        self.send_command_ok(&command)
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
        let command = WriteCommand {
            entity_id,
            field_path: field_path.to_vec(),
            value,
            writer_id,
            write_time,
            push_condition,
            adjust_behavior,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let create_response = self.send_command_get_response::<CreateEntityCommand, CreateEntityResponse>(&command)?;
        Ok(create_response.entity_id)
    }

    fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        let command = DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    fn update_schema(&mut self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        // Convert EntitySchema to EntitySchemaResp
        let fields_resp: Vec<crate::data::entity_schema::FieldSchemaResp> = schema
            .fields
            .into_iter()
            .map(|(field_type_str, field_schema)| {
                crate::data::entity_schema::FieldSchemaResp {
                    field_type: field_type_str,
                    rank: field_schema.rank(),
                    default_value: field_schema.default_value(),
                }
            })
            .collect();

        let schema_resp = crate::data::entity_schema::EntitySchemaResp {
            entity_type: schema.entity_type,
            inherit: schema.inherit,
            fields: fields_resp,
        };
        
        let command = UpdateSchemaCommand {
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
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
}
