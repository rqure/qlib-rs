use std::cell::RefCell;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::Duration;

use mio::{Events, Interest, Poll, Token};

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, PageOpts, PageResult, Result, Single, Value, Timestamp, PushCondition, AdjustBehavior
};
use crate::data::StoreTrait;
use crate::data::resp::{RespCommand, RespDecode, ReadCommand, WriteCommand, CreateEntityCommand, RespValue};

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
    
    pub fn try_receive_resp(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        // For RESP, we need to read until we have a complete response
        // This is a simplified implementation - in practice, you'd want proper buffering
        let mut buffer = [0u8; 8192];
        match self.stream.read(&mut buffer) {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                let data = buffer[..bytes_read].to_vec();
                Ok(Some(data))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(None) // No data available
            }
            Err(e) => Err(anyhow::anyhow!("TCP read error: {}", e)),
        }
    }
}

#[derive(Debug)]
pub struct StoreProxy {
    tcp_connection: Rc<RefCell<TcpConnection>>,
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

        Ok(StoreProxy {
            tcp_connection,
        })
    }

    fn poll_for_resp(&self) -> Result<Option<Vec<u8>>> {
        let result = {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.try_receive_resp()
        };

        result.map_err(|e| Error::StoreProxyError(format!("TCP error: {}", e)))
    }

    fn send_command_get_response<C, R>(&self, command: &C) -> Result<R>
    where
        C: RespCommand<'static>,
        R: for<'a> RespDecode<'a>,
    {
        let encoded = command.encode();

        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;
        }

        loop {
            match self.poll_for_resp()? {
                Some(data) => {
                    // Decode the RESP value from the received data
                    match RespValue::decode(&data) {
                        Ok((resp_value, _remaining)) => {
                            match resp_value {
                                RespValue::BulkString(data) => {
                                    let (response_struct, _) = R::decode(&data)
                                        .map_err(|_| Error::StoreProxyError("Failed to decode structured response".into()))?;
                                    return Ok(response_struct);
                                }
                                RespValue::Error(msg) => return Err(Error::StoreProxyError(msg.to_string())),
                                _ => return Err(Error::StoreProxyError("Expected bulk string response for structured data".into())),
                            }
                        }
                        Err(_) => continue, // Incomplete response, try again
                    }
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

    fn send_command_ok<C: RespCommand<'static>>(&self, command: &C) -> Result<()> {
        let encoded = command.encode();

        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;
        }

        loop {
            match self.poll_for_resp()? {
                Some(data) => {
                    // Decode the RESP value from the received data
                    match RespValue::decode(&data) {
                        Ok((resp_value, _remaining)) => {
                            return expect_ok(resp_value);
                        }
                        Err(_) => continue, // Incomplete response, try again
                    }
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

    /// Get entity type by name
    pub fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let command = crate::data::resp::GetEntityTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<crate::data::resp::GetEntityTypeCommand, crate::data::resp::IntegerResponse>(&command)?;
        Ok(EntityType(integer_response.value as u32))
    }

    /// Resolve entity type to name
    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let command = crate::data::resp::ResolveEntityTypeCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<crate::data::resp::ResolveEntityTypeCommand, crate::data::resp::StringResponse>(&command)?;
        Ok(string_response.value)
    }

    /// Get field type by name
    pub fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let command = crate::data::resp::GetFieldTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<crate::data::resp::GetFieldTypeCommand, crate::data::resp::IntegerResponse>(&command)?;
        Ok(FieldType(integer_response.value as u64))
    }

    /// Resolve field type to name
    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let command = crate::data::resp::ResolveFieldTypeCommand {
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<crate::data::resp::ResolveFieldTypeCommand, crate::data::resp::StringResponse>(&command)?;
        Ok(string_response.value)
    }

    /// Get entity schema
    pub fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        let _command = crate::data::resp::GetEntitySchemaCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        // TODO: EntitySchema needs RespDecode implementation
        unimplemented!("Entity schema decoding not yet implemented with RESP")
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(&self, _entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        // TODO: Implement RESP command for complete entity schema
        unimplemented!("Complete entity schema not yet implemented with RESP")
    }

    /// Set field schema
    pub fn set_field_schema(&mut self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()> {
        // TODO: Need to serialize FieldSchema to String
        let _command = crate::data::resp::SetFieldSchemaCommand {
            entity_type,
            field_type,
            schema: format!("{:?}", schema), // Temporary serialization
            _marker: std::marker::PhantomData,
        };
        // TODO: Implement proper FieldSchema serialization
        unimplemented!("Field schema serialization not yet implemented with RESP")
    }

    /// Get field schema
    pub fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema> {
        let _command = crate::data::resp::GetFieldSchemaCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        // TODO: FieldSchema needs RespDecode implementation
        unimplemented!("Field schema decoding not yet implemented with RESP")
    }

    /// Check if entity exists
    pub fn entity_exists(&self, entity_id: EntityId) -> bool {
        let command = crate::data::resp::EntityExistsCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<crate::data::resp::EntityExistsCommand, crate::data::resp::BooleanResponse>(&command) {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Check if field exists
    pub fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        let command = crate::data::resp::FieldExistsCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<crate::data::resp::FieldExistsCommand, crate::data::resp::BooleanResponse>(&command) {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Resolve indirection
    pub fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        let command = crate::data::resp::ResolveIndirectionCommand {
            entity_id,
            fields: fields.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let resolve_response = self.send_command_get_response::<crate::data::resp::ResolveIndirectionCommand, crate::data::resp::ResolveIndirectionResponse>(&command)?;
        Ok((resolve_response.entity_id, resolve_response.field_type))
    }

    /// Read a field value
    pub fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let read_response = self.send_command_get_response::<ReadCommand, crate::data::resp::ReadResponse>(&command)?;
        Ok((read_response.value, read_response.timestamp, read_response.writer_id))
    }

    /// Write a field value
    #[allow(unused_variables)]
    pub fn write(&mut self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
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
    pub fn create_entity(&mut self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let create_response = self.send_command_get_response::<CreateEntityCommand, crate::data::resp::CreateEntityResponse>(&command)?;
        Ok(create_response.entity_id)
    }

    /// Delete an entity
    pub fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        let command = crate::data::resp::DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command)
    }

    /// Update entity schema
    pub fn update_schema(&mut self, _schema: EntitySchema<Single, String, String>) -> Result<()> {
        // TODO: Implement RESP command for schema updates
        unimplemented!("Schema updates not yet implemented with RESP")
    }

    /// Take a snapshot of the current store state
    pub fn take_snapshot(&self) -> crate::data::Snapshot {
        // TODO: Implement RESP command for snapshots
        unimplemented!("Snapshots not yet implemented with RESP")
    }

    /// Find entities with pagination (includes inherited types)
    pub fn find_entities_paginated(&self, _entity_type: EntityType, _page_opts: Option<&PageOpts>, _filter: Option<&str>) -> Result<PageResult<EntityId>> {
        // TODO: Implement RESP command for pagination
        unimplemented!("Pagination not yet implemented with RESP")
    }

    /// Find entities exactly of the specified type (no inheritance) with pagination
    pub fn find_entities_exact(&self, _entity_type: EntityType, _page_opts: Option<&PageOpts>, _filter: Option<&str>) -> Result<PageResult<EntityId>> {
        // TODO: Implement RESP command for pagination
        unimplemented!("Pagination not yet implemented with RESP")
    }

    /// Get all entity types with pagination
    pub fn get_entity_types_paginated(&self, _page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        // TODO: Implement RESP command for pagination
        unimplemented!("Pagination not yet implemented with RESP")
    }

    pub fn find_entities(&self, entity_type: EntityType, filter: Option<&str>) -> Result<Vec<EntityId>> {
        let command = crate::data::resp::FindEntitiesCommand {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let entity_list_response = self.send_command_get_response::<crate::data::resp::FindEntitiesCommand, crate::data::resp::EntityListResponse>(&command)?;
        Ok(entity_list_response.entities)
    }

    pub fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        let command = crate::data::resp::GetEntityTypesCommand {
            _marker: std::marker::PhantomData,
        };
        
        let entity_type_list_response = self.send_command_get_response::<crate::data::resp::GetEntityTypesCommand, crate::data::resp::EntityTypeListResponse>(&command)?;
        Ok(entity_type_list_response.entity_types)
    }



    /// Register notification with provided sender
    /// Note: For proxy, this registers the notification on the remote server
    /// and stores the sender locally to forward notifications
    pub fn register_notification(
        &mut self,
        _config: crate::NotifyConfig,
        _sender: crate::NotificationQueue,
    ) -> Result<()> {
        // TODO: Implement RESP command for notifications
        unimplemented!("Notifications not yet implemented with RESP")
    }

    /// Unregister a notification by removing a specific sender
    /// Note: This will remove ALL notifications matching the config for proxy
   pub fn unregister_notification(&mut self, _target_config: &crate::NotifyConfig, _sender: &crate::NotificationQueue) -> bool {
        // TODO: Implement RESP command for notifications
        unimplemented!("Notifications not yet implemented with RESP")
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

    fn register_notification(&mut self, config: crate::NotifyConfig, sender: crate::NotificationQueue) -> Result<()> {
        self.register_notification(config, sender)
    }

    fn unregister_notification(&mut self, config: &crate::NotifyConfig, sender: &crate::NotificationQueue) -> bool {
        self.unregister_notification(config, sender)
    }
}
