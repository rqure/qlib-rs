use std::cell::RefCell;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::Duration;

use mio::{Events, Interest, Poll, Token};

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, PageOpts, PageResult, Result, Single, Value, Timestamp, PushCondition, AdjustBehavior
};
use crate::data::StoreTrait;
use crate::data::resp::{RespResponse, RespCommand, RespEncode, RespDecode, ReadCommand, WriteCommand, CreateEntityCommand, RespValue};

const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn expect_ok(response: RespResponse) -> Result<()> {
    match response {
        RespResponse::Ok => Ok(()),
        RespResponse::Null => Ok(()),
        RespResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
        _ => Err(Error::StoreProxyError(format!(
            "Unexpected response (expected OK): {:?}",
            response
        ))),
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
    
    pub fn try_receive_resp(&mut self) -> anyhow::Result<Option<RespResponse>> {
        // For RESP, we need to read until we have a complete response
        // This is a simplified implementation - in practice, you'd want proper buffering
        let mut buffer = [0u8; 8192];
        match self.stream.read(&mut buffer) {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                let data = &buffer[..bytes_read];
                // Try to decode a RESP value first
                match RespValue::decode(data) {
                    Ok((resp_value, _remaining)) => {
                        // Convert RespValue to RespResponse - this is a simplified conversion
                        let response = match resp_value {
                            RespValue::SimpleString(s) if s == "OK" => RespResponse::Ok,
                            RespValue::SimpleString(s) => RespResponse::String(s.to_string()),
                            RespValue::Error(s) => RespResponse::Error(s.to_string()),
                            RespValue::Integer(i) => RespResponse::Integer(i),
                            RespValue::BulkString(data) => RespResponse::Bulk(data.to_vec()),
                            RespValue::Null => RespResponse::Null,
                            RespValue::Array(elements) => {
                                let responses: crate::Result<Vec<RespResponse>> = elements.into_iter()
                                    .map(|v| match v {
                                        RespValue::SimpleString(s) => Ok(RespResponse::String(s.to_string())),
                                        RespValue::Error(s) => Ok(RespResponse::Error(s.to_string())),
                                        RespValue::Integer(i) => Ok(RespResponse::Integer(i)),
                                        RespValue::BulkString(data) => Ok(RespResponse::Bulk(data.to_vec())),
                                        RespValue::Null => Ok(RespResponse::Null),
                                        _ => Err(crate::Error::InvalidRequest("Unsupported array element type".to_string())),
                                    })
                                    .collect();
                                match responses {
                                    Ok(responses) => RespResponse::Array(responses),
                                    Err(_) => return Ok(None),
                                }
                            }
                        };
                        Ok(Some(response))
                    }
                    Err(_) => Ok(None), // Incomplete response, need more data
                }
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

    fn poll_for_resp(&self) -> Result<Option<RespResponse>> {
        let result = {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.try_receive_resp()
        };

        result.map_err(|e| Error::StoreProxyError(format!("TCP error: {}", e)))
    }

    fn send_resp_command<C: RespCommand<'static>>(&self, command: &C) -> Result<RespResponse> {
        let encoded = command.encode();

        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;
        }

        loop {
            match self.poll_for_resp()? {
                Some(response) => {
                    return Ok(response);
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
        let response = self.send_resp_command(command)?;
        expect_ok(response)
    }

    /// Get entity type by name
    pub fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let command_data = RespValue::Array(vec![
            RespValue::BulkString(b"GET_ENTITY_TYPE"),
            RespValue::BulkString(name.as_bytes()),
        ]);
        let encoded = command_data.encode();
        
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send get_entity_type command: {}", e)))?;
        }
        
        let response = self.poll_for_resp()?;
        match response {
            Some(RespResponse::Integer(i)) if i >= 0 => Ok(EntityType(i as u32)),
            Some(RespResponse::Error(msg)) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Unexpected response for get_entity_type".into())),
        }
    }

    /// Resolve entity type to name
    pub fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let command_data = RespValue::Array(vec![
            RespValue::BulkString(b"RESOLVE_ENTITY_TYPE"),
            RespValue::Integer(entity_type.0 as i64),
        ]);
        let encoded = command_data.encode();
        
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send resolve_entity_type command: {}", e)))?;
        }
        
        let response = self.poll_for_resp()?;
        match response {
            Some(RespResponse::String(s)) => Ok(s),
            Some(RespResponse::Error(msg)) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Unexpected response for resolve_entity_type".into())),
        }
    }

    /// Get field type by name
    pub fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let command_data = RespValue::Array(vec![
            RespValue::BulkString(b"GET_FIELD_TYPE"),
            RespValue::BulkString(name.as_bytes()),
        ]);
        let encoded = command_data.encode();
        
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send get_field_type command: {}", e)))?;
        }
        
        let response = self.poll_for_resp()?;
        match response {
            Some(RespResponse::Integer(i)) if i >= 0 => Ok(FieldType(i as u64)),
            Some(RespResponse::Error(msg)) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Unexpected response for get_field_type".into())),
        }
    }

    /// Resolve field type to name
    pub fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let command_data = RespValue::Array(vec![
            RespValue::BulkString(b"RESOLVE_FIELD_TYPE"),
            RespValue::Integer(field_type.0 as i64),
        ]);
        let encoded = command_data.encode();
        
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send resolve_field_type command: {}", e)))?;
        }
        
        let response = self.poll_for_resp()?;
        match response {
            Some(RespResponse::String(s)) => Ok(s),
            Some(RespResponse::Error(msg)) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Unexpected response for resolve_field_type".into())),
        }
    }

    /// Get entity schema
    pub fn get_entity_schema(&self, _entity_type: EntityType) -> Result<EntitySchema<Single>> {
        // TODO: Implement RESP command for entity schema
        unimplemented!("Entity schema not yet implemented with RESP")
    }

    /// Get complete entity schema
    pub fn get_complete_entity_schema(&self, _entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        // TODO: Implement RESP command for complete entity schema
        unimplemented!("Complete entity schema not yet implemented with RESP")
    }

    /// Set field schema
    pub fn set_field_schema(&mut self, _entity_type: EntityType, _field_type: FieldType, _schema: FieldSchema) -> Result<()> {
        // TODO: Implement RESP command for field schema
        unimplemented!("Field schema operations not yet implemented with RESP")
    }

    /// Get field schema
    pub fn get_field_schema(&self, _entity_type: EntityType, _field_type: FieldType) -> Result<FieldSchema> {
        // TODO: Implement RESP command for field schema
        unimplemented!("Field schema operations not yet implemented with RESP")
    }

    /// Check if entity exists
    pub fn entity_exists(&self, _entity_id: EntityId) -> bool {
        // TODO: Implement RESP command for entity existence check
        false
    }

    /// Check if field exists
    pub fn field_exists(&self, _entity_type: EntityType, _field_type: FieldType) -> bool {
        // TODO: Implement RESP command for field existence check
        false
    }

    /// Resolve indirection
    pub fn resolve_indirection(&self, _entity_id: EntityId, _fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        // TODO: Implement RESP command for indirection resolution
        unimplemented!("Indirection resolution not yet implemented with RESP")
    }

    /// Read a field value
    pub fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        let response = self.send_resp_command(&command)?;
        
        match response {
            RespResponse::Array(elements) if elements.len() >= 3 => {
                // Parse value from first element
                let value = match &elements[0] {
                    RespResponse::Bulk(data) => {
                        // Try to decode a Value from the bulk data
                        match Value::decode(data) {
                            Ok((value, _)) => value,
                            Err(_) => Value::Blob(data.clone()),
                        }
                    },
                    RespResponse::String(s) => Value::String(s.clone()),
                    RespResponse::Integer(i) => Value::Int(*i),
                    RespResponse::Null => Value::EntityReference(None),
                    _ => return Err(Error::StoreProxyError("Invalid value type in read response".into())),
                };
                
                // Parse timestamp from second element
                let timestamp = match &elements[1] {
                    RespResponse::Bulk(data) => {
                        let timestamp_str = std::str::from_utf8(data)
                            .map_err(|_| Error::StoreProxyError("Invalid UTF-8 in timestamp".into()))?;
                        // Parse as RFC3339 timestamp
                        time::OffsetDateTime::parse(timestamp_str, &time::format_description::well_known::Rfc3339)
                            .map_err(|_| Error::StoreProxyError("Invalid timestamp format".into()))?
                    },
                    RespResponse::String(s) => {
                        // Parse as RFC3339 timestamp
                        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                            .map_err(|_| Error::StoreProxyError("Invalid timestamp format".into()))?
                    },
                    _ => return Err(Error::StoreProxyError("Invalid timestamp type in read response".into())),
                };
                
                // Parse writer_id from third element
                let writer_id = match &elements[2] {
                    RespResponse::Integer(i) if *i >= 0 => Some(EntityId(*i as u64)),
                    RespResponse::Null => None,
                    _ => return Err(Error::StoreProxyError("Invalid writer_id type in read response".into())),
                };
                
                Ok((value, timestamp, writer_id))
            }
            RespResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Expected array response for read".into())),
        }
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
        let response = self.send_resp_command(&command)?;
        
        match response {
            RespResponse::Integer(i) if i >= 0 => Ok(EntityId(i as u64)),
            RespResponse::Error(msg) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Expected Integer response for create_entity".into())),
        }
    }

    /// Delete an entity
    pub fn delete_entity(&mut self, entity_id: EntityId) -> Result<()> {
        // For now, use a simple array command since we don't have a specific RESP command
        let command_data = RespValue::Array(vec![
            RespValue::BulkString(b"DELETE_ENTITY"),
            RespValue::Integer(entity_id.0 as i64),
        ]);
        let encoded = command_data.encode();
        
        {
            let mut conn = self.tcp_connection.borrow_mut();
            conn.send_bytes(&encoded)
                .map_err(|e| Error::StoreProxyError(format!("Failed to send delete_entity command: {}", e)))?;
        }
        
        let response = self.poll_for_resp()?;
        match response {
            Some(RespResponse::Ok) => Ok(()),
            Some(RespResponse::Error(msg)) => Err(Error::StoreProxyError(msg)),
            _ => Err(Error::StoreProxyError("Unexpected response for delete_entity".into())),
        }
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

    pub fn find_entities(&self, _entity_type: EntityType, _filter: Option<&str>) -> Result<Vec<EntityId>> {
        // TODO: Implement RESP command for entity finding
        unimplemented!("Entity finding not yet implemented with RESP")
    }

    pub fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        // TODO: Implement RESP command for entity types
        unimplemented!("Entity types listing not yet implemented with RESP")
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
