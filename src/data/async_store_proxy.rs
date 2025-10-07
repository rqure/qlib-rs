use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{
    Complete, EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, PageOpts, PageResult, Result, Single, Value, Timestamp, PushCondition, AdjustBehavior
};
use crate::data::resp::{RespCommand, RespDecode, ReadCommand, WriteCommand, CreateEntityCommand, RespValue, RespToBytes, RespFromBytes, NotificationCommand};

/// Expect an OK response from RESP
fn expect_ok(resp_value: RespValue) -> Result<()> {
    match resp_value {
        RespValue::SimpleString(s) if s == "OK" => Ok(()),
        RespValue::Error(msg) => Err(Error::StoreProxyError(format!("Server error: {}", msg))),
        _ => Err(Error::StoreProxyError("Expected OK response".to_string())),
    }
}

/// Async TCP connection for RESP protocol
#[derive(Debug)]
pub struct AsyncTcpConnection {
    stream: TcpStream,
    pub(crate) read_buffer: Vec<u8>,
}

impl AsyncTcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buffer: Vec::new(),
        }
    }
    
    pub async fn send_bytes(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }
    
    pub async fn read_bytes(&mut self) -> anyhow::Result<()> {
        let mut buffer = [0u8; 65536];
        match self.stream.read(&mut buffer).await {
            Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
            Ok(bytes_read) => {
                self.read_buffer.extend_from_slice(&buffer[..bytes_read]);
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("TCP read error: {}", e)),
        }
    }
}

/// Async version of StoreProxy
#[derive(Debug, Clone)]
pub struct AsyncStoreProxy {
    pub(crate) tcp_connection: Arc<Mutex<AsyncTcpConnection>>,
}

impl AsyncStoreProxy {
    /// Create a new pipeline for batching commands
    pub fn pipeline(&self) -> crate::data::pipeline::AsyncPipeline {
        crate::data::pipeline::AsyncPipeline::new(self)
    }

    /// Handle a notification command received from the server
    pub(crate) fn handle_notification(&self, _notification_cmd: NotificationCommand) {
        // TODO: Implement notification handling for async proxy
        // For now, notifications are ignored in async mode
    }

    /// Connect to TCP server
    pub async fn connect(address: &str) -> Result<Self> {
        // Connect to TCP server
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| Error::StoreProxyError(format!("Failed to connect to {}: {}", address, e)))?;
        
        // Optimize TCP socket for low latency
        stream.set_nodelay(true)
            .map_err(|e| Error::StoreProxyError(format!("Failed to set TCP_NODELAY: {}", e)))?;

        let tcp_connection = AsyncTcpConnection::new(stream);

        Ok(AsyncStoreProxy {
            tcp_connection: Arc::new(Mutex::new(tcp_connection)),
        })
    }

    async fn send_command_get_response<C, R>(&self, command: &C) -> Result<R>
    where
        C: RespCommand<'static>,
        R: for<'a> RespDecode<'a>,
    {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        
        let mut conn = self.tcp_connection.lock().await;
        
        conn.send_bytes(&encoded_bytes)
            .await
            .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;

        loop {
            // Try to parse and get the number of bytes consumed
            let consumed_opt = match RespValue::from_bytes(&conn.read_buffer) {
                Ok((resp_value, remaining)) => {
                    let consumed = conn.read_buffer.len() - remaining.len();
                    match R::decode(resp_value.clone()) {
                        Ok(response_struct) => Some((consumed, Some(response_struct))),
                        Err(_) => {
                            // Try to decode as notification
                            if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                                self.handle_notification(notification);
                                Some((consumed, None))
                            } else {
                                return Err(Error::StoreProxyError(format!("Failed to decode response or notification")));
                            }
                        }
                    }
                }
                Err(_) => None
            };
            
            if let Some((consumed, response_struct)) = consumed_opt {
                // Remove only the consumed bytes, keeping the remaining unparsed data
                conn.read_buffer.drain(..consumed);
                if let Some(response_struct) = response_struct {
                    return Ok(response_struct);
                } else {
                    // Continue loop to get the actual response
                    continue;
                }
            }
            
            // Need more data
            conn.read_bytes()
                .await
                .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
        }
    }

    async fn send_command_ok<C>(&self, command: &C) -> Result<()>
    where
        C: RespCommand<'static>,
    {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        
        let mut conn = self.tcp_connection.lock().await;
        
        conn.send_bytes(&encoded_bytes)
            .await
            .map_err(|e| Error::StoreProxyError(format!("Failed to send command: {}", e)))?;

        loop {
            // Try to parse and get the number of bytes consumed
            let result_opt = match RespValue::from_bytes(&conn.read_buffer) {
                Ok((resp_value, remaining)) => {
                    let consumed = conn.read_buffer.len() - remaining.len();
                    let result = expect_ok(resp_value);
                    Some((consumed, result))
                }
                Err(_) => None
            };
            
            if let Some((consumed, result)) = result_opt {
                // Remove only the consumed bytes, keeping the remaining unparsed data
                conn.read_buffer.drain(..consumed);
                return result;
            }
            
            // Need more data
            conn.read_bytes()
                .await
                .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
        }
    }

    /// Get entity type by name
    pub async fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let command = crate::data::resp::GetEntityTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<crate::data::resp::GetEntityTypeCommand, crate::data::resp::IntegerResponse>(&command).await?;
        Ok(EntityType(integer_response.value as u32))
    }

    /// Resolve entity type to name
    pub async fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let command = crate::data::resp::ResolveEntityTypeCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<crate::data::resp::ResolveEntityTypeCommand, crate::data::resp::StringResponse>(&command).await?;
        Ok(string_response.value)
    }

    /// Get field type by name
    pub async fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let command = crate::data::resp::GetFieldTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let integer_response = self.send_command_get_response::<crate::data::resp::GetFieldTypeCommand, crate::data::resp::IntegerResponse>(&command).await?;
        Ok(FieldType(integer_response.value as u64))
    }

    /// Resolve field type to name
    pub async fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let command = crate::data::resp::ResolveFieldTypeCommand {
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        let string_response = self.send_command_get_response::<crate::data::resp::ResolveFieldTypeCommand, crate::data::resp::StringResponse>(&command).await?;
        Ok(string_response.value)
    }

    /// Get entity schema
    pub async fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        let command = crate::data::resp::GetEntitySchemaCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        
        let schema_resp = self.send_command_get_response::<crate::data::resp::GetEntitySchemaCommand, crate::data::entity_schema::EntitySchemaResp>(&command).await?;
        
        // Convert EntitySchemaResp to EntitySchema<Single, String, String>
        let mut fields = rustc_hash::FxHashMap::default();
        for field_resp in schema_resp.fields {
            let field_type = field_resp.field_type.clone();
            let field_schema = field_resp.to_field_schema();
            fields.insert(field_type, field_schema);
        }

        let mut schema_string = EntitySchema::<Single, String, String>::new(
            schema_resp.entity_type,
            schema_resp.inherit,
        );
        schema_string.fields = fields;
        
        // Convert from string-based schema to typed schema
        let typed_entity_type = self.get_entity_type(&schema_string.entity_type).await?;
        let mut typed_inherit = Vec::new();
        for inherit_str in &schema_string.inherit {
            typed_inherit.push(self.get_entity_type(inherit_str).await?);
        }
        
        let mut typed_fields = rustc_hash::FxHashMap::default();
        for (field_type_str, field_schema) in schema_string.fields {
            let typed_field_type = self.get_field_type(&field_type_str).await?;
            
            // Convert field schema from string to typed
            let typed_field_schema = self.convert_field_schema_from_string(field_schema).await?;
            
            typed_fields.insert(typed_field_type, typed_field_schema);
        }
        
        let mut typed_schema = EntitySchema::<Single, EntityType, FieldType>::new(
            typed_entity_type,
            typed_inherit,
        );
        typed_schema.fields = typed_fields;
        Ok(typed_schema)
    }
    
    /// Helper method to convert FieldSchema<String> to FieldSchema<FieldType>
    pub(crate) async fn convert_field_schema_from_string(&self, schema: FieldSchema<String>) -> Result<FieldSchema<FieldType>> {
        Ok(match schema {
            FieldSchema::Blob { field_type, default_value, rank, storage_scope } => FieldSchema::Blob {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Bool { field_type, default_value, rank, storage_scope } => FieldSchema::Bool {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope } => FieldSchema::Choice {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                choices,
                storage_scope,
            },
            FieldSchema::EntityList { field_type, default_value, rank, storage_scope } => FieldSchema::EntityList {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::EntityReference { field_type, default_value, rank, storage_scope } => FieldSchema::EntityReference {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Float { field_type, default_value, rank, storage_scope } => FieldSchema::Float {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Int { field_type, default_value, rank, storage_scope } => FieldSchema::Int {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::String { field_type, default_value, rank, storage_scope } => FieldSchema::String {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Timestamp { field_type, default_value, rank, storage_scope } => FieldSchema::Timestamp {
                field_type: self.get_field_type(&field_type).await?,
                default_value,
                rank,
                storage_scope,
            },
        })
    }

    /// Get complete entity schema
    pub async fn get_complete_entity_schema(&self, _entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        // TODO: Implement RESP command for complete entity schema
        unimplemented!("Complete entity schema not yet implemented with RESP")
    }

    /// Set field schema
    pub async fn set_field_schema(&self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()> {
        // Convert FieldSchema to FieldSchemaResp
        let field_type_str = self.resolve_field_type(field_type.clone()).await?;
        let schema_resp = crate::data::entity_schema::FieldSchemaResp {
            field_type: field_type_str,
            rank: schema.rank(),
            default_value: schema.default_value(),
            choices: schema.choices(),
        };

        let command = crate::data::resp::SetFieldSchemaCommand {
            entity_type,
            field_type,
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        
        self.send_command_ok(&command).await
    }

    /// Get field schema
    pub async fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema> {
        let command = crate::data::resp::GetFieldSchemaCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        let field_schema_response = self.send_command_get_response::<crate::data::resp::GetFieldSchemaCommand, crate::data::resp::FieldSchemaResponse>(&command).await?;
        
        // Convert FieldSchemaResp back to FieldSchema<FieldType>
        let field_schema_string = field_schema_response.schema.to_field_schema();
        
        // Convert from string-based field schema to typed field schema
        let typed_field_schema = self.convert_field_schema_from_string(field_schema_string).await?;
        
        Ok(typed_field_schema)
    }

    /// Check if entity exists
    pub async fn entity_exists(&self, entity_id: EntityId) -> bool {
        let command = crate::data::resp::EntityExistsCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<crate::data::resp::EntityExistsCommand, crate::data::resp::BooleanResponse>(&command).await {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Check if field exists
    pub async fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        let command = crate::data::resp::FieldExistsCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<crate::data::resp::FieldExistsCommand, crate::data::resp::BooleanResponse>(&command).await {
            Ok(boolean_response) => boolean_response.result,
            Err(_) => false, // Default to false on any error
        }
    }

    /// Resolve indirection
    pub async fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        let command = crate::data::resp::ResolveIndirectionCommand {
            entity_id,
            fields: fields.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let resolve_response = self.send_command_get_response::<crate::data::resp::ResolveIndirectionCommand, crate::data::resp::ResolveIndirectionResponse>(&command).await?;
        Ok((resolve_response.entity_id, resolve_response.field_type))
    }

    /// Read a field value
    pub async fn read(&self, entity_id: EntityId, field_path: &[FieldType]) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        
        let read_response = self.send_command_get_response::<ReadCommand, crate::data::resp::ReadResponse>(&command).await?;
        Ok((read_response.value, read_response.timestamp, read_response.writer_id))
    }

    /// Write a field value
    #[allow(unused_variables)]
    pub async fn write(&self, entity_id: EntityId, field_path: &[FieldType], value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior>) -> Result<()> {
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
        self.send_command_ok(&command).await
    }

    /// Create a new entity
    pub async fn create_entity(&self, entity_type: EntityType, parent_id: Option<EntityId>, name: &str) -> Result<EntityId> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        
        let create_response = self.send_command_get_response::<CreateEntityCommand, crate::data::resp::CreateEntityResponse>(&command).await?;
        Ok(create_response.entity_id)
    }

    /// Delete an entity
    pub async fn delete_entity(&self, entity_id: EntityId) -> Result<()> {
        let command = crate::data::resp::DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command).await
    }

    /// Update entity schema
    pub async fn update_schema(&self, schema: EntitySchema<Single, String, String>) -> Result<()> {
        // Convert EntitySchema to EntitySchemaResp
        let fields_resp: Vec<crate::data::entity_schema::FieldSchemaResp> = schema
            .fields
            .into_iter()
            .map(|(field_type_str, field_schema)| {
                crate::data::entity_schema::FieldSchemaResp {
                    field_type: field_type_str,
                    rank: field_schema.rank(),
                    default_value: field_schema.default_value(),
                    choices: field_schema.choices(),
                }
            })
            .collect();

        let schema_resp = crate::data::entity_schema::EntitySchemaResp {
            entity_type: schema.entity_type,
            inherit: schema.inherit,
            fields: fields_resp,
        };
        
        let command = crate::data::resp::UpdateSchemaCommand {
            schema: schema_resp,
            _marker: std::marker::PhantomData,
        };
        self.send_command_ok(&command).await
    }

    /// Take a snapshot of the current store state
    pub async fn take_snapshot(&self) -> crate::data::Snapshot {
        let command = crate::data::resp::TakeSnapshotCommand {
            _marker: std::marker::PhantomData,
        };
        
        match self.send_command_get_response::<crate::data::resp::TakeSnapshotCommand, crate::data::resp::SnapshotResponse>(&command).await {
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
    pub async fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let command = crate::data::resp::FindEntitiesPaginatedCommand {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<crate::data::resp::FindEntitiesPaginatedCommand, crate::data::resp::PaginatedEntityResponse>(&command).await?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    /// Find entities exactly of the specified type (no inheritance) with pagination
    pub async fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        let command = crate::data::resp::FindEntitiesExactCommand {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<crate::data::resp::FindEntitiesExactCommand, crate::data::resp::PaginatedEntityResponse>(&command).await?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    /// Get all entity types with pagination
    pub async fn get_entity_types_paginated(&self, page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        let command = crate::data::resp::GetEntityTypesPaginatedCommand {
            page_opts: page_opts.cloned(),
            _marker: std::marker::PhantomData,
        };
        
        let paginated_response = self.send_command_get_response::<crate::data::resp::GetEntityTypesPaginatedCommand, crate::data::resp::PaginatedEntityTypeResponse>(&command).await?;
        
        Ok(PageResult::new(
            paginated_response.items,
            paginated_response.total,
            paginated_response.next_cursor,
        ))
    }

    /// Find entities of a specific type (includes inherited types)
    pub async fn find_entities(&self, entity_type: EntityType, filter: Option<&str>) -> Result<Vec<EntityId>> {
        let command = crate::data::resp::FindEntitiesCommand {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        
        let entity_list_response = self.send_command_get_response::<crate::data::resp::FindEntitiesCommand, crate::data::resp::EntityListResponse>(&command).await?;
        Ok(entity_list_response.entities)
    }

    /// Get all entity types
    pub async fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        let command = crate::data::resp::GetEntityTypesCommand {
            _marker: std::marker::PhantomData,
        };
        
        let entity_type_list_response = self.send_command_get_response::<crate::data::resp::GetEntityTypesCommand, crate::data::resp::EntityTypeListResponse>(&command).await?;
        Ok(entity_type_list_response.entity_types)
    }

    /// Register notification with provided sender
    /// Note: For proxy, this registers the notification on the remote server
    /// and stores the sender locally to forward notifications
    pub async fn register_notification(
        &self,
        config: crate::NotifyConfig,
        _sender: crate::NotificationQueue,
    ) -> Result<()> {
        let command = crate::data::resp::RegisterNotificationCommand {
            config,
            _marker: std::marker::PhantomData,
        };
        
        // Note: For proxy implementation, we only register on the server
        // The sender is ignored since we can't forward notifications in this simple implementation
        self.send_command_ok(&command).await
    }

    /// Unregister a notification by removing a specific sender
    /// Note: This will remove ALL notifications matching the config for proxy
    pub async fn unregister_notification(&self, target_config: &crate::NotifyConfig, _sender: &crate::NotificationQueue) -> bool {
        let command = crate::data::resp::UnregisterNotificationCommand {
            config: target_config.clone(),
            _marker: std::marker::PhantomData,
        };
        
        // Note: For proxy implementation, we only unregister on the server
        // The sender is ignored since we can't manage specific senders in this simple implementation
        match self.send_command_ok(&command).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
