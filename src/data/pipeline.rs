//! # Pipeline API
//! 
//! Redis-style pipelining API for batching multiple commands and executing them efficiently.
//! 
//! ## Overview
//! 
//! Pipelining allows you to send multiple commands to the server without waiting for individual
//! responses, significantly improving throughput by reducing round-trip latency.
//! 
//! ## Usage Example (Sync)
//! 
//! ```rust,ignore
//! use qlib_rs::data::{StoreProxy, Pipeline};
//! 
//! let proxy = StoreProxy::connect("127.0.0.1:6379")?;
//! let mut pipeline = proxy.pipeline();
//! 
//! // Queue multiple commands
//! pipeline.read(entity_id, &field_path)?;
//! pipeline.write(entity_id, &field_path, value, None, None, None, None)?;
//! pipeline.create_entity(entity_type, None, "test")?;
//! 
//! // Execute all commands at once
//! let results = pipeline.execute()?;
//! 
//! // Extract results in order
//! let read_result: (Value, Timestamp, Option<EntityId>) = results.get(0)?;
//! let write_ok: () = results.get(1)?;
//! let entity_id: EntityId = results.get(2)?;
//! ```
//! 
//! ## Usage Example (Async)
//! 
//! ```rust,ignore
//! use qlib_rs::data::{AsyncStoreProxy, AsyncPipeline};
//! 
//! let proxy = AsyncStoreProxy::connect("127.0.0.1:6379").await?;
//! let mut pipeline = proxy.pipeline();
//! 
//! // Queue multiple commands
//! pipeline.read(entity_id, &field_path)?;
//! pipeline.write(entity_id, &field_path, value, None, None, None, None)?;
//! pipeline.create_entity(entity_type, None, "test")?;
//! 
//! // Execute all commands at once
//! let results = pipeline.execute().await?;
//! 
//! // Extract results in order
//! let read_result: (Value, Timestamp, Option<EntityId>) = results.get(0)?;
//! let write_ok: () = results.get(1)?;
//! let entity_id: EntityId = results.get(2)?;
//! ```

use crate::{
    EntityId, EntitySchema, EntityType, Error, FieldSchema, FieldType, PageResult, Result, Single, Value, Timestamp, PushCondition, AdjustBehavior
};
use std::time::Duration;
use crate::data::resp::{
    RespCommand, RespDecode, RespValue, RespToBytes, RespFromBytes,
    ReadCommand, WriteCommand, CreateEntityCommand, DeleteEntityCommand,
    GetEntityTypeCommand, ResolveEntityTypeCommand, GetFieldTypeCommand, ResolveFieldTypeCommand,
    EntityExistsCommand, FieldExistsCommand,
    FindEntitiesCommand, GetEntityTypesCommand,
    NotificationCommand,
};

/// A queued command in the pipeline
#[derive(Debug)]
struct QueuedCommand {
    encoded_bytes: Vec<u8>,
    response_type: ResponseType,
}

/// Type information for decoding responses
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ResponseType {
    Read,
    Write,
    CreateEntity,
    DeleteEntity,
    GetEntityType,
    ResolveEntityType,
    GetFieldType,
    ResolveFieldType,
    GetEntitySchema,
    UpdateSchema,
    GetFieldSchema,
    SetFieldSchema,
    EntityExists,
    FieldExists,
    ResolveIndirection,
    FindEntitiesPaginated,
    FindEntitiesExact,
    FindEntities,
    GetEntityTypes,
    GetEntityTypesPaginated,
    TakeSnapshot,
}

/// Results from pipeline execution
#[derive(Debug)]
pub struct PipelineResults {
    responses: Vec<DecodedResponse>,
}

/// Decoded response variants
#[derive(Debug)]
pub enum DecodedResponse {
    Read((Value, Timestamp, Option<EntityId>)),
    Write(()),
    CreateEntity(EntityId),
    DeleteEntity(()),
    GetEntityType(EntityType),
    ResolveEntityType(String),
    GetFieldType(FieldType),
    ResolveFieldType(String),
    GetEntitySchema(EntitySchema<Single>),
    UpdateSchema(()),
    GetFieldSchema(FieldSchema<String>),  // Keep as string for sync version
    SetFieldSchema(()),
    EntityExists(bool),
    FieldExists(bool),
    ResolveIndirection((EntityId, FieldType)),
    FindEntitiesPaginated(PageResult<EntityId>),
    FindEntitiesExact(PageResult<EntityId>),
    FindEntities(Vec<EntityId>),
    GetEntityTypes(Vec<EntityType>),
    GetEntityTypesPaginated(PageResult<EntityType>),
    TakeSnapshot(String),  // JSON string
}

impl PipelineResults {
    /// Get a result at a specific index, with type checking
    pub fn get<T: FromDecodedResponse>(&self, index: usize) -> Result<T> {
        self.responses
            .get(index)
            .ok_or_else(|| Error::StoreProxyError(format!("Pipeline result index {} out of bounds", index)))
            .and_then(|r| T::from_decoded(r))
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.responses.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.responses.is_empty()
    }

    /// Iterate over all results
    pub fn iter(&self) -> impl Iterator<Item = &DecodedResponse> {
        self.responses.iter()
    }
}

/// Trait for extracting typed values from decoded responses
pub trait FromDecodedResponse: Sized {
    fn from_decoded(response: &DecodedResponse) -> Result<Self>;
}

impl FromDecodedResponse for (Value, Timestamp, Option<EntityId>) {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::Read(val) => Ok(val.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected Read response".to_string())),
        }
    }
}

impl FromDecodedResponse for () {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::Write(()) | DecodedResponse::DeleteEntity(()) | 
            DecodedResponse::UpdateSchema(()) | DecodedResponse::SetFieldSchema(()) => Ok(()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected OK response".to_string())),
        }
    }
}

impl FromDecodedResponse for EntityId {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::CreateEntity(id) => Ok(*id),
            _ => Err(Error::StoreProxyError("Type mismatch: expected CreateEntity response".to_string())),
        }
    }
}

impl FromDecodedResponse for EntityType {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetEntityType(et) => Ok(*et),
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetEntityType response".to_string())),
        }
    }
}

// Removed - conflicting with new implementation below

impl FromDecodedResponse for FieldType {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetFieldType(ft) => Ok(*ft),
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetFieldType response".to_string())),
        }
    }
}

impl FromDecodedResponse for EntitySchema<Single> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetEntitySchema(schema) => Ok(schema.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetEntitySchema response".to_string())),
        }
    }
}

// For sync pipeline, FieldSchema is returned as string-based
impl FromDecodedResponse for FieldSchema<String> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetFieldSchema(schema) => Ok(schema.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetFieldSchema response".to_string())),
        }
    }
}

// For typed FieldSchema, users need to manually convert using proxy methods
impl FromDecodedResponse for FieldSchema {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetFieldSchema(_) => {
                Err(Error::StoreProxyError("GetFieldSchema returns string-based schema. Use FieldSchema<String> or use AsyncPipeline for typed schema".to_string()))
            }
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetFieldSchema response".to_string())),
        }
    }
}

impl FromDecodedResponse for bool {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::EntityExists(b) | DecodedResponse::FieldExists(b) => Ok(*b),
            _ => Err(Error::StoreProxyError("Type mismatch: expected bool response".to_string())),
        }
    }
}

impl FromDecodedResponse for (EntityId, FieldType) {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::ResolveIndirection(val) => Ok(*val),
            _ => Err(Error::StoreProxyError("Type mismatch: expected ResolveIndirection response".to_string())),
        }
    }
}

impl FromDecodedResponse for Vec<EntityId> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::FindEntities(vec) => Ok(vec.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected FindEntities response".to_string())),
        }
    }
}

impl FromDecodedResponse for Vec<EntityType> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetEntityTypes(vec) => Ok(vec.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected GetEntityTypes response".to_string())),
        }
    }
}

impl FromDecodedResponse for PageResult<EntityId> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::FindEntitiesPaginated(page) | DecodedResponse::FindEntitiesExact(page) => Ok(page.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected paginated EntityId response".to_string())),
        }
    }
}

impl FromDecodedResponse for PageResult<EntityType> {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::GetEntityTypesPaginated(page) => Ok(page.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected paginated EntityType response".to_string())),
        }
    }
}

// TakeSnapshot returns JSON string that needs to be deserialized
impl FromDecodedResponse for String {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::ResolveEntityType(s) | DecodedResponse::ResolveFieldType(s) | DecodedResponse::TakeSnapshot(s) => Ok(s.clone()),
            _ => Err(Error::StoreProxyError("Type mismatch: expected String response".to_string())),
        }
    }
}

impl FromDecodedResponse for crate::data::Snapshot {
    fn from_decoded(response: &DecodedResponse) -> Result<Self> {
        match response {
            DecodedResponse::TakeSnapshot(json_data) => {
                serde_json::from_str(json_data)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to deserialize snapshot: {}", e)))
            }
            _ => Err(Error::StoreProxyError("Type mismatch: expected TakeSnapshot response".to_string())),
        }
    }
}

/// Synchronous pipeline for batching commands
pub struct Pipeline<'a> {
    proxy: &'a crate::data::StoreProxy,
    commands: Vec<QueuedCommand>,
}

impl<'a> Pipeline<'a> {
    pub(crate) fn new(proxy: &'a crate::data::StoreProxy) -> Self {
        Self {
            proxy,
            commands: Vec::new(),
        }
    }

    /// Queue a read command
    pub fn read(&mut self, entity_id: EntityId, field_path: &[FieldType]) -> Result<&mut Self> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::Read)?;
        Ok(self)
    }

    /// Queue a write command
    pub fn write(
        &mut self,
        entity_id: EntityId,
        field_path: &[FieldType],
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<Timestamp>,
        push_condition: Option<PushCondition>,
        adjust_behavior: Option<AdjustBehavior>,
    ) -> Result<&mut Self> {
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
        self.queue_command(command, ResponseType::Write)?;
        Ok(self)
    }

    /// Queue a create entity command
    pub fn create_entity(
        &mut self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<&mut Self> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::CreateEntity)?;
        Ok(self)
    }

    /// Queue a delete entity command
    pub fn delete_entity(&mut self, entity_id: EntityId) -> Result<&mut Self> {
        let command = DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::DeleteEntity)?;
        Ok(self)
    }

    /// Queue a get entity type command
    pub fn get_entity_type(&mut self, name: &str) -> Result<&mut Self> {
        let command = GetEntityTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetEntityType)?;
        Ok(self)
    }

    /// Queue a resolve entity type command
    pub fn resolve_entity_type(&mut self, entity_type: EntityType) -> Result<&mut Self> {
        let command = ResolveEntityTypeCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::ResolveEntityType)?;
        Ok(self)
    }

    /// Queue a get field type command
    pub fn get_field_type(&mut self, name: &str) -> Result<&mut Self> {
        let command = GetFieldTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetFieldType)?;
        Ok(self)
    }

    /// Queue a resolve field type command
    pub fn resolve_field_type(&mut self, field_type: FieldType) -> Result<&mut Self> {
        let command = ResolveFieldTypeCommand {
            field_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::ResolveFieldType)?;
        Ok(self)
    }

    /// Queue an entity exists command
    pub fn entity_exists(&mut self, entity_id: EntityId) -> Result<&mut Self> {
        let command = EntityExistsCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::EntityExists)?;
        Ok(self)
    }

    /// Queue a field exists command
    pub fn field_exists(&mut self, entity_type: EntityType, field_type: FieldType) -> Result<&mut Self> {
        let command = FieldExistsCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::FieldExists)?;
        Ok(self)
    }

    /// Queue a find entities command
    pub fn find_entities(&mut self, entity_type: EntityType, filter: Option<&str>) -> Result<&mut Self> {
        let command = FindEntitiesCommand {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::FindEntities)?;
        Ok(self)
    }

    /// Queue a get entity types command
    pub fn get_entity_types(&mut self) -> Result<&mut Self> {
        let command = GetEntityTypesCommand {
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetEntityTypes)?;
        Ok(self)
    }

    /// Helper to queue a command
    fn queue_command<C: RespCommand<'static>>(&mut self, command: C, response_type: ResponseType) -> Result<()> {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        self.commands.push(QueuedCommand {
            encoded_bytes,
            response_type,
        });
        Ok(())
    }

    /// Execute all queued commands and return results
    pub fn execute(self) -> Result<PipelineResults> {
        if self.commands.is_empty() {
            return Ok(PipelineResults {
                responses: Vec::new(),
            });
        }

        // Send all commands at once
        let mut all_bytes = Vec::new();
        for cmd in &self.commands {
            all_bytes.extend_from_slice(&cmd.encoded_bytes);
        }

        let mut conn = self.proxy.tcp_connection.borrow_mut();
        conn.send_bytes(&all_bytes)
            .map_err(|e| Error::StoreProxyError(format!("Failed to send pipeline commands: {}", e)))?;

        // Receive all responses, handling notifications
        let mut responses = Vec::new();
        let mut command_index = 0;
        loop {
            // Try to parse response
            let consumed_opt = {
                let conn_ref = &conn;
                match RespValue::from_bytes(&conn_ref.read_buffer) {
                    Ok((resp_value, remaining)) => {
                        let consumed = conn_ref.read_buffer.len() - remaining.len();
                        if command_index < self.commands.len() {
                            let cmd = &self.commands[command_index];
                            match self.decode_response(resp_value.clone(), &cmd.response_type) {
                                Ok(decoded) => {
                                    responses.push(decoded);
                                    command_index += 1;
                                    Some((consumed, true))
                                }
                                Err(_) => {
                                    // Try as notification
                                    if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                                        self.proxy.handle_notification(notification);
                                        Some((consumed, false))
                                    } else {
                                        return Err(Error::StoreProxyError(format!("Failed to decode response or notification")));
                                    }
                                }
                            }
                        } else {
                            // Extra response, try as notification
                            if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                                self.proxy.handle_notification(notification);
                                Some((consumed, false))
                            } else {
                                return Err(Error::StoreProxyError("Unexpected extra response".to_string()));
                            }
                        }
                    }
                    Err(_) => None
                }
            };

            if let Some((consumed, _is_response)) = consumed_opt {
                conn.read_buffer.drain(..consumed);
                if command_index >= self.commands.len() {
                    break;
                }
            } else {
                // Need more data
                let readable = conn.wait_for_readable(Some(Duration::from_millis(10)))
                    .map_err(|e| Error::StoreProxyError(format!("Poll error: {}", e)))?;
                if readable {
                    conn.read_bytes()
                        .map_err(|e| Error::StoreProxyError(format!("Failed to read bytes: {}", e)))?;
                }
            }
        }

        Ok(PipelineResults { responses })
    }

    /// Decode a response based on its type
    fn decode_response(&self, resp_value: RespValue, response_type: &ResponseType) -> Result<DecodedResponse> {
        match response_type {
            ResponseType::Read => {
                let response = crate::data::resp::ReadResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode Read response: {}", e)))?;
                Ok(DecodedResponse::Read((response.value, response.timestamp, response.writer_id)))
            }
            ResponseType::Write | ResponseType::DeleteEntity | ResponseType::UpdateSchema | ResponseType::SetFieldSchema => {
                match resp_value {
                    RespValue::SimpleString(s) if s == "OK" => {
                        match response_type {
                            ResponseType::Write => Ok(DecodedResponse::Write(())),
                            ResponseType::DeleteEntity => Ok(DecodedResponse::DeleteEntity(())),
                            ResponseType::UpdateSchema => Ok(DecodedResponse::UpdateSchema(())),
                            ResponseType::SetFieldSchema => Ok(DecodedResponse::SetFieldSchema(())),
                            _ => unreachable!(),
                        }
                    }
                    RespValue::Error(msg) => Err(Error::StoreProxyError(format!("Server error: {}", msg))),
                    _ => Err(Error::StoreProxyError("Expected OK response".to_string())),
                }
            }
            ResponseType::CreateEntity => {
                let response = crate::data::resp::CreateEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode CreateEntity response: {}", e)))?;
                Ok(DecodedResponse::CreateEntity(response.entity_id))
            }
            ResponseType::GetEntityType => {
                let response = crate::data::resp::IntegerResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityType response: {}", e)))?;
                Ok(DecodedResponse::GetEntityType(EntityType(response.value as u32)))
            }
            ResponseType::ResolveEntityType => {
                let response = crate::data::resp::StringResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveEntityType response: {}", e)))?;
                Ok(DecodedResponse::ResolveEntityType(response.value))
            }
            ResponseType::GetFieldType => {
                let response = crate::data::resp::IntegerResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetFieldType response: {}", e)))?;
                Ok(DecodedResponse::GetFieldType(FieldType(response.value as u64)))
            }
            ResponseType::ResolveFieldType => {
                let response = crate::data::resp::StringResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveFieldType response: {}", e)))?;
                Ok(DecodedResponse::ResolveFieldType(response.value))
            }
            ResponseType::GetEntitySchema => {
                let schema_resp = crate::data::entity_schema::EntitySchemaResp::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntitySchema response: {}", e)))?;
                let schema_string = schema_resp.to_entity_schema(self.proxy)?;
                let typed_schema = EntitySchema::from_string_schema(schema_string, self.proxy);
                Ok(DecodedResponse::GetEntitySchema(typed_schema))
            }
            ResponseType::GetFieldSchema => {
                let response = crate::data::resp::FieldSchemaResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetFieldSchema response: {}", e)))?;
                let field_schema_string = response.schema.to_field_schema();
                // Note: For now we return the string-based schema. Full typed conversion would require async context.
                Ok(DecodedResponse::GetFieldSchema(field_schema_string))
            }
            ResponseType::EntityExists => {
                let response = crate::data::resp::BooleanResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode EntityExists response: {}", e)))?;
                Ok(DecodedResponse::EntityExists(response.result))
            }
            ResponseType::FieldExists => {
                let response = crate::data::resp::BooleanResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FieldExists response: {}", e)))?;
                Ok(DecodedResponse::FieldExists(response.result))
            }
            ResponseType::ResolveIndirection => {
                let response = crate::data::resp::ResolveIndirectionResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveIndirection response: {}", e)))?;
                Ok(DecodedResponse::ResolveIndirection((response.entity_id, response.field_type)))
            }
            ResponseType::FindEntitiesPaginated => {
                let response = crate::data::resp::PaginatedEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntitiesPaginated response: {}", e)))?;
                Ok(DecodedResponse::FindEntitiesPaginated(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::FindEntitiesExact => {
                let response = crate::data::resp::PaginatedEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntitiesExact response: {}", e)))?;
                Ok(DecodedResponse::FindEntitiesExact(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::FindEntities => {
                let response = crate::data::resp::EntityListResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntities response: {}", e)))?;
                Ok(DecodedResponse::FindEntities(response.entities))
            }
            ResponseType::GetEntityTypes => {
                let response = crate::data::resp::EntityTypeListResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityTypes response: {}", e)))?;
                Ok(DecodedResponse::GetEntityTypes(response.entity_types))
            }
            ResponseType::GetEntityTypesPaginated => {
                let response = crate::data::resp::PaginatedEntityTypeResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityTypesPaginated response: {}", e)))?;
                Ok(DecodedResponse::GetEntityTypesPaginated(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::TakeSnapshot => {
                let response = crate::data::resp::SnapshotResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode TakeSnapshot response: {}", e)))?;
                Ok(DecodedResponse::TakeSnapshot(response.data))
            }
        }
    }

    /// Get the number of queued commands
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear all queued commands
    pub fn clear(&mut self) {
        self.commands.clear();
    }
}

/// Asynchronous pipeline for batching commands
pub struct AsyncPipeline<'a> {
    proxy: &'a crate::data::AsyncStoreProxy,
    commands: Vec<QueuedCommand>,
}

impl<'a> AsyncPipeline<'a> {
    pub(crate) fn new(proxy: &'a crate::data::AsyncStoreProxy) -> Self {
        Self {
            proxy,
            commands: Vec::new(),
        }
    }

    /// Queue a read command
    pub fn read(&mut self, entity_id: EntityId, field_path: &[FieldType]) -> Result<&mut Self> {
        let command = ReadCommand {
            entity_id,
            field_path: field_path.to_vec(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::Read)?;
        Ok(self)
    }

    /// Queue a write command
    pub fn write(
        &mut self,
        entity_id: EntityId,
        field_path: &[FieldType],
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<Timestamp>,
        push_condition: Option<PushCondition>,
        adjust_behavior: Option<AdjustBehavior>,
    ) -> Result<&mut Self> {
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
        self.queue_command(command, ResponseType::Write)?;
        Ok(self)
    }

    /// Queue a create entity command
    pub fn create_entity(
        &mut self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<&mut Self> {
        let command = CreateEntityCommand {
            entity_type,
            parent_id,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::CreateEntity)?;
        Ok(self)
    }

    /// Queue a delete entity command
    pub fn delete_entity(&mut self, entity_id: EntityId) -> Result<&mut Self> {
        let command = DeleteEntityCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::DeleteEntity)?;
        Ok(self)
    }

    /// Queue a get entity type command
    pub fn get_entity_type(&mut self, name: &str) -> Result<&mut Self> {
        let command = GetEntityTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetEntityType)?;
        Ok(self)
    }

    /// Queue a resolve entity type command
    pub fn resolve_entity_type(&mut self, entity_type: EntityType) -> Result<&mut Self> {
        let command = ResolveEntityTypeCommand {
            entity_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::ResolveEntityType)?;
        Ok(self)
    }

    /// Queue a get field type command
    pub fn get_field_type(&mut self, name: &str) -> Result<&mut Self> {
        let command = GetFieldTypeCommand {
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetFieldType)?;
        Ok(self)
    }

    /// Queue a resolve field type command
    pub fn resolve_field_type(&mut self, field_type: FieldType) -> Result<&mut Self> {
        let command = ResolveFieldTypeCommand {
            field_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::ResolveFieldType)?;
        Ok(self)
    }

    /// Queue an entity exists command
    pub fn entity_exists(&mut self, entity_id: EntityId) -> Result<&mut Self> {
        let command = EntityExistsCommand {
            entity_id,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::EntityExists)?;
        Ok(self)
    }

    /// Queue a field exists command
    pub fn field_exists(&mut self, entity_type: EntityType, field_type: FieldType) -> Result<&mut Self> {
        let command = FieldExistsCommand {
            entity_type,
            field_type,
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::FieldExists)?;
        Ok(self)
    }

    /// Queue a find entities command
    pub fn find_entities(&mut self, entity_type: EntityType, filter: Option<&str>) -> Result<&mut Self> {
        let command = FindEntitiesCommand {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::FindEntities)?;
        Ok(self)
    }

    /// Queue a get entity types command
    pub fn get_entity_types(&mut self) -> Result<&mut Self> {
        let command = GetEntityTypesCommand {
            _marker: std::marker::PhantomData,
        };
        self.queue_command(command, ResponseType::GetEntityTypes)?;
        Ok(self)
    }

    /// Helper to queue a command
    fn queue_command<C: RespCommand<'static>>(&mut self, command: C, response_type: ResponseType) -> Result<()> {
        let encoded = command.encode();
        let encoded_bytes = encoded.to_bytes();
        self.commands.push(QueuedCommand {
            encoded_bytes,
            response_type,
        });
        Ok(())
    }

    /// Execute all queued commands and return results
    pub async fn execute(self) -> Result<PipelineResults> {
        if self.commands.is_empty() {
            return Ok(PipelineResults {
                responses: Vec::new(),
            });
        }

        // Send all commands at once
        let mut all_bytes = Vec::new();
        for cmd in &self.commands {
            all_bytes.extend_from_slice(&cmd.encoded_bytes);
        }

        let mut conn = self.proxy.tcp_connection.lock().await;
        conn.send_bytes(&all_bytes)
            .await
            .map_err(|e| Error::StoreProxyError(format!("Failed to send pipeline commands: {}", e)))?;

        // Receive all responses, handling notifications
        let mut responses = Vec::new();
        let mut command_index = 0;
        loop {
            // Try to parse response
            let consumed_opt = match RespValue::from_bytes(&conn.read_buffer) {
                Ok((resp_value, remaining)) => {
                    let consumed = conn.read_buffer.len() - remaining.len();
                    if command_index < self.commands.len() {
                        let cmd = &self.commands[command_index];
                        match self.decode_response(resp_value.clone(), &cmd.response_type).await {
                            Ok(decoded) => {
                                responses.push(decoded);
                                command_index += 1;
                                Some((consumed, true))
                            }
                            Err(_) => {
                                // Try as notification
                                if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                                    self.proxy.handle_notification(notification);
                                    Some((consumed, false))
                                } else {
                                    return Err(Error::StoreProxyError(format!("Failed to decode response or notification")));
                                }
                            }
                        }
                    } else {
                        // Extra response, try as notification
                        if let Ok(notification) = NotificationCommand::decode(resp_value.clone()) {
                            self.proxy.handle_notification(notification);
                            Some((consumed, false))
                        } else {
                            return Err(Error::StoreProxyError("Unexpected extra response".to_string()));
                        }
                    }
                }
                Err(_) => None
            };

            if let Some((consumed, _is_response)) = consumed_opt {
                conn.read_buffer.drain(..consumed);
                if command_index >= self.commands.len() {
                    break;
                }
            } else {
                // Need more data
                conn.read_bytes()
                    .await
                    .map_err(|e| Error::StoreProxyError(format!("Failed to read pipeline response: {}", e)))?;
            }
        }

        Ok(PipelineResults { responses })
    }

    /// Decode a response based on its type
    async fn decode_response(&self, resp_value: RespValue<'_>, response_type: &ResponseType) -> Result<DecodedResponse> {
        match response_type {
            ResponseType::Read => {
                let response = crate::data::resp::ReadResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode Read response: {}", e)))?;
                Ok(DecodedResponse::Read((response.value, response.timestamp, response.writer_id)))
            }
            ResponseType::Write | ResponseType::DeleteEntity | ResponseType::UpdateSchema | ResponseType::SetFieldSchema => {
                match resp_value {
                    RespValue::SimpleString(s) if s == "OK" => {
                        match response_type {
                            ResponseType::Write => Ok(DecodedResponse::Write(())),
                            ResponseType::DeleteEntity => Ok(DecodedResponse::DeleteEntity(())),
                            ResponseType::UpdateSchema => Ok(DecodedResponse::UpdateSchema(())),
                            ResponseType::SetFieldSchema => Ok(DecodedResponse::SetFieldSchema(())),
                            _ => unreachable!(),
                        }
                    }
                    RespValue::Error(msg) => Err(Error::StoreProxyError(format!("Server error: {}", msg))),
                    _ => Err(Error::StoreProxyError("Expected OK response".to_string())),
                }
            }
            ResponseType::CreateEntity => {
                let response = crate::data::resp::CreateEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode CreateEntity response: {}", e)))?;
                Ok(DecodedResponse::CreateEntity(response.entity_id))
            }
            ResponseType::GetEntityType => {
                let response = crate::data::resp::IntegerResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityType response: {}", e)))?;
                Ok(DecodedResponse::GetEntityType(EntityType(response.value as u32)))
            }
            ResponseType::ResolveEntityType => {
                let response = crate::data::resp::StringResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveEntityType response: {}", e)))?;
                Ok(DecodedResponse::ResolveEntityType(response.value))
            }
            ResponseType::GetFieldType => {
                let response = crate::data::resp::IntegerResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetFieldType response: {}", e)))?;
                Ok(DecodedResponse::GetFieldType(FieldType(response.value as u64)))
            }
            ResponseType::ResolveFieldType => {
                let response = crate::data::resp::StringResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveFieldType response: {}", e)))?;
                Ok(DecodedResponse::ResolveFieldType(response.value))
            }
            ResponseType::GetEntitySchema => {
                let schema_resp = crate::data::entity_schema::EntitySchemaResp::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntitySchema response: {}", e)))?;
                
                // Convert to typed schema
                let mut fields = rustc_hash::FxHashMap::default();
                for field_resp in schema_resp.fields {
                    fields.insert(field_resp.field_type.clone(), field_resp.to_field_schema());
                }

                let mut schema_string = EntitySchema::<Single, String, String>::new(
                    schema_resp.entity_type.clone(),
                    schema_resp.inherit.clone(),
                );
                schema_string.fields = fields;
                
                let typed_entity_type = self.proxy.get_entity_type(&schema_string.entity_type).await?;
                let mut typed_inherit = Vec::new();
                for inherit_str in &schema_string.inherit {
                    typed_inherit.push(self.proxy.get_entity_type(inherit_str).await?);
                }
                
                let mut typed_fields = rustc_hash::FxHashMap::default();
                for (field_type_str, field_schema) in schema_string.fields {
                    let field_type = self.proxy.get_field_type(&field_type_str).await?;
                    let typed_field_schema = self.proxy.convert_field_schema_from_string(field_schema).await?;
                    typed_fields.insert(field_type, typed_field_schema);
                }
                
                let mut typed_schema = EntitySchema::<Single, EntityType, FieldType>::new(
                    typed_entity_type,
                    typed_inherit,
                );
                typed_schema.fields = typed_fields;
                Ok(DecodedResponse::GetEntitySchema(typed_schema))
            }
            ResponseType::GetFieldSchema => {
                let response = crate::data::resp::FieldSchemaResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetFieldSchema response: {}", e)))?;
                let field_schema_string = response.schema.to_field_schema();
                // For async pipeline, we also keep it as string to avoid complex type conversion
                Ok(DecodedResponse::GetFieldSchema(field_schema_string))
            }
            ResponseType::EntityExists => {
                let response = crate::data::resp::BooleanResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode EntityExists response: {}", e)))?;
                Ok(DecodedResponse::EntityExists(response.result))
            }
            ResponseType::FieldExists => {
                let response = crate::data::resp::BooleanResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FieldExists response: {}", e)))?;
                Ok(DecodedResponse::FieldExists(response.result))
            }
            ResponseType::ResolveIndirection => {
                let response = crate::data::resp::ResolveIndirectionResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode ResolveIndirection response: {}", e)))?;
                Ok(DecodedResponse::ResolveIndirection((response.entity_id, response.field_type)))
            }
            ResponseType::FindEntitiesPaginated => {
                let response = crate::data::resp::PaginatedEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntitiesPaginated response: {}", e)))?;
                Ok(DecodedResponse::FindEntitiesPaginated(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::FindEntitiesExact => {
                let response = crate::data::resp::PaginatedEntityResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntitiesExact response: {}", e)))?;
                Ok(DecodedResponse::FindEntitiesExact(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::FindEntities => {
                let response = crate::data::resp::EntityListResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode FindEntities response: {}", e)))?;
                Ok(DecodedResponse::FindEntities(response.entities))
            }
            ResponseType::GetEntityTypes => {
                let response = crate::data::resp::EntityTypeListResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityTypes response: {}", e)))?;
                Ok(DecodedResponse::GetEntityTypes(response.entity_types))
            }
            ResponseType::GetEntityTypesPaginated => {
                let response = crate::data::resp::PaginatedEntityTypeResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode GetEntityTypesPaginated response: {}", e)))?;
                Ok(DecodedResponse::GetEntityTypesPaginated(PageResult {
                    items: response.items,
                    total: response.total,
                    next_cursor: response.next_cursor,
                }))
            }
            ResponseType::TakeSnapshot => {
                let response = crate::data::resp::SnapshotResponse::decode(resp_value)
                    .map_err(|e| Error::StoreProxyError(format!("Failed to decode TakeSnapshot response: {}", e)))?;
                Ok(DecodedResponse::TakeSnapshot(response.data))
            }
        }
    }

    /// Get the number of queued commands
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear all queued commands
    pub fn clear(&mut self) {
        self.commands.clear();
    }
}
