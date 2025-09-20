use anyhow::Result;
#[allow(unused_imports)] // Used by bincode
use serde::{Serialize, Deserialize};
use rkyv::Deserialize as RkyvDeserialize;
use crate::{EntityId, EntityType, FieldType};

/// Magic bytes to identify protocol messages (4 bytes)
const PROTOCOL_MAGIC: [u8; 4] = [0x51, 0x43, 0x4F, 0x52]; // "QCOR" in ASCII

/// Maximum message size to prevent memory exhaustion (16MB)
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Binary protocol header (12 bytes total)
#[derive(Debug, Clone)]
pub struct MessageHeader {
    /// Magic bytes for protocol identification
    pub magic: [u8; 4],
    /// Message length (excluding header)
    pub length: u32,
    /// Message type identifier
    pub message_type: u32,
}

impl MessageHeader {
    pub const SIZE: usize = 12;
    
    pub fn new(message_type: u32, length: u32) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            length,
            message_type,
        }
    }
    
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.length.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.message_type.to_le_bytes());
        bytes
    }
    
    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Result<Self> {
        let magic = [bytes[0], bytes[1], bytes[2], bytes[3]];
        if magic != PROTOCOL_MAGIC {
            return Err(anyhow::anyhow!("Invalid protocol magic"));
        }
        
        let length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if length > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Message too large: {} bytes", length));
        }
        
        let message_type = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        
        Ok(Self {
            magic,
            length,
            message_type,
        })
    }
}

/// Message types for the binary protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // Core store messages (1000-1999)
    StoreMessage = 1000,        // Uses bincode for compatibility (legacy)
    FastStoreMessage = 1001,    // Uses rkyv for high-performance operations (new)
}

impl MessageType {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1000 => Some(Self::StoreMessage),
            1001 => Some(Self::FastStoreMessage),
            _ => None,
        }
    }
    
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Fast Store message that uses rkyv for direct processing without bincode deserialization
/// This provides true zero-copy performance by containing actual message data in rkyv format
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct FastStoreMessage {
    /// Message ID for correlation
    pub id: String,
    /// The actual message data in rkyv-compatible format
    pub message: FastStoreMessageType,
}

/// rkyv-compatible message types for fast processing
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum FastStoreMessageType {
    // Authentication messages
    Authenticate {
        subject_name: String,
        credential: String,
    },
    AuthenticateResponse {
        response: Result<FastAuthenticationResult, String>,
    },

    // Simple existence checks (most common operations)
    EntityExists {
        entity_id: EntityId,
    },
    EntityExistsResponse {
        response: bool,
    },

    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
    },
    FieldExistsResponse {
        response: bool,
    },

    // Type resolution (simple operations)
    GetEntityType {
        name: String,
    },
    GetEntityTypeResponse {
        response: Result<EntityType, String>,
    },

    ResolveEntityType {
        entity_type: EntityType,
    },
    ResolveEntityTypeResponse {
        response: Result<String, String>,
    },

    GetFieldType {
        name: String,
    },
    GetFieldTypeResponse {
        response: Result<FieldType, String>,
    },

    ResolveFieldType {
        field_type: FieldType,
    },
    ResolveFieldTypeResponse {
        response: Result<String, String>,
    },

    // For complex operations, we can still fall back to bincode if needed
    ComplexOperation {
        /// Bincode-serialized StoreMessage for operations that don't have fast variants yet
        payload: Vec<u8>,
        operation_type: u32,
    },
}

/// rkyv-compatible version of AuthenticationResult
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct FastAuthenticationResult {
    pub subject_id: EntityId,
    pub subject_type: String,
}

impl FastStoreMessage {
    /// Create a new FastStoreMessage from a StoreMessage
    pub fn from_store_message(store_message: &crate::data::StoreMessage) -> anyhow::Result<Self> {
        // Extract message ID for optimization
        let id = crate::data::extract_message_id(store_message)
            .unwrap_or_else(|| "unknown".to_string());
        
        // Convert to fast message type
        let message = match store_message {
            crate::data::StoreMessage::Authenticate { subject_name, credential, .. } => {
                FastStoreMessageType::Authenticate {
                    subject_name: subject_name.clone(),
                    credential: credential.clone(),
                }
            },
            crate::data::StoreMessage::AuthenticateResponse { response, .. } => {
                let fast_response = match response {
                    Ok(auth_result) => Ok(FastAuthenticationResult {
                        subject_id: auth_result.subject_id,
                        subject_type: auth_result.subject_type.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                };
                FastStoreMessageType::AuthenticateResponse { response: fast_response }
            },
            crate::data::StoreMessage::EntityExists { entity_id, .. } => {
                FastStoreMessageType::EntityExists { entity_id: *entity_id }
            },
            crate::data::StoreMessage::EntityExistsResponse { response, .. } => {
                FastStoreMessageType::EntityExistsResponse { response: *response }
            },
            crate::data::StoreMessage::FieldExists { entity_type, field_type, .. } => {
                FastStoreMessageType::FieldExists { 
                    entity_type: *entity_type, 
                    field_type: *field_type 
                }
            },
            crate::data::StoreMessage::FieldExistsResponse { response, .. } => {
                FastStoreMessageType::FieldExistsResponse { response: *response }
            },
            crate::data::StoreMessage::GetEntityType { name, .. } => {
                FastStoreMessageType::GetEntityType { name: name.clone() }
            },
            crate::data::StoreMessage::GetEntityTypeResponse { response, .. } => {
                FastStoreMessageType::GetEntityTypeResponse { response: response.clone() }
            },
            crate::data::StoreMessage::ResolveEntityType { entity_type, .. } => {
                FastStoreMessageType::ResolveEntityType { entity_type: *entity_type }
            },
            crate::data::StoreMessage::ResolveEntityTypeResponse { response, .. } => {
                FastStoreMessageType::ResolveEntityTypeResponse { response: response.clone() }
            },
            crate::data::StoreMessage::GetFieldType { name, .. } => {
                FastStoreMessageType::GetFieldType { name: name.clone() }
            },
            crate::data::StoreMessage::GetFieldTypeResponse { response, .. } => {
                FastStoreMessageType::GetFieldTypeResponse { response: response.clone() }
            },
            crate::data::StoreMessage::ResolveFieldType { field_type, .. } => {
                FastStoreMessageType::ResolveFieldType { field_type: *field_type }
            },
            crate::data::StoreMessage::ResolveFieldTypeResponse { response, .. } => {
                FastStoreMessageType::ResolveFieldTypeResponse { response: response.clone() }
            },
            // For complex operations that don't have fast variants yet, fall back to bincode
            _ => {
                let payload = bincode::serialize(store_message)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize complex store message: {}", e))?;
                let operation_type = Self::get_operation_type_hint(store_message);
                FastStoreMessageType::ComplexOperation { payload, operation_type }
            }
        };
        
        Ok(FastStoreMessage { id, message })
    }
    
    /// Convert FastStoreMessage back to StoreMessage for compatibility
    pub fn to_store_message(&self) -> anyhow::Result<crate::data::StoreMessage> {
        use crate::data::StoreMessage;
        
        let store_message = match &self.message {
            FastStoreMessageType::Authenticate { subject_name, credential } => {
                StoreMessage::Authenticate {
                    id: self.id.clone(),
                    subject_name: subject_name.clone(),
                    credential: credential.clone(),
                }
            },
            FastStoreMessageType::AuthenticateResponse { response } => {
                let store_response = match response {
                    Ok(fast_result) => Ok(crate::data::AuthenticationResult {
                        subject_id: fast_result.subject_id,
                        subject_type: fast_result.subject_type.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                };
                StoreMessage::AuthenticateResponse {
                    id: self.id.clone(),
                    response: store_response,
                }
            },
            FastStoreMessageType::EntityExists { entity_id } => {
                StoreMessage::EntityExists {
                    id: self.id.clone(),
                    entity_id: *entity_id,
                }
            },
            FastStoreMessageType::EntityExistsResponse { response } => {
                StoreMessage::EntityExistsResponse {
                    id: self.id.clone(),
                    response: *response,
                }
            },
            FastStoreMessageType::FieldExists { entity_type, field_type } => {
                StoreMessage::FieldExists {
                    id: self.id.clone(),
                    entity_type: *entity_type,
                    field_type: *field_type,
                }
            },
            FastStoreMessageType::FieldExistsResponse { response } => {
                StoreMessage::FieldExistsResponse {
                    id: self.id.clone(),
                    response: *response,
                }
            },
            FastStoreMessageType::GetEntityType { name } => {
                StoreMessage::GetEntityType {
                    id: self.id.clone(),
                    name: name.clone(),
                }
            },
            FastStoreMessageType::GetEntityTypeResponse { response } => {
                StoreMessage::GetEntityTypeResponse {
                    id: self.id.clone(),
                    response: response.clone(),
                }
            },
            FastStoreMessageType::ResolveEntityType { entity_type } => {
                StoreMessage::ResolveEntityType {
                    id: self.id.clone(),
                    entity_type: *entity_type,
                }
            },
            FastStoreMessageType::ResolveEntityTypeResponse { response } => {
                StoreMessage::ResolveEntityTypeResponse {
                    id: self.id.clone(),
                    response: response.clone(),
                }
            },
            FastStoreMessageType::GetFieldType { name } => {
                StoreMessage::GetFieldType {
                    id: self.id.clone(),
                    name: name.clone(),
                }
            },
            FastStoreMessageType::GetFieldTypeResponse { response } => {
                StoreMessage::GetFieldTypeResponse {
                    id: self.id.clone(),
                    response: response.clone(),
                }
            },
            FastStoreMessageType::ResolveFieldType { field_type } => {
                StoreMessage::ResolveFieldType {
                    id: self.id.clone(),
                    field_type: *field_type,
                }
            },
            FastStoreMessageType::ResolveFieldTypeResponse { response } => {
                StoreMessage::ResolveFieldTypeResponse {
                    id: self.id.clone(),
                    response: response.clone(),
                }
            },
            FastStoreMessageType::ComplexOperation { payload, .. } => {
                // Fall back to bincode deserialization for complex operations
                bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize complex operation: {}", e))?
            },
        };
        
        Ok(store_message)
    }
    
    /// Get operation type hint for complex operations
    fn get_operation_type_hint(msg: &crate::data::StoreMessage) -> u32 {
        use crate::data::StoreMessage;
        match msg {
            StoreMessage::GetEntitySchema { .. } => 100,
            StoreMessage::GetEntitySchemaResponse { .. } => 101,
            StoreMessage::GetCompleteEntitySchema { .. } => 102,
            StoreMessage::GetCompleteEntitySchemaResponse { .. } => 103,
            StoreMessage::GetFieldSchema { .. } => 104,
            StoreMessage::GetFieldSchemaResponse { .. } => 105,
            StoreMessage::Perform { .. } => 106,
            StoreMessage::PerformResponse { .. } => 107,
            StoreMessage::FindEntities { .. } => 108,
            StoreMessage::FindEntitiesResponse { .. } => 109,
            StoreMessage::FindEntitiesExact { .. } => 110,
            StoreMessage::FindEntitiesExactResponse { .. } => 111,
            StoreMessage::GetEntityTypes { .. } => 112,
            StoreMessage::GetEntityTypesResponse { .. } => 113,
            StoreMessage::RegisterNotification { .. } => 114,
            StoreMessage::RegisterNotificationResponse { .. } => 115,
            StoreMessage::UnregisterNotification { .. } => 116,
            StoreMessage::UnregisterNotificationResponse { .. } => 117,
            StoreMessage::Notification { .. } => 118,
            StoreMessage::Error { .. } => 119,
            _ => 0,
        }
    }

    /// Check if this message can be processed without bincode deserialization
    pub fn is_fast_processable(&self) -> bool {
        !matches!(self.message, FastStoreMessageType::ComplexOperation { .. })
    }

    /// Get the message ID without any deserialization
    pub fn message_id(&self) -> &str {
        &self.id
    }
}

/// Protocol message wrapper
#[derive(Debug)]
pub enum ProtocolMessage {
    Store(crate::data::StoreMessage),           // Uses bincode (legacy compatibility)
    FastStore(FastStoreMessage),                // Uses rkyv envelope with bincode payload
}

impl ProtocolMessage {
    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Store(_) => MessageType::StoreMessage,
            Self::FastStore(_) => MessageType::FastStoreMessage,
        }
    }
    
    /// Serialize the message payload to bytes using the most appropriate method
    pub fn serialize_payload(&self) -> Result<Vec<u8>> {
        match self {
            // Use bincode for legacy StoreMessage (compatibility)
            Self::Store(msg) => bincode::serialize(msg)
                .map_err(|e| anyhow::anyhow!("Failed to serialize store message: {}", e)),
            
            // Use rkyv for FastStoreMessage (performance)
            Self::FastStore(fast_msg) => {
                let bytes = rkyv::to_bytes::<_, 256>(fast_msg)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize fast store message: {}", e))?;
                Ok(bytes.into_vec())
            },
        }
    }
    
    /// Deserialize the message payload from bytes using the appropriate method
    pub fn deserialize_payload(message_type: MessageType, payload: &[u8]) -> Result<Self> {
        match message_type {
            MessageType::StoreMessage => {
                let msg: crate::data::StoreMessage = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize store message: {}", e))?;
                Ok(Self::Store(msg))
            },
            MessageType::FastStoreMessage => {
                // Copy to aligned buffer for rkyv
                let mut aligned_data = rkyv::AlignedVec::new();
                aligned_data.extend_from_slice(payload);
                let archived = unsafe { rkyv::archived_root::<FastStoreMessage>(&aligned_data) };
                let fast_msg: FastStoreMessage = RkyvDeserialize::deserialize(archived, &mut rkyv::Infallible)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize fast store message: {:?}", e))?;
                Ok(Self::FastStore(fast_msg))
            },
        }
    }
}

/// Protocol encoder/decoder for handling message framing over TCP
pub struct ProtocolCodec;

impl ProtocolCodec {
    /// Encode a message into bytes ready for TCP transmission
    pub fn encode(message: &ProtocolMessage) -> Result<Vec<u8>> {
        let payload = message.serialize_payload()?;
        let header = MessageHeader::new(message.message_type().as_u32(), payload.len() as u32);
        
        let mut encoded = Vec::with_capacity(MessageHeader::SIZE + payload.len());
        encoded.extend_from_slice(&header.to_bytes());
        encoded.extend_from_slice(&payload);
        
        Ok(encoded)
    }
    
    /// Decode a message from a TCP stream
    /// Returns (message, bytes_consumed) or None if not enough data
    pub fn decode(buffer: &[u8]) -> Result<Option<(ProtocolMessage, usize)>> {
        // Need at least header size
        if buffer.len() < MessageHeader::SIZE {
            return Ok(None);
        }
        
        // Parse header
        let header_bytes: [u8; MessageHeader::SIZE] = buffer[0..MessageHeader::SIZE].try_into()
            .map_err(|_| anyhow::anyhow!("Failed to read header bytes"))?;
        let header = MessageHeader::from_bytes(&header_bytes)?;
        
        // Check if we have the full message
        let total_size = MessageHeader::SIZE + header.length as usize;
        if buffer.len() < total_size {
            return Ok(None);
        }
        
        // Parse message type
        let message_type = MessageType::from_u32(header.message_type)
            .ok_or_else(|| anyhow::anyhow!("Unknown message type: {}", header.message_type))?;
        
        // Deserialize payload
        let payload = &buffer[MessageHeader::SIZE..total_size];
        let message = ProtocolMessage::deserialize_payload(message_type, payload)?;
        
        Ok(Some((message, total_size)))
    }
}

/// TCP message buffer for handling partial reads
#[derive(Debug)]
pub struct MessageBuffer {
    buffer: Vec<u8>,
    capacity: usize,
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self::with_capacity(64 * 1024) // 64KB initial capacity
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }
    
    /// Add data to the buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        
        // Prevent buffer from growing indefinitely
        if self.buffer.capacity() > self.capacity * 4 {
            self.buffer.shrink_to(self.capacity);
        }
    }
    
    /// Try to decode a message from the buffer
    /// Returns (message, remaining_buffer) if successful
    pub fn try_decode(&mut self) -> Result<Option<ProtocolMessage>> {
        match ProtocolCodec::decode(&self.buffer)? {
            Some((message, bytes_consumed)) => {
                // Remove consumed bytes from buffer
                self.buffer.drain(0..bytes_consumed);
                Ok(Some(message))
            },
            None => Ok(None),
        }
    }
    
    /// Try to decode a store message specifically
    pub fn try_decode_store_message(&mut self) -> Result<Option<crate::data::StoreMessage>> {
        match self.try_decode()? {
            Some(ProtocolMessage::Store(store_msg)) => Ok(Some(store_msg)),
            Some(ProtocolMessage::FastStore(fast_msg)) => {
                // Convert FastStoreMessage back to StoreMessage
                let store_msg = fast_msg.to_store_message()?;
                Ok(Some(store_msg))
            },
            None => Ok(None),
        }
    }
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a StoreMessage for TCP transmission using the shared protocol
pub fn encode_store_message(message: &crate::data::StoreMessage) -> Result<Vec<u8>> {
    let protocol_message = ProtocolMessage::Store(message.clone());
    ProtocolCodec::encode(&protocol_message)
}

/// Encode a StoreMessage for TCP transmission using the fast rkyv protocol
pub fn encode_fast_store_message(message: &crate::data::StoreMessage) -> Result<Vec<u8>> {
    let fast_message = FastStoreMessage::from_store_message(message)?;
    let protocol_message = ProtocolMessage::FastStore(fast_message);
    ProtocolCodec::encode(&protocol_message)
}