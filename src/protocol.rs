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

/// Elegant FastStoreMessage with lazy deserialization and intelligent zero-copy optimization
/// Supports ALL store operations including read/write with smart performance routing
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct FastStoreMessage {
    /// Message ID for instant correlation (zero-copy access)
    pub id: String,
    
    /// Message type for fast routing (zero-copy access)
    pub message_type: FastMessageType,
    
    /// Primary entity ID when applicable (zero-copy access for routing/caching)
    pub primary_entity_id: Option<EntityId>,
    
    /// Operation complexity hint for intelligent processing (zero-copy access)
    pub operation_hint: OperationHint,
    
    /// Complete message payload (lazy deserialization only when needed)
    pub payload: Vec<u8>,
}

/// Comprehensive message type enumeration supporting all store operations elegantly
#[derive(Debug, Clone, PartialEq)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum FastMessageType {
    // Authentication
    Authenticate,
    AuthenticateResponse,
    
    // Entity operations  
    EntityExists,
    EntityExistsResponse,
    FieldExists,
    FieldExistsResponse,
    
    // Core read/write operations - THIS is what handles read/write elegantly!
    Perform,         // ALL read/write operations go through this!
    PerformResponse,
    
    // Schema operations
    GetEntitySchema,
    GetEntitySchemaResponse,
    GetCompleteEntitySchema,
    GetCompleteEntitySchemaResponse,
    GetFieldSchema,
    GetFieldSchemaResponse,
    
    // Find operations
    FindEntities,
    FindEntitiesResponse,
    FindEntitiesExact,
    FindEntitiesExactResponse,
    
    // Type resolution
    GetEntityTypes,
    GetEntityTypesResponse,
    GetEntityType,
    GetEntityTypeResponse,
    ResolveEntityType,
    ResolveEntityTypeResponse,
    GetFieldType,
    GetFieldTypeResponse,
    ResolveFieldType,
    ResolveFieldTypeResponse,
    
    // Notifications
    RegisterNotification,
    RegisterNotificationResponse,
    UnregisterNotification,
    UnregisterNotificationResponse,
    Notification,
    
    // Error handling
    Error,
}

/// Operation complexity hint for intelligent processing decisions
#[derive(Debug, Clone, PartialEq)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum OperationHint {
    /// Simple existence check - can often be handled without full deserialization
    SimpleRead,
    
    /// Single entity read/write - moderate complexity  
    SingleEntity,
    
    /// Multiple entities or complex queries
    BatchOperation,
    
    /// Administrative operations (auth, schema)
    Administrative,
}



impl FastStoreMessage {
    /// Create a FastStoreMessage with intelligent metadata extraction for optimal performance
    pub fn from_store_message(store_message: &crate::data::StoreMessage) -> anyhow::Result<Self> {
        use crate::data::StoreMessage;
        
        // Extract message ID
        let id = crate::data::extract_message_id(store_message)
            .unwrap_or_else(|| "unknown".to_string());
        
        // Intelligently analyze the message to extract zero-copy metadata
        let (message_type, primary_entity_id, operation_hint) = Self::analyze_message(store_message);
        
        // Serialize complete message for when full access is needed
        let payload = bincode::serialize(store_message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize store message: {}", e))?;
        
        Ok(FastStoreMessage {
            id,
            message_type,
            primary_entity_id,
            operation_hint,
            payload,
        })
    }
    
    /// Intelligently analyze a StoreMessage to extract metadata for zero-copy operations
    fn analyze_message(msg: &crate::data::StoreMessage) -> (FastMessageType, Option<EntityId>, OperationHint) {
        use crate::data::StoreMessage;
        
        match msg {
            StoreMessage::Authenticate { .. } => 
                (FastMessageType::Authenticate, None, OperationHint::Administrative),
            
            StoreMessage::AuthenticateResponse { .. } => 
                (FastMessageType::AuthenticateResponse, None, OperationHint::Administrative),
            
            StoreMessage::EntityExists { entity_id, .. } => 
                (FastMessageType::EntityExists, Some(*entity_id), OperationHint::SimpleRead),
            
            StoreMessage::EntityExistsResponse { .. } => 
                (FastMessageType::EntityExistsResponse, None, OperationHint::SimpleRead),
            
            StoreMessage::FieldExists { .. } => 
                (FastMessageType::FieldExists, None, OperationHint::SimpleRead),
            
            StoreMessage::FieldExistsResponse { .. } => 
                (FastMessageType::FieldExistsResponse, None, OperationHint::SimpleRead),
            
            // THE KEY: Perform operations handle ALL read/write operations elegantly!
            StoreMessage::Perform { requests, .. } => {
                let (primary_entity_id, hint) = Self::analyze_requests(requests);
                (FastMessageType::Perform, primary_entity_id, hint)
            },
            
            StoreMessage::PerformResponse { .. } => 
                (FastMessageType::PerformResponse, None, OperationHint::BatchOperation),
            
            StoreMessage::GetEntitySchema { .. } => 
                (FastMessageType::GetEntitySchema, None, OperationHint::Administrative),
            
            StoreMessage::GetEntitySchemaResponse { .. } => 
                (FastMessageType::GetEntitySchemaResponse, None, OperationHint::Administrative),
            
            StoreMessage::GetCompleteEntitySchema { .. } => 
                (FastMessageType::GetCompleteEntitySchema, None, OperationHint::Administrative),
            
            StoreMessage::GetCompleteEntitySchemaResponse { .. } => 
                (FastMessageType::GetCompleteEntitySchemaResponse, None, OperationHint::Administrative),
            
            StoreMessage::GetFieldSchema { .. } => 
                (FastMessageType::GetFieldSchema, None, OperationHint::Administrative),
            
            StoreMessage::GetFieldSchemaResponse { .. } => 
                (FastMessageType::GetFieldSchemaResponse, None, OperationHint::Administrative),
            
            StoreMessage::FindEntities { .. } => 
                (FastMessageType::FindEntities, None, OperationHint::BatchOperation),
            
            StoreMessage::FindEntitiesResponse { .. } => 
                (FastMessageType::FindEntitiesResponse, None, OperationHint::BatchOperation),
            
            StoreMessage::FindEntitiesExact { .. } => 
                (FastMessageType::FindEntitiesExact, None, OperationHint::BatchOperation),
            
            StoreMessage::FindEntitiesExactResponse { .. } => 
                (FastMessageType::FindEntitiesExactResponse, None, OperationHint::BatchOperation),
            
            StoreMessage::GetEntityTypes { .. } => 
                (FastMessageType::GetEntityTypes, None, OperationHint::Administrative),
            
            StoreMessage::GetEntityTypesResponse { .. } => 
                (FastMessageType::GetEntityTypesResponse, None, OperationHint::Administrative),
            
            StoreMessage::GetEntityType { .. } => 
                (FastMessageType::GetEntityType, None, OperationHint::Administrative),
            
            StoreMessage::GetEntityTypeResponse { .. } => 
                (FastMessageType::GetEntityTypeResponse, None, OperationHint::Administrative),
            
            StoreMessage::ResolveEntityType { .. } => 
                (FastMessageType::ResolveEntityType, None, OperationHint::Administrative),
            
            StoreMessage::ResolveEntityTypeResponse { .. } => 
                (FastMessageType::ResolveEntityTypeResponse, None, OperationHint::Administrative),
            
            StoreMessage::GetFieldType { .. } => 
                (FastMessageType::GetFieldType, None, OperationHint::Administrative),
            
            StoreMessage::GetFieldTypeResponse { .. } => 
                (FastMessageType::GetFieldTypeResponse, None, OperationHint::Administrative),
            
            StoreMessage::ResolveFieldType { .. } => 
                (FastMessageType::ResolveFieldType, None, OperationHint::Administrative),
            
            StoreMessage::ResolveFieldTypeResponse { .. } => 
                (FastMessageType::ResolveFieldTypeResponse, None, OperationHint::Administrative),
            
            StoreMessage::RegisterNotification { .. } => 
                (FastMessageType::RegisterNotification, None, OperationHint::Administrative),
            
            StoreMessage::RegisterNotificationResponse { .. } => 
                (FastMessageType::RegisterNotificationResponse, None, OperationHint::Administrative),
            
            StoreMessage::UnregisterNotification { .. } => 
                (FastMessageType::UnregisterNotification, None, OperationHint::Administrative),
            
            StoreMessage::UnregisterNotificationResponse { .. } => 
                (FastMessageType::UnregisterNotificationResponse, None, OperationHint::Administrative),
            
            StoreMessage::Notification { .. } => 
                (FastMessageType::Notification, None, OperationHint::Administrative),
            
            StoreMessage::Error { .. } => 
                (FastMessageType::Error, None, OperationHint::Administrative),
        }
    }
    
    /// Analyze read/write requests to extract metadata for intelligent routing
    fn analyze_requests(requests: &[crate::Request]) -> (Option<EntityId>, OperationHint) {
        if requests.is_empty() {
            return (None, OperationHint::BatchOperation);
        }
        
        // Get primary entity from first request for routing
        let primary_entity_id = requests.first().and_then(|req| req.entity_id());
        
        // Determine operation complexity
        let hint = if requests.len() == 1 {
            OperationHint::SingleEntity
        } else {
            OperationHint::BatchOperation
        };
        
        (primary_entity_id, hint)
    }
    
    /// Lazy deserialization - only deserialize when the full message is actually needed
    pub fn to_store_message(&self) -> anyhow::Result<crate::data::StoreMessage> {
        bincode::deserialize(&self.payload)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize store message: {}", e))
    }
    
    // Zero-copy accessors - these provide immediate value without any deserialization!
    
    /// Get message ID instantly (zero-copy)
    pub fn message_id(&self) -> &str {
        &self.id
    }
    
    /// Get message type for routing (zero-copy)
    pub fn message_type(&self) -> &FastMessageType {
        &self.message_type
    }
    
    /// Get operation complexity hint for intelligent processing (zero-copy)
    pub fn operation_hint(&self) -> &OperationHint {
        &self.operation_hint
    }
    
    /// Get primary entity ID for routing/caching decisions (zero-copy)
    pub fn primary_entity_id(&self) -> Option<EntityId> {
        self.primary_entity_id
    }
    
    // Intelligent query methods - these enable smart processing decisions
    
    /// Check if this is a read/write operation (the core operations!)
    pub fn is_read_write_operation(&self) -> bool {
        matches!(self.message_type, FastMessageType::Perform)
    }
    
    /// Check if this is a simple operation that might not need full deserialization
    pub fn is_simple_operation(&self) -> bool {
        matches!(self.operation_hint, OperationHint::SimpleRead)
    }
    
    /// Check if this operation involves a specific entity (useful for routing/caching)
    pub fn involves_entity(&self, entity_id: EntityId) -> bool {
        self.primary_entity_id == Some(entity_id)
    }
    
    /// Check if this is a batch operation
    pub fn is_batch_operation(&self) -> bool {
        matches!(self.operation_hint, OperationHint::BatchOperation)
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