use anyhow::Result;
#[allow(unused_imports)] // Used by bincode
use serde::{Serialize, Deserialize};

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
    StoreMessage = 1000,        // Uses rkyv for high-performance operations
}

impl MessageType {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1000 => Some(Self::StoreMessage),
            _ => None,
        }
    }
    
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Protocol message wrapper
#[derive(Debug)]
pub enum ProtocolMessage {
    Store(crate::data::StoreMessage),           // Uses rkyv (performance)
}

impl ProtocolMessage {
    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Store(_) => MessageType::StoreMessage,
        }
    }
    
    /// Serialize the message payload to bytes using the most appropriate method
    pub fn serialize_payload(&self) -> Result<Vec<u8>> {
        match self {
            // Use bincode for StoreMessage (for now)
            Self::Store(msg) => bincode::serialize(msg)
                .map_err(|e| anyhow::anyhow!("Failed to serialize store message: {}", e)),
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