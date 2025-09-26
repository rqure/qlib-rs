use anyhow::Result;
use bincode::Options;
use serde::{Serialize, Deserialize};
use crate::{Snapshot};
use bytes::BytesMut;

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
    StoreMessage = 1000,        // Uses bincode for existing StoreMessage compatibility
    
    // Peer messages (2000-2999)
    PeerMessage = 2000,
    
    // Response messages (9000-9999)
    Response = 9000,
    Error = 9999,
}

impl MessageType {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1000 => Some(Self::StoreMessage),
            2000 => Some(Self::PeerMessage),
            9000 => Some(Self::Response),
            9999 => Some(Self::Error),
            _ => None,
        }
    }
    
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Peer message types for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMessage {
    Handshake { start_time: u64, is_response: bool, machine_id: String },
    FullSyncRequest,
    FullSyncResponse { snapshot: Snapshot },
    SyncWrite { requests: crate::data::Requests },
}

/// Protocol message wrapper
#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    Store(crate::data::StoreMessage),           // Uses bincode (compatibility)
    Peer(PeerMessage),                          // All peer-related messages
    Response { id: String, data: Vec<u8> },     // Raw response data
    Error { id: Option<String>, message: String },
}

impl ProtocolMessage {
    fn bincode_options() -> impl Options {
        bincode::DefaultOptions::new()
            .with_little_endian()
    }

    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Store(_) => MessageType::StoreMessage,
            Self::Peer(_) => MessageType::PeerMessage,
            Self::Response { .. } => MessageType::Response,
            Self::Error { .. } => MessageType::Error,
        }
    }

    /// Estimate payload size for preallocation to reduce reallocations during serialization
    fn estimated_payload_size(&self) -> usize {
        match self {
            Self::Store(store_msg) => match store_msg {
                crate::data::StoreMessage::Perform { requests, .. } => 512 + requests.len() * 96,
                crate::data::StoreMessage::PerformResponse { response, .. } => {
                    match response {
                        Ok(requests) => 512 + requests.len() * 96,
                        Err(msg) => 256 + msg.len(),
                    }
                }
                crate::data::StoreMessage::RegisterNotification { .. }
                | crate::data::StoreMessage::RegisterNotificationResponse { .. }
                | crate::data::StoreMessage::UnregisterNotification { .. }
                | crate::data::StoreMessage::UnregisterNotificationResponse { .. }
                | crate::data::StoreMessage::Authenticate { .. }
                | crate::data::StoreMessage::AuthenticateResponse { .. }
                | crate::data::StoreMessage::Error { .. }
                | crate::data::StoreMessage::Notification { .. } => 256,
            },
            Self::Peer(peer_msg) => match peer_msg {
                PeerMessage::Handshake { machine_id, .. } => 256 + machine_id.len(),
                PeerMessage::FullSyncRequest => 192,
                PeerMessage::FullSyncResponse { .. } => 8192,
                PeerMessage::SyncWrite { requests } => 512 + requests.len() * 96,
            },
            Self::Response { data, .. } => 256 + data.len(),
            Self::Error { message, .. } => 256 + message.len(),
        }
    }

    /// Serialize the message payload directly into the provided buffer (appends to buffer)
    fn serialize_payload_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Store(msg) => {
                Self::bincode_options()
                    .serialize_into(buffer, msg)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize store message: {}", e))
            }
            Self::Peer(peer_msg) => {
                Self::bincode_options()
                    .serialize_into(buffer, peer_msg)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize peer message: {}", e))
            }
            Self::Response { id, data } => {
                #[derive(Serialize)]
                struct ResponsePayload<'a> {
                    id: &'a str,
                    data: &'a [u8],
                }
                let payload = ResponsePayload {
                    id: id.as_str(),
                    data,
                };
                Self::bincode_options()
                    .serialize_into(buffer, &payload)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize response: {}", e))
            }
            Self::Error { id, message } => {
                #[derive(Serialize)]
                struct ErrorPayload<'a> {
                    id: Option<&'a str>,
                    message: &'a str,
                }
                let payload = ErrorPayload {
                    id: id.as_deref(),
                    message: message.as_str(),
                };
                Self::bincode_options()
                    .serialize_into(buffer, &payload)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize error: {}", e))
            }
        }
    }
    
    /// Deserialize the message payload from bytes using the appropriate method
    pub fn deserialize_payload(message_type: MessageType, payload: &[u8]) -> Result<Self> {
        match message_type {
            MessageType::StoreMessage => {
                let msg: crate::data::StoreMessage = Self::bincode_options()
                    .deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize store message: {}", e))?;
                Ok(Self::Store(msg))
            },
            MessageType::PeerMessage => {
                let peer_msg: PeerMessage = Self::bincode_options()
                    .deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize peer message: {}", e))?;
                Ok(Self::Peer(peer_msg))
            },
            MessageType::Response => {
                #[derive(Deserialize)]
                struct ResponsePayload {
                    id: String,
                    data: Vec<u8>,
                }
                let payload_data: ResponsePayload = Self::bincode_options()
                    .deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize response: {}", e))?;
                Ok(Self::Response {
                    id: payload_data.id,
                    data: payload_data.data,
                })
            },
            MessageType::Error => {
                #[derive(Deserialize)]
                struct ErrorPayload {
                    id: Option<String>,
                    message: String,
                }
                let payload_data: ErrorPayload = Self::bincode_options()
                    .deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize error: {}", e))?;
                Ok(Self::Error {
                    id: payload_data.id,
                    message: payload_data.message,
                })
            },
        }
    }
}

/// Protocol encoder/decoder for handling message framing over TCP
pub struct ProtocolCodec;

impl ProtocolCodec {
    /// Encode a message into bytes ready for TCP transmission
    pub fn encode(message: &ProtocolMessage) -> Result<Vec<u8>> {
        let estimated = MessageHeader::SIZE + message.estimated_payload_size();
        let mut encoded = Vec::with_capacity(estimated);
        encoded.resize(MessageHeader::SIZE, 0);

        message.serialize_payload_into(&mut encoded)?;

        let payload_len = encoded.len() - MessageHeader::SIZE;
        let header = MessageHeader::new(message.message_type().as_u32(), payload_len as u32);
        encoded[..MessageHeader::SIZE].copy_from_slice(&header.to_bytes());

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

/// TCP message buffer for handling partial reads using BytesMut
#[derive(Debug)]
pub struct MessageBuffer {
    buffer: BytesMut,
    max_capacity: usize,
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self::with_capacity(64 * 1024) // 64KB initial capacity
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            max_capacity: capacity,
        }
    }
    
    /// Add data to the buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        
        // Prevent buffer from growing indefinitely by compacting when it gets too large
        if self.buffer.capacity() > self.max_capacity * 4 {
            // Reserve space and copy data to compact the buffer
            let mut new_buffer = BytesMut::with_capacity(self.max_capacity);
            new_buffer.extend_from_slice(&self.buffer);
            self.buffer = new_buffer;
        }
    }
    
    /// Try to decode a message from the buffer
    /// Returns (message, remaining_buffer) if successful
    pub fn try_decode(&mut self) -> Result<Option<ProtocolMessage>> {
        match ProtocolCodec::decode(&self.buffer)? {
            Some((message, bytes_consumed)) => {
                // Remove consumed bytes from buffer using BytesMut's efficient advance
                let _ = self.buffer.split_to(bytes_consumed);
                Ok(Some(message))
            },
            None => Ok(None),
        }
    }
    
    /// Try to decode a store message specifically
    pub fn try_decode_store_message(&mut self) -> Result<Option<crate::data::StoreMessage>> {
        match self.try_decode()? {
            Some(ProtocolMessage::Store(store_msg)) => Ok(Some(store_msg)),
            Some(_) => Err(anyhow::anyhow!("Expected store message, got different type")),
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