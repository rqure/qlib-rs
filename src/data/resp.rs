//! # RESP (REdis Serialization Protocol) Implementation
//! 
//! This module provides zero-copy RESP parsing and encoding with support for custom commands.
//! 
//! ## Core Features
//! 
//! - Zero-copy parsing for maximum performance
//! - Custom command support via traits
//! - CLI-friendly string-based numeric parsing
//! - Derive macro support for automatic encoding/decoding
//! - Command definition macro for easy command creation
//! 
//! ## Derive Macros
//! 
//! When the `derive` feature is enabled, you can automatically implement `RespEncode` and 
//! `RespDecode` for your custom types:
//! 
//! ```rust,ignore
//! #[cfg(feature = "derive")]
//! use qlib_rs::{RespEncode, RespDecode};
//! 
//! #[derive(RespEncode, RespDecode)]
//! struct MyCommand {
//!     entity_id: u64,
//!     name: String,
//!     active: bool,
//! }
//! 
//! #[derive(RespEncode, RespDecode)]
//! enum MyEnum {
//!     Variant1,
//!     Variant2(String),
//!     Variant3 { field: u32 },
//! }
//! ```
//! 
//! ## Command Definition Attribute
//! 
//! The `respc` attribute macro makes it easy to define new RESP commands with automatic
//! RESP command trait implementation:
//! 
//! ```rust,ignore
//! use qlib_rs::data::resp::{respc, RespEncode, RespDecode};
//! 
//! #[respc(name = "CUSTOM_READ")]
//! #[derive(Debug, Clone, RespEncode, RespDecode)]
//! pub struct CustomReadCommand<'a> {
//!     pub entity_id: EntityId,
//!     pub field_path: Vec<FieldType>,
//!     pub options: Option<String>,
//!     _marker: std::marker::PhantomData<&'a ()>,
//! }
//! 
//! impl<'a> RespCommand<'a> for CustomReadCommand<'a> {
//!     // Command processing is handled separately - no execute method needed
//! }
//! ```
//! 
//! This generates:
//! - The original struct definition
//! - Automatic `RespCommand` implementation with the specified command name
//! - Built-in RESP encoding/decoding support for all struct fields
//! 
//! ## CLI Compatibility
//! 
//! The RESP decoder automatically parses string representations of numbers and booleans,
//! making it easy to create CLI tools that send commands:
//! 
//! - `"42"` → `42i64`
//! - `"3.14"` → `3.14f64`  
//! - `"true"` → `true`
//! - `"false"` → `false`
//! 
//! ## Usage Example
//! 
//! ```rust,ignore
//! use qlib_rs::data::resp::{RespEncode, RespDecode, RespValue};
//! 
//! let data = MyCommand {
//!     entity_id: 42,
//!     name: "test".to_string(),
//!     active: true,
//! };
//! 
//! // Encode to RESP bytes
//! let encoded = data.encode();
//! 
//! // Decode from RESP bytes
//! let (decoded, _) = MyCommand::decode(&encoded)?;
//! ```

use crate::{
    data::{entity_schema::EntitySchemaResp, EntityId, EntityType, FieldType, Timestamp, Value}, Result
};

// Re-export derive macros when derive feature is enabled
#[cfg(feature = "derive")]
pub use qlib_rs_derive::{RespEncode, RespDecode, respc};

/// Redis RESP data types with zero-copy deserialization support
///
/// # Examples
/// 
/// ## Using derive macros (requires `derive` feature)
/// 
/// ```rust,ignore
/// use qlib_rs::data::resp::{RespEncode, RespDecode};
/// 
/// #[derive(Debug, RespEncode, RespDecode)]
/// struct MyCommand {
///     name: String,
///     value: i64,
/// }
/// 
/// #[derive(Debug, RespEncode, RespDecode)]
/// enum MyEnum {
///     Simple,
///     WithValue(String),
///     WithFields { x: i32, y: i32 },
/// }
/// ```
/// 
/// The derive macros automatically implement RESP encoding/decoding for:
/// - Structs with named fields (encoded as arrays with field names and values)
/// - Structs with unnamed fields (encoded as arrays with values only)
/// - Unit structs (encoded as empty arrays)
/// - Enums with discriminant + variant data
/// 
/// For CLI compatibility, integers and other numeric types can be provided as strings
/// and will be automatically parsed during decoding.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue<'a> {
    /// Simple strings are encoded as +<string>\r\n
    SimpleString(&'a str),
    /// Errors are encoded as -<error>\r\n  
    Error(&'a str),
    /// Integers are encoded as :<number>\r\n
    Integer(i64),
    /// Bulk strings are encoded as $<length>\r\n<data>\r\n
    BulkString(&'a [u8]),
    /// Arrays are encoded as *<count>\r\n<element1><element2>...
    Array(Vec<RespValue<'a>>),
    /// Null bulk string encoded as $-1\r\n
    Null,
}

/// Owned version of RespValue for when we need to construct values from owned data
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedRespValue {
    /// Simple strings are encoded as +<string>\r\n
    SimpleString(String),
    /// Errors are encoded as -<error>\r\n  
    Error(String),
    /// Integers are encoded as :<number>\r\n
    Integer(i64),
    /// Bulk strings are encoded as $<length>\r\n<data>\r\n
    BulkString(Vec<u8>),
    /// Arrays are encoded as *<count>\r\n<element1><element2>...
    Array(Vec<OwnedRespValue>),
    /// Null bulk string encoded as $-1\r\n
    Null,
}

pub trait RespToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait RespFromBytes<'a>: Sized {
    fn from_bytes(input: &'a [u8]) -> Result<(Self, &'a [u8])>;
}

impl<'a> RespToBytes for RespValue<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => {
                let mut result = Vec::with_capacity(s.len() + 3);
                result.push(b'+');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespValue::Error(e) => {
                let mut result = Vec::with_capacity(e.len() + 3);
                result.push(b'-');
                result.extend_from_slice(e.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespValue::Integer(i) => {
                let num_str = i.to_string();
                let mut result = Vec::with_capacity(num_str.len() + 3);
                result.push(b':');
                result.extend_from_slice(num_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespValue::BulkString(data) => {
                let len_str = data.len().to_string();
                let mut result = Vec::with_capacity(len_str.len() + data.len() + 5);
                result.push(b'$');
                result.extend_from_slice(len_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                result
            },
            RespValue::Array(elements) => {
                let count_str = elements.len().to_string();
                let mut result = Vec::new();
                result.push(b'*');
                result.extend_from_slice(count_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                
                for element in elements {
                    result.extend_from_slice(&element.to_bytes());
                }
                result
            },
            RespValue::Null => {
                b"$-1\r\n".to_vec()
            },
        }
    }
}

impl RespToBytes for OwnedRespValue {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            OwnedRespValue::SimpleString(s) => {
                let mut result = Vec::with_capacity(s.len() + 3);
                result.push(b'+');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            OwnedRespValue::Error(s) => {
                let mut result = Vec::with_capacity(s.len() + 3);
                result.push(b'-');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            OwnedRespValue::Integer(i) => {
                let s = i.to_string();
                let mut result = Vec::with_capacity(s.len() + 3);
                result.push(b':');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            OwnedRespValue::BulkString(data) => {
                let len_str = data.len().to_string();
                let mut result = Vec::with_capacity(len_str.len() + data.len() + 5);
                result.push(b'$');
                result.extend_from_slice(len_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                result
            },
            OwnedRespValue::Array(elements) => {
                let count_str = elements.len().to_string();
                let mut result = Vec::new();
                result.push(b'*');
                result.extend_from_slice(count_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                
                for element in elements {
                    result.extend_from_slice(&element.to_bytes());
                }
                result
            },
            OwnedRespValue::Null => b"$-1\r\n".to_vec(),
        }
    }
}

impl <'a> RespFromBytes<'a> for RespValue<'a> {
    fn from_bytes(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        RespParser::parse_value(input)
    }
}

/// Trait for RESP serialization  
pub trait RespEncode {
    /// Serialize to RESP format
    fn encode(&self) -> OwnedRespValue;
}

/// Trait for zero-copy RESP deserialization
pub trait RespDecode<'a>: Sized {
    /// Parse from a RESP buffer without copying data
    fn decode(input: RespValue<'a>) -> Result<Self>;
}

/// Custom command trait that all RESP commands must implement
pub trait RespCommand<'a>: RespDecode<'a> + RespEncode {
    /// The command name (e.g., "READ", "WRITE", "CREATE_ENTITY")
    const COMMAND_NAME: &'static str;
}

/// Custom command example - users can define their own commands this way
#[derive(Debug, Clone)]
pub struct CustomCommand<'a> {
    pub name: &'a str,
    pub args: Vec<RespValue<'a>>,
}

/// Error types for RESP parsing
#[derive(Debug, Clone)]
pub enum RespError {
    /// Incomplete input - need more data
    Incomplete,
    /// Invalid RESP format
    InvalidFormat(String),
    /// Invalid command
    InvalidCommand(String),
    /// UTF-8 conversion error
    Utf8Error,
    /// Integer parsing error
    IntegerError,
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::Incomplete => write!(f, "Incomplete input"),
            RespError::InvalidFormat(msg) => write!(f, "Invalid RESP format: {}", msg),
            RespError::InvalidCommand(msg) => write!(f, "Invalid command: {}", msg),
            RespError::Utf8Error => write!(f, "UTF-8 conversion error"),
            RespError::IntegerError => write!(f, "Integer parsing error"),
        }
    }
}

impl std::error::Error for RespError {}

/// Zero-copy RESP parser
pub struct RespParser;

impl RespParser {
    /// Find the end of a RESP line (\r\n)
    fn find_line_end(input: &[u8]) -> Option<usize> {
        for i in 0..input.len().saturating_sub(1) {
            if input[i] == b'\r' && input[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }
    
    /// Parse a simple string (+<string>\r\n)
    pub fn parse_simple_string(input: &[u8]) -> Result<(&str, &[u8])> {
        if input.is_empty() || input[0] != b'+' {
            return Err(crate::Error::InvalidRequest("Not a simple string".to_string()));
        }
        
        let line_end = Self::find_line_end(&input[1..])
            .ok_or_else(|| crate::Error::InvalidRequest("Incomplete simple string".to_string()))?;
            
        let str_bytes = &input[1..line_end + 1];
        let string = std::str::from_utf8(str_bytes)
            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in simple string".to_string()))?;
            
        let remaining = &input[line_end + 3..]; // Skip \r\n
        Ok((string, remaining))
    }
    
    /// Parse an error (-<error>\r\n)
    pub fn parse_error(input: &[u8]) -> Result<(&str, &[u8])> {
        if input.is_empty() || input[0] != b'-' {
            return Err(crate::Error::InvalidRequest("Not an error".to_string()));
        }
        
        let line_end = Self::find_line_end(&input[1..])
            .ok_or_else(|| crate::Error::InvalidRequest("Incomplete error".to_string()))?;
            
        let str_bytes = &input[1..line_end + 1];
        let string = std::str::from_utf8(str_bytes)
            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in error".to_string()))?;
            
        let remaining = &input[line_end + 3..]; // Skip \r\n
        Ok((string, remaining))
    }
    
    /// Parse an integer (:<number>\r\n)
    pub fn parse_integer(input: &[u8]) -> Result<(i64, &[u8])> {
        if input.is_empty() || input[0] != b':' {
            return Err(crate::Error::InvalidRequest("Not an integer".to_string()));
        }
        
        let line_end = Self::find_line_end(&input[1..])
            .ok_or_else(|| crate::Error::InvalidRequest("Incomplete integer".to_string()))?;
            
        let str_bytes = &input[1..line_end + 1];
        let num_str = std::str::from_utf8(str_bytes)
            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in integer".to_string()))?;
            
        let number = num_str.parse::<i64>()
            .map_err(|_| crate::Error::InvalidRequest("Invalid integer format".to_string()))?;
            
        let remaining = &input[line_end + 3..]; // Skip \r\n
        Ok((number, remaining))
    }
    
    /// Parse a bulk string ($<length>\r\n<data>\r\n)
    pub fn parse_bulk_string(input: &[u8]) -> Result<(Option<&[u8]>, &[u8])> {
        if input.is_empty() || input[0] != b'$' {
            return Err(crate::Error::InvalidRequest("Not a bulk string".to_string()));
        }
        
        let line_end = Self::find_line_end(&input[1..])
            .ok_or_else(|| crate::Error::InvalidRequest("Incomplete bulk string length".to_string()))?;
            
        let length_str = std::str::from_utf8(&input[1..line_end + 1])
            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in bulk string length".to_string()))?;
            
        let length = length_str.parse::<i32>()
            .map_err(|_| crate::Error::InvalidRequest("Invalid bulk string length".to_string()))?;
            
        if length == -1 {
            // Null bulk string
            let remaining = &input[line_end + 3..];
            return Ok((None, remaining));
        }
        
        if length < 0 {
            return Err(crate::Error::InvalidRequest("Invalid bulk string length".to_string()));
        }
        
        let length = length as usize;
        let data_start = line_end + 3; // Skip length and \r\n
        
        if input.len() < data_start + length + 2 {
            return Err(crate::Error::InvalidRequest("Incomplete bulk string data".to_string()));
        }
        
        let data = &input[data_start..data_start + length];
        let remaining = &input[data_start + length + 2..]; // Skip data and \r\n
        
        Ok((Some(data), remaining))
    }
    
    /// Parse an array (*<count>\r\n<element1><element2>...)
    pub fn parse_array(input: &[u8]) -> Result<(Vec<RespValue>, &[u8])> {
        if input.is_empty() || input[0] != b'*' {
            return Err(crate::Error::InvalidRequest("Not an array".to_string()));
        }
        
        let line_end = Self::find_line_end(&input[1..])
            .ok_or_else(|| crate::Error::InvalidRequest("Incomplete array count".to_string()))?;
            
        let count_str = std::str::from_utf8(&input[1..line_end + 1])
            .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in array count".to_string()))?;
            
        let count = count_str.parse::<i32>()
            .map_err(|_| crate::Error::InvalidRequest("Invalid array count".to_string()))?;
            
        if count < 0 {
            return Err(crate::Error::InvalidRequest("Invalid array count".to_string()));
        }
        
        let mut elements = Vec::with_capacity(count as usize);
        let mut remaining = &input[line_end + 3..]; // Skip count and \r\n
        
        for _ in 0..count {
            let (element, new_remaining) = Self::parse_value(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }
        
        Ok((elements, remaining))
    }
    
    /// Parse any RESP value
    pub fn parse_value(input: &[u8]) -> Result<(RespValue, &[u8])> {
        if input.is_empty() {
            return Err(crate::Error::InvalidRequest("Empty input".to_string()));
        }
        
        match input[0] {
            b'+' => {
                let (string, remaining) = Self::parse_simple_string(input)?;
                Ok((RespValue::SimpleString(string), remaining))
            },
            b'-' => {
                let (error, remaining) = Self::parse_error(input)?;
                Ok((RespValue::Error(error), remaining))
            },
            b':' => {
                let (integer, remaining) = Self::parse_integer(input)?;
                Ok((RespValue::Integer(integer), remaining))
            },
            b'$' => {
                let (bulk, remaining) = Self::parse_bulk_string(input)?;
                match bulk {
                    Some(data) => Ok((RespValue::BulkString(data), remaining)),
                    None => Ok((RespValue::Null, remaining)),
                }
            },
            b'*' => {
                let (array, remaining) = Self::parse_array(input)?;
                Ok((RespValue::Array(array), remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid RESP type marker".to_string())),
        }
    }
}

// ============================================================================
// Auto-derived RESP implementations for core types
// ============================================================================

impl<'a> RespDecode<'a> for EntityId {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 => Ok(EntityId(i as u64)),
            RespValue::BulkString(data) => {
                let id_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in EntityId".to_string()))?;
                let id = id_str.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityId format".to_string()))?;
                Ok(EntityId(id))
            },
            RespValue::SimpleString(s) => {
                let id = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityId format".to_string()))?;
                Ok(EntityId(id))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid EntityId type".to_string())),
        }
    }
}

impl RespEncode for EntityId {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(self.0 as i64)
    }
}

impl<'a> RespDecode<'a> for EntityType {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 => Ok(EntityType(i as u32)),
            RespValue::BulkString(data) => {
                let type_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in EntityType".to_string()))?;
                let type_val = type_str.parse::<u32>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityType format".to_string()))?;
                Ok(EntityType(type_val))
            },
            RespValue::SimpleString(s) => {
                let type_val = s.parse::<u32>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityType format".to_string()))?;
                Ok(EntityType(type_val))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid EntityType".to_string())),
        }
    }
}

impl RespEncode for EntityType {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(self.0 as i64)
    }
}

impl<'a> RespDecode<'a> for FieldType {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 => Ok(FieldType(i as u64)),
            RespValue::BulkString(data) => {
                let type_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in FieldType".to_string()))?;
                let type_val = type_str.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid FieldType format".to_string()))?;
                Ok(FieldType(type_val))
            },
            RespValue::SimpleString(s) => {
                let type_val = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid FieldType format".to_string()))?;
                Ok(FieldType(type_val))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid FieldType".to_string())),
        }
    }
}

impl RespEncode for FieldType {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(self.0 as i64)
    }
}

// Value will use derive macros - implementations are generated automatically
// The derive macro will handle the complex logic for CLI compatibility and type detection

// Vec implementations - we need specific implementations instead of generic ones
// to avoid conflicts and lifetime issues

// For Vec decoding, we need a different approach - we'll implement specific Vec decoders
// for the types we need rather than a generic one
impl RespDecode<'_> for Vec<String> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = String::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<String>".to_string())),
        }
    }
}

impl RespDecode<'_> for Vec<EntityId> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = EntityId::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<EntityId>".to_string())),
        }
    }
}

impl RespDecode<'_> for Vec<EntityType> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = EntityType::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<EntityType>".to_string())),
        }
    }
}

impl RespDecode<'_> for Vec<FieldType> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = FieldType::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<FieldType>".to_string())),
        }
    }
}

impl RespDecode<'_> for Vec<Vec<FieldType>> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = Vec::<FieldType>::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<Vec<FieldType>>".to_string())),
        }
    }
}

impl RespDecode<'_> for Vec<EntitySchemaResp> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = EntitySchemaResp::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<EntitySchemaResp>".to_string())),
        }
    }
}

// For FieldSchemaResp, we'll need a specific decoder too
use crate::data::entity_schema::FieldSchemaResp;

impl RespDecode<'_> for Vec<FieldSchemaResp> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for element in elements {
                    let decoded = FieldSchemaResp::decode(element)?;
                    result.push(decoded);
                }
                Ok(result)
            },
            _ => Err(crate::Error::InvalidRequest("Expected array for Vec<FieldSchemaResp>".to_string())),
        }
    }
}

// String slice decoding for command names
impl<'a> RespDecode<'a> for &'a str {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in string".to_string()))?;
                Ok(s)
            },
            RespValue::SimpleString(s) => Ok(s),
            _ => Err(crate::Error::InvalidRequest("Expected string type".to_string())),
        }
    }
}

impl RespEncode for &str {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::BulkString(self.as_bytes().to_vec())
    }
}

impl RespEncode for String {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::BulkString(self.as_bytes().to_vec())
    }
}

impl<'a> RespDecode<'a> for String {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in string".to_string()))?;
                Ok(s.to_string())
            },
            RespValue::SimpleString(s) => Ok(s.to_string()),
            _ => Err(crate::Error::InvalidRequest("Expected string type".to_string())),
        }
    }
}

// Implementations for PushCondition and AdjustBehavior
impl RespEncode for crate::PushCondition {
    fn encode(&self) -> OwnedRespValue {
        let value = match self {
            crate::PushCondition::Always => 0,
            crate::PushCondition::Changes => 1,
        };
        OwnedRespValue::Integer(value)
    }
}

impl<'a> RespDecode<'a> for crate::PushCondition {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Integer(0) => Ok(crate::PushCondition::Always),
            RespValue::Integer(1) => Ok(crate::PushCondition::Changes),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in PushCondition".to_string()))?;
                match s.to_lowercase().as_str() {
                    "always" => Ok(crate::PushCondition::Always),
                    "changes" => Ok(crate::PushCondition::Changes),
                    _ => Err(crate::Error::InvalidRequest("Invalid PushCondition value".to_string())),
                }
            },
            RespValue::SimpleString(s) => {
                match s.to_lowercase().as_str() {
                    "always" => Ok(crate::PushCondition::Always),
                    "changes" => Ok(crate::PushCondition::Changes),
                    _ => Err(crate::Error::InvalidRequest("Invalid PushCondition value".to_string())),
                }
            },
            _ => Err(crate::Error::InvalidRequest("Invalid PushCondition type".to_string())),
        }
    }
}

impl RespEncode for crate::AdjustBehavior {
    fn encode(&self) -> OwnedRespValue {
        let value = match self {
            crate::AdjustBehavior::Set => 0,
            crate::AdjustBehavior::Add => 1,
            crate::AdjustBehavior::Subtract => 2,
        };
        OwnedRespValue::Integer(value)
    }
}

impl<'a> RespDecode<'a> for crate::AdjustBehavior {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Integer(0) => Ok(crate::AdjustBehavior::Set),
            RespValue::Integer(1) => Ok(crate::AdjustBehavior::Add),
            RespValue::Integer(2) => Ok(crate::AdjustBehavior::Subtract),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in AdjustBehavior".to_string()))?;
                match s.to_lowercase().as_str() {
                    "set" => Ok(crate::AdjustBehavior::Set),
                    "add" => Ok(crate::AdjustBehavior::Add),
                    "subtract" => Ok(crate::AdjustBehavior::Subtract),
                    _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior value".to_string())),
                }
            },
            RespValue::SimpleString(s) => {
                match s.to_lowercase().as_str() {
                    "set" => Ok(crate::AdjustBehavior::Set),
                    "add" => Ok(crate::AdjustBehavior::Add),
                    "subtract" => Ok(crate::AdjustBehavior::Subtract),
                    _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior value".to_string())),
                }
            },
            _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior type".to_string())),
        }
    }
}

impl<'a> RespDecode<'a> for Timestamp {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::BulkString(data) => {
                let timestamp_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in Timestamp".to_string()))?;
                // Try parsing as RFC3339 format first
                let timestamp = time::OffsetDateTime::parse(timestamp_str, &time::format_description::well_known::Rfc3339)
                    .or_else(|_| {
                        // Try parsing as unix timestamp string
                        timestamp_str.parse::<i64>()
                            .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp format".to_string()))
                            .and_then(|ts| time::OffsetDateTime::from_unix_timestamp(ts)
                                .map_err(|_| crate::Error::InvalidRequest("Invalid unix timestamp".to_string())))
                    })
                    .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp format".to_string()))?;
                Ok(timestamp)
            },
            RespValue::SimpleString(s) => {
                // Try parsing as RFC3339 format first
                let timestamp = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .or_else(|_| {
                        // Try parsing as unix timestamp string
                        s.parse::<i64>()
                            .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp format".to_string()))
                            .and_then(|ts| time::OffsetDateTime::from_unix_timestamp(ts)
                                .map_err(|_| crate::Error::InvalidRequest("Invalid unix timestamp".to_string())))
                    })
                    .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp format".to_string()))?;
                Ok(timestamp)
            },
            RespValue::Integer(i) => {
                let timestamp = time::OffsetDateTime::from_unix_timestamp(i)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp value".to_string()))?;
                Ok(timestamp)
            },
            _ => Err(crate::Error::InvalidRequest("Invalid Timestamp type".to_string())),
        }
    }
}

impl RespEncode for Timestamp {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::BulkString(self.to_string().into_bytes())
    }
}

// Implementation for PhantomData
impl<T> RespEncode for std::marker::PhantomData<T> {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Null
    }
}

impl<'a, T> RespDecode<'a> for std::marker::PhantomData<T> {
    fn decode(_input: RespValue<'a>) -> Result<Self> {
        // PhantomData doesn't consume actual data
        Ok(std::marker::PhantomData)
    }
}

// Implementation for Option<T> where T implements the traits
impl<T: RespEncode> RespEncode for Option<T> {
    fn encode(&self) -> OwnedRespValue {
        match self {
            Some(value) => value.encode(),
            None => OwnedRespValue::Null,
        }
    }
}

impl<'a, T: RespDecode<'a>> RespDecode<'a> for Option<T> {
    fn decode(input: RespValue<'a>) -> Result<Self> {
        match input {
            RespValue::Null => Ok(None),
            _ => {
                match T::decode(input) {
                    Ok(decoded_value) => Ok(Some(decoded_value)),
                    Err(_) => Ok(None), // If T can't be decoded, treat as None
                }
            }
        }
    }
}

impl RespEncode for u64 {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(*self as i64)
    }
}

impl RespDecode<'_> for u64 {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 => Ok(i as u64),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in u64".to_string()))?;
                let val = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid u64 format".to_string()))?;
                Ok(val)
            },
            RespValue::SimpleString(s) => {
                let val = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid u64 format".to_string()))?;
                Ok(val)
            },
            _ => Err(crate::Error::InvalidRequest("Expected non-negative integer for u64".to_string())),
        }
    }
}

impl RespEncode for bool {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(if *self { 1 } else { 0 })
    }
}

impl RespDecode<'_> for bool {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(0) => Ok(false),
            RespValue::Integer(1) => Ok(true),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in bool".to_string()))?;
                match s.to_lowercase().as_str() {
                    "true" | "1" => Ok(true),
                    "false" | "0" => Ok(false),
                    _ => Err(crate::Error::InvalidRequest("Invalid bool format".to_string())),
                }
            },
            RespValue::SimpleString(s) => {
                match s.to_lowercase().as_str() {
                    "true" | "1" => Ok(true),
                    "false" | "0" => Ok(false),
                    _ => Err(crate::Error::InvalidRequest("Invalid bool format".to_string())),
                }
            },
            _ => Err(crate::Error::InvalidRequest("Expected 0 or 1 for bool".to_string())),
        }
    }
}

impl RespEncode for i64 {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(*self)
    }
}

impl RespDecode<'_> for i64 {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(i) => Ok(i),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in i64".to_string()))?;
                let val = s.parse::<i64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid i64 format".to_string()))?;
                Ok(val)
            },
            RespValue::SimpleString(s) => {
                let val = s.parse::<i64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid i64 format".to_string()))?;
                Ok(val)
            },
            _ => Err(crate::Error::InvalidRequest("Expected integer for i64".to_string())),
        }
    }
}

impl RespEncode for usize {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(*self as i64)
    }
}

impl RespEncode for u8 {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::Integer(*self as i64)
    }
}

impl RespDecode<'_> for u8 {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 && i <= 255 => Ok(i as u8),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in u8".to_string()))?;
                let val = s.parse::<u8>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid u8 format".to_string()))?;
                Ok(val)
            },
            RespValue::SimpleString(s) => {
                let val = s.parse::<u8>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid u8 format".to_string()))?;
                Ok(val)
            },
            _ => Err(crate::Error::InvalidRequest("Expected integer for u8".to_string())),
        }
    }
}

impl RespEncode for f64 {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::BulkString(self.to_string().into_bytes())
    }
}

impl RespDecode<'_> for f64 {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(i) => Ok(i as f64),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in f64".to_string()))?;
                let val = s.parse::<f64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid f64 format".to_string()))?;
                Ok(val)
            },
            RespValue::SimpleString(s) => {
                let val = s.parse::<f64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid f64 format".to_string()))?;
                Ok(val)
            },
            _ => Err(crate::Error::InvalidRequest("Expected number for f64".to_string())),
        }
    }
}

// Vec<u8> implementation for binary data
impl RespEncode for Vec<u8> {
    fn encode(&self) -> OwnedRespValue {
        OwnedRespValue::BulkString(self.clone())
    }
}

impl RespDecode<'_> for Vec<u8> {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::BulkString(data) => Ok(data.to_vec()),
            RespValue::SimpleString(s) => Ok(s.as_bytes().to_vec()),
            _ => Err(crate::Error::InvalidRequest("Expected binary data for Vec<u8>".to_string())),
        }
    }
}

// Vec<EntityId> implementation
impl RespEncode for Vec<EntityId> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

// Vec<EntityType> implementation
impl RespEncode for Vec<EntityType> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

// Vec<FieldType> implementation
impl RespEncode for Vec<FieldType> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

// Vec<String> implementation
impl RespEncode for Vec<String> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

// Vec<Vec<FieldType>> implementation
impl RespEncode for Vec<Vec<FieldType>> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

// Vec<FieldSchemaResp> implementation
impl RespEncode for Vec<FieldSchemaResp> {
    fn encode(&self) -> OwnedRespValue {
        let elements: Vec<OwnedRespValue> = self.iter()
            .map(|item| item.encode())
            .collect();
        OwnedRespValue::Array(elements)
    }
}

impl RespDecode<'_> for usize {
    fn decode(input: RespValue<'_>) -> Result<Self> {
        match input {
            RespValue::Integer(i) if i >= 0 => Ok(i as usize),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in usize".to_string()))?;
                let val = s.parse::<usize>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid usize format".to_string()))?;
                Ok(val)
            },
            RespValue::SimpleString(s) => {
                let val = s.parse::<usize>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid usize format".to_string()))?;
                Ok(val)
            },
            _ => Err(crate::Error::InvalidRequest("Expected non-negative integer for usize".to_string())),
        }
    }
}

// ============================================================================
// RESP Commands for all StoreTrait methods
// ============================================================================

/// Read command for reading field values
#[respc(name = "READ")]
#[derive(Debug, Clone, RespDecode)]
pub struct ReadCommand<'a> {
    pub entity_id: EntityId,
    pub field_path: Vec<FieldType>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Write command for writing field values
#[respc(name = "WRITE")]
#[derive(Debug, Clone, RespDecode)]
pub struct WriteCommand<'a> {
    pub entity_id: EntityId,
    pub field_path: Vec<FieldType>,
    pub value: Value,
    pub writer_id: Option<EntityId>,
    pub write_time: Option<Timestamp>,
    pub push_condition: Option<crate::PushCondition>,
    pub adjust_behavior: Option<crate::AdjustBehavior>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Create entity command
#[respc(name = "CREATE_ENTITY")]
#[derive(Debug, Clone, RespDecode)]
pub struct CreateEntityCommand<'a> {
    pub entity_type: EntityType,
    pub parent_id: Option<EntityId>,
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Delete entity command
#[respc(name = "DELETE_ENTITY")]
#[derive(Debug, Clone, RespDecode)]
pub struct DeleteEntityCommand<'a> {
    pub entity_id: EntityId,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity type by name command
#[respc(name = "GET_ENTITY_TYPE")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetEntityTypeCommand<'a> {
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve entity type to name command
#[respc(name = "RESOLVE_ENTITY_TYPE")]
#[derive(Debug, Clone, RespDecode)]
pub struct ResolveEntityTypeCommand<'a> {
    pub entity_type: EntityType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get field type by name command
#[respc(name = "GET_FIELD_TYPE")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetFieldTypeCommand<'a> {
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve field type to name command
#[respc(name = "RESOLVE_FIELD_TYPE")]
#[derive(Debug, Clone, RespDecode)]
pub struct ResolveFieldTypeCommand<'a> {
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity schema command
#[respc(name = "GET_ENTITY_SCHEMA")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetEntitySchemaCommand<'a> {
    pub entity_type: EntityType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

#[respc(name = "UPDATE_SCHEMA")]
#[derive(Debug, Clone, RespDecode)]
pub struct UpdateSchemaCommand<'a> {
    pub schema: EntitySchemaResp,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get field schema command
#[respc(name = "GET_FIELD_SCHEMA")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetFieldSchemaCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Set field schema command
#[respc(name = "SET_FIELD_SCHEMA")]
#[derive(Debug, Clone, RespDecode)]
pub struct SetFieldSchemaCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub schema: crate::data::entity_schema::FieldSchemaResp,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Entity exists check command
#[respc(name = "ENTITY_EXISTS")]
#[derive(Debug, Clone, RespDecode)]
pub struct EntityExistsCommand<'a> {
    pub entity_id: EntityId,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Field exists check command
#[respc(name = "FIELD_EXISTS")]
#[derive(Debug, Clone, RespDecode)]
pub struct FieldExistsCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve indirection command
#[respc(name = "RESOLVE_INDIRECTION")]
#[derive(Debug, Clone, RespDecode)]
pub struct ResolveIndirectionCommand<'a> {
    pub entity_id: EntityId,
    pub fields: Vec<FieldType>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find entities with pagination command
#[respc(name = "FIND_ENTITIES_PAGINATED")]
#[derive(Debug, Clone, RespDecode)]
pub struct FindEntitiesPaginatedCommand<'a> {
    pub entity_type: EntityType,
    pub page_opts: Option<crate::data::PageOpts>,
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find entities exactly (no inheritance) with pagination command
#[respc(name = "FIND_ENTITIES_EXACT")]
#[derive(Debug, Clone, RespDecode)]
pub struct FindEntitiesExactCommand<'a> {
    pub entity_type: EntityType,
    pub page_opts: Option<crate::data::PageOpts>,
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find all entities command
#[respc(name = "FIND_ENTITIES")]
#[derive(Debug, Clone, RespDecode)]
pub struct FindEntitiesCommand<'a> {
    pub entity_type: EntityType,
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get all entity types command
#[respc(name = "GET_ENTITY_TYPES")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetEntityTypesCommand<'a> {
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity types with pagination command
#[respc(name = "GET_ENTITY_TYPES_PAGINATED")]
#[derive(Debug, Clone, RespDecode)]
pub struct GetEntityTypesPaginatedCommand<'a> {
    pub page_opts: Option<crate::data::PageOpts>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Take snapshot command
#[respc(name = "TAKE_SNAPSHOT")]
#[derive(Debug, Clone, RespDecode)]
pub struct TakeSnapshotCommand<'a> {
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Register notification command
#[respc(name = "REGISTER_NOTIFICATION")]
#[derive(Debug, Clone, RespDecode)]
pub struct RegisterNotificationCommand<'a> {
    pub config: crate::NotifyConfig,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Unregister notification command
#[respc(name = "UNREGISTER_NOTIFICATION")]
#[derive(Debug, Clone, RespDecode)]
pub struct UnregisterNotificationCommand<'a> {
    pub config: crate::NotifyConfig,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

// ============================================================================
// RESP Response Structs for complex return types
// ============================================================================

/// Response for read operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ReadResponse {
    pub value: Value,
    pub timestamp: Timestamp,
    pub writer_id: Option<EntityId>,
}

/// Response for resolve indirection operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ResolveIndirectionResponse {
    pub entity_id: EntityId,
    pub field_type: FieldType,
}

/// Response for create entity operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct CreateEntityResponse {
    pub entity_id: EntityId,
}

/// Response for simple boolean operations (exists checks)
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct BooleanResponse {
    pub result: bool,
}

/// Response for string operations (resolve operations)
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct StringResponse {
    pub value: String,
}

/// Response for integer operations (get operations)
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct IntegerResponse {
    pub value: i64,
}

/// Response for entity list operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct EntityListResponse {
    pub entities: Vec<EntityId>,
}

/// Response for entity type list operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct EntityTypeListResponse {
    pub entity_types: Vec<EntityType>,
}

/// Response for field schema operations
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FieldSchemaResponse {
    pub schema: crate::data::entity_schema::FieldSchemaResp,
}

/// Response for snapshot operations - simplified version of Snapshot for RESP transport
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct SnapshotResponse {
    pub data: String, // JSON-serialized snapshot data
}

/// Response for paginated entity results
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct PaginatedEntityResponse {
    pub items: Vec<EntityId>,
    pub total: usize,
    pub next_cursor: Option<usize>,
}

/// Response for paginated entity type results
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct PaginatedEntityTypeResponse {
    pub items: Vec<EntityType>,
    pub total: usize,
    pub next_cursor: Option<usize>,
}

/// Response for paginated results
#[derive(Debug, Clone)]
pub struct PageResultResponse<T> {
    pub items: Vec<T>,
    pub total_count: Option<u64>,
    pub has_more: bool,
    pub cursor: Option<String>,
}

// ============================================================================
// RESP Commands for Peer Communication
// ============================================================================

/// Peer handshake command
#[respc(name = "PEER_HANDSHAKE")]
#[derive(Debug, Clone, RespDecode)]
pub struct PeerHandshakeCommand<'a> {
    pub start_time: u64,
    pub is_response: bool,
    pub machine_id: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Full sync request command
#[respc(name = "FULL_SYNC_REQUEST")]
#[derive(Debug, Clone, RespDecode)]
pub struct FullSyncRequestCommand<'a> {
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Full sync response command
#[respc(name = "FULL_SYNC_RESPONSE")]
#[derive(Debug, Clone, RespDecode)]
pub struct FullSyncResponseCommand<'a> {
    pub snapshot_data: String, // JSON-serialized snapshot
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Sync write command
#[respc(name = "SYNC_WRITE")]
#[derive(Debug, Clone, RespDecode)]
pub struct SyncWriteCommand<'a> {
    pub requests_data: String, // JSON-serialized requests
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Notification message command
#[respc(name = "NOTIFICATION")]
#[derive(Debug, Clone, RespDecode)]
pub struct NotificationCommand<'a> {
    pub notification_data: String, // JSON-serialized notification
    pub _marker: std::marker::PhantomData<&'a ()>,
}
