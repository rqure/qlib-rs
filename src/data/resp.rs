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
    data::{EntityId, EntityType, FieldType, Timestamp, Value},
    Result,
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

/// Trait for RESP serialization  
pub trait RespEncode {
    /// Serialize to RESP format
    fn encode(&self) -> Vec<u8>;
}

/// Trait for zero-copy RESP deserialization
pub trait RespDecode<'a>: Sized {
    /// Parse from a RESP buffer without copying data
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])>;
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
// RESP Encoding Implementations
// ============================================================================

impl RespEncode for RespValue<'_> {
    fn encode(&self) -> Vec<u8> {
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
                    result.extend_from_slice(&element.encode());
                }
                result
            },
            RespValue::Null => {
                b"$-1\r\n".to_vec()
            },
        }
    }
}

// ============================================================================
// Zero-Copy Decoding Implementations for Core Types
// ============================================================================

impl<'a> RespDecode<'a> for RespValue<'a> {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        RespParser::parse_value(input)
    }
}

// ============================================================================
// Auto-derived RESP implementations for core types
// ============================================================================

impl<'a> RespDecode<'a> for EntityId {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Integer(i) if i >= 0 => Ok((EntityId(i as u64), remaining)),
            RespValue::BulkString(data) => {
                let id_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in EntityId".to_string()))?;
                let id = id_str.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityId format".to_string()))?;
                Ok((EntityId(id), remaining))
            },
            RespValue::SimpleString(s) => {
                let id = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityId format".to_string()))?;
                Ok((EntityId(id), remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid EntityId type".to_string())),
        }
    }
}

impl RespEncode for EntityId {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(self.0 as i64).encode()
    }
}

impl<'a> RespDecode<'a> for EntityType {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Integer(i) if i >= 0 => Ok((EntityType(i as u32), remaining)),
            RespValue::BulkString(data) => {
                let type_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in EntityType".to_string()))?;
                let type_val = type_str.parse::<u32>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityType format".to_string()))?;
                Ok((EntityType(type_val), remaining))
            },
            RespValue::SimpleString(s) => {
                let type_val = s.parse::<u32>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid EntityType format".to_string()))?;
                Ok((EntityType(type_val), remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid EntityType".to_string())),
        }
    }
}

impl RespEncode for EntityType {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(self.0 as i64).encode()
    }
}

impl<'a> RespDecode<'a> for FieldType {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Integer(i) if i >= 0 => Ok((FieldType(i as u64), remaining)),
            RespValue::BulkString(data) => {
                let type_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in FieldType".to_string()))?;
                let type_val = type_str.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid FieldType format".to_string()))?;
                Ok((FieldType(type_val), remaining))
            },
            RespValue::SimpleString(s) => {
                let type_val = s.parse::<u64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid FieldType format".to_string()))?;
                Ok((FieldType(type_val), remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid FieldType".to_string())),
        }
    }
}

impl RespEncode for FieldType {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(self.0 as i64).encode()
    }
}

impl<'a> RespDecode<'a> for Value {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        let decoded_value = match value {
            RespValue::SimpleString(s) => {
                // Try to parse various numeric types from strings for CLI compatibility
                if let Ok(i) = s.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    Value::Float(f)
                } else if s.eq_ignore_ascii_case("true") {
                    Value::Bool(true)
                } else if s.eq_ignore_ascii_case("false") {
                    Value::Bool(false)
                } else {
                    Value::String(s.to_string())
                }
            },
            RespValue::BulkString(data) => {
                // Try to parse as UTF-8 string first
                match std::str::from_utf8(data) {
                    Ok(s) => {
                        // Try to parse various numeric types from strings for CLI compatibility
                        if let Ok(i) = s.parse::<i64>() {
                            Value::Int(i)
                        } else if let Ok(f) = s.parse::<f64>() {
                            Value::Float(f)
                        } else if s.eq_ignore_ascii_case("true") {
                            Value::Bool(true)
                        } else if s.eq_ignore_ascii_case("false") {
                            Value::Bool(false)
                        } else {
                            Value::String(s.to_string())
                        }
                    },
                    Err(_) => Value::Blob(data.to_vec()),
                }
            },
            RespValue::Integer(i) => Value::Int(i),
            RespValue::Null => Value::EntityReference(None),
            RespValue::Array(elements) => {
                // Try to parse as EntityList first
                let mut all_valid = true;
                let mut entity_ids = Vec::new();
                for element in &elements {
                    match element {
                        RespValue::Integer(i) if *i >= 0 => entity_ids.push(EntityId(*i as u64)),
                        RespValue::SimpleString(s) => {
                            if let Ok(id) = s.parse::<u64>() {
                                entity_ids.push(EntityId(id));
                            } else {
                                all_valid = false;
                                break;
                            }
                        },
                        RespValue::BulkString(data) => {
                            if let Ok(s) = std::str::from_utf8(data) {
                                if let Ok(id) = s.parse::<u64>() {
                                    entity_ids.push(EntityId(id));
                                } else {
                                    all_valid = false;
                                    break;
                                }
                            } else {
                                all_valid = false;
                                break;
                            }
                        }
                        _ => {
                            all_valid = false;
                            break;
                        }
                    }
                }
                if all_valid && entity_ids.len() == elements.len() {
                    Value::EntityList(entity_ids)
                } else {
                    Value::String(format!("{:?}", elements))
                }
            },
            RespValue::Error(e) => Value::String(e.to_string()),
        };
        Ok((decoded_value, remaining))
    }
}

impl RespEncode for Value {
    fn encode(&self) -> Vec<u8> {
        match self {
            Value::String(s) => RespValue::BulkString(s.as_bytes()).encode(),
            Value::Int(i) => RespValue::Integer(*i).encode(),
            Value::Float(f) => RespValue::BulkString(f.to_string().as_bytes()).encode(),
            Value::Bool(b) => RespValue::Integer(if *b { 1 } else { 0 }).encode(),
            Value::Blob(data) => RespValue::BulkString(data).encode(),
            Value::EntityReference(Some(entity_id)) => entity_id.encode(),
            Value::EntityReference(None) => RespValue::Null.encode(),
            Value::EntityList(entities) => {
                let elements: Vec<RespValue> = entities.iter()
                    .map(|entity_id| RespValue::Integer(entity_id.0 as i64))
                    .collect();
                RespValue::Array(elements).encode()
            },
            Value::Choice(choice) => RespValue::Integer(*choice).encode(),
            Value::Timestamp(timestamp) => RespValue::BulkString(timestamp.to_string().as_bytes()).encode(),
        }
    }
}

// Helper macro to decode Vec<T> from RESP arrays
macro_rules! impl_vec_decode {
    ($typ:ty) => {
        impl<'a> RespDecode<'a> for Vec<$typ> {
            fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
                let (value, remaining) = RespValue::decode(input)?;
                match value {
                    RespValue::Array(elements) => {
                        let mut result = Vec::with_capacity(elements.len());
                        for element in elements {
                            let element_bytes = element.encode();
                            let (decoded, _) = <$typ>::decode(&element_bytes)?;
                            result.push(decoded);
                        }
                        Ok((result, remaining))
                    },
                    _ => Err(crate::Error::InvalidRequest("Expected array for Vec".to_string())),
                }
            }
        }
    };
}

impl_vec_decode!(FieldType);
impl_vec_decode!(EntityId);
impl_vec_decode!(EntityType);

// String slice decoding for command names
impl<'a> RespDecode<'a> for &'a str {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in string".to_string()))?;
                Ok((s, remaining))
            },
            RespValue::SimpleString(s) => Ok((s, remaining)),
            _ => Err(crate::Error::InvalidRequest("Expected string type".to_string())),
        }
    }
}

impl RespEncode for &str {
    fn encode(&self) -> Vec<u8> {
        RespValue::BulkString(self.as_bytes()).encode()
    }
}

impl RespEncode for String {
    fn encode(&self) -> Vec<u8> {
        RespValue::BulkString(self.as_bytes()).encode()
    }
}

impl<'a> RespDecode<'a> for String {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in string".to_string()))?;
                Ok((s.to_string(), remaining))
            },
            RespValue::SimpleString(s) => Ok((s.to_string(), remaining)),
            _ => Err(crate::Error::InvalidRequest("Expected string type".to_string())),
        }
    }
}

impl RespEncode for Vec<FieldType> {
    fn encode(&self) -> Vec<u8> {
        let elements: Vec<RespValue> = self.iter()
            .map(|ft| RespValue::Integer(ft.0 as i64))
            .collect();
        RespValue::Array(elements).encode()
    }
}

impl RespEncode for Vec<EntityId> {
    fn encode(&self) -> Vec<u8> {
        let elements: Vec<RespValue> = self.iter()
            .map(|id| RespValue::Integer(id.0 as i64))
            .collect();
        RespValue::Array(elements).encode()
    }
}

impl RespEncode for Vec<EntityType> {
    fn encode(&self) -> Vec<u8> {
        let elements: Vec<RespValue> = self.iter()
            .map(|et| RespValue::Integer(et.0 as i64))
            .collect();
        RespValue::Array(elements).encode()
    }
}

// Implementations for PushCondition and AdjustBehavior
impl RespEncode for crate::PushCondition {
    fn encode(&self) -> Vec<u8> {
        let value = match self {
            crate::PushCondition::Always => 0,
            crate::PushCondition::Changes => 1,
        };
        RespValue::Integer(value).encode()
    }
}

impl<'a> RespDecode<'a> for crate::PushCondition {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Integer(0) => Ok((crate::PushCondition::Always, remaining)),
            RespValue::Integer(1) => Ok((crate::PushCondition::Changes, remaining)),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in PushCondition".to_string()))?;
                match s.to_lowercase().as_str() {
                    "always" => Ok((crate::PushCondition::Always, remaining)),
                    "changes" => Ok((crate::PushCondition::Changes, remaining)),
                    _ => Err(crate::Error::InvalidRequest("Invalid PushCondition value".to_string())),
                }
            },
            RespValue::SimpleString(s) => {
                match s.to_lowercase().as_str() {
                    "always" => Ok((crate::PushCondition::Always, remaining)),
                    "changes" => Ok((crate::PushCondition::Changes, remaining)),
                    _ => Err(crate::Error::InvalidRequest("Invalid PushCondition value".to_string())),
                }
            },
            _ => Err(crate::Error::InvalidRequest("Invalid PushCondition type".to_string())),
        }
    }
}

impl RespEncode for crate::AdjustBehavior {
    fn encode(&self) -> Vec<u8> {
        let value = match self {
            crate::AdjustBehavior::Set => 0,
            crate::AdjustBehavior::Add => 1,
            crate::AdjustBehavior::Subtract => 2,
        };
        RespValue::Integer(value).encode()
    }
}

impl<'a> RespDecode<'a> for crate::AdjustBehavior {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Integer(0) => Ok((crate::AdjustBehavior::Set, remaining)),
            RespValue::Integer(1) => Ok((crate::AdjustBehavior::Add, remaining)),
            RespValue::Integer(2) => Ok((crate::AdjustBehavior::Subtract, remaining)),
            RespValue::BulkString(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in AdjustBehavior".to_string()))?;
                match s.to_lowercase().as_str() {
                    "set" => Ok((crate::AdjustBehavior::Set, remaining)),
                    "add" => Ok((crate::AdjustBehavior::Add, remaining)),
                    "subtract" => Ok((crate::AdjustBehavior::Subtract, remaining)),
                    _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior value".to_string())),
                }
            },
            RespValue::SimpleString(s) => {
                match s.to_lowercase().as_str() {
                    "set" => Ok((crate::AdjustBehavior::Set, remaining)),
                    "add" => Ok((crate::AdjustBehavior::Add, remaining)),
                    "subtract" => Ok((crate::AdjustBehavior::Subtract, remaining)),
                    _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior value".to_string())),
                }
            },
            _ => Err(crate::Error::InvalidRequest("Invalid AdjustBehavior type".to_string())),
        }
    }
}

impl<'a> RespDecode<'a> for Timestamp {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
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
                Ok((timestamp, remaining))
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
                Ok((timestamp, remaining))
            },
            RespValue::Integer(i) => {
                let timestamp = time::OffsetDateTime::from_unix_timestamp(i)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid Timestamp value".to_string()))?;
                Ok((timestamp, remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Invalid Timestamp type".to_string())),
        }
    }
}

impl RespEncode for Timestamp {
    fn encode(&self) -> Vec<u8> {
        RespValue::BulkString(self.to_string().as_bytes()).encode()
    }
}

// Implementation for PhantomData
impl<T> RespEncode for std::marker::PhantomData<T> {
    fn encode(&self) -> Vec<u8> {
        RespValue::Null.encode()
    }
}

impl<'a, T> RespDecode<'a> for std::marker::PhantomData<T> {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (_, remaining) = RespValue::decode(input)?;
        // PhantomData doesn't consume actual data, just advance the parser
        Ok((std::marker::PhantomData, remaining))
    }
}

// Implementation for Option<T> where T implements the traits
impl<T: RespEncode> RespEncode for Option<T> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Some(value) => value.encode(),
            None => RespValue::Null.encode(),
        }
    }
}

impl<'a, T: RespDecode<'a>> RespDecode<'a> for Option<T> {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::Null => Ok((None, remaining)),
            _ => {
                // For Option<T>, we need to decode T from the same input since we can't extend lifetimes
                match T::decode(input) {
                    Ok((decoded_value, new_remaining)) => Ok((Some(decoded_value), new_remaining)),
                    Err(_) => Ok((None, remaining)), // If T can't be decoded, treat as None
                }
            }
        }
    }
}

// ============================================================================
// RESP Commands for all StoreTrait methods
// ============================================================================

/// Read command for reading field values
#[respc(name = "READ")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ReadCommand<'a> {
    pub entity_id: EntityId,
    pub field_path: Vec<FieldType>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Write command for writing field values
#[respc(name = "WRITE")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
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
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct CreateEntityCommand<'a> {
    pub entity_type: EntityType,
    pub parent_id: Option<EntityId>,
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Delete entity command
#[respc(name = "DELETE_ENTITY")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct DeleteEntityCommand<'a> {
    pub entity_id: EntityId,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity type by name command
#[respc(name = "GET_ENTITY_TYPE")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetEntityTypeCommand<'a> {
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve entity type to name command
#[respc(name = "RESOLVE_ENTITY_TYPE")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ResolveEntityTypeCommand<'a> {
    pub entity_type: EntityType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get field type by name command
#[respc(name = "GET_FIELD_TYPE")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetFieldTypeCommand<'a> {
    pub name: String,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve field type to name command
#[respc(name = "RESOLVE_FIELD_TYPE")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ResolveFieldTypeCommand<'a> {
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity schema command
#[respc(name = "GET_ENTITY_SCHEMA")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetEntitySchemaCommand<'a> {
    pub entity_type: EntityType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get field schema command
#[respc(name = "GET_FIELD_SCHEMA")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetFieldSchemaCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Set field schema command
#[respc(name = "SET_FIELD_SCHEMA")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct SetFieldSchemaCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub schema: String, // Serialized FieldSchema
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Entity exists check command
#[respc(name = "ENTITY_EXISTS")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct EntityExistsCommand<'a> {
    pub entity_id: EntityId,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Field exists check command
#[respc(name = "FIELD_EXISTS")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FieldExistsCommand<'a> {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Resolve indirection command
#[respc(name = "RESOLVE_INDIRECTION")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ResolveIndirectionCommand<'a> {
    pub entity_id: EntityId,
    pub fields: Vec<FieldType>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find entities with pagination command
#[respc(name = "FIND_ENTITIES_PAGINATED")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FindEntitiesPaginatedCommand<'a> {
    pub entity_type: EntityType,
    pub page_opts: Option<String>, // Serialized PageOpts
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find entities exactly (no inheritance) with pagination command
#[respc(name = "FIND_ENTITIES_EXACT")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FindEntitiesExactCommand<'a> {
    pub entity_type: EntityType,
    pub page_opts: Option<String>, // Serialized PageOpts
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Find all entities command
#[respc(name = "FIND_ENTITIES")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FindEntitiesCommand<'a> {
    pub entity_type: EntityType,
    pub filter: Option<String>,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get all entity types command
#[respc(name = "GET_ENTITY_TYPES")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetEntityTypesCommand<'a> {
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Get entity types with pagination command
#[respc(name = "GET_ENTITY_TYPES_PAGINATED")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct GetEntityTypesPaginatedCommand<'a> {
    pub page_opts: Option<String>, // Serialized PageOpts
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Take snapshot command
#[respc(name = "TAKE_SNAPSHOT")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct TakeSnapshotCommand<'a> {
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Register notification command
#[respc(name = "REGISTER_NOTIFICATION")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct RegisterNotificationCommand<'a> {
    pub config: String, // Serialized NotifyConfig
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Unregister notification command
#[respc(name = "UNREGISTER_NOTIFICATION")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct UnregisterNotificationCommand<'a> {
    pub config: String, // Serialized NotifyConfig
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

/// Response for paginated results
#[derive(Debug, Clone)]
pub struct PageResultResponse<T> {
    pub items: Vec<T>,
    pub total_count: Option<u64>,
    pub has_more: bool,
    pub cursor: Option<String>,
}

// ============================================================================
// Additional RespEncode/RespDecode implementations for standard types
// ============================================================================

impl RespEncode for u64 {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(*self as i64).encode()
    }
}

impl RespDecode<'_> for u64 {
    fn decode(data: &[u8]) -> Result<(Self, &[u8])> {
        let (value, remaining) = RespValue::decode(data)?;
        match value {
            RespValue::Integer(i) if i >= 0 => Ok((i as u64, remaining)),
            _ => Err(crate::Error::InvalidRequest("Expected non-negative integer for u64".to_string())),
        }
    }
}

impl RespEncode for bool {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(if *self { 1 } else { 0 }).encode()
    }
}

impl RespDecode<'_> for bool {
    fn decode(data: &[u8]) -> Result<(Self, &[u8])> {
        let (value, remaining) = RespValue::decode(data)?;
        match value {
            RespValue::Integer(0) => Ok((false, remaining)),
            RespValue::Integer(1) => Ok((true, remaining)),
            _ => Err(crate::Error::InvalidRequest("Expected 0 or 1 for bool".to_string())),
        }
    }
}

impl RespEncode for i64 {
    fn encode(&self) -> Vec<u8> {
        RespValue::Integer(*self).encode()
    }
}

impl RespDecode<'_> for i64 {
    fn decode(data: &[u8]) -> Result<(Self, &[u8])> {
        let (value, remaining) = RespValue::decode(data)?;
        match value {
            RespValue::Integer(i) => Ok((i, remaining)),
            _ => Err(crate::Error::InvalidRequest("Expected integer for i64".to_string())),
        }
    }
}