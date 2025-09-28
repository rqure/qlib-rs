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
//! ## Command Definition Macro
//! 
//! The `resp_command!` macro makes it easy to define new RESP commands with automatic
//! encoding/decoding and execution boilerplate:
//! 
//! ```rust,ignore
//! use qlib_rs::resp_command;
//! 
//! resp_command! {
//!     "CUSTOM_READ" => CustomReadCommand {
//!         entity_id: EntityId,
//!         field_path: Vec<FieldType>,
//!         ?options: String,  // Optional field (prefix with ?)
//!         {
//!             // Execution logic
//!             let (value, timestamp, writer_id) = store.read(self.entity_id, &self.field_path)?;
//!             Ok(RespResponse::Array(vec![
//!                 RespResponse::Bulk(value.encode()),
//!                 RespResponse::Bulk(timestamp.to_string().into_bytes()),
//!                 match writer_id {
//!                     Some(id) => RespResponse::Integer(id.0 as i64),
//!                     None => RespResponse::Null,
//!                 },
//!             ]))
//!         }
//!     }
//! }
//! ```
//! 
//! This generates:
//! - A struct with the specified fields plus a lifetime marker
//! - `RespDecode` implementation with proper command name validation
//! - `RespEncode` implementation 
//! - `RespCommand` implementation with the execute logic
//! 
//! Optional fields (prefixed with `?`) are wrapped in `Option<T>` and encoded as 
//! `Null` when `None`.
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
pub use qlib_rs_derive::{RespEncode, RespDecode, resp_command};

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
    
    /// Execute the command against a StoreTrait implementation
    fn execute(&self, store: &mut dyn crate::data::StoreTrait) -> Result<RespResponse>;
}

/// Response types for RESP commands
#[derive(Debug, Clone)]
pub enum RespResponse {
    /// Simple success response
    Ok,
    /// String response
    String(String),
    /// Integer response  
    Integer(i64),
    /// Binary data response
    Bulk(Vec<u8>),
    /// Array of responses
    Array(Vec<RespResponse>),
    /// Error response
    Error(String),
    /// Null response
    Null,
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

impl RespEncode for RespResponse {
    fn encode(&self) -> Vec<u8> {
        match self {
            RespResponse::Ok => b"+OK\r\n".to_vec(),
            RespResponse::String(s) => {
                let len_str = s.len().to_string();
                let mut result = Vec::with_capacity(s.len() + len_str.len() + 5);
                result.push(b'$');
                result.extend_from_slice(len_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespResponse::Integer(i) => {
                let num_str = i.to_string();
                let mut result = Vec::with_capacity(num_str.len() + 3);
                result.push(b':');
                result.extend_from_slice(num_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespResponse::Bulk(data) => {
                let len_str = data.len().to_string();
                let mut result = Vec::with_capacity(data.len() + len_str.len() + 5);
                result.push(b'$');
                result.extend_from_slice(len_str.as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                result
            },
            RespResponse::Array(elements) => {
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
            RespResponse::Error(e) => {
                let mut result = Vec::with_capacity(e.len() + 3);
                result.push(b'-');
                result.extend_from_slice(e.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            },
            RespResponse::Null => {
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

impl<'a> RespDecode<'a> for Timestamp {
    fn decode(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (value, remaining) = RespValue::decode(input)?;
        match value {
            RespValue::BulkString(data) => {
                let timestamp_str = std::str::from_utf8(data)
                    .map_err(|_| crate::Error::InvalidRequest("Invalid UTF-8 in timestamp".to_string()))?;
                // Try to parse as seconds since epoch first
                if let Ok(secs) = timestamp_str.parse::<i64>() {
                    let timestamp = crate::data::secs_to_timestamp(secs as u64);
                    Ok((timestamp, remaining))
                } else {
                    // TODO: Add proper timestamp string parsing when needed
                    Err(crate::Error::InvalidRequest("Timestamp parsing not implemented for string format".to_string()))
                }
            },
            RespValue::SimpleString(s) => {
                // Try to parse as seconds since epoch
                let secs = s.parse::<i64>()
                    .map_err(|_| crate::Error::InvalidRequest("Invalid timestamp format".to_string()))?;
                let timestamp = crate::data::secs_to_timestamp(secs as u64);
                Ok((timestamp, remaining))
            },
            RespValue::Integer(i) => {
                // Treat as seconds since epoch
                let timestamp = crate::data::secs_to_timestamp(i as u64);
                Ok((timestamp, remaining))
            },
            _ => Err(crate::Error::InvalidRequest("Expected timestamp type".to_string())),
        }
    }
}

impl RespEncode for Timestamp {
    fn encode(&self) -> Vec<u8> {
        RespValue::BulkString(self.to_string().as_bytes()).encode()
    }
}

// ============================================================================
// RESP Commands for all StoreTrait methods
// ============================================================================

/*

Example
```

#[resp_command(name = "READ")]
#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct ReadCommand<'a> {
    pub entity_id: EntityId,
    pub field_path: Vec<FieldType>,
    pub options: Option<String>, // Optional field
    _marker: std::marker::PhantomData<&'a ()>,
}

```

*/