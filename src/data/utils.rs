use base64::{engine::general_purpose, Engine};

use crate::{Result, Value};


/// Create a blob value from a base64-encoded string
pub fn from_base64(base64_str: &str) -> Result<Vec<u8>> {
    match general_purpose::STANDARD.decode(base64_str) {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(crate::Error::BadValueCast(
            Value::String(base64_str.to_string()),
            Value::Blob(vec![])
        )),
    }
}

/// Convert a blob value to a base64-encoded string
pub fn to_base64(blob: Vec<u8>) -> String {
    general_purpose::STANDARD.encode(blob)
}