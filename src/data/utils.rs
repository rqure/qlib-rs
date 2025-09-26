use base64::{engine::general_purpose, Engine as _};

use crate::data::value::{ArcBlob, ArcString, Value};

/// Create a blob value from a base64-encoded string
pub fn from_base64(base64_str: &str) -> crate::Result<Vec<u8>> {
    match general_purpose::STANDARD.decode(base64_str) {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(crate::Error::BadValueCast(
            Value::String(ArcString::new(base64_str.to_string())),
            Value::Blob(ArcBlob::new(vec![])),
        )),
    }
}

/// Convert a blob value to a base64-encoded string
pub fn to_base64(blob: Vec<u8>) -> String {
    general_purpose::STANDARD.encode(blob)
}
