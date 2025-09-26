use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::{data::Timestamp, epoch, EntityId, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Wrapper around Arc<String> that implements Serialize/Deserialize
#[derive(Debug, Clone, PartialEq)]
pub struct ArcString(Arc<String>);

impl ArcString {
    pub fn new(s: String) -> Self {
        ArcString(Arc::new(s))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> Arc<String> {
        self.0
    }

    pub fn to_string(&self) -> String {
        (*self.0).clone()
    }

    pub fn eq_ignore_ascii_case(&self, other: &str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl std::fmt::Display for ArcString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for ArcString {
    fn from(s: String) -> Self {
        ArcString::new(s)
    }
}

impl From<Arc<String>> for ArcString {
    fn from(arc: Arc<String>) -> Self {
        ArcString(arc)
    }
}

impl Serialize for ArcString {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArcString {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ArcString(Arc::new(s)))
    }
}

impl Hash for ArcString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_str().hash(state);
    }
}

/// Wrapper around Arc<Vec<u8>> that implements Serialize/Deserialize
#[derive(Debug, Clone, PartialEq)]
pub struct ArcBlob(Arc<Vec<u8>>);

impl ArcBlob {
    pub fn new(data: Vec<u8>) -> Self {
        ArcBlob(Arc::new(data))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn into_inner(self) -> Arc<Vec<u8>> {
        self.0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.0.iter()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        (*self.0).clone()
    }
}

impl From<Vec<u8>> for ArcBlob {
    fn from(data: Vec<u8>) -> Self {
        ArcBlob::new(data)
    }
}

impl From<Arc<Vec<u8>>> for ArcBlob {
    fn from(arc: Arc<Vec<u8>>) -> Self {
        ArcBlob(arc)
    }
}

impl Serialize for ArcBlob {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArcBlob {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = Vec::<u8>::deserialize(deserializer)?;
        Ok(ArcBlob(Arc::new(data)))
    }
}

impl Hash for ArcBlob {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Blob(ArcBlob),
    Bool(bool),
    Choice(i64),
    EntityList(Vec<EntityId>),
    EntityReference(Option<EntityId>),
    Float(f64),
    Int(i64),
    String(ArcString),
    Timestamp(Timestamp),
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Blob(b) => {
                b.hash(state);
            }
            Value::Bool(b) => {
                b.hash(state);
            }
            Value::Choice(c) => {
                c.hash(state);
            }
            Value::EntityList(e) => {
                e.hash(state);
            }
            Value::EntityReference(e) => {
                e.hash(state);
            }
            Value::Float(f) => {
                f.to_bits().hash(state);
            }
            Value::Int(i) => {
                i.hash(state);
            }
            Value::String(s) => {
                s.hash(state);
            }
            Value::Timestamp(t) => {
                t.hash(state);
            }
        }
    }
}

impl Value {
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, Value::Blob(_))
    }

    pub fn is_entity_reference(&self) -> bool {
        matches!(self, Value::EntityReference(_))
    }

    pub fn is_entity_list(&self) -> bool {
        matches!(self, Value::EntityList(_))
    }

    pub fn is_choice(&self) -> bool {
        matches!(self, Value::Choice(_))
    }

    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        if let Value::Float(f) = self {
            Some(*f)
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        if let Value::Blob(b) = self {
            Some(b.as_slice())
        } else {
            None
        }
    }

    pub fn as_entity_reference(&self) -> Option<&Option<EntityId>> {
        if let Value::EntityReference(e) = self {
            Some(e)
        } else {
            None
        }
    }

    pub fn as_entity_list(&self) -> Option<&Vec<EntityId>> {
        if let Value::EntityList(e) = self {
            Some(e)
        } else {
            None
        }
    }

    pub fn as_choice(&self) -> Option<i64> {
        if let Value::Choice(c) = self {
            Some(*c)
        } else {
            None
        }
    }

    pub fn as_timestamp(&self) -> Option<Timestamp> {
        if let Value::Timestamp(t) = self {
            Some(*t)
        } else {
            None
        }
    }

    pub fn from_bool(b: bool) -> Self {
        Value::Bool(b)
    }

    pub fn from_int(i: i64) -> Self {
        Value::Int(i)
    }

    pub fn from_float(f: f64) -> Self {
        Value::Float(f)
    }

    pub fn from_string(s: String) -> Self {
        Value::String(ArcString::new(s))
    }

    pub fn from_blob(b: Vec<u8>) -> Self {
        Value::Blob(ArcBlob::new(b))
    }

    pub fn from_entity_reference(e: Option<EntityId>) -> Self {
        Value::EntityReference(e)
    }

    pub fn from_entity_list(e: Vec<EntityId>) -> Self {
        Value::EntityList(e)
    }

    pub fn from_choice(c: i64) -> Self {
        Value::Choice(c)
    }

    pub fn from_timestamp(t: Timestamp) -> Self {
        Value::Timestamp(t)
    }

    pub fn expect_bool(&self) -> Result<bool> {
        if let Value::Bool(b) = self {
            Ok(*b)
        } else {
            Err(crate::Error::BadValueCast(self.clone(), Value::Bool(false)))
        }
    }

    pub fn expect_float(&self) -> Result<f64> {
        if let Value::Float(f) = self {
            Ok(*f)
        } else {
            Err(crate::Error::BadValueCast(self.clone(), Value::Float(0.0)))
        }
    }

    pub fn expect_entity_reference(&self) -> Result<&Option<EntityId>> {
        if let Value::EntityReference(e) = self {
            Ok(e)
        } else {
            Err(crate::Error::BadValueCast(
                self.clone(),
                Value::EntityReference(None),
            ))
        }
    }

    pub fn expect_entity_list(&self) -> Result<&Vec<EntityId>> {
        if let Value::EntityList(e) = self {
            Ok(e)
        } else {
            Err(crate::Error::BadValueCast(
                self.clone(),
                Value::EntityList(vec![]),
            ))
        }
    }

    pub fn expect_int(&self) -> Result<i64> {
        if let Value::Int(i) = self {
            Ok(*i)
        } else {
            Err(crate::Error::BadValueCast(self.clone(), Value::Int(0)))
        }
    }

    pub fn expect_string(&self) -> Result<&str> {
        if let Value::String(s) = self {
            Ok(s.as_str())
        } else {
            Err(crate::Error::BadValueCast(
                self.clone(),
                Value::String(ArcString::new("".to_string())),
            ))
        }
    }

    pub fn expect_blob(&self) -> Result<&[u8]> {
        if let Value::Blob(b) = self {
            Ok(b.as_slice())
        } else {
            Err(crate::Error::BadValueCast(
                self.clone(),
                Value::Blob(ArcBlob::new(vec![])),
            ))
        }
    }

    pub fn expect_choice(&self) -> Result<i64> {
        if let Value::Choice(c) = self {
            Ok(*c)
        } else {
            Err(crate::Error::BadValueCast(self.clone(), Value::Choice(0)))
        }
    }

    pub fn expect_timestamp(&self) -> Result<Timestamp> {
        if let Value::Timestamp(t) = self {
            Ok(*t)
        } else {
            Err(crate::Error::BadValueCast(
                self.clone(),
                Value::Timestamp(epoch()),
            ))
        }
    }
}

impl Into<String> for Value {
    fn into(self) -> String {
        format!("{:?}", self)
    }
}

impl Eq for Value {}
