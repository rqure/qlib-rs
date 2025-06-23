use crate::{data::{Shared, Timestamp}, EntityId};
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    BinaryFile(Vec<u8>),
    Bool(bool),
    Choice(i64),
    EntityList(Vec<EntityId>),
    EntityReference(Option<EntityId>),
    Float(f64),
    Int(i64),
    String(String),
    Timestamp(Timestamp),
}

impl Into<Shared<Value>> for Value {
    fn into(self) -> Shared<Value> {
        Shared::new(self)
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

    pub fn is_binary_file(&self) -> bool {
        matches!(self, Value::BinaryFile(_))
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

    pub fn as_string(&self) -> Option<&String> {
        if let Value::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_binary_file(&self) -> Option<&Vec<u8>> {
        if let Value::BinaryFile(b) = self {
            Some(b)
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
        Value::String(s)
    }

    pub fn from_binary_file(b: Vec<u8>) -> Self {
        Value::BinaryFile(b)
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
}