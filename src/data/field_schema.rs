use crate::{data::{FieldType, Timestamp}, EntityId, Value};
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldSchema {
    Blob {
        field_type: FieldType,
        default_value: Vec<u8>,
        rank: i64,
    },
    Bool {
        field_type: FieldType,
        default_value: bool,
        rank: i64,
    },
    Choice {
        field_type: FieldType,
        default_value: i64,
        rank: i64,
        choices: Vec<String>,
    },
    EntityList {
        field_type: FieldType,
        default_value: Vec<EntityId>,
        rank: i64,
    },
    EntityReference {
        field_type: FieldType,
        default_value: Option<EntityId>,
        rank: i64,
    },
    Float {
        field_type: FieldType,
        default_value: f64,
        rank: i64,
    },
    Int {
        field_type: FieldType,
        default_value: i64,
        rank: i64,
    },
    String {
        field_type: FieldType,
        default_value: String,
        rank: i64,
    },
    Timestamp {
        field_type: FieldType,
        default_value: Timestamp,
        rank: i64,
    }
}

impl FieldSchema {
    pub fn field_type(&self) -> &FieldType {
        match self {
            FieldSchema::Blob { field_type, .. } => field_type,
            FieldSchema::Bool { field_type, .. } => field_type,
            FieldSchema::Choice { field_type, .. } => field_type,
            FieldSchema::EntityList { field_type, .. } => field_type,
            FieldSchema::EntityReference { field_type, .. } => field_type,
            FieldSchema::Float { field_type, .. } => field_type,
            FieldSchema::Int { field_type, .. } => field_type,
            FieldSchema::String { field_type, .. } => field_type,
            FieldSchema::Timestamp { field_type, .. } => field_type,
        }
    }

    pub fn default_value(&self) -> Value {
        match self {
            FieldSchema::Blob { default_value, .. } => Value::Blob(default_value.clone()),
            FieldSchema::Bool { default_value, .. } => Value::Bool(*default_value),
            FieldSchema::Choice { default_value, .. } => Value::Choice(*default_value),
            FieldSchema::EntityList { default_value, .. } => Value::EntityList(default_value.clone()),
            FieldSchema::EntityReference { default_value, .. } => Value::EntityReference(default_value.clone()),
            FieldSchema::Float { default_value, .. } => Value::Float(*default_value),
            FieldSchema::Int { default_value, .. } => Value::Int(*default_value),
            FieldSchema::String { default_value, .. } => Value::String(default_value.clone()),
            FieldSchema::Timestamp { default_value, .. } => Value::Timestamp(*default_value),
        }
    }

    pub fn rank(&self) -> i64 {
        match self {
            FieldSchema::Blob { rank, .. } => *rank,
            FieldSchema::Bool { rank, .. } => *rank,
            FieldSchema::Choice { rank, .. } => *rank,
            FieldSchema::EntityList { rank, .. } => *rank,
            FieldSchema::EntityReference { rank, .. } => *rank,
            FieldSchema::Float { rank, .. } => *rank,
            FieldSchema::Int { rank, .. } => *rank,
            FieldSchema::String { rank, .. } => *rank,
            FieldSchema::Timestamp { rank, .. } => *rank,
        }
    }
}