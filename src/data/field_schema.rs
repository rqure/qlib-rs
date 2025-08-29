use crate::{data::{FieldType, Timestamp}, EntityId, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StorageScope {
    Runtime,
    Configuration
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldSchema {
    Blob {
        field_type: FieldType,
        default_value: Vec<u8>,
        rank: i64,
        storage_scope: StorageScope,
    },
    Bool {
        field_type: FieldType,
        default_value: bool,
        rank: i64,
        storage_scope: StorageScope,
    },
    Choice {
        field_type: FieldType,
        default_value: i64,
        rank: i64,
        choices: Vec<String>,
        storage_scope: StorageScope,
    },
    EntityList {
        field_type: FieldType,
        default_value: Vec<EntityId>,
        rank: i64,
        storage_scope: StorageScope,
    },
    EntityReference {
        field_type: FieldType,
        default_value: Option<EntityId>,
        rank: i64,
        storage_scope: StorageScope,
    },
    Float {
        field_type: FieldType,
        default_value: f64,
        rank: i64,
        storage_scope: StorageScope,
    },
    Int {
        field_type: FieldType,
        default_value: i64,
        rank: i64,
        storage_scope: StorageScope,
    },
    String {
        field_type: FieldType,
        default_value: String,
        rank: i64,
        storage_scope: StorageScope,
    },
    Timestamp {
        field_type: FieldType,
        default_value: Timestamp,
        rank: i64,
        storage_scope: StorageScope,
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

    pub fn storage_scope(&self) -> &StorageScope {
        match self {
            FieldSchema::Blob { storage_scope, .. } => storage_scope,
            FieldSchema::Bool { storage_scope, .. } => storage_scope,
            FieldSchema::Choice { storage_scope, .. } => storage_scope,
            FieldSchema::EntityList { storage_scope, .. } => storage_scope,
            FieldSchema::EntityReference { storage_scope, .. } => storage_scope,
            FieldSchema::Float { storage_scope, .. } => storage_scope,
            FieldSchema::Int { storage_scope, .. } => storage_scope,
            FieldSchema::String { storage_scope, .. } => storage_scope,
            FieldSchema::Timestamp { storage_scope, .. } => storage_scope,
        }
    }
}