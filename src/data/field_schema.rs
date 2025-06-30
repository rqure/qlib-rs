use crate::{data::{FieldType}, EntityId, Value};
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldSchema {
    Blob {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    Bool {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    Choice {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
        choices: Vec<String>,
    },
    EntityList {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    EntityReference {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    Float {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    Int {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    String {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
    },
    Timestamp {
        field_type: FieldType,
        default_value: Value,
        rank: i64,
        read_permission: Option<EntityId>,
        write_permission: Option<EntityId>,
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

    pub fn default_value(&self) -> &Value {
        match self {
            FieldSchema::Blob { default_value, .. } => default_value,
            FieldSchema::Bool { default_value, .. } => default_value,
            FieldSchema::Choice { default_value, .. } => default_value,
            FieldSchema::EntityList { default_value, .. } => default_value,
            FieldSchema::EntityReference { default_value, .. } => default_value,
            FieldSchema::Float { default_value, .. } => default_value,
            FieldSchema::Int { default_value, .. } => default_value,
            FieldSchema::String { default_value, .. } => default_value,
            FieldSchema::Timestamp { default_value, .. } => default_value,
        }
    }

    pub fn read_permission(&self) -> &Option<EntityId> {
        match self {
            FieldSchema::Blob { read_permission, .. } => read_permission,
            FieldSchema::Bool { read_permission, .. } => read_permission,
            FieldSchema::Choice { read_permission, .. } => read_permission,
            FieldSchema::EntityList { read_permission, .. } => read_permission,
            FieldSchema::EntityReference { read_permission, .. } => read_permission,
            FieldSchema::Float { read_permission, .. } => read_permission,
            FieldSchema::Int { read_permission, .. } => read_permission,
            FieldSchema::String { read_permission, .. } => read_permission,
            FieldSchema::Timestamp { read_permission, .. } => read_permission,
        }
    }

    pub fn write_permission(&self) -> &Option<EntityId> {
        match self {
            FieldSchema::Blob { write_permission, .. } => write_permission,
            FieldSchema::Bool { write_permission, .. } => write_permission,
            FieldSchema::Choice { write_permission, .. } => write_permission,
            FieldSchema::EntityList { write_permission, .. } => write_permission,
            FieldSchema::EntityReference { write_permission, .. } => write_permission,
            FieldSchema::Float { write_permission, .. } => write_permission,
            FieldSchema::Int { write_permission, .. } => write_permission,
            FieldSchema::String { write_permission, .. } => write_permission,
            FieldSchema::Timestamp { write_permission, .. } => write_permission,
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