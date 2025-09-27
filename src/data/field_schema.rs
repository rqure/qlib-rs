use crate::{data::{FieldType, Timestamp}, EntityId, StoreTrait, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StorageScope {
    Runtime,
    Configuration
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldSchema<T=FieldType> {
    Blob {
        field_type: T,
        default_value: Vec<u8>,
        rank: i64,
        storage_scope: StorageScope,
    },
    Bool {
        field_type: T,
        default_value: bool,
        rank: i64,
        storage_scope: StorageScope,
    },
    Choice {
        field_type: T,
        default_value: i64,
        rank: i64,
        choices: Vec<String>,
        storage_scope: StorageScope,
    },
    EntityList {
        field_type: T,
        default_value: Vec<EntityId>,
        rank: i64,
        storage_scope: StorageScope,
    },
    EntityReference {
        field_type: T,
        default_value: Option<EntityId>,
        rank: i64,
        storage_scope: StorageScope,
    },
    Float {
        field_type: T,
        default_value: f64,
        rank: i64,
        storage_scope: StorageScope,
    },
    Int {
        field_type: T,
        default_value: i64,
        rank: i64,
        storage_scope: StorageScope,
    },
    String {
        field_type: T,
        default_value: String,
        rank: i64,
        storage_scope: StorageScope,
    },
    Timestamp {
        field_type: T,
        default_value: Timestamp,
        rank: i64,
        storage_scope: StorageScope,
    }
}

impl<T: Clone> FieldSchema<T> {
    pub fn field_type(&self) -> T {
        match self {
            FieldSchema::Blob { field_type, .. } => field_type.clone(),
            FieldSchema::Bool { field_type, .. } => field_type.clone(),
            FieldSchema::Choice { field_type, .. } => field_type.clone(),
            FieldSchema::EntityList { field_type, .. } => field_type.clone(),
            FieldSchema::EntityReference { field_type, .. } => field_type.clone(),
            FieldSchema::Float { field_type, .. } => field_type.clone(),
            FieldSchema::Int { field_type, .. } => field_type.clone(),
            FieldSchema::String { field_type, .. } => field_type.clone(),
            FieldSchema::Timestamp { field_type, .. } => field_type.clone(),
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

impl FieldSchema {
    pub fn from_string_schema(schema: FieldSchema<String>, store: &impl StoreTrait) -> Self {
        match schema {
            FieldSchema::Blob { field_type, default_value, rank, storage_scope } => FieldSchema::Blob {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Bool { field_type, default_value, rank, storage_scope } => FieldSchema::Bool {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope } => FieldSchema::Choice {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                choices,
                storage_scope,
            },
            FieldSchema::EntityList { field_type, default_value, rank, storage_scope } => FieldSchema::EntityList {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::EntityReference { field_type, default_value, rank, storage_scope } => FieldSchema::EntityReference {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Float { field_type, default_value, rank, storage_scope } => FieldSchema::Float {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Int { field_type, default_value, rank, storage_scope } => FieldSchema::Int {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::String { field_type, default_value, rank, storage_scope } => FieldSchema::String {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
            FieldSchema::Timestamp { field_type, default_value, rank, storage_scope } => FieldSchema::Timestamp {
                field_type: store.get_field_type(field_type.as_str()).expect("Field type not found"),
                default_value,
                rank,
                storage_scope,
            },
        }
    }

    pub fn to_string_schema(&self, store: &impl StoreTrait) -> FieldSchema<String> {
        match self {
            FieldSchema::Blob { field_type, default_value, rank, storage_scope } => FieldSchema::Blob {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: default_value.clone(),
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::Bool { field_type, default_value, rank, storage_scope } => FieldSchema::Bool {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: *default_value,
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope } => FieldSchema::Choice {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: *default_value,
                rank: *rank,
                choices: choices.clone(),
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::EntityList { field_type, default_value, rank, storage_scope } => FieldSchema::EntityList {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: default_value.clone(),
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::EntityReference { field_type, default_value, rank, storage_scope } => FieldSchema::EntityReference {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: default_value.clone(),
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::Float { field_type, default_value, rank, storage_scope } => FieldSchema::Float {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: *default_value,
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::Int { field_type, default_value, rank, storage_scope } => FieldSchema::Int {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: *default_value,
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::String { field_type, default_value, rank, storage_scope } => FieldSchema::String {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: default_value.clone(),
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
            FieldSchema::Timestamp { field_type, default_value, rank, storage_scope } => FieldSchema::Timestamp {
                field_type: store.resolve_field_type(*field_type).expect("Field type not found"),
                default_value: *default_value,
                rank: *rank,
                storage_scope: storage_scope.clone(),
            },
        }
    }
}