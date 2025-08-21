use crate::{data::{EntityId, FieldType, Timestamp, Value}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PushCondition {
    Always,
    Changes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdjustBehavior {
    Set,
    Add,
    Subtract,
}
impl std::fmt::Display for AdjustBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdjustBehavior::Set => write!(f, "Set"),
            AdjustBehavior::Add => write!(f, "Add"),
            AdjustBehavior::Subtract => write!(f, "Subtract"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Read {
        entity_id: EntityId,
        field_type: FieldType,
        value: Option<Value>,
        write_time: Option<Timestamp>,
        writer_id: Option<EntityId>,
    },
    Write {
        entity_id: EntityId,
        field_type: FieldType,
        value: Option<Value>,
        push_condition: PushCondition,
        adjust_behavior: AdjustBehavior,
        write_time: Option<Timestamp>,
        writer_id: Option<EntityId>,
        originator: Option<String>,
    }
}

impl Request {
    pub fn entity_id(&self) -> &EntityId {
        match self {
            Request::Read { entity_id, .. } => entity_id,
            Request::Write { entity_id, .. } => entity_id,
        }
    }

    pub fn field_type(&self) -> &FieldType {
        match self {
            Request::Read { field_type, .. } => field_type,
            Request::Write { field_type, .. } => field_type,
        }
    }

    pub fn value(&self) -> Option<&Value> {
        match self {
            Request::Read { value, .. } => value.as_ref(),
            Request::Write { value, .. } => value.as_ref(),
        }
    }

    pub fn write_time(&self) -> Option<Timestamp> {
        match self {
            Request::Read { write_time, .. } => *write_time,
            Request::Write { write_time, .. } => *write_time,
        }
    }

    pub fn writer_id(&self) -> Option<&EntityId> {
        match self {
            Request::Read { writer_id, .. } => writer_id.as_ref(),
            Request::Write { writer_id, .. } => writer_id.as_ref(),
        }
    }
}