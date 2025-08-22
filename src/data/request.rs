use crate::{data::{EntityId, EntityType, FieldType, Timestamp, Value}, EntitySchema, Single};
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
    },
    Create {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
        created_entity_id: Option<EntityId>,
        originator: Option<String>,
    },
    Delete {
        entity_id: EntityId,
        originator: Option<String>,
    },
    SchemaUpdate {
        schema: EntitySchema<Single>,
        originator: Option<String>,
    },
}

impl Request {
    pub fn entity_id(&self) -> Option<&EntityId> {
        match self {
            Request::Read { entity_id, .. } => Some(entity_id),
            Request::Write { entity_id, .. } => Some(entity_id),
            Request::Create { created_entity_id, .. } => created_entity_id.as_ref(),
            Request::Delete { entity_id, .. } => Some(entity_id),
            Request::SchemaUpdate { .. } => None,
        }
    }

    pub fn field_type(&self) -> Option<&FieldType> {
        match self {
            Request::Read { field_type, .. } => Some(field_type),
            Request::Write { field_type, .. } => Some(field_type),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
        }
    }

    pub fn value(&self) -> Option<&Value> {
        match self {
            Request::Read { value, .. } => value.as_ref(),
            Request::Write { value, .. } => value.as_ref(),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
        }
    }

    pub fn write_time(&self) -> Option<Timestamp> {
        match self {
            Request::Read { write_time, .. } => *write_time,
            Request::Write { write_time, .. } => *write_time,
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
        }
    }

    pub fn writer_id(&self) -> Option<&EntityId> {
        match self {
            Request::Read { writer_id, .. } => writer_id.as_ref(),
            Request::Write { writer_id, .. } => writer_id.as_ref(),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
        }
    }

    pub fn try_set_originator(&mut self, originator: String) {
        match self {
            Request::Read { .. } => {}
            Request::Write { originator: o, .. } => {
                if o.is_none() {
                    *o = Some(originator);
                }
            }
            Request::Create { originator: o, .. } => {
                if o.is_none() {
                    *o = Some(originator);
                }
            }
            Request::Delete { originator: o, .. } => {
                if o.is_none() {
                    *o = Some(originator);
                }
            }
            Request::SchemaUpdate { originator: o, .. } => {
                if o.is_none() {
                    *o = Some(originator);
                }
            }
        }
    }
}