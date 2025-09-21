use crate::{data::{EntityId, EntityType, FieldType, Timestamp, Value}, EntitySchema, Single};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

pub type IndirectFieldType = SmallVec<[FieldType; 4]>;

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
        field_types: IndirectFieldType,
        value: Option<Value>,
        write_time: Option<Timestamp>,
        writer_id: Option<EntityId>,
    },
    Write {
        entity_id: EntityId,
        field_types: IndirectFieldType,
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
        timestamp: Option<Timestamp>,
        originator: Option<String>,
    },
    Delete {
        entity_id: EntityId,
        timestamp: Option<Timestamp>,
        originator: Option<String>,
    },
    SchemaUpdate {
        schema: EntitySchema<Single, String, String>,
        timestamp: Option<Timestamp>,
        originator: Option<String>,
    },
    Snapshot {
        snapshot_counter: u64,
        timestamp: Option<Timestamp>,
        originator: Option<String>,
    },
    GetEntityType {
        name: String,
        entity_type: Option<EntityType>,
    },
    ResolveEntityType {
        entity_type: EntityType,
        name: Option<String>,
    },
    GetFieldType {
        name: String,
        field_type: Option<FieldType>,
    },
    ResolveFieldType {
        field_type: FieldType,
        name: Option<String>,
    },
}

impl Request {
    pub fn entity_id(&self) -> Option<EntityId> {
        match self {
            Request::Read { entity_id, .. } => Some(*entity_id),
            Request::Write { entity_id, .. } => Some(*entity_id),
            Request::Create { created_entity_id, .. } => created_entity_id.clone(),
            Request::Delete { entity_id, .. } => Some(*entity_id),
            Request::SchemaUpdate { .. } => None,
            Request::Snapshot { .. } => None,
            Request::GetEntityType { .. } => None,
            Request::ResolveEntityType { .. } => None,
            Request::GetFieldType { .. } => None,
            Request::ResolveFieldType { .. } => None,
        }
    }

    pub fn field_type(&self) -> Option<&IndirectFieldType> {
        match self {
            Request::Read { field_types, .. } => Some(field_types),
            Request::Write { field_types, .. } => Some(field_types),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
            Request::Snapshot { .. } => None,
            Request::GetEntityType { .. } => None,
            Request::ResolveEntityType { .. } => None,
            Request::GetFieldType { .. } => None,
            Request::ResolveFieldType { .. } => None,
        }
    }

    pub fn value(&self) -> Option<&Value> {
        match self {
            Request::Read { value, .. } => value.as_ref(),
            Request::Write { value, .. } => value.as_ref(),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
            Request::Snapshot { .. } => None,
            Request::GetEntityType { .. } => None,
            Request::ResolveEntityType { .. } => None,
            Request::GetFieldType { .. } => None,
            Request::ResolveFieldType { .. } => None,
        }
    }

    pub fn write_time(&self) -> Option<Timestamp> {
        match self {
            Request::Read { write_time, .. } => *write_time,
            Request::Write { write_time, .. } => *write_time,
            Request::Create { timestamp, .. } => *timestamp,
            Request::Delete { timestamp, .. } => *timestamp,
            Request::SchemaUpdate { timestamp, .. } => *timestamp,
            Request::Snapshot { timestamp, .. } => *timestamp,
            _ => None,
        }
    }

    pub fn writer_id(&self) -> Option<EntityId> {
        match self {
            Request::Read { writer_id, .. } => writer_id.clone(),
            Request::Write { writer_id, .. } => writer_id.clone(),
            Request::Create { .. } => None,
            Request::Delete { .. } => None,
            Request::SchemaUpdate { .. } => None,
            Request::Snapshot { .. } => None,
            Request::GetEntityType { .. } => None,
            Request::ResolveEntityType { .. } => None,
            Request::GetFieldType { .. } => None,
            Request::ResolveFieldType { .. } => None,
        }
    }

    pub fn originator(&self) -> Option<&String> {
        match self {
            Request::Read { .. } => None,
            Request::Write { originator, .. } => originator.as_ref(),
            Request::Create { originator, .. } => originator.as_ref(),
            Request::Delete { originator, .. } => originator.as_ref(),
            Request::SchemaUpdate { originator, .. } => originator.as_ref(),
            Request::Snapshot { originator, .. } => originator.as_ref(),
            _ => None,
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
            Request::Snapshot { originator: o, .. } => {
                if o.is_none() {
                    *o = Some(originator);
                }
            }
            Request::GetEntityType { .. } => {}
            Request::ResolveEntityType { .. } => {}
            Request::GetFieldType { .. } => {}
            Request::ResolveFieldType { .. } => {}
        }
    }

    pub fn try_set_writer_id(&mut self, writer_id: EntityId) {
        match self {
            Request::Read { .. } => {}
            Request::Write { writer_id: w, .. } => {
                if w.is_none() {
                    *w = Some(writer_id);
                }
            }
            Request::Create { .. } => {}
            Request::Delete { .. } => {}
            Request::SchemaUpdate { .. } => {}
            Request::Snapshot { .. } => {}
            Request::GetEntityType { .. } => {}
            Request::ResolveEntityType { .. } => {}
            Request::GetFieldType { .. } => {}
            Request::ResolveFieldType { .. } => {}
        }
    }

    pub fn try_set_timestamp(&mut self, timestamp: Timestamp) {
        match self {
            Request::Read { write_time, .. } => {
                if write_time.is_none() {
                    *write_time = Some(timestamp);
                }
            }
            Request::Write { write_time, .. } => {
                if write_time.is_none() {
                    *write_time = Some(timestamp);
                }
            }
            Request::Create { timestamp: t, .. } => {
                if t.is_none() {
                    *t = Some(timestamp);
                }
            }
            Request::Delete { timestamp: t, .. } => {
                if t.is_none() {
                    *t = Some(timestamp);
                }
            }
            Request::SchemaUpdate { timestamp: t, .. } => {
                if t.is_none() {
                    *t = Some(timestamp);
                }
            }
            Request::Snapshot { timestamp: t, .. } => {
                if t.is_none() {
                    *t = Some(timestamp);
                }
            }
            Request::GetEntityType { .. } => {}
            Request::ResolveEntityType { .. } => {}
            Request::GetFieldType { .. } => {}
            Request::ResolveFieldType { .. } => {}
        }
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Read { entity_id, field_types: field_type, value, write_time, writer_id } => {
                write!(f, "Read Request - Entity ID: {:?}, Field Type: {:?}, Value: {:?}, Write Time: {:?}, Writer ID: {:?}", entity_id, field_type, value, write_time, writer_id)
            }
            Request::Write { entity_id, field_types: field_type, value, push_condition, adjust_behavior, write_time, writer_id, originator } => {
                write!(f, "Write Request - Entity ID: {:?}, Field Type: {:?}, Value: {:?}, Push Condition: {:?}, Adjust Behavior: {}, Write Time: {:?}, Writer ID: {:?}, Originator: {:?}", entity_id, field_type, value, push_condition, adjust_behavior, write_time, writer_id, originator)
            }
            Request::Create { entity_type, parent_id, name, created_entity_id, timestamp, originator } => {
                write!(f, "Create Request - Entity Type: {:?}, Parent ID: {:?}, Name: {:?}, Created Entity ID: {:?}, Timestamp: {:?}, Originator: {:?}", entity_type, parent_id, name, created_entity_id, timestamp, originator)
            }
            Request::Delete { entity_id, timestamp, originator } => {
                write!(f, "Delete Request - Entity ID: {:?}, Timestamp: {:?}, Originator: {:?}", entity_id, timestamp, originator)
            }
            Request::SchemaUpdate { schema, timestamp, originator } => {
                write!(f, "Schema Update Request - Schema: {:?}, Timestamp: {:?}, Originator: {:?}", schema, timestamp, originator)
            }
            Request::Snapshot { snapshot_counter, timestamp, originator } => {
                write!(f, "Snapshot Request - Snapshot Counter: {:?}, Timestamp: {:?}, Originator: {:?}", snapshot_counter, timestamp, originator)
            }
            Request::GetEntityType { name, entity_type } => {
                write!(f, "Get Entity Type Request - Name: {:?}, Entity Type: {:?}", name, entity_type)
            }
            Request::ResolveEntityType { entity_type, name } => {
                write!(f, "Resolve Entity Type Request - Entity Type: {:?}, Name: {:?}", entity_type, name)
            }
            Request::GetFieldType { name, field_type } => {
                write!(f, "Get Field Type Request - Name: {:?}, Field Type: {:?}", name, field_type)
            }
            Request::ResolveFieldType { field_type, name } => {
                write!(f, "Resolve Field Type Request - Field Type: {:?}, Name: {:?}", field_type, name)
            }
        }
    }
}