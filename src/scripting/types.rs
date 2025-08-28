use serde::{Deserialize, Serialize};
use crate::{EntityId, FieldType, Value, Request, AdjustBehavior, PushCondition, millis_to_timestamp, epoch};

/// JSON-serializable version of EntityId for WASM interop
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntityId {
    pub entity_type: String,
    pub id: String,
}

impl From<&EntityId> for JsonEntityId {
    fn from(entity_id: &EntityId) -> Self {
        Self {
            entity_type: entity_id.get_type().to_string(),
            id: entity_id.get_id(),
        }
    }
}

impl TryFrom<JsonEntityId> for EntityId {
    type Error = crate::Error;

    fn try_from(json_id: JsonEntityId) -> Result<Self, Self::Error> {
        EntityId::try_from(json_id.id.as_str())
            .map_err(|e| crate::Error::Scripting(format!("Invalid EntityId: {}", e)))
    }
}

/// JSON-serializable version of FieldType for WASM interop
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonFieldType(pub String);

impl From<&FieldType> for JsonFieldType {
    fn from(field_type: &FieldType) -> Self {
        Self(field_type.as_ref().to_string())
    }
}

impl From<JsonFieldType> for FieldType {
    fn from(json_field: JsonFieldType) -> Self {
        FieldType::from(json_field.0)
    }
}

/// JSON-serializable version of Value for WASM interop
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum JsonValue {
    Blob(Vec<u8>),
    Bool(bool),
    Choice(i64),
    EntityList(Vec<JsonEntityId>),
    EntityReference(Option<JsonEntityId>),
    Float(f64),
    Int(i64),
    String(String),
    Timestamp(i64), // Unix timestamp in milliseconds
}

impl From<&Value> for JsonValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Blob(b) => JsonValue::Blob(b.clone()),
            Value::Bool(b) => JsonValue::Bool(*b),
            Value::Choice(c) => JsonValue::Choice(*c),
            Value::EntityList(list) => JsonValue::EntityList(
                list.iter().map(JsonEntityId::from).collect()
            ),
            Value::EntityReference(opt) => JsonValue::EntityReference(
                opt.as_ref().map(JsonEntityId::from)
            ),
            Value::Float(f) => JsonValue::Float(*f),
            Value::Int(i) => JsonValue::Int(*i),
            Value::String(s) => JsonValue::String(s.clone()),
            Value::Timestamp(ts) => {
                let duration = ts.duration_since(epoch()).unwrap_or_default();
                JsonValue::Timestamp(duration.as_millis() as i64)
            }
        }
    }
}

impl TryFrom<JsonValue> for Value {
    type Error = crate::Error;

    fn try_from(json_value: JsonValue) -> Result<Self, Self::Error> {
        match json_value {
            JsonValue::Blob(b) => Ok(Value::Blob(b)),
            JsonValue::Bool(b) => Ok(Value::Bool(b)),
            JsonValue::Choice(c) => Ok(Value::Choice(c)),
            JsonValue::EntityList(list) => {
                let entity_list: Result<Vec<EntityId>, _> = list
                    .into_iter()
                    .map(EntityId::try_from)
                    .collect();
                Ok(Value::EntityList(entity_list?))
            }
            JsonValue::EntityReference(opt) => {
                if let Some(json_id) = opt {
                    Ok(Value::EntityReference(Some(EntityId::try_from(json_id)?)))
                } else {
                    Ok(Value::EntityReference(None))
                }
            }
            JsonValue::Float(f) => Ok(Value::Float(f)),
            JsonValue::Int(i) => Ok(Value::Int(i)),
            JsonValue::String(s) => Ok(Value::String(s)),
            JsonValue::Timestamp(ts) => {
                Ok(Value::Timestamp(millis_to_timestamp(ts as u64)))
            }
        }
    }
}

/// JSON-serializable version of Request for WASM interop
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JsonRequest {
    Read {
        entity_id: JsonEntityId,
        field_type: JsonFieldType,
        value: Option<JsonValue>,
        write_time: Option<i64>,
        writer_id: Option<JsonEntityId>,
    },
    Write {
        entity_id: JsonEntityId,
        field_type: JsonFieldType,
        value: Option<JsonValue>,
        push_condition: String, // "Always" or "Changes"
        adjust_behavior: Option<String>, // "Set", "Add", "Subtract"
        write_time: Option<i64>,
        writer_id: Option<JsonEntityId>,
        originator: Option<String>,
    },
    Delete {
        entity_id: JsonEntityId,
        originator: Option<String>,
    },
}

impl TryFrom<JsonRequest> for Request {
    type Error = crate::Error;

    fn try_from(json_req: JsonRequest) -> Result<Self, Self::Error> {
        match json_req {
            JsonRequest::Read { entity_id, field_type, value, write_time, writer_id } => {
                Ok(Request::Read {
                    entity_id: EntityId::try_from(entity_id)?,
                    field_type: FieldType::from(field_type),
                    value: value.map(Value::try_from).transpose()?,
                    write_time: write_time.map(|ts| millis_to_timestamp(ts as u64)),
                    writer_id: writer_id.map(EntityId::try_from).transpose()?,
                })
            }
            JsonRequest::Write { entity_id, field_type, value, push_condition, adjust_behavior, write_time, writer_id, originator } => {
                let push_cond = match push_condition.as_str() {
                    "Always" => PushCondition::Always,
                    "Changes" => PushCondition::Changes,
                    _ => return Err(crate::Error::Scripting(format!("Invalid push condition: {}", push_condition))),
                };

                let adjust_behavior = if let Some(behavior) = adjust_behavior {
                    Some(match behavior.as_str() {
                        "Set" => AdjustBehavior::Set,
                        "Add" => AdjustBehavior::Add,
                        "Subtract" => AdjustBehavior::Subtract,
                        _ => return Err(crate::Error::Scripting(format!("Invalid adjust behavior: {}", behavior))),
                    })
                } else {
                    None
                };

                Ok(Request::Write {
                    entity_id: EntityId::try_from(entity_id)?,
                    field_type: FieldType::from(field_type),
                    value: value.map(Value::try_from).transpose()?,
                    push_condition: push_cond,
                    adjust_behavior: adjust_behavior.unwrap_or(AdjustBehavior::Set),
                    write_time: write_time.map(|ts| millis_to_timestamp(ts as u64)),
                    writer_id: writer_id.map(EntityId::try_from).transpose()?,
                    originator,
                })
            }
            JsonRequest::Delete { entity_id, originator } => {
                Ok(Request::Delete {
                    entity_id: EntityId::try_from(entity_id)?,
                    originator,
                })
            }
        }
    }
}

impl From<&Request> for JsonRequest {
    fn from(request: &Request) -> Self {
        match request {
            Request::Read { entity_id, field_type, value, write_time, writer_id } => {
                JsonRequest::Read {
                    entity_id: JsonEntityId::from(entity_id),
                    field_type: JsonFieldType::from(field_type),
                    value: value.as_ref().map(JsonValue::from),
                    write_time: write_time.map(|ts| {
                        let duration = ts.duration_since(epoch()).unwrap_or_default();
                        duration.as_millis() as i64
                    }),
                    writer_id: writer_id.as_ref().map(JsonEntityId::from),
                }
            }
            Request::Write { entity_id, field_type, value, push_condition, adjust_behavior, write_time, writer_id, originator } => {
                JsonRequest::Write {
                    entity_id: JsonEntityId::from(entity_id),
                    field_type: JsonFieldType::from(field_type),
                    value: value.as_ref().map(JsonValue::from),
                    push_condition: match push_condition {
                        PushCondition::Always => "Always".to_string(),
                        PushCondition::Changes => "Changes".to_string(),
                    },
                    adjust_behavior: Some(match adjust_behavior {
                        AdjustBehavior::Set => "Set".to_string(),
                        AdjustBehavior::Add => "Add".to_string(),
                        AdjustBehavior::Subtract => "Subtract".to_string(),
                    }),
                    write_time: write_time.map(|ts| {
                        let duration = ts.duration_since(epoch()).unwrap_or_default();
                        duration.as_millis() as i64
                    }),
                    writer_id: writer_id.as_ref().map(JsonEntityId::from),
                    originator: originator.clone(),
                }
            }
            Request::Delete { entity_id, originator } => {
                JsonRequest::Delete {
                    entity_id: JsonEntityId::from(entity_id),
                    originator: originator.clone(),
                }
            }
            _ => {
                // For other request types, return a dummy delete request
                JsonRequest::Delete {
                    entity_id: JsonEntityId { entity_type: "Object".to_string(), id: "0".to_string() },
                    originator: None,
                }
            }
        }
    }
}

/// Context passed to plugins containing store access
#[derive(Debug)]
pub struct PluginContext<T: crate::data::StoreTrait> {
    pub store: std::sync::Arc<tokio::sync::RwLock<T>>,
}

impl<T: crate::data::StoreTrait> Clone for PluginContext<T> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}