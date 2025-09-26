use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

use crate::{
    data::{EntityId, EntityType, FieldType, Timestamp, Value},
    Complete, EntitySchema, FieldSchema, PageOpts, PageResult, Single,
    qresp::{QrespFrameRef, QrespError, Result as QrespResult},
};
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use smallvec::SmallVec;

pub type IndirectFieldType = SmallVec<[FieldType; 4]>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PushCondition {
    Always,
    Changes,
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
        write_processed: bool,
    },
    Create {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
        created_entity_id: Option<EntityId>,
        timestamp: Option<Timestamp>,
    },
    Delete {
        entity_id: EntityId,
        timestamp: Option<Timestamp>,
    },
    SchemaUpdate {
        schema: EntitySchema<Single, String, String>,
        timestamp: Option<Timestamp>,
    },
    Snapshot {
        snapshot_counter: u64,
        timestamp: Option<Timestamp>,
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
    GetEntitySchema {
        entity_type: EntityType,
        schema: Option<EntitySchema<Single>>,
    },
    GetCompleteEntitySchema {
        entity_type: EntityType,
        schema: Option<EntitySchema<Complete>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        schema: Option<FieldSchema>,
    },
    EntityExists {
        entity_id: EntityId,
        exists: Option<bool>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        exists: Option<bool>,
    },
    FindEntities {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        result: Option<PageResult<EntityId>>,
    },
    FindEntitiesExact {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        result: Option<PageResult<EntityId>>,
    },
    GetEntityTypes {
        page_opts: Option<PageOpts>,
        result: Option<PageResult<EntityType>>,
    },
}

/// Zero-copy version of Request that references data in a QrespFrameRef
#[derive(Debug, Clone, PartialEq)]
pub struct QrespRequestRef<'a> {
    frame: &'a QrespFrameRef<'a>,
    request_type: QrespRequestType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QrespRequestType {
    Read,
    Write,
    Create,
    Delete,
    SchemaUpdate,
    Snapshot,
    GetEntityType,
    ResolveEntityType,
    GetFieldType,
    ResolveFieldType,
    GetEntitySchema,
    GetCompleteEntitySchema,
    GetFieldSchema,
    EntityExists,
    FieldExists,
    FindEntities,
    FindEntitiesExact,
    GetEntityTypes,
}

impl<'a> QrespRequestRef<'a> {
    /// Create a RequestRef from a QrespFrameRef (must be a Map frame representing a request)
    pub fn new(frame: &'a QrespFrameRef<'a>) -> QrespResult<Self> {
        let request_type = Self::extract_request_type(frame)?;
        Ok(QrespRequestRef {
            frame,
            request_type,
        })
    }

    /// Extract the request type from the frame
    fn extract_request_type(frame: &'a QrespFrameRef<'a>) -> QrespResult<QrespRequestType> {
        let map = match frame {
            QrespFrameRef::Map(pairs) => pairs,
            _ => return Err(QrespError::Invalid("Request must be a Map".to_string())),
        };

        // Find the "type" field
        for (key, value) in map {
            if Self::matches_bulk_str(key, "type") {
                let type_str = Self::extract_string_value(value)?;
                return match type_str {
                    "read" => Ok(QrespRequestType::Read),
                    "write" => Ok(QrespRequestType::Write),
                    "create" => Ok(QrespRequestType::Create),
                    "delete" => Ok(QrespRequestType::Delete),
                    "schema_update" => Ok(QrespRequestType::SchemaUpdate),
                    "snapshot" => Ok(QrespRequestType::Snapshot),
                    "get_entity_type" => Ok(QrespRequestType::GetEntityType),
                    "resolve_entity_type" => Ok(QrespRequestType::ResolveEntityType),
                    "get_field_type" => Ok(QrespRequestType::GetFieldType),
                    "resolve_field_type" => Ok(QrespRequestType::ResolveFieldType),
                    "get_entity_schema" => Ok(QrespRequestType::GetEntitySchema),
                    "get_complete_entity_schema" => Ok(QrespRequestType::GetCompleteEntitySchema),
                    "get_field_schema" => Ok(QrespRequestType::GetFieldSchema),
                    "entity_exists" => Ok(QrespRequestType::EntityExists),
                    "field_exists" => Ok(QrespRequestType::FieldExists),
                    "find_entities" => Ok(QrespRequestType::FindEntities),
                    "find_entities_exact" => Ok(QrespRequestType::FindEntitiesExact),
                    "get_entity_types" => Ok(QrespRequestType::GetEntityTypes),
                    _ => Err(QrespError::Invalid(format!("Unknown request type: {}", type_str))),
                };
            }
        }
        Err(QrespError::Invalid("Request missing type field".to_string()))
    }

    /// Helper to check if a frame matches a specific bulk string
    fn matches_bulk_str(frame: &QrespFrameRef, expected: &str) -> bool {
        match frame {
            QrespFrameRef::Bulk(bytes) => *bytes == expected.as_bytes(),
            QrespFrameRef::Simple(text) => *text == expected,
            _ => false,
        }
    }

    /// Extract string value from a QrespFrameRef
    fn extract_string_value(frame: &'a QrespFrameRef<'a>) -> QrespResult<&'a str> {
        match frame {
            QrespFrameRef::Bulk(bytes) => {
                std::str::from_utf8(bytes).map_err(|_| QrespError::Invalid("Invalid UTF-8".to_string()))
            }
            QrespFrameRef::Simple(text) => Ok(text),
            _ => Err(QrespError::Invalid("Expected string frame".to_string())),
        }
    }

    /// Get a field value from the request map
    fn get_field(&self, field_name: &str) -> Option<&QrespFrameRef<'a>> {
        let map = match self.frame {
            QrespFrameRef::Map(pairs) => pairs,
            _ => return None,
        };

        for (key, value) in map {
            if Self::matches_bulk_str(key, field_name) {
                return Some(value);
            }
        }
        None
    }

    /// Extract entity_id from the request if present
    pub fn entity_id(&self) -> QrespResult<Option<EntityId>> {
        match self.get_field("entity_id") {
            Some(frame) => match frame {
                QrespFrameRef::Integer(id) => Ok(Some(EntityId(*id as u64))),
                QrespFrameRef::Null => Ok(None),
                _ => Err(QrespError::Invalid("Invalid entity_id format".to_string())),
            },
            None => Ok(None),
        }
    }

    /// Get the request type
    pub fn request_type(&self) -> &QrespRequestType {
        &self.request_type
    }

    /// Convert this RequestRef to an owned Request
    pub fn to_owned(&self) -> QrespResult<Request> {
        // Convert to owned and decode - this is a simplified implementation
        match self.request_type {
            QrespRequestType::Read => {
                // Extract fields for Read request
                let entity_id = self.entity_id()?.unwrap_or(EntityId(0));
                // For now, return a basic Read request
                // In a full implementation, you'd extract all fields
                Ok(Request::Read {
                    entity_id,
                    field_types: SmallVec::new(),
                    value: None,
                    write_time: None,
                    writer_id: None,
                })
            }
            // Add other request types as needed
            _ => Err(QrespError::Invalid("Request type conversion not implemented yet".to_string())),
        }
    }
}

/// Zero-copy version of Requests that references data in QrespFrameRef
#[derive(Debug, Clone)]
pub struct QrespRequestsRef<'a> {
    frame: &'a QrespFrameRef<'a>,
    originator: Option<EntityId>,
}

/// Iterator over RequestRef items
pub struct QrespRequestsRefIterator<'a> {
    requests: std::slice::Iter<'a, QrespFrameRef<'a>>,
}

impl<'a> QrespRequestsRefIterator<'a> {
    fn new(frame: &'a QrespFrameRef<'a>) -> Self {
        let requests = match frame {
            QrespFrameRef::Array(items) if items.len() >= 2 => {
                // Skip first element (originator) and get requests array
                match &items[1] {
                    QrespFrameRef::Array(requests) => requests.iter(),
                    _ => [].iter(), // Empty slice if not array
                }
            }
            _ => [].iter(), // Empty slice if invalid format
        };
        QrespRequestsRefIterator { requests }
    }
}

impl<'a> Iterator for QrespRequestsRefIterator<'a> {
    type Item = QrespResult<QrespRequestRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.requests.next().map(|frame| QrespRequestRef::new(frame))
    }
}

impl<'a> QrespRequestsRef<'a> {
    /// Create RequestsRef from a QrespFrameRef (must be an Array with originator and requests)
    pub fn new(frame: &'a QrespFrameRef<'a>) -> QrespResult<Self> {
        let array = match frame {
            QrespFrameRef::Array(items) => items,
            _ => return Err(QrespError::Invalid("Requests must be an Array".to_string())),
        };

        if array.len() != 2 {
            return Err(QrespError::Invalid("Requests array must have exactly 2 elements".to_string()));
        }

        // Extract originator from first element
        let originator = match &array[0] {
            QrespFrameRef::Integer(id) => Some(EntityId(*id as u64)),
            QrespFrameRef::Null => None,
            _ => return Err(QrespError::Invalid("Invalid originator format".to_string())),
        };

        Ok(QrespRequestsRef {
            frame,
            originator,
        })
    }

    /// Get the originator entity ID
    pub fn originator(&self) -> Option<EntityId> {
        self.originator
    }

    /// Get an iterator over the requests
    pub fn iter(&self) -> QrespRequestsRefIterator<'a> {
        QrespRequestsRefIterator::new(self.frame)
    }

    /// Get the number of requests
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to owned Requests
    pub fn to_owned(&self) -> QrespResult<Requests> {
        let mut requests = Vec::new();
        for request_ref in self.iter() {
            requests.push(request_ref?.to_owned()?);
        }
        let owned_requests = Requests::new(requests);
        owned_requests.set_originator(self.originator);
        Ok(owned_requests)
    }
}

impl Request {
    pub fn entity_id(&self) -> Option<EntityId> {
        match self {
            Request::Read { entity_id, .. } => Some(*entity_id),
            Request::Write { entity_id, .. } => Some(*entity_id),
            Request::Create {
                created_entity_id, ..
            } => created_entity_id.clone(),
            Request::Delete { entity_id, .. } => Some(*entity_id),
            Request::SchemaUpdate { .. } => None,
            Request::Snapshot { .. } => None,
            Request::GetEntityType { .. } => None,
            Request::ResolveEntityType { .. } => None,
            Request::GetFieldType { .. } => None,
            Request::ResolveFieldType { .. } => None,
            Request::GetEntitySchema { .. } => None,
            Request::GetCompleteEntitySchema { .. } => None,
            Request::GetFieldSchema { .. } => None,
            Request::EntityExists { entity_id, .. } => Some(*entity_id),
            Request::FieldExists { .. } => None,
            Request::FindEntities { .. } => None,
            Request::FindEntitiesExact { .. } => None,
            Request::GetEntityTypes { .. } => None,
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
            Request::GetEntitySchema { .. } => None,
            Request::GetCompleteEntitySchema { .. } => None,
            Request::GetFieldSchema { .. } => None,
            Request::EntityExists { .. } => None,
            Request::FieldExists { .. } => None,
            Request::FindEntities { .. } => None,
            Request::FindEntitiesExact { .. } => None,
            Request::GetEntityTypes { .. } => None,
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
            Request::GetEntitySchema { .. } => None,
            Request::GetCompleteEntitySchema { .. } => None,
            Request::GetFieldSchema { .. } => None,
            Request::EntityExists { .. } => None,
            Request::FieldExists { .. } => None,
            Request::FindEntities { .. } => None,
            Request::FindEntitiesExact { .. } => None,
            Request::GetEntityTypes { .. } => None,
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
            Request::GetEntitySchema { .. } => None,
            Request::GetCompleteEntitySchema { .. } => None,
            Request::GetFieldSchema { .. } => None,
            Request::EntityExists { .. } => None,
            Request::FieldExists { .. } => None,
            Request::FindEntities { .. } => None,
            Request::FindEntitiesExact { .. } => None,
            Request::GetEntityTypes { .. } => None,
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
            Request::GetEntitySchema { .. } => {}
            Request::GetCompleteEntitySchema { .. } => {}
            Request::GetFieldSchema { .. } => {}
            Request::EntityExists { .. } => {}
            Request::FieldExists { .. } => {}
            Request::FindEntities { .. } => {}
            Request::FindEntitiesExact { .. } => {}
            Request::GetEntityTypes { .. } => {}
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
            Request::GetEntitySchema { .. } => {}
            Request::GetCompleteEntitySchema { .. } => {}
            Request::GetFieldSchema { .. } => {}
            Request::EntityExists { .. } => {}
            Request::FieldExists { .. } => {}
            Request::FindEntities { .. } => {}
            Request::FindEntitiesExact { .. } => {}
            Request::GetEntityTypes { .. } => {}
        }
    }

    // === Helper methods for elegant value extraction ===

    /// Extract EntityList value from a Read request as a reference
    pub fn extract_entity_list(&self) -> Option<&Vec<crate::EntityId>> {
        match self {
            Request::Read {
                value: Some(crate::Value::EntityList(list)),
                ..
            } => Some(list),
            _ => None,
        }
    }

    /// Extract EntityReference value from a Read request
    pub fn extract_entity_reference(&self) -> Option<crate::EntityId> {
        match self {
            Request::Read {
                value: Some(crate::Value::EntityReference(entity_ref)),
                ..
            } => *entity_ref,
            _ => None,
        }
    }

    /// Extract Choice value from a Read request
    pub fn extract_choice(&self) -> Option<i64> {
        match self {
            Request::Read {
                value: Some(crate::Value::Choice(choice)),
                ..
            } => Some(*choice),
            _ => None,
        }
    }

    /// Extract Int value from a Read request
    pub fn extract_int(&self) -> Option<i64> {
        match self {
            Request::Read {
                value: Some(crate::Value::Int(int_val)),
                ..
            } => Some(*int_val),
            _ => None,
        }
    }

    /// Extract String value from a Read request as a reference
    pub fn extract_string(&self) -> Option<&str> {
        match self {
            Request::Read {
                value: Some(crate::Value::String(string_val)),
                ..
            } => Some(string_val.as_str()),
            _ => None,
        }
    }

    /// Extract Bool value from a Read request
    pub fn extract_bool(&self) -> Option<bool> {
        match self {
            Request::Read {
                value: Some(crate::Value::Bool(bool_val)),
                ..
            } => Some(*bool_val),
            _ => None,
        }
    }

    /// Extract Blob value from a Read request as a reference
    pub fn extract_blob(&self) -> Option<&[u8]> {
        match self {
            Request::Read {
                value: Some(crate::Value::Blob(blob)),
                ..
            } => Some(blob.as_slice()),
            _ => None,
        }
    }

    /// Extract Float value from a Read request
    pub fn extract_float(&self) -> Option<f64> {
        match self {
            Request::Read {
                value: Some(crate::Value::Float(float_val)),
                ..
            } => Some(*float_val),
            _ => None,
        }
    }

    /// Extract Timestamp value from a Read request
    pub fn extract_timestamp(&self) -> Option<crate::Timestamp> {
        match self {
            Request::Read {
                value: Some(crate::Value::Timestamp(timestamp)),
                ..
            } => Some(*timestamp),
            _ => None,
        }
    }

    /// Extract write_time from a Read request
    pub fn extract_write_time(&self) -> Option<crate::Timestamp> {
        match self {
            Request::Read { write_time, .. } => *write_time,
            _ => None,
        }
    }

    /// Check if this is a successful Read request (has a value)
    pub fn has_value(&self) -> bool {
        matches!(self, Request::Read { value: Some(_), .. })
    }

    /// Check if this is a Read request with no value
    pub fn is_empty(&self) -> bool {
        matches!(self, Request::Read { value: None, .. })
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Read {
                entity_id,
                field_types: field_type,
                value,
                write_time,
                writer_id,
            } => {
                write!(f, "Read Request - Entity ID: {:?}, Field Type: {:?}, Value: {:?}, Write Time: {:?}, Writer ID: {:?}", entity_id, field_type, value, write_time, writer_id)
            }
            Request::Write {
                entity_id,
                field_types: field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time,
                writer_id,
                write_processed,
            } => {
                write!(f, "Write Request - Entity ID: {:?}, Field Type: {:?}, Value: {:?}, Push Condition: {:?}, Adjust Behavior: {}, Write Time: {:?}, Writer ID: {:?}, Write Processed: {}", entity_id, field_type, value, push_condition, adjust_behavior, write_time, writer_id, write_processed)
            }
            Request::Create {
                entity_type,
                parent_id,
                name,
                created_entity_id,
                timestamp,
            } => {
                write!(f, "Create Request - Entity Type: {:?}, Parent ID: {:?}, Name: {:?}, Created Entity ID: {:?}, Timestamp: {:?}", entity_type, parent_id, name, created_entity_id, timestamp)
            }
            Request::Delete {
                entity_id,
                timestamp,
            } => {
                write!(
                    f,
                    "Delete Request - Entity ID: {:?}, Timestamp: {:?}",
                    entity_id, timestamp
                )
            }
            Request::SchemaUpdate { schema, timestamp } => {
                write!(
                    f,
                    "Schema Update Request - Schema: {:?}, Timestamp: {:?}",
                    schema, timestamp
                )
            }
            Request::Snapshot {
                snapshot_counter,
                timestamp,
            } => {
                write!(
                    f,
                    "Snapshot Request - Snapshot Counter: {:?}, Timestamp: {:?}",
                    snapshot_counter, timestamp
                )
            }
            Request::GetEntityType { name, entity_type } => {
                write!(
                    f,
                    "Get Entity Type Request - Name: {:?}, Entity Type: {:?}",
                    name, entity_type
                )
            }
            Request::ResolveEntityType { entity_type, name } => {
                write!(
                    f,
                    "Resolve Entity Type Request - Entity Type: {:?}, Name: {:?}",
                    entity_type, name
                )
            }
            Request::GetFieldType { name, field_type } => {
                write!(
                    f,
                    "Get Field Type Request - Name: {:?}, Field Type: {:?}",
                    name, field_type
                )
            }
            Request::ResolveFieldType { field_type, name } => {
                write!(
                    f,
                    "Resolve Field Type Request - Field Type: {:?}, Name: {:?}",
                    field_type, name
                )
            }
            Request::GetEntitySchema {
                entity_type,
                schema,
            } => {
                write!(
                    f,
                    "Get Entity Schema Request - Entity Type: {:?}, Schema: {:?}",
                    entity_type,
                    schema.is_some()
                )
            }
            Request::GetCompleteEntitySchema {
                entity_type,
                schema,
            } => {
                write!(
                    f,
                    "Get Complete Entity Schema Request - Entity Type: {:?}, Schema: {:?}",
                    entity_type,
                    schema.is_some()
                )
            }
            Request::GetFieldSchema {
                entity_type,
                field_type,
                schema,
            } => {
                write!(
                    f,
                    "Get Field Schema Request - Entity Type: {:?}, Field Type: {:?}, Schema: {:?}",
                    entity_type,
                    field_type,
                    schema.is_some()
                )
            }
            Request::EntityExists { entity_id, exists } => {
                write!(
                    f,
                    "Entity Exists Request - Entity ID: {:?}, Exists: {:?}",
                    entity_id, exists
                )
            }
            Request::FieldExists {
                entity_type,
                field_type,
                exists,
            } => {
                write!(
                    f,
                    "Field Exists Request - Entity Type: {:?}, Field Type: {:?}, Exists: {:?}",
                    entity_type, field_type, exists
                )
            }
            Request::FindEntities {
                entity_type,
                page_opts,
                filter,
                result,
            } => {
                write!(f, "Find Entities Request - Entity Type: {:?}, Page Options: {:?}, Filter: {:?}, Result: {:?}", entity_type, page_opts, filter, result.is_some())
            }
            Request::FindEntitiesExact {
                entity_type,
                page_opts,
                filter,
                result,
            } => {
                write!(f, "Find Entities Exact Request - Entity Type: {:?}, Page Options: {:?}, Filter: {:?}, Result: {:?}", entity_type, page_opts, filter, result.is_some())
            }
            Request::GetEntityTypes { page_opts, result } => {
                write!(
                    f,
                    "Get Entity Types Request - Page Options: {:?}, Result: {:?}",
                    page_opts,
                    result.is_some()
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Requests(Arc<RwLock<Vec<Request>>>, Arc<RwLock<Option<EntityId>>>);

impl Serialize for Requests {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let requests = self.0.read();
        let originator = self.1.read();

        let mut state = serializer.serialize_struct("RequestsData", 2)?;
        state.serialize_field("requests", &*requests)?;
        state.serialize_field("originator", &*originator)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Requests {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RequestsData {
            requests: Vec<Request>,
            originator: Option<EntityId>,
        }

        let data = RequestsData::deserialize(deserializer)?;
        Ok(Requests(
            Arc::new(RwLock::new(data.requests)),
            Arc::new(RwLock::new(data.originator)),
        ))
    }
}

impl Requests {
    pub fn new(requests: Vec<Request>) -> Self {
        Self(Arc::new(RwLock::new(requests)), Arc::new(RwLock::new(None)))
    }

    pub fn push(&self, request: Request) {
        let mut requests = self.0.write();
        requests.push(request);
    }

    pub fn originator(&self) -> Option<EntityId> {
        *self.1.read()
    }

    pub fn set_originator(&self, originator: Option<EntityId>) {
        *self.1.write() = originator;
    }

    pub fn extend(&self, other: Requests) {
        let mut requests = self.0.write();
        requests.extend(other.read().clone());
        // If we don't have an originator but the other does, adopt it
        if self.1.read().is_none() && other.1.read().is_some() {
            *self.1.write() = *other.1.read();
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Vec<Request>> {
        self.0.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, Vec<Request>> {
        self.0.write()
    }

    pub fn len(&self) -> usize {
        let requests = self.0.read();
        requests.len()
    }

    pub fn is_empty(&self) -> bool {
        let requests = self.0.read();
        requests.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<Request> {
        let requests = self.0.read();
        requests.get(index).cloned()
    }

    pub fn clear(&self) {
        let mut requests = self.0.write();
        requests.clear();
    }

    pub fn first(&self) -> Option<Request> {
        let requests = self.0.read();
        requests.first().cloned()
    }

    // === Helper methods for elegant access to request results ===

    /// Extract EntityList from the request at given index as a reference (no cloning)
    /// Returns None if index is out of bounds or the request doesn't contain an EntityList
    pub fn extract_entity_list(&self, index: usize) -> Option<Vec<crate::EntityId>> {
        let requests = self.0.read();
        requests
            .get(index)
            .and_then(|req| req.extract_entity_list().cloned())
    }

    /// Extract EntityReference from the request at given index
    pub fn extract_entity_reference(&self, index: usize) -> Option<crate::EntityId> {
        let requests = self.0.read();
        requests
            .get(index)
            .and_then(|req| req.extract_entity_reference())
    }

    /// Extract Choice from the request at given index
    pub fn extract_choice(&self, index: usize) -> Option<i64> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_choice())
    }

    /// Extract Int from the request at given index
    pub fn extract_int(&self, index: usize) -> Option<i64> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_int())
    }

    /// Extract String from the request at given index (cloned to avoid lifetime issues)
    pub fn extract_string(&self, index: usize) -> Option<String> {
        let requests = self.0.read();
        requests
            .get(index)
            .and_then(|req| req.extract_string().map(|s| s.to_string()))
    }

    /// Extract Bool from the request at given index
    pub fn extract_bool(&self, index: usize) -> Option<bool> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_bool())
    }

    /// Extract Blob from the request at given index (cloned to return owned Vec<u8>)
    pub fn extract_blob(&self, index: usize) -> Option<Vec<u8>> {
        let requests = self.0.read();
        requests
            .get(index)
            .and_then(|req| req.extract_blob().map(|b| b.to_vec()))
    }

    /// Extract Float from the request at given index
    pub fn extract_float(&self, index: usize) -> Option<f64> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_float())
    }

    /// Extract Timestamp from the request at given index
    pub fn extract_timestamp(&self, index: usize) -> Option<crate::Timestamp> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_timestamp())
    }

    /// Extract write_time from the request at given index
    pub fn extract_write_time(&self, index: usize) -> Option<crate::Timestamp> {
        let requests = self.0.read();
        requests.get(index).and_then(|req| req.extract_write_time())
    }
}
