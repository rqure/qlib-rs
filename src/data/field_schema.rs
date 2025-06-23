use crate::{data::{EntityType, FieldType}, EntityId, Value};
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub entity_type: EntityType,
    pub field_type: FieldType,
    pub default_value: Value,
    pub rank: i64,
    pub read_permission: Option<EntityId>,
    pub write_permission: Option<EntityId>,
    pub choices: Option<Vec<String>>,
}

