use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::data::{EntityType, FieldSchema, FieldType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntitySchema {
    pub entity_type: EntityType,
    pub fields: HashMap<FieldType, FieldSchema>,
}

impl EntitySchema {
    pub fn new(entity_type: impl Into<EntityType>) -> Self {
        EntitySchema {
            entity_type: entity_type.into(),
            fields: HashMap::new(),
        }
    }
}