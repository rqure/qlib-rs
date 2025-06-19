use std::collections::HashMap;

use crate::data::{EntityType, FieldSchema, FieldType};

#[derive(Debug, Clone, PartialEq)]
pub struct EntitySchema {
    pub entity_type: EntityType,
    pub fields: HashMap<FieldType, FieldSchema>,
}

impl EntitySchema {
    pub fn new(entity_type: EntityType) -> Self {
        EntitySchema {
            entity_type,
            fields: HashMap::new(),
        }
    }
}