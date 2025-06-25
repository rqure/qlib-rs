use std::collections::HashMap;

use crate::{data::FieldType, EntityId, Field};


pub struct Entity {
    pub entity_id: EntityId,
    pub fields: HashMap<FieldType, Field>,
}

impl Entity {
    pub fn new(entity_id: impl Into<EntityId>) -> Self {
        Self {
            entity_id: entity_id.into(),
            fields: HashMap::new(),
        }
    }
}