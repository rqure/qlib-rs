use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::{data::FieldType, EntityId, Field};

#[derive(Debug, Clone, Serialize, Deserialize)]
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