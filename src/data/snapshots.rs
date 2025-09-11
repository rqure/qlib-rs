use ahash::AHashMap;

use serde::{Deserialize, Serialize};

use crate::{EntityId, EntitySchema, EntityType, Field, FieldType, Single};

/// Represents a complete snapshot of the store at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub schemas: AHashMap<EntityType, EntitySchema<Single>>,
    pub entities: AHashMap<EntityType, Vec<EntityId>>,
    pub types: Vec<EntityType>,
    pub fields: AHashMap<EntityId, AHashMap<FieldType, Field>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            schemas: AHashMap::new(),
            entities: AHashMap::new(),
            types: Vec::new(),
            fields: AHashMap::new(),
        }
    }
}
