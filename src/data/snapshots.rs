use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{EntityId, EntitySchema, EntityType, Field, FieldType, Single};

/// Represents a complete snapshot of the store at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub schemas: HashMap<EntityType, EntitySchema<Single>>,
    pub entities: HashMap<EntityType, Vec<EntityId>>,
    pub types: Vec<EntityType>,
    pub fields: HashMap<EntityId, HashMap<FieldType, Field>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            schemas: HashMap::new(),
            entities: HashMap::new(),
            types: Vec::new(),
            fields: HashMap::new(),
        }
    }
}
