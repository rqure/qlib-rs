use ahash::AHashMap;
use rustc_hash::FxHashMap;

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

impl Snapshot {
    /// Convert from FxHashMap-based store data to AHashMap for serialization
    pub fn from_fx_hashmaps(
        schemas: FxHashMap<EntityType, EntitySchema<Single>>,
        entities: FxHashMap<EntityType, Vec<EntityId>>,
        types: Vec<EntityType>,
        fields: FxHashMap<EntityId, FxHashMap<FieldType, Field>>,
    ) -> Self {
        let ahash_schemas = schemas.into_iter().collect();
        let ahash_entities = entities.into_iter().collect();
        let ahash_fields = fields
            .into_iter()
            .map(|(id, fields)| (id, fields.into_iter().collect()))
            .collect();
            
        Self {
            schemas: ahash_schemas,
            entities: ahash_entities,
            types,
            fields: ahash_fields,
        }
    }
    
    /// Convert to FxHashMap-based store data from AHashMap serialization
    pub fn to_fx_hashmaps(self) -> (
        FxHashMap<EntityType, EntitySchema<Single>>,
        FxHashMap<EntityType, Vec<EntityId>>,
        Vec<EntityType>,
        FxHashMap<EntityId, FxHashMap<FieldType, Field>>,
    ) {
        let fx_schemas = self.schemas.into_iter().collect();
        let fx_entities = self.entities.into_iter().collect();
        let fx_fields = self.fields
            .into_iter()
            .map(|(id, fields)| (id, fields.into_iter().collect()))
            .collect();
            
        (fx_schemas, fx_entities, self.types, fx_fields)
    }
}
