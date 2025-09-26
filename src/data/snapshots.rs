use rustc_hash::FxHashMap;
use sorted_vec::SortedVec;

use serde::{Deserialize, Serialize};

use crate::data::interner::Interner;
use crate::{EntityId, EntitySchema, EntityType, Field, FieldType, Single};

/// Represents a complete snapshot of the store at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub schemas: FxHashMap<EntityType, EntitySchema<Single>>,
    pub entities: FxHashMap<EntityType, SortedVec<EntityId>>,
    pub entity_type_interner: Interner,
    pub field_type_interner: Interner,
    pub fields: FxHashMap<EntityId, FxHashMap<FieldType, Field>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            schemas: FxHashMap::default(),
            entities: FxHashMap::default(),
            entity_type_interner: Interner::new(),
            field_type_interner: Interner::new(),
            fields: FxHashMap::default(),
        }
    }
}

impl Snapshot {
    /// Create a new Snapshot with the provided data
    pub fn new(
        schemas: FxHashMap<EntityType, EntitySchema<Single>>,
        entities: FxHashMap<EntityType, SortedVec<EntityId>>,
        entity_type_interner: Interner,
        field_type_interner: Interner,
        fields: FxHashMap<EntityId, FxHashMap<FieldType, Field>>,
    ) -> Self {
        Self {
            schemas,
            entities,
            entity_type_interner,
            field_type_interner,
            fields,
        }
    }
}
