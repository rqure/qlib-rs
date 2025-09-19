use ahash::AHashMap;
use rustc_hash::FxHashMap;
use sorted_vec::SortedVec;

use serde::{Deserialize, Serialize};

use crate::{EntityId, EntitySchema, EntityType, Field, FieldType, Single};
use crate::data::interner::Interner;

fn serialize_sorted_vec_map<S>(
    map: &AHashMap<EntityType, SortedVec<EntityId>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut ser_map = serializer.serialize_map(Some(map.len()))?;
    for (key, sorted_vec) in map {
        let vec: Vec<EntityId> = sorted_vec.iter().cloned().collect();
        ser_map.serialize_entry(key, &vec)?;
    }
    ser_map.end()
}

fn deserialize_sorted_vec_map<'de, D>(
    deserializer: D,
) -> Result<AHashMap<EntityType, SortedVec<EntityId>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map: AHashMap<EntityType, Vec<EntityId>> = AHashMap::deserialize(deserializer)?;
    let mut result = AHashMap::new();
    for (key, vec) in map {
        result.insert(key, SortedVec::from_unsorted(vec));
    }
    Ok(result)
}

/// Represents a complete snapshot of the store at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub schemas: AHashMap<EntityType, EntitySchema<Single>>,
    #[serde(
        serialize_with = "serialize_sorted_vec_map",
        deserialize_with = "deserialize_sorted_vec_map"
    )]
    pub entities: AHashMap<EntityType, SortedVec<EntityId>>,
    pub entity_type_interner: Interner,
    pub field_type_interner: Interner,
    pub fields: AHashMap<EntityId, AHashMap<FieldType, Field>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            schemas: AHashMap::new(),
            entities: AHashMap::new(),
            entity_type_interner: Interner::new(),
            field_type_interner: Interner::new(),
            fields: AHashMap::new(),
        }
    }
}

impl Snapshot {
    /// Convert from FxHashMap-based store data to AHashMap for serialization
    pub fn from_fx_hashmaps(
        schemas: FxHashMap<EntityType, EntitySchema<Single>>,
        entities: FxHashMap<EntityType, SortedVec<EntityId>>,
        entity_type_interner: Interner,
        field_type_interner: Interner,
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
            entity_type_interner,
            field_type_interner,
            fields: ahash_fields,
        }
    }
    
    /// Convert to FxHashMap-based store data from AHashMap serialization
    pub fn to_fx_hashmaps(self) -> (
        FxHashMap<EntityType, EntitySchema<Single>>,
        FxHashMap<EntityType, SortedVec<EntityId>>,
        Interner,
        Interner,
        FxHashMap<EntityId, FxHashMap<FieldType, Field>>,
    ) {
        let fx_schemas = self.schemas.into_iter().collect();
        let fx_entities = self.entities.into_iter().collect();
        let fx_fields = self.fields
            .into_iter()
            .map(|(id, fields)| (id, fields.into_iter().collect()))
            .collect();
            
        (fx_schemas, fx_entities, self.entity_type_interner, self.field_type_interner, fx_fields)
    }
}
