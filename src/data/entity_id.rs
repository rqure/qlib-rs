use crate::data::EntityType;
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct EntityId(pub u64);

impl EntityId {
    pub fn new(entity_type: EntityType, id: u32) -> Self {
        EntityId(((entity_type.0 as u64) << 32) | (id as u64))
    }

    pub fn extract_id(&self) -> u32 {
        (self.0 & 0xFFFFFFFF) as u32
    }

    pub fn extract_type(&self) -> EntityType {
        EntityType((self.0 >> 32) as u32)
    }
}