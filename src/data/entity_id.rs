use crate::data::EntityType;
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
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

impl From<u64> for EntityId {
    fn from(value: u64) -> Self {
        EntityId(value)
    }
}

impl From<EntityId> for u64 {
    fn from(value: EntityId) -> Self {
        value.0
    }
}

impl From<EntityId> for String {
    fn from(value: EntityId) -> Self {
        value.0.to_string()
    }
}

impl From<String> for EntityId {
    fn from(value: String) -> Self {
        EntityId(value.parse().unwrap_or(0))
    }
}

impl From<&str> for EntityId {
    fn from(value: &str) -> Self {
        EntityId(value.parse().unwrap_or(0))
    }
}