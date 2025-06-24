mod entity;
mod entity_id;
mod entity_schema;
mod field;
mod field_schema;
mod request;
mod snowflake;
mod store;
mod value;

use std::fmt;

pub use entity::Entity;
pub use entity_id::EntityId;
pub use entity_schema::EntitySchema;
pub use field::Field;
pub use field_schema::FieldSchema;
pub use request::{AdjustBehavior, PushCondition, Request};
use serde::{Deserialize, Serialize};
pub use snowflake::Snowflake;
pub use store::{
    resolve_indirection, BadIndirection, BadIndirectionReason, Context, MapStore, PageOpts,
    PageResult, INDIRECTION_DELIMITER,
};
pub use value::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct EntityType(pub String);
impl From<String> for EntityType {
    fn from(s: String) -> Self {
        EntityType(s)
    }
}

impl From<&str> for EntityType {
    fn from(s: &str) -> Self {
        EntityType(s.to_string())
    }
}

impl AsRef<EntityType> for EntityType {
    fn as_ref(&self) -> &EntityType {
        self
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct FieldType(pub String);

impl From<String> for FieldType {
    fn from(s: String) -> Self {
        FieldType(s)
    }
}

impl From<&str> for FieldType {
    fn from(s: &str) -> Self {
        FieldType(s.to_string())
    }
}

impl AsRef<FieldType> for FieldType {
    fn as_ref(&self) -> &FieldType {
        self
    }
}

impl AsRef<str> for FieldType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FieldType {
    pub fn indirect_fields(&self) -> Vec<Self> {
        return self.0.split(INDIRECTION_DELIMITER)
            .map(|s| s.into())
            .collect::<Vec<Self>>();
    }
}

pub type Timestamp = std::time::SystemTime;

pub fn now() -> Timestamp {
    std::time::SystemTime::now()
}

pub fn epoch() -> Timestamp {
    std::time::UNIX_EPOCH
}
