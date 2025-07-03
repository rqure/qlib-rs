mod entity;
mod entity_id;
mod entity_schema;
mod field;
mod field_schema;
mod notify_token;
mod request;
mod scripting;
mod snowflake;
mod store;
mod store_proxy;
mod value;
mod constants;
mod notifications;

use std::fmt;

pub use entity::Entity;
pub use entity_id::EntityId;
pub use entity_schema::{EntitySchema, Single, Complete};
pub use field::Field;
pub use field_schema::FieldSchema;
pub use notify_token::NotifyToken;
pub use request::{AdjustBehavior, PushCondition, Request};
use serde::{Deserialize, Serialize};
pub use scripting::{ScriptEngine, ScriptContext};
pub use snowflake::Snowflake;
pub use store::{
    resolve_indirection, BadIndirection, BadIndirectionReason, Context, Store, PageOpts,
    PageResult, NotificationCallback, Snapshot,
};
pub use store_proxy::{StoreProxy, StoreMessage};
pub use value::Value;
pub use constants::{INDIRECTION_DELIMITER};
pub use notifications::{NotifyConfig, Notification};

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

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
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
