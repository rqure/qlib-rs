pub mod et;
mod entity_id;
mod entity_schema;
mod field_schema;
mod field;
pub mod ft;
mod indirection;
mod json_snapshot;
mod notifications;
mod pagination;
mod request;
mod snapshots;
mod snowflake;
mod store_proxy;
mod store;
mod store_trait;
mod value;
mod cache;
mod utils;

use std::{fmt, time::Duration};

pub use entity_id::EntityId;
pub use entity_schema::{EntitySchema, Single, Complete};
pub use field::Field;
pub use field_schema::{FieldSchema, StorageScope};
pub use request::{AdjustBehavior, PushCondition, Request};
use serde::{Deserialize, Serialize};
pub use snowflake::Snowflake;
pub use store::{AsyncStore, Store};
pub use store_trait::StoreTrait;
pub use indirection::{BadIndirectionReason, INDIRECTION_DELIMITER, resolve_indirection, resolve_indirection_async, path_async};
pub use pagination::{PageOpts, PageResult};
pub use snapshots::Snapshot;
pub use json_snapshot::{JsonSnapshot, JsonEntitySchema, JsonEntity, value_to_json_value, json_value_to_value, value_to_json_value_with_paths, build_json_entity_tree, take_json_snapshot, restore_json_snapshot, restore_entity_recursive, factory_restore_json_snapshot, restore_json_snapshot_via_proxy};
pub use cache::Cache;

pub use store_proxy::{StoreProxy, StoreMessage, extract_message_id, AuthenticationResult};
pub use value::Value;
pub use notifications::{NotifyConfig, Notification, NotificationSender, NotificationReceiver, notification_channel, hash_notify_config};

pub use utils::{from_base64, to_base64};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct EntityType(pub String);

impl AsRef<str> for EntityType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

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

pub fn nanos_to_timestamp(nanos: u64) -> Timestamp {
    epoch() + Duration::from_nanos(nanos)
}

pub fn secs_to_timestamp(secs: u64) -> Timestamp {
    epoch() + Duration::from_secs(secs)
}

pub fn millis_to_timestamp(millis: u64) -> Timestamp {
    epoch() + Duration::from_millis(millis)
}

pub fn micros_to_timestamp(micros: u64) -> Timestamp {
    epoch() + Duration::from_micros(micros)
}