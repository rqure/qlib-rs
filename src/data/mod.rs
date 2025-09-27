pub mod et;
mod entity_id;
mod entity_schema;
mod field_schema;
mod field;
pub mod ft;
mod interner;
mod indirection;
mod json_snapshot;
mod notifications;
mod pagination;
mod request;
mod snapshots;
mod store_proxy;
mod async_store_proxy;
mod store;
mod store_trait;
mod value;
mod cache;
mod utils;

pub use entity_id::EntityId;
pub use entity_schema::{EntitySchema, Single, Complete};
pub use field::Field;
pub use field_schema::{FieldSchema, StorageScope};
pub use request::{AdjustBehavior, PushCondition, Request, Requests};
use serde::{Deserialize, Serialize};
pub use store::{Store};
pub use store_trait::{StoreTrait};
pub use indirection::{BadIndirectionReason, INDIRECTION_DELIMITER, path, path_to_entity_id};
pub use pagination::{PageOpts, PageResult};
pub use snapshots::Snapshot;
pub use json_snapshot::{JsonSnapshot, JsonEntitySchema, JsonEntity, value_to_json_value, json_value_to_value, value_to_json_value_with_paths, build_json_entity_tree, take_json_snapshot, restore_json_snapshot, restore_entity_recursive, factory_restore_json_snapshot, restore_json_snapshot_via_proxy};
pub use cache::Cache;
pub use request::IndirectFieldType;

pub use store_proxy::{StoreProxy, StoreMessage, extract_message_id};
pub use async_store_proxy::AsyncStoreProxy;
pub use value::{Value, ArcString, ArcBlob};
pub use notifications::{NotifyConfig, Notification, NotificationQueue, NotifyInfo, hash_notify_config};

pub use utils::{from_base64, to_base64};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct EntityType(pub u32);

#[derive(Debug, Clone, Copy,PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
pub struct FieldType(pub u64);

pub type Timestamp = time::OffsetDateTime;

pub fn now() -> Timestamp {
    time::OffsetDateTime::now_utc()
}

pub fn epoch() -> Timestamp {
    time::OffsetDateTime::UNIX_EPOCH
}

pub fn nanos_to_timestamp(nanos: u64) -> Timestamp {
    epoch() + time::Duration::nanoseconds(nanos as i64)
}

pub fn secs_to_timestamp(secs: u64) -> Timestamp {
    epoch() + time::Duration::seconds(secs as i64)
}

pub fn millis_to_timestamp(millis: u64) -> Timestamp {
    epoch() + time::Duration::milliseconds(millis as i64)
}

pub fn micros_to_timestamp(micros: u64) -> Timestamp {
    epoch() + time::Duration::microseconds(micros as i64)
}