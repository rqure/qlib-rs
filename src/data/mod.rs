mod async_store_proxy;
mod cache;
mod entity_id;
mod entity_schema;
pub mod et;
mod field;
mod field_schema;
pub mod ft;
mod indirection;
mod interner;
mod json_snapshot;
mod notifications;
mod pagination;
mod request;
mod snapshots;
mod store;
mod store_proxy;
mod store_trait;
mod utils;
mod value;

pub use cache::Cache;
pub use entity_id::EntityId;
pub use entity_schema::{Complete, EntitySchema, Single};
pub use field::Field;
pub use field_schema::{FieldSchema, StorageScope};
pub use indirection::{path, path_to_entity_id, BadIndirectionReason, INDIRECTION_DELIMITER};
pub use json_snapshot::{
    build_json_entity_tree, factory_restore_json_snapshot, json_value_to_value,
    restore_entity_recursive, restore_json_snapshot, restore_json_snapshot_via_proxy,
    take_json_snapshot, value_to_json_value, value_to_json_value_with_paths, JsonEntity,
    JsonEntitySchema, JsonSnapshot,
};
pub use pagination::{PageOpts, PageResult};
pub use request::IndirectFieldType;
pub use request::{AdjustBehavior, PushCondition, Request, Requests, RequestRef, RequestsRef, RequestType, RequestsRefIterator};
use serde::{Deserialize, Serialize};
pub use snapshots::Snapshot;
pub use store::Store;
pub use store_trait::StoreTrait;

pub use async_store_proxy::AsyncStoreProxy;
pub use notifications::{hash_notify_config, Notification, NotificationQueue, NotifyConfig};
pub use store_proxy::{extract_message_id, AuthenticationResult, StoreMessage, StoreProxy};
pub use value::{ArcBlob, ArcString, Value};

pub use utils::{from_base64, to_base64};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct EntityType(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
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
