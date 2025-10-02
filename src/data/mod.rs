pub mod et;
mod entity_id;
pub mod entity_schema;
mod field_schema;
mod field;
pub mod ft;
pub mod interner;
mod indirection;
mod json_snapshot;
mod notifications;
mod pagination;
pub mod resp;
mod snapshots;
mod store_proxy;
mod async_store_proxy;
mod store;
mod store_trait;
mod value;
mod cache;
mod utils;
pub mod pipeline;

pub use entity_id::EntityId;
pub use entity_schema::{EntitySchema, Single, Complete};
pub use field::Field;
pub use field_schema::{FieldSchema, StorageScope};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
pub use store::{Store};
pub use store_trait::{StoreTrait};
pub use indirection::{BadIndirectionReason, INDIRECTION_DELIMITER, path, path_to_entity_id};
pub use pagination::{PageOpts, PageResult};
pub use snapshots::Snapshot;
pub use json_snapshot::{JsonSnapshot, JsonEntitySchema, JsonEntity, value_to_json_value, json_value_to_value, value_to_json_value_with_paths, build_json_entity_tree, take_json_snapshot, restore_json_snapshot, restore_entity_recursive, factory_restore_json_snapshot, restore_json_snapshot_via_proxy};
pub use cache::Cache;

pub use store_proxy::StoreProxy;
pub use async_store_proxy::AsyncStoreProxy;
pub use value::Value;
pub use notifications::{NotifyConfig, Notification, NotificationQueue, NotifyInfo, hash_notify_config};
pub use interner::Interner;
pub use pipeline::{Pipeline, AsyncPipeline, PipelineResults, FromDecodedResponse};

pub use utils::{from_base64, to_base64};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct EntityType(pub u32);

#[derive(Debug, Clone, Copy,PartialEq, Eq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
pub struct FieldType(pub u64);

pub type IndirectFieldType = SmallVec<[FieldType; 4]>;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PushCondition {
    Always,
    Changes
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdjustBehavior {
    Set,
    Add,
    Subtract,
}
impl std::fmt::Display for AdjustBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdjustBehavior::Set => write!(f, "Set"),
            AdjustBehavior::Add => write!(f, "Add"),
            AdjustBehavior::Subtract => write!(f, "Subtract"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WriteInfo {
    FieldUpdate {
        entity_id: EntityId,
        field_type: FieldType,
        value: Option<Value>,
        push_condition: PushCondition,
        adjust_behavior: AdjustBehavior,
        write_time: Option<Timestamp>,
        writer_id: Option<EntityId>,
    },
    CreateEntity {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
        created_entity_id: EntityId,
        timestamp: Timestamp,
    },
    DeleteEntity {
        entity_id: EntityId,
        timestamp: Timestamp,
    },
    SchemaUpdate {
        schema: EntitySchema<Single>,
        timestamp: Timestamp,
    },
    Snapshot {
        snapshot_counter: u64,
        timestamp: Timestamp,
    },
}