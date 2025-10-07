use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use serde::{Deserialize, Serialize};
use qlib_rs_derive::{RespDecode, RespEncode};

use crate::{EntityId, EntityType, FieldType, IndirectFieldType, Value, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash, RespEncode, RespDecode)]
pub enum NotifyConfig {
    EntityId {
        entity_id: EntityId,
        field_type: FieldType,
        trigger_on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<Vec<FieldType>>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    },
    EntityType {
        entity_type: EntityType,
        field_type: FieldType,
        trigger_on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<Vec<FieldType>>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyInfo {
    pub entity_id: EntityId,
    pub field_path: IndirectFieldType,
    pub value: Option<Value>,
    pub timestamp: Option<Timestamp>,
    pub writer_id: Option<EntityId>,
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub current: NotifyInfo,   // Current field value and metadata
    pub previous: NotifyInfo,  // Previous field value and metadata
    pub context: BTreeMap<Vec<FieldType>, NotifyInfo>, // Context fields as NotifyInfo (no Option since we'll include failed reads as well)
    pub config_hash: u64,  // Hash of the NotifyConfig that triggered this notification
}

/// Notification sender type for sending notifications to a specific channel
#[derive(Clone, Debug)]
pub struct NotificationQueue(Rc<RefCell<VecDeque<Notification>>>);

impl NotificationQueue {
    pub fn new() -> Self {
        NotificationQueue(Rc::new(RefCell::new(VecDeque::new())))
    }

    pub fn push(&self, notification: Notification) {
        self.0.borrow_mut().push_back(notification);
    }

    pub fn pop(&self) -> Option<Notification> {
        self.0.borrow_mut().pop_front()
    }
}

/// Calculate a hash for a NotifyConfig for fast lookup
pub fn hash_notify_config(config: &NotifyConfig) -> u64 {
    let mut hasher = DefaultHasher::new();
    config.hash(&mut hasher);
    hasher.finish()
}

// Custom serialization for Notification to handle Vec<FieldType> keys
impl Serialize for Notification {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("Notification", 4)?;
        state.serialize_field("current", &self.current)?;
        state.serialize_field("previous", &self.previous)?;
        // Send as string to avoid JavaScript number precision loss (53-bit vs 64-bit)
        state.serialize_field("config_hash", &self.config_hash.to_string())?;

        // Convert context map with Vec<FieldType> keys to string keys
        let context_map: std::collections::BTreeMap<String, &NotifyInfo> = self
            .context
            .iter()
            .map(|(key, value)| {
                let key_str = key
                    .iter()
                    .map(|ft| ft.0.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                (key_str, value)
            })
            .collect();

        state.serialize_field("context", &context_map)?;
        state.end()
    }
}

// Custom deserialization for Notification
impl<'de> Deserialize<'de> for Notification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct NotificationHelper {
            current: NotifyInfo,
            previous: NotifyInfo,
            context: std::collections::BTreeMap<String, NotifyInfo>,
            config_hash: u64,
        }

        let helper = NotificationHelper::deserialize(deserializer)?;

        // Convert string keys back to Vec<FieldType>
        let context: BTreeMap<Vec<FieldType>, NotifyInfo> = helper
            .context
            .into_iter()
            .map(|(key_str, value)| {
                let field_types: Vec<FieldType> = key_str
                    .split(',')
                    .map(|s| s.parse::<u64>().map(FieldType).unwrap_or(FieldType(0)))
                    .collect();
                (field_types, value)
            })
            .collect();

        Ok(Notification {
            current: helper.current,
            previous: helper.previous,
            context,
            config_hash: helper.config_hash,
        })
    }
}