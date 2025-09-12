use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{EntityId, EntityType, FieldType, Request};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum NotifyConfig {
    EntityId {
        entity_id: EntityId,
        field_type: FieldType,
        trigger_on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<FieldType>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    },
    EntityType {
        entity_type: EntityType,
        field_type: FieldType,
        trigger_on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<FieldType>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub current: Request,   // Request::Read with current field value and metadata
    pub previous: Request,  // Request::Read with previous field value and metadata
    pub context: BTreeMap<FieldType, Request>, // Context fields as Request::Read (no Option since we'll include failed reads as well)
    pub config_hash: u64,  // Hash of the NotifyConfig that triggered this notification
}

/// Notification sender type for sending notifications to a specific channel
pub type NotificationSender = Sender<Notification>;

/// Notification receiver type for receiving notifications from a specific channel
pub type NotificationReceiver = Receiver<Notification>;

/// Create a new notification channel pair
pub fn notification_channel() -> (NotificationSender, NotificationReceiver) {
    tokio::sync::mpsc::channel::<Notification>(8192)
}

/// Calculate a hash for a NotifyConfig for fast lookup
pub fn hash_notify_config(config: &NotifyConfig) -> u64 {
    let mut hasher = DefaultHasher::new();
    config.hash(&mut hasher);
    hasher.finish()
}