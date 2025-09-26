use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::{hash_map::DefaultHasher, BTreeMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::{EntityId, EntityType, FieldType, Request};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
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
pub struct Notification {
    pub current: Request,  // Request::Read with current field value and metadata
    pub previous: Request, // Request::Read with previous field value and metadata
    pub context: BTreeMap<Vec<FieldType>, Request>, // Context fields as Request::Read (no Option since we'll include failed reads as well)
    pub config_hash: u64, // Hash of the NotifyConfig that triggered this notification
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
