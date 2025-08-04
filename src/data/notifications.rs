use std::collections::{BTreeMap};

use serde::{Deserialize, Serialize};

use crate::{EntityId, EntityType, FieldType, Value};

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification {
    pub entity_id: EntityId,
    pub field_type: FieldType,
    pub current_value: Value,
    pub previous_value: Value,
    pub context: BTreeMap<FieldType, Option<Value>>, // Option because the indirection may fail
}