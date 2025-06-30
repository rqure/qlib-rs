use std::collections::{BTreeMap};

use serde::{Deserialize, Serialize};

use crate::{EntityId, FieldType, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotifyConfig {
    EntityId {
        entity_id: EntityId,
        field_type: FieldType,
        on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
    },
    EntityType {
        entity_type: String,
        field_type: FieldType,
        on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyData {
    entity_id: EntityId,
    field_type: FieldType,
    value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification {
    current: NotifyData,
    previous: NotifyData,
    context: BTreeMap<FieldType, NotifyData>,
}