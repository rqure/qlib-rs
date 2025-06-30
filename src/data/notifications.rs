use std::collections::{BTreeMap};

use serde::{Deserialize, Serialize};

use crate::{EntityId, FieldType, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotifyConfig {
    EntityId {
        entity_id: EntityId,
        field_type: FieldType,
        on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<FieldType>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    },
    EntityType {
        entity_type: String,
        field_type: FieldType,
        on_change: bool, // Notification will always trigger on write, but can be configured to trigger on change instead
        context: Vec<FieldType>, // Context fields to include in the notification (these fields are relative to the entity with indirection support)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification {
    entity_id: EntityId,
    field_type: FieldType,
    current_value: Value,
    previous_value: Value,
    context: BTreeMap<FieldType, Option<Value>>, // Option because the indirection may fail
}