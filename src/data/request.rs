use crate::{data::{EntityId, FieldType, Shared, Timestamp, Value}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WriteOption {
    Normal,
    Changes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdjustOption {
    Increment,
    Push,
    Pop,
}

#[derive(Debug, Clone)]
pub enum Request {
    Read {
        entity_id: EntityId,
        field_type: FieldType,
        #[allow(dead_code)]
        value: Shared<Option<Value>>,
        #[allow(dead_code)]
        write_time: Shared<Option<Timestamp>>,
        #[allow(dead_code)]
        writer_id: Shared<Option<EntityId>>,
    },
    Write {
        entity_id: EntityId,
        field_type: FieldType,
        value: Option<Value>,
        write_option: WriteOption,
        write_time: Option<Timestamp>,
        writer_id: Option<EntityId>,
    },
}
