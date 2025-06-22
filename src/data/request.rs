use crate::{data::{EntityId, FieldType, Shared, Timestamp, Value}};

#[derive(Debug, Clone, PartialEq)]
pub enum WriteOption {
    Normal,
    Changes
}

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
        value: Shared<Option<Value>>,
        write_time: Shared<Option<Timestamp>>,
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
