use crate::{data::{EntityId, FieldType, Timestamp, Value}};

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: FieldType,
    pub value: Value,
    pub write_time: Timestamp,
    pub writer_id: Option<EntityId>,
}
