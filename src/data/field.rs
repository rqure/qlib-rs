use crate::data::{EntityId, FieldType, Shared, Timestamp, Value};

#[derive(Debug, Clone)]
pub struct Field {
    pub entity_id: EntityId,
    pub field_type: FieldType,
    pub value: Option<Shared<Value>>,
    pub write_time: Timestamp,
    pub writer_id: Option<EntityId>,
}
