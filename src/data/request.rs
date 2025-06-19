use crate::{data::{now, EntityId, FieldType, Shared, Timestamp, Value}, Field};

#[derive(Debug, Clone, PartialEq)]
pub enum WriteOption {
    Normal,
    Changes
}

#[derive(Debug, Clone)]
pub struct Request {
    pub entity_id: EntityId,
    pub field_type: FieldType,
    pub value: Option<Shared<Value>>,
    pub write_option: WriteOption,
    pub write_time: Option<Timestamp>, // Optional write time, otherwise current time is used
    pub writer_id: Option<EntityId>, // Optional writer ID, otherwise current user ID is used
    pub success: bool,
    pub error_message: Option<String>,
}

impl Request {
    pub fn new(
        entity_id: &EntityId,
        field_type: &FieldType,
        value: Option<Shared<Value>>,
    ) -> Self {
        Request {
            entity_id: entity_id.clone(),
            field_type: field_type.clone(),
            value,
            write_option: WriteOption::Normal,
            write_time: None,
            writer_id: None,
            success: false,
            error_message: None,
        }
    }

    pub fn new2(
        entity_id: &EntityId,
        field_type: &FieldType
    ) -> Self {
            Self::new(entity_id, field_type, None)
    }
}

impl Into<Field> for Request {
    fn into(self) -> Field {
        Field {
            entity_id: self.entity_id,
            field_type: self.field_type,
            value: self.value,
            write_time: self.write_time.unwrap_or_else(|| now()),
            writer_id: self.writer_id,
        }
    }
}

impl From<Field> for Request {
    fn from(field: Field) -> Self {
        Request {
            entity_id: field.entity_id,
            field_type: field.field_type,
            value: field.value,
            write_option: WriteOption::Normal,
            write_time: None,
            writer_id: field.writer_id,
            success: true,
            error_message: None,
        }
    }
}