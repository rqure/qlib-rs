use crate::{data::{request::WriteOption, EntityId, FieldType, Shared, Timestamp, Value}, Request};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    entity_id: EntityId,
    field_type: FieldType,
    #[serde(skip)]
    value: Shared<Option<Value>>,
    #[serde(skip)]
    write_time: Shared<Option<Timestamp>>,
    #[serde(skip)]
    writer_id: Shared<Option<EntityId>>,
}

impl Field {
    pub fn new(
        entity_id: &EntityId,
        field_type: impl Into<FieldType>,
    ) -> Self {
        Field {
            entity_id: entity_id.clone(),
            field_type: field_type.into(),
            value: Shared::new(None),
            write_time: Shared::new(None),
            writer_id: Shared::new(None),
        }
    }

    pub fn get_entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    pub fn get_field_type(&self) -> &FieldType {
        &self.field_type
    }

    pub fn get_shared_value(&self) -> Shared<Option<Value>> {
        self.value.clone()
    }

    pub fn get_shared_write_time(&self) -> Shared<Option<Timestamp>> {
        self.write_time.clone()
    }

    pub fn get_shared_writer_id(&self) -> Shared<Option<EntityId>> {
        self.writer_id.clone()
    }

    pub async fn get_value(&self) -> Option<Value> {
        let value_lock = self.value.get().await;
        value_lock.clone()
    }

    pub async fn get_write_time(&self) -> Option<Timestamp> {
        let write_time_lock = self.write_time.get().await;
        write_time_lock.clone()
    }

    pub async fn get_writer_id(&self) -> Option<EntityId> {
        let writer_id_lock = self.writer_id.get().await;
        writer_id_lock.clone()
    }

    pub async fn set_value(&mut self, value: Option<Value>) {
        self.value.set(value).await;
    }

    pub async fn set_write_time(&mut self, write_time: Option<Timestamp>) {
        self.write_time.set(write_time).await;
    }

    pub async fn set_writer_id(&mut self, writer_id: Option<EntityId>) {
        self.writer_id.set(writer_id).await;
    }

    pub async fn read_request(&mut self) -> Request {
        Request::Read {
            entity_id: self.entity_id.clone(),
            field_type: self.field_type.clone(),
            value: self.value.clone(),
            write_time: self.write_time.clone(),
            writer_id: self.writer_id.clone(),
        }
    }

    pub async fn write_request(&mut self) -> Request {
        Request::Write {
            entity_id: self.entity_id.clone(),
            field_type: self.field_type.clone(),
            value: self.value.get().await.clone(),
            write_option: WriteOption::Normal,
            write_time: self.write_time.get().await.clone(),
            writer_id: self.writer_id.get().await.clone(),
        }
    }

}
