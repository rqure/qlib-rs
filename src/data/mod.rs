
mod entity_id;
mod entity;
mod field;
mod entity_schema;
mod field_schema;
mod request;
mod snowflake;
mod value;
mod store;

use std::sync::Arc;

pub use entity_id::EntityId;
pub use entity::Entity;
pub use entity_schema::EntitySchema;
pub use field::Field;
pub use field_schema::FieldSchema;
pub use request::Request;
pub use snowflake::Snowflake;
use tokio::sync::RwLock;
pub use value::Value;
pub use store::MapStore;

pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type EntityType = String;
pub type FieldType = String;

pub fn now() -> Timestamp {
    chrono::Utc::now()
}

#[derive(Debug, Clone)]
pub struct Shared<T>(Arc<RwLock<T>>);

impl<T> Shared<T> 
where
    T: PartialEq + Send + Sync,
{
    pub async fn async_eq(&self, other: &Self) -> bool {
        // Compare the inner values for equality
        let self_lock = self.0.read().await;
        let other_lock = other.0.read().await;
        *self_lock == *other_lock
    }
}

impl<T> Shared<T> {
    pub fn new(value: T) -> Self {
        Shared(Arc::new(RwLock::new(value)))
    }

    pub fn clone(&self) -> Self {
        Shared(self.0.clone())
    }
}