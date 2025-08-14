use crate::{Context, EntityId, EntityType, Error, FieldType, Request, Result, StoreType, Value};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Wrapper around StoreTrait that provides JavaScript-friendly methods
pub struct StoreWrapper {
    store: Arc<Mutex<StoreType>>,
    context: Context,
}

impl Clone for StoreWrapper {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            context: self.context.clone(),
        }
    }
}

impl StoreWrapper {
    /// Create a new store wrapper
    pub fn new(store: Arc<Mutex<StoreType>>, context: Context) -> Self {
        Self { store, context }
    }

    /// Create a new entity
    pub async fn create_entity(
        &self,
        entity_type: &str,
        parent_id: Option<&str>,
        name: &str,
    ) -> Result<JsonValue> {
        let mut store = self.store.lock().await;
        
        let entity_type = EntityType::from(entity_type);
        let parent_id = parent_id.map(|id| {
            EntityId::try_from(id).map_err(|e| Error::Scripting(format!("Invalid parent ID: {}", e)))
        }).transpose()?;

        let entity = store.create_entity(&self.context, &entity_type, parent_id, name).await?;
        
        Ok(serde_json::to_value(entity)
            .map_err(|e| Error::Scripting(format!("Failed to serialize entity: {}", e)))?)
    }

    /// Delete an entity
    pub async fn delete_entity(&self, entity_id: &str) -> Result<()> {
        let mut store = self.store.lock().await;
        
        let entity_id = EntityId::try_from(entity_id)
            .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))?;

        store.delete_entity(&self.context, &entity_id).await
    }

    /// Check if an entity exists
    pub async fn entity_exists(&self, entity_id: &str) -> Result<bool> {
        let store = self.store.lock().await;
        
        let entity_id = EntityId::try_from(entity_id)
            .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))?;

        Ok(store.entity_exists(&self.context, &entity_id).await)
    }

    /// Find entities by type
    pub async fn find_entities(&self, entity_type: &str) -> Result<JsonValue> {
        let store = self.store.lock().await;
        
        let entity_type = EntityType::from(entity_type);
        let entities = store.find_entities(&self.context, &entity_type).await?;
        
        Ok(serde_json::to_value(entities)
            .map_err(|e| Error::Scripting(format!("Failed to serialize entities: {}", e)))?)
    }

    /// Perform store operations (read/write requests)
    pub async fn perform(&self, requests_json: &JsonValue) -> Result<JsonValue> {
        let mut store = self.store.lock().await;
        
        // Parse the JSON into Request objects
        let mut requests: Vec<Request> = serde_json::from_value(requests_json.clone())
            .map_err(|e| Error::Scripting(format!("Failed to parse requests: {}", e)))?;

        store.perform(&self.context, &mut requests).await?;

        // Serialize the results back to JSON
        Ok(serde_json::to_value(requests)
            .map_err(|e| Error::Scripting(format!("Failed to serialize results: {}", e)))?)
    }

    /// Helper method to create a read request
    pub fn create_read_request(&self, entity_id: &str, field_type: &str) -> Result<JsonValue> {
        let entity_id = EntityId::try_from(entity_id)
            .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))?;
        let field_type = FieldType::from(field_type);

        let request = Request::Read {
            entity_id,
            field_type,
            value: None,
            write_time: None,
            writer_id: None,
        };

        Ok(serde_json::to_value(request)
            .map_err(|e| Error::Scripting(format!("Failed to serialize request: {}", e)))?)
    }

    /// Helper method to create a write request
    pub fn create_write_request(
        &self,
        entity_id: &str,
        field_type: &str,
        value: Option<JsonValue>,
    ) -> Result<JsonValue> {
        let entity_id = EntityId::try_from(entity_id)
            .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))?;
        let field_type = FieldType::from(field_type);
        
        // Convert JSON value to internal Value type
        let value = value.map(|v| self.json_to_value(v)).transpose()?;

        let request = Request::Write {
            entity_id,
            field_type,
            value,
            push_condition: crate::data::PushCondition::Always,
            adjust_behavior: crate::data::AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        };

        Ok(serde_json::to_value(request)
            .map_err(|e| Error::Scripting(format!("Failed to serialize request: {}", e)))?)
    }

    /// Convert JSON value to internal Value type
    fn json_to_value(&self, json_value: JsonValue) -> Result<Value> {
        match json_value {
            JsonValue::Bool(b) => Ok(Value::Bool(b)),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Err(Error::Scripting("Invalid number value".to_string()))
                }
            }
            JsonValue::String(s) => Ok(Value::String(s)),
            JsonValue::Array(arr) => {
                // Try to parse as EntityList first
                if arr.iter().all(|v| v.is_string()) {
                    let entity_ids: Result<Vec<EntityId>> = arr
                        .into_iter()
                        .map(|v| {
                            EntityId::try_from(v.as_str().unwrap())
                                .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))
                        })
                        .collect();
                    Ok(Value::EntityList(entity_ids?))
                } else {
                    // Treat as blob (bytes)
                    let bytes: Result<Vec<u8>> = arr
                        .into_iter()
                        .map(|v| {
                            v.as_u64()
                                .and_then(|n| if n <= 255 { Some(n as u8) } else { None })
                                .ok_or_else(|| Error::Scripting("Array elements must be 0-255 for blob".to_string()))
                        })
                        .collect();
                    Ok(Value::Blob(bytes?))
                }
            }
            JsonValue::Object(obj) => {
                // Check if it's an EntityReference
                if let Some(entity_id_value) = obj.get("entityId") {
                    if let Some(entity_id_str) = entity_id_value.as_str() {
                        let entity_id = EntityId::try_from(entity_id_str)
                            .map_err(|e| Error::Scripting(format!("Invalid entity ID: {}", e)))?;
                        return Ok(Value::EntityReference(Some(entity_id)));
                    }
                }
                
                // Check if it's a Timestamp
                if let Some(timestamp_value) = obj.get("timestamp") {
                    if let Some(timestamp_str) = timestamp_value.as_str() {
                        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
                            .map_err(|e| Error::Scripting(format!("Invalid timestamp: {}", e)))?;
                        return Ok(Value::Timestamp(timestamp.into()));
                    }
                }

                Err(Error::Scripting("Unsupported object type".to_string()))
            }
            JsonValue::Null => Ok(Value::EntityReference(None)),
        }
    }
}

// Implement helper functions for the JavaScript environment
impl StoreWrapper {
    /// Get available entity types
    pub async fn get_entity_types(&self) -> Result<JsonValue> {
        let store = self.store.lock().await;
        let page_result = store.get_entity_types(&self.context, None).await?;
        
        Ok(serde_json::to_value(page_result.items)
            .map_err(|e| Error::Scripting(format!("Failed to serialize entity types: {}", e)))?)
    }

    /// Get entity schema
    pub async fn get_entity_schema(&self, entity_type: &str) -> Result<JsonValue> {
        let store = self.store.lock().await;
        let entity_type = EntityType::from(entity_type);
        let schema = store.get_entity_schema(&self.context, &entity_type).await?;
        
        Ok(serde_json::to_value(schema)
            .map_err(|e| Error::Scripting(format!("Failed to serialize schema: {}", e)))?)
    }

    /// Get complete entity schema with inheritance
    pub async fn get_complete_entity_schema(&self, entity_type: &str) -> Result<JsonValue> {
        let store = self.store.lock().await;
        let entity_type = EntityType::from(entity_type);
        let schema = store.get_complete_entity_schema(&self.context, &entity_type).await?;
        
        Ok(serde_json::to_value(schema)
            .map_err(|e| Error::Scripting(format!("Failed to serialize complete schema: {}", e)))?)
    }
}
