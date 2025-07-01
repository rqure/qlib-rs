use rhai::{Dynamic, Map, Array, ImmutableString};
use std::sync::{Arc, Mutex};
use crate::{
    Context, Store, StoreProxy, EntityId, EntityType, FieldType, Value, Request,
    Snowflake, PushCondition, AdjustBehavior,
};

/// A wrapper around Store that provides a perform-based interface for Rhai scripts
#[derive(Clone)]
pub struct RhaiStoreWrapper {
    store: Arc<Mutex<Store>>,
    context: Context,
}

impl RhaiStoreWrapper {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            context: Context {},
        }
    }

    pub fn from_snowflake(snowflake: Arc<Snowflake>) -> Self {
        Self::new(Store::new(snowflake))
    }

    /// Perform a batch of read/write operations
    pub fn perform(&mut self, requests: Array) -> std::result::Result<Array, Box<rhai::EvalAltResult>> {
        let mut store = self.store.lock().unwrap();
        let mut req_vec = Vec::new();
        
        // Convert Rhai requests to native requests
        for req_dynamic in requests {
            let request = dynamic_to_request(req_dynamic)?;
            req_vec.push(request);
        }
        
        // Perform the operations
        store.perform(&self.context, &mut req_vec)
            .map_err(|e| format!("Failed to perform operations: {}", e))?;
        
        // Convert results back to Rhai format
        let mut results = Array::new();
        for request in req_vec {
            results.push(request_to_dynamic(request));
        }
        
        Ok(results)
    }

    /// Create a new entity
    pub fn create_entity(&mut self, entity_type: &str, parent_id: &str, name: &str) -> std::result::Result<String, Box<rhai::EvalAltResult>> {
        let mut store = self.store.lock().unwrap();
        let parent = if parent_id.is_empty() { 
            None 
        } else { 
            Some(EntityId::try_from(parent_id).map_err(|e| format!("Invalid parent ID: {}", e))?) 
        };
        
        let entity = store.create_entity(&self.context, &EntityType::from(entity_type), parent, name)
            .map_err(|e| format!("Failed to create entity: {}", e))?;
        
        Ok(entity.entity_id.to_string())
    }

    /// Delete an entity
    pub fn delete_entity(&mut self, entity_id: &str) -> std::result::Result<(), Box<rhai::EvalAltResult>> {
        let mut store = self.store.lock().unwrap();
        let entity_id = EntityId::try_from(entity_id).map_err(|e| format!("Invalid entity ID: {}", e))?;
        
        store.delete_entity(&self.context, &entity_id)
            .map_err(|e| format!("Failed to delete entity: {}", e))?;
        
        Ok(())
    }

    /// Check if an entity exists
    pub fn entity_exists(&self, entity_id: &str) -> bool {
        let store = self.store.lock().unwrap();
        if let Ok(entity_id) = EntityId::try_from(entity_id) {
            store.entity_exists(&self.context, &entity_id)
        } else {
            false
        }
    }
}

/// A wrapper around StoreProxy for async operations
#[derive(Clone)]
pub struct RhaiStoreProxyWrapper {
    proxy: Arc<StoreProxy>,
    context: Context,
}

impl RhaiStoreProxyWrapper {
    pub fn new(proxy: StoreProxy) -> Self {
        Self {
            proxy: Arc::new(proxy),
            context: Context {},
        }
    }

    /// Perform a batch of read/write operations (async operation made blocking)
    pub fn perform(&mut self, requests: Array) -> std::result::Result<Array, Box<rhai::EvalAltResult>> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "No async runtime available")?;
        
        let mut req_vec = Vec::new();
        
        // Convert Rhai requests to native requests
        for req_dynamic in requests {
            let request = dynamic_to_request(req_dynamic)?;
            req_vec.push(request);
        }
        
        // Perform the operations
        rt.block_on(async {
            self.proxy.perform(&self.context, &mut req_vec).await
        }).map_err(|e| format!("Failed to perform operations: {}", e))?;
        
        // Convert results back to Rhai format
        let mut results = Array::new();
        for request in req_vec {
            results.push(request_to_dynamic(request));
        }
        
        Ok(results)
    }

    /// Create a new entity (async operation made blocking)
    pub fn create_entity(&mut self, entity_type: &str, parent_id: &str, name: &str) -> std::result::Result<String, Box<rhai::EvalAltResult>> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "No async runtime available")?;
        
        let parent = if parent_id.is_empty() { 
            None 
        } else { 
            Some(EntityId::try_from(parent_id).map_err(|e| format!("Invalid parent ID: {}", e))?) 
        };
        
        let entity = rt.block_on(async {
            self.proxy.create_entity(&self.context, &EntityType::from(entity_type), parent, name).await
        }).map_err(|e| format!("Failed to create entity: {}", e))?;
        
        Ok(entity.entity_id.to_string())
    }

    /// Check if an entity exists (async operation made blocking)
    pub fn entity_exists(&self, entity_id: &str) -> std::result::Result<bool, Box<rhai::EvalAltResult>> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "No async runtime available")?;
        
        let entity_id = EntityId::try_from(entity_id).map_err(|e| format!("Invalid entity ID: {}", e))?;
        
        let exists = rt.block_on(async {
            self.proxy.entity_exists(&self.context, &entity_id).await
        }).map_err(|e| format!("Failed to check entity existence: {}", e))?;
        
        Ok(exists)
    }

    /// Set schema for an entity type (async operation made blocking)
    pub fn set_entity_schema(&self, entity_type: &str, schema_fields: Map) -> std::result::Result<(), Box<rhai::EvalAltResult>> {
        use crate::{EntitySchema, FieldSchema, FieldType, Single};
        
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "No async runtime available")?;
        
        let mut entity_schema = EntitySchema::<Single>::new(entity_type, None);
        
        for (field_name, field_def) in schema_fields {
            let field_map: Map = field_def.try_cast()
                .ok_or_else(|| "Field definition must be a map")?;
            
            let field_type_str = field_map.get("type")
                .and_then(|v| v.clone().try_cast::<String>())
                .ok_or_else(|| "Field type is required")?;
            
            let field_type = FieldType::from(field_name.to_string());
            
            let rank = field_map.get("rank")
                .and_then(|v| v.clone().try_cast::<i64>())
                .unwrap_or(0);
            
            let field_schema = match field_type_str.as_str() {
                "String" => {
                    let default_value = field_map.get("default")
                        .and_then(|v| v.clone().try_cast::<String>())
                        .unwrap_or_default();
                    FieldSchema::String {
                        field_type: field_type.clone(),
                        default_value,
                        rank,
                        read_permission: None,
                        write_permission: None,
                    }
                },
                "Int" => {
                    let default_value = field_map.get("default")
                        .and_then(|v| v.clone().try_cast::<i64>())
                        .unwrap_or(0);
                    FieldSchema::Int {
                        field_type: field_type.clone(),
                        default_value,
                        rank,
                        read_permission: None,
                        write_permission: None,
                    }
                },
                "Bool" => {
                    let default_value = field_map.get("default")
                        .and_then(|v| v.clone().try_cast::<bool>())
                        .unwrap_or(false);
                    FieldSchema::Bool {
                        field_type: field_type.clone(),
                        default_value,
                        rank,
                        read_permission: None,
                        write_permission: None,
                    }
                },
                "Float" => {
                    let default_value = field_map.get("default")
                        .and_then(|v| v.clone().try_cast::<f64>())
                        .unwrap_or(0.0);
                    FieldSchema::Float {
                        field_type: field_type.clone(),
                        default_value,
                        rank,
                        read_permission: None,
                        write_permission: None,
                    }
                },
                _ => return Err(format!("Unknown field type: {}", field_type_str).into()),
            };
            
            entity_schema.fields.insert(field_type, field_schema);
        }
        
        rt.block_on(async {
            self.proxy.set_entity_schema(&self.context, &entity_schema).await
        }).map_err(|e| format!("Failed to set entity schema: {}", e))?;
        
        Ok(())
    }
}

/// Syntax sugar functions for creating requests

/// Create a read request map
pub fn create_read_request(entity_id: &str, field_type: &str) -> std::result::Result<Map, Box<rhai::EvalAltResult>> {
    let entity_id = EntityId::try_from(entity_id)
        .map_err(|e| format!("Invalid entity ID: {}", e))?;
    
    let mut map = Map::new();
    map.insert("type".into(), Dynamic::from("Read"));
    map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
    map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
    
    Ok(map)
}

/// Create a write request map with Set behavior
pub fn create_write_request(entity_id: &str, field_type: &str, value: Dynamic) -> std::result::Result<Map, Box<rhai::EvalAltResult>> {
    let entity_id = EntityId::try_from(entity_id)
        .map_err(|e| format!("Invalid entity ID: {}", e))?;
    
    let mut map = Map::new();
    map.insert("type".into(), Dynamic::from("Write"));
    map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
    map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
    map.insert("value".into(), value);
    map.insert("adjust_behavior".into(), Dynamic::from("Set"));
    map.insert("push_condition".into(), Dynamic::from("Always"));
    
    Ok(map)
}

/// Create an add request map (using Write with Add behavior)
pub fn create_add_request(entity_id: &str, field_type: &str, value: Dynamic) -> std::result::Result<Map, Box<rhai::EvalAltResult>> {
    let entity_id = EntityId::try_from(entity_id)
        .map_err(|e| format!("Invalid entity ID: {}", e))?;
    
    let mut map = Map::new();
    map.insert("type".into(), Dynamic::from("Write"));
    map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
    map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
    map.insert("value".into(), value);
    map.insert("adjust_behavior".into(), Dynamic::from("Add"));
    map.insert("push_condition".into(), Dynamic::from("Always"));
    
    Ok(map)
}

/// Create a subtract request map (using Write with Subtract behavior)
pub fn create_subtract_request(entity_id: &str, field_type: &str, value: Dynamic) -> std::result::Result<Map, Box<rhai::EvalAltResult>> {
    let entity_id = EntityId::try_from(entity_id)
        .map_err(|e| format!("Invalid entity ID: {}", e))?;
    
    let mut map = Map::new();
    map.insert("type".into(), Dynamic::from("Write"));
    map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
    map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
    map.insert("value".into(), value);
    map.insert("adjust_behavior".into(), Dynamic::from("Subtract"));
    map.insert("push_condition".into(), Dynamic::from("Always"));
    
    Ok(map)
}

/// Convert a Rhai Map to a qlib Request
fn dynamic_to_request(dynamic: Dynamic) -> std::result::Result<Request, Box<rhai::EvalAltResult>> {
    let map = dynamic.try_cast::<Map>()
        .ok_or("Request must be a map")?;
    
    let request_type = map.get("type")
        .ok_or("Request must have a 'type' field")?
        .clone()
        .try_cast::<ImmutableString>()
        .ok_or("Request type must be a string")?;
    
    let entity_id_str = map.get("entity_id")
        .ok_or("Request must have an 'entity_id' field")?
        .clone()
        .try_cast::<ImmutableString>()
        .ok_or("Entity ID must be a string")?;
    
    let entity_id = EntityId::try_from(entity_id_str.as_str())
        .map_err(|e| format!("Invalid entity ID: {}", e))?;
    
    let field_type_str = map.get("field_type")
        .ok_or("Request must have a 'field_type' field")?
        .clone()
        .try_cast::<ImmutableString>()
        .ok_or("Field type must be a string")?;
    
    let field_type = FieldType::from(field_type_str.as_str());
    
    match request_type.as_str() {
        "Read" => Ok(Request::Read {
            entity_id,
            field_type,
            value: None,
            write_time: None,
            writer_id: None,
        }),
        "Write" => {
            let value_dynamic = map.get("value")
                .ok_or("Write request must have a 'value' field")?
                .clone();
            let value = dynamic_to_value(value_dynamic)?;
            
            let adjust_behavior_str = map.get("adjust_behavior")
                .unwrap_or(&Dynamic::from("Set"))
                .clone()
                .try_cast::<ImmutableString>()
                .ok_or("Adjust behavior must be a string")?;
            
            let adjust_behavior = match adjust_behavior_str.as_str() {
                "Set" => AdjustBehavior::Set,
                "Add" => AdjustBehavior::Add,
                "Subtract" => AdjustBehavior::Subtract,
                _ => return Err(format!("Unsupported adjust behavior: {}", adjust_behavior_str).into()),
            };
            
            let push_condition_str = map.get("push_condition")
                .unwrap_or(&Dynamic::from("Always"))
                .clone()
                .try_cast::<ImmutableString>()
                .ok_or("Push condition must be a string")?;
            
            let push_condition = match push_condition_str.as_str() {
                "Always" => PushCondition::Always,
                "Changes" => PushCondition::Changes,
                _ => return Err(format!("Unsupported push condition: {}", push_condition_str).into()),
            };
            
            Ok(Request::Write {
                entity_id,
                field_type,
                value,
                push_condition,
                adjust_behavior,
                write_time: None,
                writer_id: None,
            })
        },
        _ => Err(format!("Unsupported request type: {}", request_type).into()),
    }
}

/// Convert a qlib Request to a Rhai Map
fn request_to_dynamic(request: Request) -> Dynamic {
    let mut map = Map::new();
    
    match request {
        Request::Read { entity_id, field_type, value, .. } => {
            map.insert("type".into(), Dynamic::from("Read"));
            map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
            map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
            if let Some(val) = value {
                map.insert("value".into(), value_to_dynamic(&val));
            }
        },
        Request::Write { entity_id, field_type, value, push_condition, adjust_behavior, .. } => {
            map.insert("type".into(), Dynamic::from("Write"));
            map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
            map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
            if let Some(val) = value {
                map.insert("value".into(), value_to_dynamic(&val));
            }
            
            let adjust_str = match adjust_behavior {
                AdjustBehavior::Set => "Set",
                AdjustBehavior::Add => "Add",
                AdjustBehavior::Subtract => "Subtract",
            };
            map.insert("adjust_behavior".into(), Dynamic::from(adjust_str));
            
            let push_str = match push_condition {
                PushCondition::Always => "Always",
                PushCondition::Changes => "Changes",
            };
            map.insert("push_condition".into(), Dynamic::from(push_str));
        },
    }
    
    Dynamic::from(map)
}

/// Convert a qlib Value to a Rhai Dynamic
fn value_to_dynamic(value: &Value) -> Dynamic {
    match value {
        Value::Bool(b) => Dynamic::from(*b),
        Value::Int(i) => Dynamic::from(*i),
        Value::Float(f) => Dynamic::from(*f),
        Value::String(s) => Dynamic::from(s.clone()),
        Value::Blob(b) => Dynamic::from(b.clone()),
        Value::EntityReference(Some(entity_id)) => Dynamic::from(entity_id.to_string()),
        Value::EntityReference(None) => Dynamic::UNIT,
        Value::EntityList(list) => {
            let array: Array = list.iter()
                .map(|id| Dynamic::from(id.to_string()))
                .collect();
            Dynamic::from(array)
        },
        Value::Choice(choice) => Dynamic::from(*choice),
        Value::Timestamp(ts) => {
            // Convert timestamp to milliseconds since epoch
            let millis = ts.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            Dynamic::from(millis)
        },
    }
}

/// Convert a Rhai Dynamic to a qlib Value (wrapped in Some)
fn dynamic_to_value(dynamic: Dynamic) -> std::result::Result<Option<Value>, Box<rhai::EvalAltResult>> {
    match dynamic.type_name() {
        "bool" => Ok(Some(Value::Bool(dynamic.as_bool().unwrap()))),
        "i64" => Ok(Some(Value::Int(dynamic.as_int().unwrap()))),
        "f64" => Ok(Some(Value::Float(dynamic.as_float().unwrap()))),
        "string" | "ImmutableString" => {
            let s = dynamic.cast::<ImmutableString>();
            Ok(Some(Value::String(s.to_string())))
        },
        "array" => {
            let array = dynamic.cast::<Array>();
            let mut entity_ids = Vec::new();
            for item in array {
                if let Some(entity_str) = item.try_cast::<ImmutableString>() {
                    let entity_id = EntityId::try_from(entity_str.as_str())
                        .map_err(|e| format!("Invalid entity ID in array: {}", e))?;
                    entity_ids.push(entity_id);
                } else {
                    return Err("Array must contain entity ID strings".into());
                }
            }
            Ok(Some(Value::EntityList(entity_ids)))
        },
        "()" => Ok(None),
        _ => Err(format!("Unsupported type: {}", dynamic.type_name()).into()),
    }
}
