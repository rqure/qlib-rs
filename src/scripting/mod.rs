use rhai::{Array, Dynamic, Engine, Map, Scope};
use std::collections::HashMap;

use crate::{AdjustBehavior, EntityId, FieldType, PushCondition, Request, Result, Store, Value};

/// Executes a Rhai script with access to the store
pub fn execute(store: &mut Store, script: &str) -> Result<Dynamic> {
    let mut engine = Engine::new();
    let mut scope = Scope::new();

    // Register helper functions for creating requests
    engine.register_fn("read", |entity_id: &str, field_type: &str| {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("read"));
        map.insert("entity_id".into(), Dynamic::from(entity_id));
        map.insert("field_type".into(), Dynamic::from(field_type));
        map
    });

    engine.register_fn("write", |entity_id: &str, field_type: &str, value: Dynamic| {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("write"));
        map.insert("entity_id".into(), Dynamic::from(entity_id));
        map.insert("field_type".into(), Dynamic::from(field_type));
        map.insert("value".into(), value);
        map.insert("push_condition".into(), Dynamic::from("always"));
        map.insert("adjust_behavior".into(), Dynamic::from("set"));
        map
    });

    engine.register_fn("write_if_changes", |entity_id: &str, field_type: &str, value: Dynamic| {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("write"));
        map.insert("entity_id".into(), Dynamic::from(entity_id));
        map.insert("field_type".into(), Dynamic::from(field_type));
        map.insert("value".into(), value);
        map.insert("push_condition".into(), Dynamic::from("changes"));
        map.insert("adjust_behavior".into(), Dynamic::from("set"));
        map
    });

    engine.register_fn("add", |entity_id: &str, field_type: &str, value: Dynamic| {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("write"));
        map.insert("entity_id".into(), Dynamic::from(entity_id));
        map.insert("field_type".into(), Dynamic::from(field_type));
        map.insert("value".into(), value);
        map.insert("push_condition".into(), Dynamic::from("always"));
        map.insert("adjust_behavior".into(), Dynamic::from("add"));
        map
    });

    engine.register_fn("subtract", |entity_id: &str, field_type: &str, value: Dynamic| {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("write"));
        map.insert("entity_id".into(), Dynamic::from(entity_id));
        map.insert("field_type".into(), Dynamic::from(field_type));
        map.insert("value".into(), value);
        map.insert("push_condition".into(), Dynamic::from("always"));
        map.insert("adjust_behavior".into(), Dynamic::from("subtract"));
        map
    });

    // Register perform function that processes an array of request maps
    engine.register_fn("perform", move |requests: Array| {
        let requests = requests
            .into_iter()
            .filter_map(|req| {
                req.try_cast::<Map>()
            })
            .filter_map(|map| {
                if let Some(action) = map.get("action") {
                    if action.is::<String>() {
                        return Some(map);
                    }
                }
                None
            })
            .collect::<Vec<Map>>();
        
        if !requests.is_empty() {
            // Convert Rhai maps to Request objects
            let mut store_requests = Vec::new();
            
            for req_map in &requests {
                let action = req_map.get("action").unwrap().clone().cast::<String>();
                let entity_id_str = req_map.get("entity_id").unwrap().clone().cast::<String>();
                let field_type_str = req_map.get("field_type").unwrap().clone().cast::<String>();
                
                let entity_id = match EntityId::try_from(entity_id_str.as_str()) {
                    Ok(id) => id,
                    Err(_) => continue, // Skip invalid entity IDs
                };
                
                let field_type = FieldType::from(field_type_str);
                
                match action.as_str() {
                    "read" => {
                        store_requests.push(Request::Read {
                            entity_id,
                            field_type,
                            value: None,
                            write_time: None,
                            writer_id: None,
                        });
                    },
                    "write" => {
                        let value = convert_rhai_to_value(req_map.get("value"));
                        let push_condition = req_map.get("push_condition")
                            .and_then(|pc| pc.clone().try_cast::<String>())
                            .map(|s| match s.as_str() {
                                "changes" => PushCondition::Changes,
                                _ => PushCondition::Always,
                            })
                            .unwrap_or(PushCondition::Always);
                            
                        let adjust_behavior = req_map.get("adjust_behavior")
                            .and_then(|ab| ab.clone().try_cast::<String>())
                            .map(|s| match s.as_str() {
                                "add" => AdjustBehavior::Add,
                                "subtract" => AdjustBehavior::Subtract,
                                _ => AdjustBehavior::Set,
                            })
                            .unwrap_or(AdjustBehavior::Set);
                            
                        store_requests.push(Request::Write {
                            entity_id,
                            field_type,
                            value,
                            push_condition,
                            adjust_behavior,
                            write_time: None,
                            writer_id: None,
                        });
                    },
                    _ => continue, // Skip unknown actions
                }
            }
            
            // Execute the requests
            if let Err(e) = store.perform(&crate::Context {  }, &mut store_requests) {
                // Handle error - in this case we'll just print it and continue
                eprintln!("Error performing store requests: {}", e);
                return Dynamic::UNIT;
            }
            
            // Update the Rhai maps with the results
            for (i, req) in store_requests.iter().enumerate() {
                if i >= requests.len() {
                    break;
                }
                
                let req_map = &requests[i];
                
                match req {
                    Request::Read { value, write_time, writer_id, .. } => {
                        if let Some(v) = value {
                            let rhai_value = convert_value_to_rhai(v);
                            let mut updated_map = req_map.clone();
                            updated_map.insert("value".into(), rhai_value);
                            
                            if let Some(wt) = write_time {
                                updated_map.insert("write_time".into(), 
                                    Dynamic::from(wt.duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs_f64()));
                            }
                            
                            if let Some(wid) = writer_id {
                                updated_map.insert("writer_id".into(), 
                                    Dynamic::from(wid.to_string()));
                            }
                            
                            // Copy updated map back to the original array position
                            if let Some(array_ref) = scope.get_value::<&mut Array>("_last_requests") {
                                if i < array_ref.len() {
                                    array_ref[i] = Dynamic::from(updated_map);
                                }
                            }
                        }
                    },
                    Request::Write { value, write_time, writer_id, .. } => {
                        // Update write result if needed
                        let mut updated_map = req_map.clone();
                        
                        if let Some(v) = value {
                            updated_map.insert("value".into(), convert_value_to_rhai(v));
                        }
                        
                        if let Some(wt) = write_time {
                            updated_map.insert("write_time".into(), 
                                Dynamic::from(wt.duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs_f64()));
                        }
                        
                        if let Some(wid) = writer_id {
                            updated_map.insert("writer_id".into(), 
                                Dynamic::from(wid.to_string()));
                        }
                        
                        // Copy updated map back to the original array position
                        if let Some(array_ref) = scope.get_value::<&mut Array>("_last_requests") {
                            if i < array_ref.len() {
                                array_ref[i] = Dynamic::from(updated_map);
                            }
                        }
                    }
                }
            }
            
            // Store the requests array in scope so we can access it for updates
            scope.push("_last_requests", requests);
        }
        
        Dynamic::UNIT
    });

    // Execute the script
    match engine.eval_with_scope::<Dynamic>(&mut scope, script) {
        Ok(result) => Ok(result),
        Err(e) => Err(format!("Script execution error: {}", e).into()),
    }
}

// Helper function to convert Rhai Dynamic to Option<Value>
fn convert_rhai_to_value(dynamic_opt: Option<&Dynamic>) -> Option<Value> {
    match dynamic_opt {
        None => None,
        Some(dynamic) => {
            if dynamic.is::<()>() || dynamic.is_unit() {
                return None;
            } else if dynamic.is::<bool>() {
                return Some(Value::Bool(dynamic.as_bool().unwrap_or_default()));
            } else if dynamic.is::<i64>() {
                return Some(Value::Int(dynamic.as_int().unwrap_or_default()));
            } else if dynamic.is::<f64>() {
                return Some(Value::Float(dynamic.as_float().unwrap_or_default()));
            } else if dynamic.is::<String>() {
                let s = dynamic.clone().into_string().unwrap_or_default();
                return Some(Value::String(s));
            } else if dynamic.is_array() {
                if let Some(array) = dynamic.clone().try_cast::<Array>() {
                    // Try to convert to EntityList if all elements are strings
                    let mut entity_ids = Vec::new();
                    
                    for item in array {
                        if let Some(id_str) = item.try_cast::<String>() {
                            if let Ok(entity_id) = EntityId::try_from(id_str.as_str()) {
                                entity_ids.push(entity_id);
                            } else {
                                // If any conversion fails, we give up on EntityList
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    
                    return Some(Value::EntityList(entity_ids));
                }
            } else if dynamic.is_map() {
                // We might have a special representation for EntityReference
                if let Some(map) = dynamic.clone().try_cast::<Map>() {
                    if let (Some(ref_type), Some(entity_str)) = (map.get("type"), map.get("id")) {
                        if let (Some(typ), Some(id_str)) = (ref_type.try_cast::<String>(), entity_str.try_cast::<String>()) {
                            if typ == "entity_reference" {
                                if let Ok(entity_id) = EntityId::try_from(id_str.as_str()) {
                                    return Some(Value::EntityReference(Some(entity_id)));
                                }
                            }
                        }
                    }
                }
            }
            
            // Default case: convert to string
            Some(Value::String(dynamic.to_string()))
        }
    }
}

// Helper function to convert Option<Value> to Rhai Dynamic
fn convert_value_to_rhai(value: &Value) -> Dynamic {
    match value {
        Value::Bool(b) => Dynamic::from(*b),
        Value::Int(i) => Dynamic::from(*i),
        Value::Float(f) => Dynamic::from(*f),
        Value::String(s) => Dynamic::from(s.clone()),
        Value::EntityList(list) => {
            let array: Array = list.iter()
                .map(|id| Dynamic::from(id.to_string()))
                .collect();
            Dynamic::from(array)
        },
        Value::EntityReference(maybe_id) => {
            if let Some(id) = maybe_id {
                let mut map = Map::new();
                map.insert("type".into(), Dynamic::from("entity_reference"));
                map.insert("id".into(), Dynamic::from(id.to_string()));
                Dynamic::from(map)
            } else {
                Dynamic::UNIT
            }
        },
        Value::Choice(c) => Dynamic::from(*c),
        Value::Timestamp(ts) => {
            let secs = ts.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            Dynamic::from(secs)
        },
        Value::Blob(bytes) => {
            // Convert blob to array of integers
            let array: Array = bytes.iter()
                .map(|b| Dynamic::from(*b as i64))
                .collect();
            Dynamic::from(array)
        }
    }
}