use std::{cell::RefCell, rc::Rc};

use rhai::{Array, Dynamic, Engine, EvalAltResult, Map, Position, Scope};

use crate::{data::nanos_to_timestamp, AdjustBehavior, EntityId, FieldType, PushCondition, Request, Store, Value};

trait IntoEvalError<T> {
    fn err(self) -> Result<T, Box<EvalAltResult>>;
}

impl<T> IntoEvalError<T> for &str {
    fn err(self) -> Result<T, Box<EvalAltResult>> {
        Err(EvalAltResult::ErrorRuntime(self.into(), Position::NONE).into())
    }
}

pub struct ScriptingEngine {
    store: Rc<RefCell<Store>>,
    engine: Engine,
}

impl ScriptingEngine {
    pub fn new(store: Rc<RefCell<Store>>) -> Self {
        let mut engine = Engine::new();

        engine.register_fn("read", |entity_id: &str, field_type: &str| {
            let mut map = Map::new();
            map.insert("action".into(), Dynamic::from("read"));
            map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
            map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
            map
        });

        engine.register_fn(
            "write",
            |entity_id: &str, field_type: &str, value: Dynamic| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("always"));
                map.insert("adjust_behavior".into(), Dynamic::from("set"));
                map
            },
        );

        let store_clone = store.clone();
        engine.register_fn("perform", move |requests: Array| -> Result<(), Box<EvalAltResult>> {
            let mut requests = requests
                .into_iter()
                .filter_map(|req| req.try_cast::<Map>())
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
                    let action = req_map
                        .get("action")
                        .cloned()
                        .ok_or("Request map must contain 'action' field")?
                        .try_cast::<String>()
                        .ok_or("Failed to cast 'action' field to String")?;

                    let entity_id_str = req_map
                        .get("entity_id")
                        .cloned()
                        .ok_or("Request map must contain 'entity_id' field")?
                        .try_cast::<String>()
                        .ok_or("Failed to cast 'entity_id' field to String")?;

                    let field_type_str = req_map
                        .get("field_type")
                        .cloned()
                        .ok_or("Request map must contain 'field_type' field")?
                        .try_cast::<String>()
                        .ok_or("Failed to cast 'field_type' field to String")?;

                    let entity_id = EntityId::try_from(entity_id_str.as_str())?;

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
                        }
                        "write" => {
                            let value = convert_rhai_to_value(req_map
                                .get("value")
                                .ok_or("Request map must contain 'value' field")?
                            )?;

                            let push_condition = req_map
                                .get("push_condition")
                                .and_then(|pc| pc.clone().try_cast::<String>())
                                .map(|s| match s.as_str() {
                                    "changes" => PushCondition::Changes,
                                    _ => PushCondition::Always,
                                })
                                .unwrap_or(PushCondition::Always);

                            let adjust_behavior = req_map
                                .get("adjust_behavior")
                                .and_then(|ab| ab.clone().try_cast::<String>())
                                .map(|s| match s.as_str() {
                                    "add" => AdjustBehavior::Add,
                                    "subtract" => AdjustBehavior::Subtract,
                                    _ => AdjustBehavior::Set,
                                })
                                .unwrap_or(AdjustBehavior::Set);

                            let write_time = req_map
                                .get("write_time")
                                .and_then(|wt| wt.clone().try_cast::<u64>())
                                .map(|wt| nanos_to_timestamp(wt));

                            let writer_id = req_map
                                .get("writer_id")
                                .and_then(|wid| wid.clone().try_cast::<String>())
                                .map(|s| EntityId::try_from(s.as_str()).ok())
                                .unwrap_or_else(|| None);

                            store_requests.push(Request::Write {
                                entity_id,
                                field_type,
                                value: Some(value),
                                push_condition,
                                adjust_behavior,
                                write_time,
                                writer_id,
                            });
                        }
                        _ => return Err("Action must be 'read' or 'write'".into()),
                    }
                }

                // Execute the requests
                if let Err(e) = store_clone.borrow_mut().perform(&crate::Context {}, &mut store_requests) {
                    return Err(format!("Failed to perform requests: {}", e).into());
                }

                // Update the Rhai maps with the results
                for (i, req) in store_requests.iter().enumerate() {
                    if i >= requests.len() {
                        break;
                    }

                    let req_map = &mut requests[i];

                    match req {
                        Request::Read {
                            value,
                            write_time,
                            writer_id,
                            ..
                        } => {
                            if let Some(v) = value {
                                let rhai_value = convert_value_to_rhai(v);
                                req_map.insert("value".into(), rhai_value);

                                if let Some(wt) = write_time {
                                    req_map.insert(
                                        "write_time".into(),
                                        Dynamic::from(
                                            wt.duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs_f64(),
                                        ),
                                    );
                                }

                                if let Some(wid) = writer_id {
                                    req_map
                                        .insert("writer_id".into(), Dynamic::from(wid.to_string()));
                                }
                            }
                        }
                        Request::Write { .. } => {
                            // For write requests, we don't need to modify the map
                            // as they are already constructed with the necessary fields.
                        }
                    }
                }
            }

            Ok(())
        });

        ScriptingEngine { store, engine }
    }

    /// Executes a Rhai script with access to the store
    pub fn execute(&self, script: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();

        match self.engine.eval_with_scope::<Dynamic>(&mut scope, script) {
            Ok(result) => Ok(result),
            Err(e) => Err(format!("Script execution error: {}", e).into()),
        }
    }
}

fn convert_rhai_to_value(dynamic: &Dynamic) -> Result<Value, Box<EvalAltResult>> {
    let map = dynamic
        .clone()
        .try_cast::<Map>()
        .ok_or("Expected a Rhai Map")?;

    let value_type = map.get("type")
        .cloned()
        .ok_or("Expected 'type' field in Rhai Map")?
        .try_cast::<String>()
        .ok_or("Failed to cast 'type' field to String")?;

    match value_type.as_str() {
        "string" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<String>()
                .ok_or("Failed to cast 'value' field to String")?;
            Ok(Value::String(value))
        }
        "int" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<i64>()
                .ok_or("Failed to cast 'value' field to i64")?;
            Ok(Value::Int(value))
        }
        "bool" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<bool>()
                .ok_or("Failed to cast 'value' field to bool")?;
            Ok(Value::Bool(value))
        }
        "float" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<f64>()
                .ok_or("Failed to cast 'value' field to f64")?;
            Ok(Value::Float(value))
        }
        "choice" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<i64>()
                .ok_or("Failed to cast 'value' field to i64")?;
            Ok(Value::Choice(value))
        }
        "entity_reference" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<String>()
                .ok_or("Failed to cast 'value' field to String")?;

            if !value.is_empty() {
                let value = EntityId::try_from(value.as_str())
                    .map_err(|e| format!("Failed to convert entity reference: {}", e))?;

                Ok(Value::EntityReference(Some(value)))
            } else {
                Ok(Value::EntityReference(None))
            }
        }
        "entity_list" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<Array>()
                .ok_or("Failed to cast 'value' field to Array")?;

            let entity_ids = value
                .iter()
                .filter_map(|v| v.clone().try_cast::<String>())
                .filter_map(|s| EntityId::try_from(s.as_str()).ok())
                .collect();

            Ok(Value::EntityList(entity_ids))
        }
        "blob" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<Vec<u8>>()
                .ok_or("Failed to cast 'value' field to Vec<u8>")?;
            Ok(Value::Blob(value))
        }
        "timestamp" => {
            let value = map.get("value")
                .cloned()
                .ok_or("Expected 'value' field in Rhai Map")?
                .try_cast::<u64>()
                .ok_or("Failed to cast 'value' field to u64")?;
            Ok(Value::Timestamp(nanos_to_timestamp(value)))
        }
        _ => Err(format!("Unsupported value type: {}", value_type).into()),
    }
}

fn convert_value_to_rhai(value: &Value) -> Dynamic {
    let mut map = Map::new();
    match value {
        Value::String(s) => {
            map.insert("type".into(), Dynamic::from("string"));
            map.insert("value".into(), Dynamic::from(s.clone()));
        }
        Value::Int(i) => {
            map.insert("type".into(), Dynamic::from("int"));
            map.insert("value".into(), Dynamic::from(*i));
        }
        Value::Bool(b) => {
            map.insert("type".into(), Dynamic::from("bool"));
            map.insert("value".into(), Dynamic::from(*b));
        }
        Value::Float(f) => {
            map.insert("type".into(), Dynamic::from("float"));
            map.insert("value".into(), Dynamic::from(*f));
        }
        Value::Choice(c) => {
            map.insert("type".into(), Dynamic::from("choice"));
            map.insert("value".into(), Dynamic::from(*c));
        }
        Value::EntityReference(Some(e)) => {
            map.insert("type".into(), Dynamic::from("entity_reference"));
            map.insert("value".into(), Dynamic::from(e.to_string()));
        }
        Value::EntityReference(None) => {
            map.insert("type".into(), Dynamic::from("entity_reference"));
            map.insert("value".into(), Dynamic::from(""));
        }
        Value::EntityList(ids) => {
            let array: Array = ids.iter().map(|id| Dynamic::from(id.to_string())).collect();
            map.insert("type".into(), Dynamic::from("entity_list"));
            map.insert("value".into(), Dynamic::from(array));
        }
        Value::Blob(b) => {
            map.insert("type".into(), Dynamic::from("blob"));
            map.insert("value".into(), Dynamic::from(b.clone()));
        }
        Value::Timestamp(ts) => {
            map.insert("type".into(), Dynamic::from("timestamp"));
            let nanos = ts.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            map.insert("value".into(), Dynamic::from(nanos));
        }
    }
    Dynamic::from(map)
}