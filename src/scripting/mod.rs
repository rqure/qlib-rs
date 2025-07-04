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

        // Overloaded write function with push_condition
        engine.register_fn(
            "write",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("set"));
                map
            },
        );

        // Overloaded write function with push_condition and adjust_behavior
        engine.register_fn(
            "write",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, adjust_behavior: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from(adjust_behavior.to_string()));
                map
            },
        );

        // Add function - equivalent to write with Add adjust behavior
        engine.register_fn(
            "add",
            |entity_id: &str, field_type: &str, value: Dynamic| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("always"));
                map.insert("adjust_behavior".into(), Dynamic::from("add"));
                map
            },
        );

        // Overloaded add function with push_condition
        engine.register_fn(
            "add",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("add"));
                map
            },
        );

        // Sub function - equivalent to write with Subtract adjust behavior
        engine.register_fn(
            "sub",
            |entity_id: &str, field_type: &str, value: Dynamic| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("always"));
                map.insert("adjust_behavior".into(), Dynamic::from("subtract"));
                map
            },
        );

        // Overloaded sub function with push_condition
        engine.register_fn(
            "sub",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("subtract"));
                map
            },
        );

        // Write function with writer_id (6 parameters)
        engine.register_fn(
            "write",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, adjust_behavior: &str, writer_id: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from(adjust_behavior.to_string()));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map
            },
        );

        // Write function with writer_id and write_time (7 parameters)
        engine.register_fn(
            "write",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, adjust_behavior: &str, writer_id: &str, write_time: i64| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from(adjust_behavior.to_string()));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map.insert("write_time".into(), Dynamic::from(write_time as u64));
                map
            },
        );

        // Add function with writer_id (5 parameters)
        engine.register_fn(
            "add",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, writer_id: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("add"));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map
            },
        );

        // Add function with writer_id and write_time (6 parameters)
        engine.register_fn(
            "add",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, writer_id: &str, write_time: i64| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("add"));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map.insert("write_time".into(), Dynamic::from(write_time as u64));
                map
            },
        );

        // Sub function with writer_id (5 parameters)
        engine.register_fn(
            "sub",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, writer_id: &str| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("subtract"));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map
            },
        );

        // Sub function with writer_id and write_time (6 parameters)
        engine.register_fn(
            "sub",
            |entity_id: &str, field_type: &str, value: Dynamic, push_condition: &str, writer_id: &str, write_time: i64| {
                let mut map = Map::new();
                map.insert("action".into(), Dynamic::from("write"));
                map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from(push_condition.to_string()));
                map.insert("adjust_behavior".into(), Dynamic::from("subtract"));
                map.insert("writer_id".into(), Dynamic::from(writer_id.to_string()));
                map.insert("write_time".into(), Dynamic::from(write_time as u64));
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
                            let entity_schema = store_clone
                                .borrow()
                                .get_complete_entity_schema(&crate::Context {  }, &entity_id.get_type())
                                .map_err(|e| format!("Failed to get entity schema: {}", e))?;

                            let field_schema = entity_schema.fields
                                .get(&field_type)
                                .ok_or("Field type not found in entity schema")?;

                            let value = convert_rhai_to_value(req_map
                                .get("value")
                                .ok_or("Request map must contain 'value' field")?,
                                field_schema.default_value()                                
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

fn convert_rhai_to_value(dynamic: &Dynamic, type_hint: Value) -> Result<Value, Box<EvalAltResult>> {
    match type_hint {
        Value::String(_) => {
            if let Some(s) = dynamic.as_immutable_string_ref().ok() {
                Ok(Value::from_string(s.to_string()))
            } else {
                Err("Expected a string value".into())
            }
        },
        Value::Int(_) => {
            if let Some(i) = dynamic.as_int().ok() {
                Ok(Value::from_int(i))
            } else {
                Err("Expected an integer value".into())
            }
        },
        Value::Bool(_) => {
            if let Some(b) = dynamic.as_bool().ok() {
                Ok(Value::from_bool(b))
            } else {
                Err("Expected a boolean value".into())
            }
        },
        Value::Float(_) => {
            if let Some(f) = dynamic.as_float().ok() {
                Ok(Value::from_float(f))
            } else {
                Err("Expected a float value".into())
            }
        },
        Value::Blob(_) => {
            if let Some(blob) = dynamic.as_blob_ref().ok() {
                Ok(Value::from_blob(blob.clone()))
            } else {
                Err("Expected a blob value".into())
            }
        },
        Value::EntityReference(_) => {
            if let Some(entity_id) = dynamic.as_immutable_string_ref().ok() {
                let entity_id = EntityId::try_from(entity_id.as_str())
                    .map_err(|_| "Invalid entity ID format")?;
                Ok(Value::from_entity_reference(Some(entity_id)))
            } else {
                Err("Expected an entity reference value".into())
            }
        },
        Value::EntityList(_) => {
            if let Some(list) = dynamic.as_array_ref().ok() {
                let entity_ids = list
                    .iter()
                    .filter_map(|item| item.as_immutable_string_ref().ok())
                    .filter_map(|s| EntityId::try_from(s.as_str()).ok())
                    .collect::<Vec<EntityId>>();
                Ok(Value::from_entity_list(entity_ids))
            } else {
                Err("Expected an entity list value".into())
            }
        },
        Value::Choice(_) => {
            if let Some(choice) = dynamic.as_int().ok() {
                Ok(Value::from_choice(choice))
            } else {
                Err("Expected a choice value".into())
            }
        },
        Value::Timestamp(_) => {
            if let Some(ts) = dynamic.as_int().ok() {
                Ok(Value::from_timestamp(nanos_to_timestamp(ts as u64)))
            } else {
                Err("Expected a timestamp value".into())
            }
        },
    }
}

fn convert_value_to_rhai(value: &Value) -> Dynamic {
    match value {
        Value::String(s) => Dynamic::from(s.clone()),
        Value::Int(i) => Dynamic::from(*i),
        Value::Bool(b) => Dynamic::from(*b),
        Value::Float(f) => Dynamic::from(*f),
        Value::Blob(b) => Dynamic::from(b.clone()),
        Value::EntityReference(Some(e)) => Dynamic::from(e.to_string()),
        Value::EntityReference(None) => Dynamic::from(""),
        Value::EntityList(e_list) => {
            let array = Array::from_iter(e_list.iter().map(|e| Dynamic::from(e.to_string())));
            Dynamic::from(array)
        },
        Value::Choice(c) => Dynamic::from(*c),
        Value::Timestamp(t) => {
            let nanos = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
            Dynamic::from(nanos)
        },
    }
}