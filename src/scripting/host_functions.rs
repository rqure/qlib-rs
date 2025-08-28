use wasmtime::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::data::{StoreTrait, EntityId, EntityType, FieldType, Request};
use crate::scripting::types::{JsonEntityId, JsonEntityType, JsonEntitySchema, JsonFieldSchema, JsonRequest};
use serde_json;

/// Context that WASM functions can access
pub struct StoreContext<T: StoreTrait + Send + Sync + 'static> {
    pub store: Arc<RwLock<T>>,
}

/// Define host functions that can be called from WASM modules
pub fn define_functions<T: StoreTrait + Send + Sync + 'static>(
    linker: &mut Linker<StoreContext<T>>,
) -> Result<(), anyhow::Error> {
    // Basic debugging functions
    linker.func_wrap("env", "host_log", |_caller: Caller<'_, StoreContext<T>>, value: i32| {
        println!("WASM log: {}", value);
    })?;

    linker.func_wrap("env", "always_true", |_caller: Caller<'_, StoreContext<T>>| -> i32 {
        1
    })?;

    // Check if an entity exists (async)
    linker.func_wrap_async(
        "env",
        "entity_exists",
        |mut caller: Caller<'_, StoreContext<T>>, (entity_id_ptr, entity_id_len): (i32, i32)| {
            Box::new(async move {
                // Get memory and read the entity ID JSON
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return 0, // Return false if no memory
                };

                let data = match memory.data(&caller).get(entity_id_ptr as usize..(entity_id_ptr + entity_id_len) as usize) {
                    Some(data) => data,
                    None => return 0,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return 0,
                };

                let json_entity_id: JsonEntityId = match serde_json::from_str(json_str) {
                    Ok(id) => id,
                    Err(_) => return 0,
                };

                let entity_id: EntityId = match json_entity_id.try_into() {
                    Ok(id) => id,
                    Err(_) => return 0,
                };

                let store = caller.data().store.clone();

                // Now we can properly await the async call
                let store_guard = store.read().await;
                match store_guard.entity_exists(&entity_id).await {
                    true => 1,
                    false => 0,
                }
            })
        },
    )?;

    // Perform a batch of requests (async)
    linker.func_wrap_async(
        "env",
        "perform_requests",
        |mut caller: Caller<'_, StoreContext<T>>, (requests_ptr, requests_len): (i32, i32)| {
            Box::new(async move {
                // Get memory and read the requests JSON
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return 0, // Return false if no memory
                };

                let data = match memory.data(&caller).get(requests_ptr as usize..(requests_ptr + requests_len) as usize) {
                    Some(data) => data,
                    None => return 0,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return 0,
                };

                let json_requests: Vec<JsonRequest> = match serde_json::from_str(json_str) {
                    Ok(reqs) => reqs,
                    Err(_) => return 0,
                };

                let mut requests: Vec<Request> = match json_requests.into_iter()
                    .map(|r| r.try_into())
                    .collect::<Result<Vec<_>, _>>() {
                    Ok(reqs) => reqs,
                    Err(_) => return 0,
                };

                let store = caller.data().store.clone();

                // Now we can properly await the async call
                let mut store_guard = store.write().await;
                match store_guard.perform(&mut requests).await {
                    Ok(_) => 1,
                    Err(_) => 0,
                }
            })
        },
    )?;

    // Get all entity types (async) - returns length of JSON written to output buffer
    linker.func_wrap_async(
        "env",
        "get_entity_types",
        |mut caller: Caller<'_, StoreContext<T>>, (output_ptr, output_max_len): (i32, i32)| {
            Box::new(async move {
                let store = caller.data().store.clone();
                let store_guard = store.read().await;
                
                match store_guard.get_entity_types().await {
                    Ok(entity_types) => {
                        let json_types: Vec<JsonEntityType> = entity_types.iter().map(JsonEntityType::from).collect();
                        let json_str = match serde_json::to_string(&json_types) {
                            Ok(s) => s,
                            Err(_) => return -1, // Error
                        };
                        
                        let json_bytes = json_str.as_bytes();
                        if json_bytes.len() > output_max_len as usize {
                            return -2; // Buffer too small
                        }
                        
                        // Write result to WASM memory
                        let memory = match caller.get_export("memory") {
                            Some(Extern::Memory(mem)) => mem,
                            _ => return -1,
                        };
                        
                        let memory_data = memory.data_mut(&mut caller);
                        let output_slice = match memory_data.get_mut(output_ptr as usize..(output_ptr as usize + json_bytes.len())) {
                            Some(slice) => slice,
                            None => return -1,
                        };
                        
                        output_slice.copy_from_slice(json_bytes);
                        json_bytes.len() as i32
                    }
                    Err(_) => -1,
                }
            })
        },
    )?;

    // Find entities by type (async) - returns length of JSON written to output buffer
    linker.func_wrap_async(
        "env",
        "find_entities",
        |mut caller: Caller<'_, StoreContext<T>>, (entity_type_ptr, entity_type_len, output_ptr, output_max_len): (i32, i32, i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = match memory.data(&caller).get(entity_type_ptr as usize..(entity_type_ptr + entity_type_len) as usize) {
                    Some(data) => data,
                    None => return -1,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return -1,
                };

                let json_entity_type: JsonEntityType = match serde_json::from_str(json_str) {
                    Ok(t) => t,
                    Err(_) => return -1,
                };

                let entity_type: EntityType = json_entity_type.into();
                let store = caller.data().store.clone();
                let store_guard = store.read().await;

                match store_guard.find_entities(&entity_type).await {
                    Ok(entities) => {
                        let json_entities: Vec<JsonEntityId> = entities.iter().map(JsonEntityId::from).collect();
                        let json_result = match serde_json::to_string(&json_entities) {
                            Ok(s) => s,
                            Err(_) => return -1,
                        };
                        
                        let json_bytes = json_result.as_bytes();
                        if json_bytes.len() > output_max_len as usize {
                            return -2; // Buffer too small
                        }
                        
                        // Write result to WASM memory
                        let memory_data = memory.data_mut(&mut caller);
                        let output_slice = match memory_data.get_mut(output_ptr as usize..(output_ptr as usize + json_bytes.len())) {
                            Some(slice) => slice,
                            None => return -1,
                        };
                        
                        output_slice.copy_from_slice(json_bytes);
                        json_bytes.len() as i32
                    }
                    Err(_) => -1,
                }
            })
        },
    )?;

    // Check if a field exists (async)
    linker.func_wrap_async(
        "env",
        "field_exists",
        |mut caller: Caller<'_, StoreContext<T>>, (params_ptr, params_len): (i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return 0,
                };

                let data = match memory.data(&caller).get(params_ptr as usize..(params_ptr + params_len) as usize) {
                    Some(data) => data,
                    None => return 0,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return 0,
                };

                let params: serde_json::Value = match serde_json::from_str(json_str) {
                    Ok(p) => p,
                    Err(_) => return 0,
                };

                let entity_type_str = match params.get("entity_type").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return 0,
                };

                let field_type_str = match params.get("field_type").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return 0,
                };

                let entity_type = EntityType::from(entity_type_str.to_string());
                let field_type = FieldType::from(field_type_str.to_string());
                let store = caller.data().store.clone();
                let store_guard = store.read().await;

                match store_guard.field_exists(&entity_type, &field_type).await {
                    true => 1,
                    false => 0,
                }
            })
        },
    )?;

    // Get field schema (async) - returns length of JSON written to output buffer
    linker.func_wrap_async(
        "env",
        "get_field_schema",
        |mut caller: Caller<'_, StoreContext<T>>, (params_ptr, params_len, output_ptr, output_max_len): (i32, i32, i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = match memory.data(&caller).get(params_ptr as usize..(params_ptr + params_len) as usize) {
                    Some(data) => data,
                    None => return -1,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return -1,
                };

                let params: serde_json::Value = match serde_json::from_str(json_str) {
                    Ok(p) => p,
                    Err(_) => return -1,
                };

                let entity_type_str = match params.get("entity_type").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return -1,
                };

                let field_type_str = match params.get("field_type").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return -1,
                };

                let entity_type = EntityType::from(entity_type_str.to_string());
                let field_type = FieldType::from(field_type_str.to_string());
                let store = caller.data().store.clone();
                let store_guard = store.read().await;

                match store_guard.get_field_schema(&entity_type, &field_type).await {
                    Ok(schema) => {
                        let json_schema = JsonFieldSchema::from(&schema);
                        let json_result = match serde_json::to_string(&json_schema) {
                            Ok(s) => s,
                            Err(_) => return -1,
                        };
                        
                        let json_bytes = json_result.as_bytes();
                        if json_bytes.len() > output_max_len as usize {
                            return -2; // Buffer too small
                        }
                        
                        // Write result to WASM memory
                        let memory_data = memory.data_mut(&mut caller);
                        let output_slice = match memory_data.get_mut(output_ptr as usize..(output_ptr as usize + json_bytes.len())) {
                            Some(slice) => slice,
                            None => return -1,
                        };
                        
                        output_slice.copy_from_slice(json_bytes);
                        json_bytes.len() as i32
                    }
                    Err(_) => -1,
                }
            })
        },
    )?;

    // Get entity schema (async) - returns length of JSON written to output buffer
    linker.func_wrap_async(
        "env",
        "get_entity_schema",
        |mut caller: Caller<'_, StoreContext<T>>, (entity_type_ptr, entity_type_len, output_ptr, output_max_len): (i32, i32, i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = match memory.data(&caller).get(entity_type_ptr as usize..(entity_type_ptr + entity_type_len) as usize) {
                    Some(data) => data,
                    None => return -1,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return -1,
                };

                let json_entity_type: JsonEntityType = match serde_json::from_str(json_str) {
                    Ok(t) => t,
                    Err(_) => return -1,
                };

                let entity_type: EntityType = json_entity_type.into();
                let store = caller.data().store.clone();
                let store_guard = store.read().await;

                match store_guard.get_entity_schema(&entity_type).await {
                    Ok(schema) => {
                        let json_schema = JsonEntitySchema::from(&schema);
                        let json_result = match serde_json::to_string(&json_schema) {
                            Ok(s) => s,
                            Err(_) => return -1,
                        };
                        
                        let json_bytes = json_result.as_bytes();
                        if json_bytes.len() > output_max_len as usize {
                            return -2; // Buffer too small
                        }
                        
                        // Write result to WASM memory
                        let memory_data = memory.data_mut(&mut caller);
                        let output_slice = match memory_data.get_mut(output_ptr as usize..(output_ptr as usize + json_bytes.len())) {
                            Some(slice) => slice,
                            None => return -1,
                        };
                        
                        output_slice.copy_from_slice(json_bytes);
                        json_bytes.len() as i32
                    }
                    Err(_) => -1,
                }
            })
        },
    )?;

    // Get complete entity schema (async) - returns length of JSON written to output buffer
    linker.func_wrap_async(
        "env",
        "get_complete_entity_schema",
        |mut caller: Caller<'_, StoreContext<T>>, (entity_type_ptr, entity_type_len, output_ptr, output_max_len): (i32, i32, i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return -1,
                };

                let data = match memory.data(&caller).get(entity_type_ptr as usize..(entity_type_ptr + entity_type_len) as usize) {
                    Some(data) => data,
                    None => return -1,
                };

                let json_str = match std::str::from_utf8(data) {
                    Ok(s) => s,
                    Err(_) => return -1,
                };

                let json_entity_type: JsonEntityType = match serde_json::from_str(json_str) {
                    Ok(t) => t,
                    Err(_) => return -1,
                };

                let entity_type: EntityType = json_entity_type.into();
                let store = caller.data().store.clone();
                let store_guard = store.read().await;

                match store_guard.get_complete_entity_schema(&entity_type).await {
                    Ok(schema) => {
                        let json_schema = JsonEntitySchema::from(&schema);
                        let json_result = match serde_json::to_string(&json_schema) {
                            Ok(s) => s,
                            Err(_) => return -1,
                        };
                        
                        let json_bytes = json_result.as_bytes();
                        if json_bytes.len() > output_max_len as usize {
                            return -2; // Buffer too small
                        }
                        
                        // Write result to WASM memory
                        let memory_data = memory.data_mut(&mut caller);
                        let output_slice = match memory_data.get_mut(output_ptr as usize..(output_ptr as usize + json_bytes.len())) {
                            Some(slice) => slice,
                            None => return -1,
                        };
                        
                        output_slice.copy_from_slice(json_bytes);
                        json_bytes.len() as i32
                    }
                    Err(_) => -1,
                }
            })
        },
    )?;

    Ok(())
}