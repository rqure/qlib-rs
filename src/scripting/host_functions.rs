use wasmtime::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::data::{StoreTrait, EntityId, Request};
use crate::scripting::types::{JsonEntityId, JsonRequest};
use serde_json;

/// Context that WASM functions can access
pub struct StoreContext<T: StoreTrait + Send + Sync + 'static> {
    pub store: Arc<RwLock<T>>,
}

/// Define host functions that can be called from WASM modules
pub fn define_functions<T: StoreTrait + Send + Sync + 'static>(
    linker: &mut Linker<StoreContext<T>>,
) -> Result<(), anyhow::Error> {
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

    Ok(())
}