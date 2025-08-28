use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{ft, scripting::execute, sread, Cache, EntityId, Error, FieldType, Result, Store, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

#[allow(dead_code)]
pub async fn get_scope(
    store: Arc<RwLock<Store>>,
    auth_rule_cache: &Cache,
    subject_entity_id: &EntityId,
    resource_entity_id: &EntityId,
    resource_field: &FieldType,
) -> Result<AuthorizationScope> {
    let rules = auth_rule_cache.get(vec![
        Value::String(resource_entity_id.get_type().to_string()),
        Value::String(resource_field.to_string()),
    ]);
    let mut filtered_rules: Vec<AuthorizationScope> = Vec::new();

    if let Some(rules) = rules {
        for rule in rules {
            let scope = rule
                .get(&ft::scope())
                .ok_or(Error::CacheFieldNotFound(ft::scope()))?
                .expect_choice()?;

            let rule_resource_type = rule
                .get(&ft::resource_type())
                .ok_or(Error::CacheFieldNotFound(ft::resource_type()))?
                .expect_string()?;

            let rule_resource_field = rule
                .get(&ft::resource_field())
                .ok_or(Error::CacheFieldNotFound(ft::resource_field()))?
                .expect_string()?;

            let permission = rule
                .get(&ft::permission())
                .unwrap()
                .expect_entity_reference()?;

            if let Some(permission) = permission {
                let mut reqs = vec![
                    sread!(permission.clone(), ft::test_fn()),
                ];
                store.write().await.perform(&mut reqs).await?;
                let test_fn = reqs.first().unwrap().value().unwrap().expect_string()?.clone();

                if *rule_resource_type == resource_entity_id.get_type().to_string()
                    && *rule_resource_field == resource_field.to_string()
                {
                    let scope = match scope {
                        0 => AuthorizationScope::None,
                        1 => AuthorizationScope::ReadOnly,
                        2 => AuthorizationScope::ReadWrite,
                        _ => continue, // Invalid scope
                    };

                    // TODO: Update to compile JavaScript to WebAssembly or use a different approach
                    // For now, assume test functions are WebAssembly bytecode or skip execution
                    let result = if test_fn.starts_with("(module") {
                        // WAT format - convert to WASM and execute
                        match wat::parse_str(&test_fn) {
                            Ok(wasm_bytes) => {
                                execute(
                                    store.clone(),
                                    &wasm_bytes,
                                    Some("main"),
                                    serde_json::json!({
                                        "subject_id": subject_entity_id.to_string(),
                                        "resource_id": resource_entity_id.to_string(),
                                        "resource_field": resource_field.to_string(),
                                    }),
                                )
                                .await
                            }
                            Err(_) => {
                                // Not WAT format, assume deny for safety
                                continue;
                            }
                        }
                    } else {
                        // Not WASM format, assume deny for safety
                        continue;
                    };

                    if let Ok(result) = result {
                        if result.success {
                            if let Some(value) = result.value.as_bool() {
                                if value {
                                    filtered_rules.push(scope);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(filtered_rules
        .into_iter()
        .max()
        .unwrap_or(AuthorizationScope::ReadWrite))
}
