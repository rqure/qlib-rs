use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{ft, AsyncStore, Cache, CelExecutor, EntityId, Error, FieldType, Result, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

#[allow(dead_code)]
pub async fn get_scope(
    store: Arc<RwLock<AsyncStore>>,
    executor: &mut CelExecutor,
    permission_cache: &Cache<AsyncStore>,
    subject_entity_id: &EntityId,
    resource_entity_id: &EntityId,
    resource_field: &FieldType,
) -> Result<AuthorizationScope> {
    let mut filtered_rules: Vec<AuthorizationScope> = Vec::new();

    let entity_types = {
        let mut entity_types = store
            .read()
            .await
            .inner()
            .get_parent_types(resource_entity_id.get_type());
        entity_types.push(resource_entity_id.get_type().clone());
        entity_types
    };

    for entity_type in entity_types.iter() {
        let permissions = permission_cache.get(vec![
            Value::String(entity_type.to_string()),
            Value::String(resource_field.to_string()),
        ]);

        if let Some(permissions) = permissions {
            // If there are any permissions, default to None scope
            if !permissions.is_empty() {
                filtered_rules.push(AuthorizationScope::None);
            }

            for permission in permissions {
                let scope = permission
                    .get(&ft::scope())
                    .ok_or(Error::CacheFieldNotFound(ft::scope()))?
                    .expect_choice()?;

                let condition = permission.get(&ft::condition()).unwrap().expect_string()?;

                let scope = match scope {
                    0 => AuthorizationScope::ReadOnly,
                    1 => AuthorizationScope::ReadWrite,
                    _ => AuthorizationScope::None,
                };

                let result = executor.execute(
                    &condition.as_str(),
                    &subject_entity_id,
                    store.write().await.inner_mut(),
                );

                if let Ok(result) = result {
                    match result {
                        cel::Value::Bool(true) => {
                            filtered_rules.push(scope);
                        }
                        _ => {}
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
