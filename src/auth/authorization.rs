use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{ft, scripting::execute, sstr, Cache, EntityId, Error, FieldType, Result, StoreProxy};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

pub async fn get_scope(
    store: Arc<Mutex<StoreProxy>>,
    auth_rule_cache: &Cache,
    subject_entity_id: &EntityId,
    resource_entity_id: &EntityId,
    resource_field: &FieldType,
) -> Result<AuthorizationScope> {
    let rules = auth_rule_cache.get(vec![
        sstr!(resource_entity_id.get_type()),
        sstr!(resource_field.0),
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

            let permission_test_fn = rule
                .get(&ft::permission_test_fn())
                .unwrap()
                .expect_string()?;

            if *rule_resource_type == resource_entity_id.get_type().0
                && *rule_resource_field == resource_field.to_string()
            {
                let scope = match scope {
                    0 => AuthorizationScope::None,
                    1 => AuthorizationScope::ReadOnly,
                    2 => AuthorizationScope::ReadWrite,
                    _ => continue, // Invalid scope
                };

                let result = execute(
                    store.clone(),
                    permission_test_fn,
                    serde_json::json!({
                        "subject_id": subject_entity_id.to_string(),
                        "resource_id": resource_entity_id.to_string(),
                        "resource_field": resource_field.to_string(),
                    }),
                )
                .await;

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

    Ok(filtered_rules
        .into_iter()
        .max()
        .unwrap_or(AuthorizationScope::ReadWrite))
}
