use crate::{et, ft, scripting, sread, Context, EntityId, FieldType, StoreType};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

pub async fn get_scope(
    store: &mut StoreType,
    ctx: &Context,
    subject_entity_id: &EntityId,
    resource_entity_id: &EntityId,
    resource_field: &FieldType,
) -> AuthorizationScope {
    let rules = store.find_entities(ctx, &et::authorization_rule()).await.ok();
    let mut filtered_rules: Vec<AuthorizationScope> = Vec::new();

    if let Some(entities) = rules {
        for entity_id in entities {
            let mut req = vec![
                sread!( entity_id.clone(), ft::scope() ),
                sread!( entity_id.clone(), ft::resource_type() ),
                sread!( entity_id.clone(), ft::resource_field() ),
                sread!( entity_id.clone(), ft::permission_test_fn() ),
            ];

            if store.perform(ctx, &mut req).await.is_ok() {
                let scope = req[0].value().unwrap().as_int();
                let rule_resource_type = req[1].value().unwrap().as_string().unwrap();
                let rule_resource_field = req[2].value().unwrap().as_string().unwrap();
                let permission_test_fn = req[3].value().unwrap().as_string().unwrap();
                
                if *rule_resource_type == resource_entity_id.get_type().0 &&
                   *rule_resource_field == resource_field.to_string() {
                    let scope = match scope {
                        Some(0) => AuthorizationScope::None,
                        Some(1) => AuthorizationScope::ReadOnly,
                        Some(2) => AuthorizationScope::ReadWrite,
                        _ => continue, // Invalid scope
                    };

                    let result = scripting::execute(store, Context::new(), permission_test_fn, serde_json::json!({
                        "subject_id": subject_entity_id.to_string(),
                        "resource_id": resource_entity_id.to_string(),
                        "resource_field": resource_field.to_string(),
                    })).await;

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
            } else {
                return AuthorizationScope::None;
            }
        }
    }
    
    filtered_rules.into_iter().max().unwrap_or(AuthorizationScope::ReadWrite)
}