use crate::{Store, Cache, CelExecutor, EntityId, Error, FieldType, Result, Value, StoreTrait};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

#[allow(dead_code)]
pub fn get_scope(
    store: &Store,
    executor: &mut CelExecutor,
    permission_cache: &Cache,
    subject_entity_id: EntityId,
    resource_entity_id: EntityId,
    resource_field: FieldType,
) -> Result<AuthorizationScope> {
    let mut filtered_rules: Vec<AuthorizationScope> = Vec::new();

    let entity_types = {
        let mut entity_types = store
            .get_parent_types(resource_entity_id.extract_type());
        entity_types.push(resource_entity_id.extract_type());
        entity_types
    };

    let ft = store.ft.as_ref().unwrap();
    let scope_ft = ft.scope;
    let condition_ft = ft.condition;

    for entity_type in entity_types.iter() {
        let entity_type_str = store.resolve_entity_type(*entity_type)?;
        let resource_field_str = store.resolve_field_type(resource_field)?;
        
        let permissions = permission_cache.get(vec![
            Value::String(entity_type_str),
            Value::String(resource_field_str),
        ]);

        if let Some(permissions) = permissions {
            // If there are any permissions, default to None scope
            if !permissions.is_empty() {
                filtered_rules.push(AuthorizationScope::None);
            }

            for permission in permissions {
                let scope = permission
                    .get(&scope_ft)
                    .ok_or(Error::CacheFieldNotFound(scope_ft))?
                    .expect_choice()?;

                let condition = permission.get(&condition_ft).unwrap().expect_string()?;

                let scope = match scope {
                    0 => AuthorizationScope::ReadOnly,
                    1 => AuthorizationScope::ReadWrite,
                    _ => AuthorizationScope::None,
                };

                let result = executor.execute(
                    &condition.as_str(),
                    subject_entity_id,
                    store,
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
