use crate::{Cache, CelExecutor, EntityId, Error, FieldType, Result, Value, StoreProxy};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

pub fn get_scope(
    store: &StoreProxy,
    executor: &mut CelExecutor,
    permission_cache: &Cache,
    subject_entity_id: EntityId,
    resource_entity_id: EntityId,
    resource_field: FieldType,
) -> Result<AuthorizationScope> {
    let mut filtered_rules: Vec<AuthorizationScope> = Vec::new();

    let entity_types = {
        let mut entity_types = store
            .get_complete_entity_schema(resource_entity_id.extract_type())?.inherit.clone();
        entity_types.push(resource_entity_id.extract_type());
        entity_types
    };

    let scope_ft = store.get_field_type("Scope")?;
    let condition_ft = store.get_field_type("Condition")?;

    for entity_type in entity_types.iter() {
        let entity_type_str = store.resolve_entity_type(*entity_type)?;
        let resource_field_str = store.resolve_field_type(resource_field)?;
        
        let permissions = permission_cache.get(vec![
            Value::String(entity_type_str.into()),
            Value::String(resource_field_str.into()),
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
                    condition,
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
