use crate::{auth::authorization_rule_entity_type, Context, EntityId, FieldType, Store};

pub enum AuthorizationScope {
    None,
    ReadOnly,
    ReadWrite,
}

pub fn get_scope(
    store: &mut Store,
    ctx: &Context,
    resource_entity_id: &EntityId,
    resource_field: &FieldType,
) -> AuthorizationScope {
    store.find_entities(authorization_rule_entity_type(), None)
}