use crate::{data::StoreTrait, et, ft, EntityId, FieldType, Result};

pub const INDIRECTION_DELIMITER: &str = "->";

#[derive(Debug, Clone)]
pub enum BadIndirectionReason {
    NegativeIndex(i64),
    ArrayIndexOutOfBounds(usize, usize),
    EmptyEntityReference,
    InvalidEntityId(EntityId),
    UnexpectedValueType(FieldType, String),
    ExpectedIndexAfterEntityList(FieldType),
    FailedToResolveField(FieldType, String),
}
impl std::fmt::Display for BadIndirectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BadIndirectionReason::NegativeIndex(index) => write!(f, "negative index: {}", index),
            BadIndirectionReason::ArrayIndexOutOfBounds(index, size) => {
                write!(f, "array index out of bounds: {} >= {}", index, size)
            }
            BadIndirectionReason::EmptyEntityReference => write!(f, "empty entity reference"),
            BadIndirectionReason::InvalidEntityId(id) => write!(f, "invalid entity id: {:?}", id),
            BadIndirectionReason::UnexpectedValueType(field, value) => {
                write!(f, "unexpected value type for field {:?}: {}", field, value)
            }
            BadIndirectionReason::ExpectedIndexAfterEntityList(field) => {
                write!(f, "expected index after EntityList, got: {:?}", field)
            }
            BadIndirectionReason::FailedToResolveField(field, error) => {
                write!(f, "failed to resolve field {:?}: {}", field, error)
            }
        }
    }
}

/// Resolve indirection using the StoreTrait interface (for StoreProxy and other trait objects)
/// This version uses the perform() method since it doesn't have direct field access
pub fn resolve_indirection_via_trait<T: StoreTrait>(
    store: &T,
    entity_id: EntityId,
    fields: &[FieldType],
) -> Result<(EntityId, FieldType)> {
    if fields.len() == 1 {
        return Ok((entity_id, fields[0].clone()));
    }

    let mut current_entity_id = entity_id;

    for (i, field) in fields.iter().enumerate() {
        // Normal field resolution
        let reqs = crate::sreq![crate::sread!(
            current_entity_id,
            crate::sfield![field.clone()]
        )];

        let reqs = match store.perform(reqs) {
            Ok(reqs) => reqs,
            Err(e) => {
                return Err(crate::Error::BadIndirection(
                    current_entity_id,
                    fields.to_vec(),
                    crate::BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
                ));
            }
        };

        if let crate::Request::Read { value, .. } = &reqs.clone().read()[0] {
            // If this is the last field in the path, we're done - return the current entity and field
            if i == fields.len() - 1 {
                break;
            }

            // For intermediate fields, they must be EntityReferences
            if let Some(crate::Value::EntityReference(reference)) = value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid
                        if !store.entity_exists(ref_id.clone()) {
                            return Err(crate::Error::BadIndirection(
                                current_entity_id,
                                fields.to_vec(),
                                crate::BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            ));
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(crate::Error::BadIndirection(
                            current_entity_id,
                            fields.to_vec(),
                            crate::BadIndirectionReason::EmptyEntityReference,
                        ));
                    }
                }

                continue;
            }

            return Err(crate::Error::BadIndirection(
                current_entity_id,
                fields.to_vec(),
                crate::BadIndirectionReason::UnexpectedValueType(
                    field.clone(),
                    format!("{:?}", value),
                ),
            ));
        }
    }

    Ok((
        current_entity_id,
        fields.last().cloned().ok_or_else(|| {
            crate::Error::BadIndirection(
                entity_id,
                fields.to_vec(),
                crate::BadIndirectionReason::UnexpectedValueType(
                    FieldType(0),
                    "Empty field path".to_string(),
                ),
            )
        })?,
    ))
}

/// Resolve an entity ID to its path by traversing up the parent chain
/// This works with both Store and StoreProxy since they have the same method signatures
pub fn path<T: StoreTrait>(store: &T, entity_id: EntityId) -> Result<String> {
    let mut path_parts = Vec::new();
    let mut current_id = entity_id;
    let mut visited = std::collections::HashSet::new();
    let parent_ft = store.get_field_type(ft::PARENT)?;
    let name_ft = store.get_field_type(ft::NAME)?;

    loop {
        // Prevent infinite loops in case of circular references
        if visited.contains(&current_id) {
            return Err(crate::Error::BadIndirection(
                current_id.clone(),
                vec![parent_ft.clone()], // Convert to Vec for error reporting
                crate::BadIndirectionReason::UnexpectedValueType(
                    parent_ft.clone(),
                    "Circular reference detected in parent chain".to_string(),
                ),
            ));
        }
        visited.insert(current_id.clone());

        // Read the name of the current entity
        let name_requests = crate::sreq![crate::sread!(
            current_id.clone(),
            crate::sfield![name_ft.clone()]
        )];

        let entity_name = if let Ok(reqs) = store.perform(name_requests) {
            if let crate::Request::Read {
                value: Some(crate::Value::String(name)),
                ..
            } = &reqs.clone().read()[0]
            {
                name.as_str().to_string()
            } else {
                // Fallback to entity ID if no name field
                current_id.0.to_string()
            }
        } else {
            // Fallback to entity ID if name read fails
            current_id.0.to_string()
        };

        path_parts.push(entity_name);

        // Read the parent of the current entity
        let parent_requests = crate::sreq![crate::sread!(
            current_id.clone(),
            crate::sfield![parent_ft.clone()]
        )];

        if let Ok(reqs) = store.perform(parent_requests) {
            if let crate::Request::Read {
                value: Some(crate::Value::EntityReference(Some(parent_id))),
                ..
            } = &reqs.clone().read()[0]
            {
                current_id = parent_id.clone();
            } else {
                // No parent, we've reached the root
                break;
            }
        } else {
            // Parent read failed, we've reached the root
            break;
        }
    }

    // Reverse to get path from root to entity
    path_parts.reverse();
    Ok::<String, crate::Error>(path_parts.join("/"))
}

/// Resolve a path to an entity ID by traversing down from the root
/// This works with both Store and StoreProxy since they have the same method signatures
pub fn path_to_entity_id<T: StoreTrait>(store: &T, path: &str) -> Result<EntityId> {
    if path.is_empty() {
        return Err(crate::Error::InvalidFieldValue("Empty path".to_string()));
    }

    let path_parts: Vec<&str> = path.split('/').collect();
    let root_et = store.get_entity_type(et::ROOT)?;
    let name_ft = store.get_field_type(ft::NAME)?;
    let children_ft = store.get_field_type(ft::CHILDREN)?;

    // Start by finding the root entity with the first part of the path
    let root_entities = store.find_entities(root_et.clone(), None)?;
    let mut current_entity_id = None;

    // Find the root entity that matches the first path part
    for root_id in root_entities {
        let name_requests = crate::sreq![crate::sread!(
            root_id.clone(),
            crate::sfield![name_ft.clone()]
        )];

        if let Ok(reqs) = store.perform(name_requests) {
            if let crate::Request::Read {
                value: Some(crate::Value::String(name)),
                ..
            } = &reqs.clone().read()[0]
            {
                if name.as_str() == path_parts[0] {
                    current_entity_id = Some(root_id);
                    break;
                }
            }
        }
    }

    let mut current_id = current_entity_id.ok_or_else(|| {
        crate::Error::EntityNotFound(crate::EntityId::new(root_et.clone(), 0 as u32))
    })?;

    // Traverse down the path by following Children relationships
    for part in &path_parts[1..] {
        let children_requests = crate::sreq![crate::sread!(
            current_id.clone(),
            crate::sfield![children_ft.clone()]
        )];

        if let Ok(reqs) = store.perform(children_requests) {
            if let crate::Request::Read {
                value: Some(crate::Value::EntityList(children)),
                ..
            } = &reqs.clone().read()[0]
            {
                let mut found = false;
                for child_id in children {
                    let child_name_requests = crate::sreq![crate::sread!(
                        child_id.clone(),
                        crate::sfield![name_ft.clone()]
                    )];

                    if let Ok(reqs) = store.perform(child_name_requests) {
                        if let crate::Request::Read {
                            value: Some(crate::Value::String(child_name)),
                            ..
                        } = &reqs.clone().read()[0]
                        {
                            if child_name.as_str() == *part {
                                current_id = child_id.clone();
                                found = true;
                                break;
                            }
                        }
                    }
                }

                if !found {
                    return Err(crate::Error::EntityNameNotFound(part.to_string()));
                }
            } else {
                return Err(crate::Error::EntityNameNotFound(part.to_string()));
            }
        } else {
            return Err(crate::Error::EntityNameNotFound(part.to_string()));
        }
    }

    Ok(current_id)
}
