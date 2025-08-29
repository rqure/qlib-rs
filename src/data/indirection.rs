use crate::{
    data::{store::Store, StoreTrait},
    EntityId, FieldType, Result,
};

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
            BadIndirectionReason::InvalidEntityId(id) => write!(f, "invalid entity id: {}", id),
            BadIndirectionReason::UnexpectedValueType(field, value) => {
                write!(f, "unexpected value type for field {}: {}", field, value)
            }
            BadIndirectionReason::ExpectedIndexAfterEntityList(field) => {
                write!(f, "expected index after EntityList, got: {}", field)
            }
            BadIndirectionReason::FailedToResolveField(field, error) => {
                write!(f, "failed to resolve field {}: {}", field, error)
            }
        }
    }
}

pub async fn resolve_indirection_async<T: StoreTrait>(
    store: &mut T,
    entity_id: &EntityId,
    field_type: &FieldType,
) -> Result<(EntityId, FieldType)> {
    let fields = field_type.indirect_fields();

    if fields.len() == 1 {
        return Ok((entity_id.clone(), field_type.clone()));
    }

    let mut current_entity_id = entity_id.clone();

    for i in 0..fields.len() - 1 {
        let field = &fields[i];

        // Handle array index navigation (for EntityList fields)
        if i > 0 && field.0.parse::<i64>().is_ok() {
            let index = field.0.parse::<i64>().unwrap();
            if index < 0 {
                return Err(crate::Error::BadIndirection(
                    current_entity_id.clone(),
                    field_type.clone(),
                    crate::BadIndirectionReason::NegativeIndex(index),
                ));
            }

            // The previous field should have been an EntityList
            let prev_field = &fields[i - 1];

            let mut reqs = vec![crate::sread!(current_entity_id.clone(), prev_field.clone())];
            store.perform_mut(&mut reqs).await?;

            if let crate::Request::Read { value, .. } = &reqs[0] {
                if let Some(crate::Value::EntityList(entities)) = value {
                    let index_usize = index as usize;
                    if index_usize >= entities.len() {
                        return Err(crate::Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            crate::BadIndirectionReason::ArrayIndexOutOfBounds(
                                index_usize,
                                entities.len(),
                            ),
                        ));
                    }

                    current_entity_id = entities[index_usize].clone();
                } else {
                    return Err(crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        crate::BadIndirectionReason::UnexpectedValueType(
                            prev_field.clone(),
                            format!("{:?}", value),
                        ),
                    ));
                }
            }

            continue;
        }

        // Normal field resolution
        let mut reqs = vec![crate::sread!(current_entity_id.clone(), field.clone())];

        if let Err(e) = store.perform_mut(&mut reqs).await {
            return Err(crate::Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
                crate::BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
            ));
        }

        if let crate::Request::Read { value, .. } = &reqs[0] {
            if let Some(crate::Value::EntityReference(reference)) = value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid
                        if !store.entity_exists(ref_id).await {
                            return Err(crate::Error::BadIndirection(
                                current_entity_id.clone(),
                                field_type.clone(),
                                crate::BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            ));
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(crate::Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            crate::BadIndirectionReason::EmptyEntityReference,
                        ));
                    }
                }

                continue;
            }

            if let Some(crate::Value::EntityList(_)) = value {
                // If next segment is not an index, this is an error
                if i + 1 >= fields.len() - 1 || fields[i + 1].0.parse::<i64>().is_err() {
                    return Err(crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        crate::BadIndirectionReason::ExpectedIndexAfterEntityList(
                            fields[i + 1].clone(),
                        ),
                    ));
                }
                // The index will be processed in the next iteration
                continue;
            }

            return Err(crate::Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
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
                entity_id.clone(),
                field_type.clone(),
                crate::BadIndirectionReason::UnexpectedValueType(
                    "".into(),
                    "Empty field path".to_string(),
                ),
            )
        })?,
    ))
}

pub fn resolve_indirection(
    store: &Store,
    entity_id: &EntityId,
    field_type: &FieldType,
) -> Result<(EntityId, FieldType)> {
    let fields = field_type.indirect_fields();

    if fields.len() == 1 {
        return Ok((entity_id.clone(), field_type.clone()));
    }

    let mut current_entity_id = entity_id.clone();

    for i in 0..fields.len() - 1 {
        let field = &fields[i];

        // Handle array index navigation (for EntityList fields)
        if i > 0 && field.0.parse::<i64>().is_ok() {
            let index = field.0.parse::<i64>().unwrap();
            if index < 0 {
                return Err(crate::Error::BadIndirection(
                    current_entity_id.clone(),
                    field_type.clone(),
                    crate::BadIndirectionReason::NegativeIndex(index),
                ));
            }

            // The previous field should have been an EntityList
            let prev_field = &fields[i - 1];

            let mut reqs = vec![crate::sread!(current_entity_id.clone(), prev_field.clone())];
            store.perform(&mut reqs)?;

            if let crate::Request::Read { value, .. } = &reqs[0] {
                if let Some(crate::Value::EntityList(entities)) = value {
                    let index_usize = index as usize;
                    if index_usize >= entities.len() {
                        return Err(crate::Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            crate::BadIndirectionReason::ArrayIndexOutOfBounds(
                                index_usize,
                                entities.len(),
                            ),
                        ));
                    }

                    current_entity_id = entities[index_usize].clone();
                } else {
                    return Err(crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        crate::BadIndirectionReason::UnexpectedValueType(
                            prev_field.clone(),
                            format!("{:?}", value),
                        ),
                    ));
                }
            }

            continue;
        }

        // Normal field resolution
        let mut reqs = vec![crate::sread!(current_entity_id.clone(), field.clone())];

        if let Err(e) = store.perform(&mut reqs) {
            return Err(crate::Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
                crate::BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
            ));
        }

        if let crate::Request::Read { value, .. } = &reqs[0] {
            if let Some(crate::Value::EntityReference(reference)) = value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid
                        if !store.entity_exists(ref_id) {
                            return Err(crate::Error::BadIndirection(
                                current_entity_id.clone(),
                                field_type.clone(),
                                crate::BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            ));
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(crate::Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            crate::BadIndirectionReason::EmptyEntityReference,
                        ));
                    }
                }

                continue;
            }

            if let Some(crate::Value::EntityList(_)) = value {
                // If next segment is not an index, this is an error
                if i + 1 >= fields.len() - 1 || fields[i + 1].0.parse::<i64>().is_err() {
                    return Err(crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        crate::BadIndirectionReason::ExpectedIndexAfterEntityList(
                            fields[i + 1].clone(),
                        ),
                    ));
                }
                // The index will be processed in the next iteration
                continue;
            }

            return Err(crate::Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
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
                entity_id.clone(),
                field_type.clone(),
                crate::BadIndirectionReason::UnexpectedValueType(
                    "".into(),
                    "Empty field path".to_string(),
                ),
            )
        })?,
    ))
}

/// Resolve an entity ID to its path by traversing up the parent chain
/// This works with both AsyncStore and StoreProxy since they have the same method signatures
pub async fn path_async<T: StoreTrait>(store: &mut T, entity_id: &EntityId) -> Result<String> {
    let mut path_parts = Vec::new();
    let mut current_id = entity_id.clone();
    let mut visited = std::collections::HashSet::new();

    loop {
        // Prevent infinite loops in case of circular references
        if visited.contains(&current_id) {
            return Err(crate::Error::BadIndirection(
                current_id.clone(),
                crate::FieldType::from("Parent"),
                crate::BadIndirectionReason::UnexpectedValueType(
                    crate::FieldType::from("Parent"),
                    "Circular reference detected in parent chain".to_string(),
                ),
            ));
        }
        visited.insert(current_id.clone());

        // Read the name of the current entity
        let mut name_requests = vec![crate::sread!(
            current_id.clone(),
            crate::FieldType::from("Name")
        )];

        let entity_name = if store.perform_mut(&mut name_requests).await.is_ok() {
            if let crate::Request::Read {
                value: Some(crate::Value::String(name)),
                ..
            } = &name_requests[0]
            {
                name.clone()
            } else {
                // Fallback to entity ID if no name field
                current_id.get_id()
            }
        } else {
            // Fallback to entity ID if name read fails
            current_id.get_id()
        };

        path_parts.push(entity_name);

        // Read the parent of the current entity
        let mut parent_requests = vec![crate::sread!(
            current_id.clone(),
            crate::FieldType::from("Parent")
        )];

        if store.perform_mut(&mut parent_requests).await.is_ok() {
            if let crate::Request::Read {
                value: Some(crate::Value::EntityReference(Some(parent_id))),
                ..
            } = &parent_requests[0]
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
