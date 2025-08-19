use crate::{data::store, Context, EntityId, Error, FieldType, Request, Result, Store, StoreType, Value};

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

/// Resolves indirection using the store from the context
/// 
/// This function retrieves the store from the context and resolves indirection paths.
pub async fn resolve_indirection(
    ctx: &Context,
    mut store: StoreType,
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
                return Err(Error::BadIndirection(
                    current_entity_id.clone(),
                    field_type.clone(),
                    BadIndirectionReason::NegativeIndex(index),
                ));
            }

            // The previous field should have been an EntityList
            let prev_field = &fields[i - 1];

            let mut reqs = vec![Request::Read {
                entity_id: current_entity_id.clone(),
                field_type: prev_field.clone(),
                value: None,
                write_time: None,
                writer_id: None,
            }];
            store.perform(ctx, &mut reqs).await?;

            if let Request::Read { value, .. } = &reqs[0] {
                if let Some(Value::EntityList(entities)) = value {
                    let index_usize = index as usize;
                    if index_usize >= entities.len() {
                        return Err(Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            BadIndirectionReason::ArrayIndexOutOfBounds(
                                index_usize,
                                entities.len(),
                            ),
                        ));
                    }

                    current_entity_id = entities[index_usize].clone();
                } else {
                    return Err(Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::UnexpectedValueType(
                            prev_field.clone(),
                            format!("{:?}", value),
                        ),
                    ));
                }
            }

            continue;
        }

        // Normal field resolution
        let mut reqs = vec![Request::Read {
            entity_id: current_entity_id.clone(),
            field_type: field.clone(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        if let Err(e) = store.perform(ctx, &mut reqs).await {
            return Err(Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
            ));
        }

        if let Request::Read { value, .. } = &reqs[0] {
            if let Some(Value::EntityReference(reference)) = value {
                match reference {
                    Some(ref_id) => {
                        // Check if the reference is valid
                        if !store.entity_exists(ctx, ref_id).await {
                            return Err(Error::BadIndirection(
                                current_entity_id.clone(),
                                field_type.clone(),
                                BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                            ));
                        }
                        current_entity_id = ref_id.clone();
                    }
                    None => {
                        // If the reference is None, this is an error
                        return Err(Error::BadIndirection(
                            current_entity_id.clone(),
                            field_type.clone(),
                            BadIndirectionReason::EmptyEntityReference,
                        ));
                    }
                }

                continue;
            }

            if let Some(Value::EntityList(_)) = value {
                // If next segment is not an index, this is an error
                if i + 1 >= fields.len() - 1 || fields[i + 1].0.parse::<i64>().is_err() {
                    return Err(Error::BadIndirection(
                        current_entity_id.clone(),
                        field_type.clone(),
                        BadIndirectionReason::ExpectedIndexAfterEntityList(fields[i + 1].clone()),
                    ));
                }
                // The index will be processed in the next iteration
                continue;
            }

            return Err(Error::BadIndirection(
                current_entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::UnexpectedValueType(field.clone(), format!("{:?}", value)),
            ));
        }
    }

    Ok((
        current_entity_id,
        fields.last().cloned().ok_or_else(|| {
            Error::BadIndirection(
                entity_id.clone(),
                field_type.clone(),
                BadIndirectionReason::FailedToResolveField(
                    FieldType("".to_string()),
                    "Empty field path".to_string(),
                ),
            )
        })?,
    ))
}
