use crate::{EntityId, FieldType};

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

/// Macro to resolve indirection for any store type (Store, or StoreProxy)
#[macro_export]
macro_rules! sresolve {
    ($store:expr, $entity_id:expr, $field_type:expr) => {{
        async {
            let fields = $field_type.indirect_fields();

            if fields.len() == 1 {
                return Ok(($entity_id.clone(), $field_type.clone()));
            }

            let mut current_entity_id = $entity_id.clone();

            for i in 0..fields.len() - 1 {
                let field = &fields[i];

                // Handle array index navigation (for EntityList fields)
                if i > 0 && field.0.parse::<i64>().is_ok() {
                    let index = field.0.parse::<i64>().unwrap();
                    if index < 0 {
                        return Err($crate::Error::BadIndirection(
                            current_entity_id.clone(),
                            $field_type.clone(),
                            $crate::BadIndirectionReason::NegativeIndex(index),
                        ));
                    }

                    // The previous field should have been an EntityList
                    let prev_field = &fields[i - 1];

                    let mut reqs = vec![$crate::sread!(current_entity_id.clone(), prev_field.clone())];
                    $store.perform(&mut reqs).await?;

                    if let $crate::Request::Read { value, .. } = &reqs[0] {
                        if let Some($crate::Value::EntityList(entities)) = value {
                            let index_usize = index as usize;
                            if index_usize >= entities.len() {
                                return Err($crate::Error::BadIndirection(
                                    current_entity_id.clone(),
                                    $field_type.clone(),
                                    $crate::BadIndirectionReason::ArrayIndexOutOfBounds(
                                        index_usize,
                                        entities.len(),
                                    ),
                                ));
                            }

                            current_entity_id = entities[index_usize].clone();
                        } else {
                            return Err($crate::Error::BadIndirection(
                                current_entity_id.clone(),
                                $field_type.clone(),
                                $crate::BadIndirectionReason::UnexpectedValueType(
                                    prev_field.clone(),
                                    format!("{:?}", value),
                                ),
                            ));
                        }
                    }

                    continue;
                }

                // Normal field resolution
                let mut reqs = vec![$crate::sread!(current_entity_id.clone(), field.clone())];

                if let Err(e) = $store.perform(&mut reqs).await {
                    return Err($crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        $field_type.clone(),
                        $crate::BadIndirectionReason::FailedToResolveField(field.clone(), e.to_string()),
                    ));
                }

                if let $crate::Request::Read { value, .. } = &reqs[0] {
                    if let Some($crate::Value::EntityReference(reference)) = value {
                        match reference {
                            Some(ref_id) => {
                                // Check if the reference is valid
                                if !$store.entity_exists(ref_id).await {
                                    return Err($crate::Error::BadIndirection(
                                        current_entity_id.clone(),
                                        $field_type.clone(),
                                        $crate::BadIndirectionReason::InvalidEntityId(ref_id.clone()),
                                    ));
                                }
                                current_entity_id = ref_id.clone();
                            }
                            None => {
                                // If the reference is None, this is an error
                                return Err($crate::Error::BadIndirection(
                                    current_entity_id.clone(),
                                    $field_type.clone(),
                                    $crate::BadIndirectionReason::EmptyEntityReference,
                                ));
                            }
                        }

                        continue;
                    }

                    if let Some($crate::Value::EntityList(_)) = value {
                        // If next segment is not an index, this is an error
                        if i + 1 >= fields.len() - 1 || fields[i + 1].0.parse::<i64>().is_err() {
                            return Err($crate::Error::BadIndirection(
                                current_entity_id.clone(),
                                $field_type.clone(),
                                $crate::BadIndirectionReason::ExpectedIndexAfterEntityList(fields[i + 1].clone()),
                            ));
                        }
                        // The index will be processed in the next iteration
                        continue;
                    }

                    return Err($crate::Error::BadIndirection(
                        current_entity_id.clone(),
                        $field_type.clone(),
                        $crate::BadIndirectionReason::UnexpectedValueType(field.clone(), format!("{:?}", value)),
                    ));
                }
            }

            Ok((
                current_entity_id,
                fields.last().cloned().ok_or_else(|| {
                    $crate::Error::BadIndirection(
                        $entity_id.clone(),
                        $field_type.clone(),
                        $crate::BadIndirectionReason::UnexpectedValueType(
                            "".into(),
                            "Empty field path".to_string(),
                        ),
                    )
                })?,
            ))
        }
    }};
}
