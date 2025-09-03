use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType, Single, Value, Error, Result
};
use crate::data::{StoreTrait, StorageScope};

/// JSON-friendly representation of a field schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonFieldSchema {
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: String,
    pub default: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "storageScope")]
    pub storage_scope: Option<String>,
}

/// JSON-friendly representation of an entity schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntitySchema {
    #[serde(rename = "entityType")]
    pub entity_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "inheritsFrom")]
    pub inherits_from: Vec<String>,
    pub fields: Vec<JsonFieldSchema>,
}

/// JSON-friendly representation of an entity with its data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntity {
    #[serde(rename = "entityType")]
    pub entity_type: String,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, JsonValue>,
}

/// JSON snapshot format matching the user's requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSnapshot {
    pub schemas: Vec<JsonEntitySchema>,
    pub tree: JsonEntity,
}

impl JsonFieldSchema {
    /// Convert from internal FieldSchema to JSON format
    pub fn from_field_schema(field_schema: &FieldSchema) -> Self {
        let (data_type, default, choices) = match field_schema {
            FieldSchema::Blob { default_value, .. } => {
                ("Blob".to_string(), serde_json::to_value(default_value).unwrap_or(JsonValue::Null), None)
            },
            FieldSchema::Bool { default_value, .. } => {
                ("Bool".to_string(), JsonValue::Bool(*default_value), None)
            },
            FieldSchema::Choice { default_value, choices, .. } => {
                let choice_name = if *default_value >= 0 && (*default_value as usize) < choices.len() {
                    JsonValue::String(choices[*default_value as usize].clone())
                } else {
                    JsonValue::Null
                };
                ("Choice".to_string(), choice_name, Some(choices.clone()))
            },
            FieldSchema::EntityList { default_value, .. } => {
                let json_list: Vec<String> = default_value.iter().map(|id| id.get_id()).collect();
                ("EntityList".to_string(), serde_json::to_value(json_list).unwrap_or(JsonValue::Array(vec![])), None)
            },
            FieldSchema::EntityReference { default_value, .. } => {
                let json_ref = default_value.as_ref().map(|id| id.get_id());
                ("EntityReference".to_string(), serde_json::to_value(json_ref).unwrap_or(JsonValue::Null), None)
            },
            FieldSchema::Float { default_value, .. } => {
                ("Float".to_string(), JsonValue::Number(serde_json::Number::from_f64(*default_value).unwrap_or_else(|| serde_json::Number::from(0))), None)
            },
            FieldSchema::Int { default_value, .. } => {
                ("Int".to_string(), JsonValue::Number(serde_json::Number::from(*default_value)), None)
            },
            FieldSchema::String { default_value, .. } => {
                ("String".to_string(), JsonValue::String(default_value.clone()), None)
            },
            FieldSchema::Timestamp { default_value, .. } => {
                ("Timestamp".to_string(), serde_json::to_value(default_value.unix_timestamp()).unwrap_or(JsonValue::Null), None)
            },
        };

        Self {
            name: field_schema.field_type().as_ref().to_string(),
            data_type,
            default,
            choices,
            rank: Some(field_schema.rank()),
            storage_scope: Some(match field_schema.storage_scope() {
                StorageScope::Runtime => "Runtime".to_string(),
                StorageScope::Configuration => "Configuration".to_string(),
            }),
        }
    }

    /// Convert to internal FieldSchema
    pub fn to_field_schema(&self) -> Result<FieldSchema> {
        let field_type = FieldType::from(self.name.clone());
        let rank = self.rank.unwrap_or(0);
        let storage_scope = match self.storage_scope.as_deref() {
            Some("Configuration") => StorageScope::Configuration,
            _ => StorageScope::Runtime, // Default to Runtime if not specified or invalid
        };

        match self.data_type.as_str() {
            "Blob" => {
                let default_value: Vec<u8> = serde_json::from_value(self.default.clone())
                    .unwrap_or_default();
                Ok(FieldSchema::Blob { field_type, default_value, rank, storage_scope })
            },
            "Bool" => {
                let default_value = self.default.as_bool().unwrap_or(false);
                Ok(FieldSchema::Bool { field_type, default_value, rank, storage_scope })
            },
            "Choice" => {
                let choices = self.choices.clone().unwrap_or_default();
                let default_value = if let Some(choice_str) = self.default.as_str() {
                    choices.iter().position(|c| c == choice_str).unwrap_or(0) as i64
                } else {
                    0
                };
                Ok(FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope })
            },
            "EntityList" => {
                let default_value = if let Some(array) = self.default.as_array() {
                    array.iter()
                        .filter_map(|v| v.as_str())
                        .filter_map(|s| EntityId::try_from(s).ok())
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(FieldSchema::EntityList { field_type, default_value, rank, storage_scope })
            },
            "EntityReference" => {
                let default_value = self.default.as_str()
                    .and_then(|s| EntityId::try_from(s).ok());
                Ok(FieldSchema::EntityReference { field_type, default_value, rank, storage_scope })
            },
            "Float" => {
                let default_value = self.default.as_f64().unwrap_or(0.0);
                Ok(FieldSchema::Float { field_type, default_value, rank, storage_scope })
            },
            "Int" => {
                let default_value = self.default.as_i64().unwrap_or(0);
                Ok(FieldSchema::Int { field_type, default_value, rank, storage_scope })
            },
            "String" => {
                let default_value = self.default.as_str().unwrap_or("").to_string();
                Ok(FieldSchema::String { field_type, default_value, rank, storage_scope })
            },
            "Timestamp" => {
                let unix_timestamp: i64 = serde_json::from_value(self.default.clone())
                    .unwrap_or(0);
                let default_value = time::OffsetDateTime::from_unix_timestamp(unix_timestamp)
                    .unwrap_or_else(|_| super::epoch());
                Ok(FieldSchema::Timestamp { field_type, default_value, rank, storage_scope })
            },
            _ => Err(Error::InvalidFieldType(format!("Unknown data type: {}", self.data_type))),
        }
    }
}

impl JsonEntitySchema {
    /// Convert from internal EntitySchema to JSON format
    pub fn from_entity_schema(schema: &EntitySchema<Single>) -> Self {
        let mut fields: Vec<JsonFieldSchema> = schema.fields
            .values()
            .map(JsonFieldSchema::from_field_schema)
            .collect();
        
        // Sort fields by rank then by name for consistent output
        fields.sort_by(|a, b| {
            let rank_a = a.rank.unwrap_or(0);
            let rank_b = b.rank.unwrap_or(0);
            rank_a.cmp(&rank_b).then_with(|| a.name.cmp(&b.name))
        });

        let inherits_from: Vec<String> = schema.inherit.iter().map(|t| t.as_ref().to_string()).collect();

        Self {
            entity_type: schema.entity_type.as_ref().to_string(),
            inherits_from,
            fields,
        }
    }

    /// Convert to internal EntitySchema
    pub fn to_entity_schema(&self) -> Result<EntitySchema<Single>> {
        let inherits_from: Vec<EntityType> = self.inherits_from.iter()
            .map(|s| EntityType::from(s.clone()))
            .collect();
            
        let mut schema = EntitySchema::<Single>::new(
            self.entity_type.clone(),
            inherits_from
        );

        // Assign ranks based on the order of fields in the JSON array
        // (rank adjustment for inheritance is handled at the restore level)
        for (index, field) in self.fields.iter().enumerate() {
            let mut field_schema = field.to_field_schema()?;
            // Use the rank from the field if provided, otherwise use the index
            let rank = field.rank.unwrap_or(index as i64);
            
            // Override the rank to maintain file order
            field_schema = match field_schema {
                FieldSchema::Blob { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::Blob { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::Bool { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::Bool { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::Choice { field_type, default_value, choices, storage_scope, .. } => {
                    FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope }
                },
                FieldSchema::EntityList { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::EntityList { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::EntityReference { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::EntityReference { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::Float { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::Float { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::Int { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::Int { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::String { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::String { field_type, default_value, rank, storage_scope }
                },
                FieldSchema::Timestamp { field_type, default_value, storage_scope, .. } => {
                    FieldSchema::Timestamp { field_type, default_value, rank, storage_scope }
                },
            };
            schema.fields.insert(field_schema.field_type().clone(), field_schema);
        }

        Ok(schema)
    }
}

/// Helper function to convert Value to JsonValue for entity data
pub fn value_to_json_value(value: &Value, choices: Option<&Vec<String>>) -> JsonValue {
    match value {
        Value::Blob(v) => serde_json::to_value(v).unwrap_or(JsonValue::Null),
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Choice(v) => {
            if let Some(choices) = choices {
                if *v >= 0 && (*v as usize) < choices.len() {
                    JsonValue::String(choices[*v as usize].clone())
                } else {
                    JsonValue::Null
                }
            } else {
                JsonValue::Number(serde_json::Number::from(*v))
            }
        },
        Value::EntityList(v) => {
            JsonValue::Array(v.iter().map(|id| JsonValue::String(id.get_id())).collect())
        },
        Value::EntityReference(v) => {
            v.as_ref().map(|id| JsonValue::String(id.get_id())).unwrap_or(JsonValue::Null)
        },
        Value::Float(v) => {
            JsonValue::Number(serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0)))
        },
        Value::Int(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Timestamp(v) => serde_json::to_value(v.unix_timestamp()).unwrap_or(JsonValue::Null),
    }
}

/// Convert Value to JsonValue with path resolution for entity references
/// This works with any type implementing StoreTrait
pub async fn value_to_json_value_with_paths<T: StoreTrait>(
    store: &mut T,
    value: &Value,
    choices: Option<&Vec<String>>,
) -> JsonValue {
    match value {
        Value::Blob(v) => serde_json::to_value(v).unwrap_or(JsonValue::Null),
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Choice(v) => {
            if let Some(choices) = choices {
                if *v >= 0 && (*v as usize) < choices.len() {
                    JsonValue::String(choices[*v as usize].clone())
                } else {
                    JsonValue::Null
                }
            } else {
                JsonValue::Number(serde_json::Number::from(*v))
            }
        },
        Value::EntityList(v) => {
            let mut path_array = Vec::new();
            for entity_id in v {
                match crate::path_async(store, entity_id).await {
                    Ok(path) => path_array.push(JsonValue::String(path)),
                    Err(_) => path_array.push(JsonValue::String(entity_id.get_id())),
                }
            }
            JsonValue::Array(path_array)
        },
        Value::EntityReference(v) => {
            if let Some(entity_id) = v {
                match crate::path_async(store, entity_id).await {
                    Ok(path) => JsonValue::String(path),
                    Err(_) => JsonValue::String(entity_id.get_id()),
                }
            } else {
                JsonValue::Null
            }
        },
        Value::Float(v) => {
            JsonValue::Number(serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0)))
        },
        Value::Int(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Timestamp(v) => serde_json::to_value(v.unix_timestamp()).unwrap_or(JsonValue::Null),
    }
}

/// Helper function to convert JsonValue to Value for entity data
pub fn json_value_to_value(json_value: &JsonValue, field_schema: &FieldSchema) -> Result<Value> {
    match field_schema {
        FieldSchema::Blob { .. } => {
            let blob: Vec<u8> = serde_json::from_value(json_value.clone())
                .map_err(|_| Error::InvalidFieldValue("Invalid blob data".to_string()))?;
            Ok(Value::Blob(blob))
        },
        FieldSchema::Bool { .. } => {
            let bool_val = json_value.as_bool()
                .ok_or_else(|| Error::InvalidFieldValue("Expected boolean value".to_string()))?;
            Ok(Value::Bool(bool_val))
        },
        FieldSchema::Choice { choices, .. } => {
            let choice_idx = if let Some(choice_str) = json_value.as_str() {
                choices.iter().position(|c| c == choice_str).unwrap_or(0) as i64
            } else if let Some(choice_num) = json_value.as_i64() {
                choice_num
            } else {
                return Err(Error::InvalidFieldValue("Expected string or number for choice".to_string()));
            };
            Ok(Value::Choice(choice_idx))
        },
        FieldSchema::EntityList { .. } => {
            let entity_ids = if let Some(array) = json_value.as_array() {
                array.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| EntityId::try_from(s))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| Error::InvalidFieldValue(format!("Invalid entity ID in list: {}", e)))?
            } else {
                Vec::new()
            };
            Ok(Value::EntityList(entity_ids))
        },
        FieldSchema::EntityReference { .. } => {
            let entity_ref = if let Some(id_str) = json_value.as_str() {
                Some(EntityId::try_from(id_str)
                    .map_err(|e| Error::InvalidFieldValue(format!("Invalid entity ID: {}", e)))?)
            } else {
                None
            };
            Ok(Value::EntityReference(entity_ref))
        },
        FieldSchema::Float { .. } => {
            let float_val = json_value.as_f64()
                .ok_or_else(|| Error::InvalidFieldValue("Expected float value".to_string()))?;
            Ok(Value::Float(float_val))
        },
        FieldSchema::Int { .. } => {
            let int_val = json_value.as_i64()
                .ok_or_else(|| Error::InvalidFieldValue("Expected integer value".to_string()))?;
            Ok(Value::Int(int_val))
        },
        FieldSchema::String { .. } => {
            let string_val = json_value.as_str()
                .ok_or_else(|| Error::InvalidFieldValue("Expected string value".to_string()))?;
            
            // Check for special password hashing function
            if string_val.starts_with("__hashpw__(") && string_val.ends_with(")") {
                // Extract password from __hashpw__(password) syntax
                let password = &string_val[11..string_val.len()-1]; // Remove "__hashpw__(" and ")"
                let config = crate::auth::AuthConfig::default();
                match crate::auth::hash_password(password, &config) {
                    Ok(hashed) => {
                        Ok(Value::String(hashed))
                    },
                    Err(_) => {
                        Err(Error::InvalidFieldValue("Failed to hash password".to_string()))
                    }
                }
            } else {
                Ok(Value::String(string_val.to_string()))
            }
        },
        FieldSchema::Timestamp { .. } => {
            let unix_timestamp: i64 = serde_json::from_value(json_value.clone())
                .map_err(|_| Error::InvalidFieldValue("Invalid timestamp data".to_string()))?;
            let timestamp = time::OffsetDateTime::from_unix_timestamp(unix_timestamp)
                .map_err(|_| Error::InvalidFieldValue("Invalid unix timestamp".to_string()))?;
            Ok(Value::Timestamp(timestamp))
        },
    }
}

/// Helper function to convert JsonValue to Value with path resolution for entity data
/// This is used during restore to resolve paths to EntityIds
pub async fn json_value_to_value_with_resolution<T: StoreTrait>(
    store: &mut T,
    json_value: &JsonValue, 
    field_schema: &FieldSchema
) -> Result<Value> {
    match field_schema {
        FieldSchema::EntityList { .. } => {
            let entity_ids = if let Some(array) = json_value.as_array() {
                let mut resolved_ids = Vec::new();
                for v in array {
                    if let Some(s) = v.as_str() {
                        // Try to parse as EntityId first, then as path
                        if let Ok(entity_id) = EntityId::try_from(s) {
                            resolved_ids.push(entity_id);
                        } else {
                            // Try to resolve as path
                            match crate::path_to_entity_id_async(store, s).await {
                                Ok(entity_id) => resolved_ids.push(entity_id),
                                Err(_) => {
                                    // Skip invalid paths/IDs
                                    continue;
                                }
                            }
                        }
                    }
                }
                resolved_ids
            } else {
                Vec::new()
            };
            Ok(Value::EntityList(entity_ids))
        },
        FieldSchema::EntityReference { .. } => {
            let entity_ref = if let Some(id_str) = json_value.as_str() {
                // Try to parse as EntityId first, then as path
                if let Ok(entity_id) = EntityId::try_from(id_str) {
                    Some(entity_id)
                } else {
                    // Try to resolve as path
                    match crate::path_to_entity_id_async(store, id_str).await {
                        Ok(entity_id) => Some(entity_id),
                        Err(_) => None,
                    }
                }
            } else {
                None
            };
            Ok(Value::EntityReference(entity_ref))
        },
        // For all other field types, use the regular conversion
        _ => json_value_to_value(json_value, field_schema),
    }
}

/// Take a JSON snapshot of the current store state
/// This finds the Root entity automatically and creates a hierarchical representation
/// Works with any type implementing StoreTrait
pub async fn take_json_snapshot<T: StoreTrait>(store: &mut T) -> Result<JsonSnapshot> {
    // Collect all schemas by getting all entity types first
    let mut json_schemas = Vec::new();
    let entity_types = store.get_entity_types().await?;
    for entity_type in entity_types {
        if let Ok(schema) = store.get_entity_schema(&entity_type).await {
            json_schemas.push(JsonEntitySchema::from_entity_schema(&schema));
        }
    }

    // Sort schemas for consistent output
    json_schemas.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));

    // Find the Root entity
    let root_entities = store.find_entities(&EntityType::from("Root"), None).await?;
    let root_entity_id = root_entities.first()
        .ok_or_else(|| Error::EntityNotFound(EntityId::new("Root", 0)))?;

    // Build the entity tree starting from root using the helper function
    let root_entity = build_json_entity_tree(store, root_entity_id).await?;

    Ok(JsonSnapshot {
        schemas: json_schemas,
        tree: root_entity,
    })
}

/// Helper function to build a JSON entity tree with special handling for Children fields
/// This function works with any type implementing StoreTrait
pub async fn build_json_entity_tree<T: StoreTrait>(
    store: &mut T,
    entity_id: &EntityId,
) -> Result<JsonEntity> {
    if !store.entity_exists(entity_id).await {
        return Err(Error::EntityNotFound(entity_id.clone()));
    }

    let entity_type = entity_id.get_type();
    let complete_schema = store.get_complete_entity_schema(entity_type).await?;
    
    // First, collect and sort all fields by rank to ensure correct processing order
    let mut schema_fields: Vec<(&crate::FieldType, &crate::FieldSchema)> = complete_schema.fields
        .iter()
        .filter(|(_, field_schema)| {
            // Only include configuration fields in snapshots, excluding runtime fields
            !matches!(field_schema.storage_scope(), crate::data::StorageScope::Runtime)
        })
        .collect();
    
    // Sort by rank to ensure consistent field ordering
    schema_fields.sort_by_key(|(_, field_schema)| field_schema.rank());
    
    // Collect fields with their rank for ordering
    let mut field_data: Vec<(i64, String, serde_json::Value)> = Vec::new();

    // Read all field values for this entity using perform()
    for (field_type, field_schema) in schema_fields {
        // Create a read request
        let mut read_requests = vec![crate::Request::Read {
            entity_id: entity_id.clone(),
            field_type: field_type.clone(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        // Perform the read operation
        if let Ok(_) = store.perform_mut(&mut read_requests).await {
            if let Some(crate::Request::Read { value: Some(ref value), .. }) = read_requests.first() {
                // Special handling for Children field - show nested entities instead of paths
                if field_type.as_ref() == "Children" {
                    if let crate::Value::EntityList(child_ids) = value {
                        let mut children = Vec::new();
                        for child_id in child_ids {
                            // Recursively build each child entity
                            if let Ok(child_entity) = Box::pin(build_json_entity_tree(store, child_id)).await {
                                children.push(serde_json::to_value(child_entity).unwrap_or(serde_json::Value::Null));
                            }
                        }
                        field_data.push((field_schema.rank(), "Children".to_string(), serde_json::Value::Array(children)));
                    } else {
                        field_data.push((field_schema.rank(), "Children".to_string(), serde_json::Value::Array(vec![])));
                    }
                } else {
                    // For other fields, use path-aware value conversion for entity references
                    let choices_ref = if let crate::FieldSchema::Choice { choices, .. } = field_schema {
                        Some(choices)
                    } else {
                        None
                    };
                    
                    // Use path resolution for EntityReference and EntityList fields (but not Children)
                    let json_value = match value {
                        crate::Value::EntityReference(_) | crate::Value::EntityList(_) => {
                            value_to_json_value_with_paths(store, value, choices_ref).await
                        },
                        _ => value_to_json_value(value, choices_ref)
                    };
                    field_data.push((field_schema.rank(), field_type.as_ref().to_string(), json_value));
                }
            }
        }
    }

    // Sort fields by rank to maintain order
    field_data.sort_by_key(|(rank, _, _)| *rank);
    
    // Create the ordered fields map using serde_json::Map for ordered insertion
    let mut fields = serde_json::Map::new();
    for (_, field_name, field_value) in field_data {
        fields.insert(field_name, field_value);
    }

    Ok(JsonEntity {
        entity_type: entity_type.as_ref().to_string(),
        fields,
    })
}

/// Restore the store state from a JSON snapshot
/// This recreates the entity hierarchy from the JSON snapshot
/// Works with any type implementing StoreTrait
pub async fn restore_json_snapshot<T: StoreTrait>(store: &mut T, json_snapshot: &JsonSnapshot) -> Result<()> {
    // Sort schemas by dependency order (base classes first)
    let mut sorted_schemas = json_snapshot.schemas.clone();
    sorted_schemas.sort_by(|a, b| {
        // If a inherits from b, b should come first
        if a.inherits_from.contains(&b.entity_type) {
            return std::cmp::Ordering::Greater;
        }
        // If b inherits from a, a should come first  
        if b.inherits_from.contains(&a.entity_type) {
            return std::cmp::Ordering::Less;
        }
        // Otherwise sort alphabetically for consistency
        a.entity_type.cmp(&b.entity_type)
    });

    // Calculate proper rank offsets for inheritance
    let mut max_ranks: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    // First, restore schemas in dependency order with proper rank adjustments
    let mut schema_requests = Vec::new();
    for json_schema in &sorted_schemas {
        // Calculate rank offset based on ALL inherited schemas
        // For multiple inheritance, we need to accumulate offsets properly
        let mut rank_offset = 0i64;
        
        // Calculate the total offset by summing the field counts of all inherited types
        for parent_type in &json_schema.inherits_from {
            if let Some(&parent_max_rank) = max_ranks.get(parent_type) {
                // Add 1 to the parent's max rank to get the starting offset for this inheritance
                rank_offset += parent_max_rank + 1;
            }
        }

        // Create a modified schema with adjusted ranks
        let mut adjusted_schema = json_schema.clone();
        let mut current_max_rank = rank_offset - 1;
        
        for (index, field) in adjusted_schema.fields.iter_mut().enumerate() {
            // Use the field's original rank if provided, otherwise use the index
            let original_rank = field.rank.unwrap_or(index as i64);
            field.rank = Some(rank_offset + original_rank);
            current_max_rank = current_max_rank.max(field.rank.unwrap());
        }
        
        // Store the maximum rank for this schema type
        max_ranks.insert(json_schema.entity_type.clone(), current_max_rank);
        
        let schema = adjusted_schema.to_entity_schema()?;
        schema_requests.push(crate::Request::SchemaUpdate { 
            schema, 
            timestamp: None,
            originator: None 
        });
    }

    // Perform schema updates first
    store.perform_mut(&mut schema_requests).await?;

    // Restore the entity tree starting from the root
    restore_entity_recursive(store, &json_snapshot.tree, None).await?;

    Ok(())
}

/// Helper function to recursively restore entities from JSON
/// Works with any type implementing StoreTrait
pub async fn restore_entity_recursive<T: StoreTrait>(
    store: &mut T,
    json_entity: &JsonEntity,
    parent_id: Option<crate::EntityId>,
) -> Result<crate::EntityId> {
    // Create the entity
    let name = json_entity.fields.get("Name")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();

    let mut create_requests = vec![crate::Request::Create {
        entity_type: crate::EntityType::from(json_entity.entity_type.clone()),
        parent_id: parent_id.clone(),
        name: name.clone(),
        created_entity_id: None,
        timestamp: None,
        originator: None,
    }];
    store.perform_mut(&mut create_requests).await?;

    // Get the created entity ID
    let entity_id = if let Some(crate::Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        id.clone()
    } else {
        return Err(crate::Error::EntityNotFound(crate::EntityId::new(json_entity.entity_type.clone(), 0)));
    };

    // Get the entity schema to understand field types
    let complete_schema = store.get_complete_entity_schema(&crate::EntityType::from(json_entity.entity_type.clone())).await?;

    // Debug: Print the complete schema fields
    // Set field values (except Children - we'll handle that last)
    let mut write_requests = Vec::new();
    for (field_name, json_value) in &json_entity.fields {
        if field_name == "Children" {
            continue; // Handle children separately
        }

        let field_type = crate::FieldType::from(field_name.clone());
        if let Some(field_schema) = complete_schema.fields.get(&field_type) {
            // Use path resolution for EntityReference and EntityList fields
            let value_result = match field_schema {
                crate::FieldSchema::EntityList { .. } | crate::FieldSchema::EntityReference { .. } => {
                    json_value_to_value_with_resolution(store, json_value, field_schema).await
                },
                _ => crate::json_value_to_value(json_value, field_schema),
            };
            
            match value_result {
                Ok(value) => {
                    write_requests.push(crate::Request::Write {
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: Some(value),
                        push_condition: crate::PushCondition::Always,
                        adjust_behavior: crate::AdjustBehavior::Set,
                        write_time: None,
                        writer_id: None,
                        originator: None,
                    });
                }
                Err(_) => {
                    // Skip invalid values
                }
            }
        }
    }

    if !write_requests.is_empty() {
        store.perform_mut(&mut write_requests).await?;
    }

    // Handle Children - recursively create child entities
    if let Some(children_json) = json_entity.fields.get("Children") {
        if let Some(children_array) = children_json.as_array() {
            let mut child_ids = Vec::new();
            for child_json in children_array {
                if let Ok(child_entity) = serde_json::from_value::<JsonEntity>(child_json.clone()) {
                    let child_id = Box::pin(restore_entity_recursive(store, &child_entity, Some(entity_id.clone()))).await?;
                    child_ids.push(child_id);
                }
            }

            // Update the Children field with the created child IDs
            if !child_ids.is_empty() {
                let mut children_write_requests = vec![crate::Request::Write {
                    entity_id: entity_id.clone(),
                    field_type: crate::FieldType::from("Children"),
                    value: Some(crate::Value::EntityList(child_ids)),
                    push_condition: crate::PushCondition::Always,
                    adjust_behavior: crate::AdjustBehavior::Set,
                    write_time: None,
                    writer_id: None,
                    originator: None,
                }];
                store.perform_mut(&mut children_write_requests).await?;
            }
        }
    }

    Ok(entity_id)
}

/// Helper function to clear all contents from a directory
async fn clear_directory_contents(dir_path: &std::path::Path) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(dir_path).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    
    Ok(())
}

/// Factory restore: Create snapshot and WAL files in a target data directory
/// This is useful for creating a fresh QCore data directory from a JSON snapshot
pub async fn factory_restore_json_snapshot(
    json_snapshot: &JsonSnapshot,
    data_dir: std::path::PathBuf,
    machine_id: String,
) -> Result<()> {
    use crate::{Snowflake, AsyncStore};
    use std::sync::Arc;

    // Create the directory structure
    let machine_data_dir = data_dir.join(&machine_id);
    let snapshots_dir = machine_data_dir.join("snapshots");
    let wal_dir = machine_data_dir.join("wal");

    tokio::fs::create_dir_all(&snapshots_dir).await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to create snapshots directory: {}", e)))?;
    tokio::fs::create_dir_all(&wal_dir).await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to create WAL directory: {}", e)))?;

    // Clear snapshots directory if it contains any files
    clear_directory_contents(&snapshots_dir).await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to clear snapshots directory: {}", e)))?;
    
    // Clear WAL directory if it contains any files
    clear_directory_contents(&wal_dir).await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to clear WAL directory: {}", e)))?;

    // Create a temporary Store instance and restore the JSON snapshot into it
    let snowflake = Arc::new(Snowflake::new());
    let mut temp_store = AsyncStore::new(snowflake);
    
    // Restore the JSON snapshot into the temporary store
    restore_json_snapshot(&mut temp_store, json_snapshot).await?;
    
    // Take a snapshot from the temporary store - this handles all the complex logic
    let snapshot = temp_store.inner().take_snapshot();

    // Write snapshot binary file - using bincode instead of serde_json to handle non-string HashMap keys
    let snapshot_filename = "snapshot_0000000000.bin";
    let snapshot_path = snapshots_dir.join(snapshot_filename);
    
    let serialized_snapshot = bincode::serialize(&snapshot)
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to serialize snapshot: {}", e)))?;
    
    tokio::fs::write(&snapshot_path, &serialized_snapshot).await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to write snapshot file: {}", e)))?;

    // Write WAL file with snapshot marker
    let wal_filename = "wal_0000000000.log";
    let wal_path = wal_dir.join(wal_filename);
    
    let snapshot_request = crate::Request::Snapshot {
        snapshot_counter: 0,
        timestamp: None,
        originator: Some("factory-restore".to_string()),
    };
    
    let serialized_request = serde_json::to_vec(&snapshot_request)
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to serialize snapshot request: {}", e)))?;
    
    // Write to WAL file with length prefix (matching QCore format)
    use tokio::io::AsyncWriteExt;
    let mut wal_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal_path)
        .await
        .map_err(|e| crate::Error::StoreProxyError(format!("Failed to open WAL file: {}", e)))?;
    
    // Write length prefix (4 bytes little-endian) followed by the serialized data
    let len_bytes = (serialized_request.len() as u32).to_le_bytes();
    wal_file.write_all(&len_bytes).await.map_err(|e| crate::Error::StoreProxyError(format!("Failed to write WAL length: {}", e)))?;
    wal_file.write_all(&serialized_request).await.map_err(|e| crate::Error::StoreProxyError(format!("Failed to write WAL data: {}", e)))?;
    wal_file.flush().await.map_err(|e| crate::Error::StoreProxyError(format!("Failed to flush WAL file: {}", e)))?;

    Ok(())
}

/// Normal restore via StoreProxy: Take a diff and apply changes
/// This connects to a running QCore service and applies the differences between current state and snapshot
pub async fn restore_json_snapshot_via_proxy(
    store_proxy: &mut crate::StoreProxy,
    json_snapshot: &JsonSnapshot,
) -> Result<()> {
    // Take current snapshot to compute diff
    let current_snapshot = take_json_snapshot(store_proxy).await?;
    
    // Compute and apply schema differences first
    apply_schema_diff(store_proxy, &current_snapshot.schemas, &json_snapshot.schemas).await?;
    
    // Compute and apply entity differences
    apply_entity_diff(store_proxy, &current_snapshot.tree, &json_snapshot.tree).await?;

    Ok(())
}

/// Apply schema differences between current and target snapshots
async fn apply_schema_diff(
    store: &mut crate::StoreProxy,
    current_schemas: &[JsonEntitySchema],
    target_schemas: &[JsonEntitySchema],
) -> Result<()> {
    let mut current_map: HashMap<String, &JsonEntitySchema> = current_schemas.iter()
        .map(|s| (s.entity_type.clone(), s))
        .collect();
    
    let mut schema_requests = Vec::new();
    
    // Add or update schemas that are different
    for target_schema in target_schemas {
        let needs_update = match current_map.get(&target_schema.entity_type) {
            Some(current_schema) => {
                // Compare schemas (simplified comparison)
                serde_json::to_string(current_schema).unwrap_or_default() != 
                serde_json::to_string(target_schema).unwrap_or_default()
            },
            None => true, // Schema doesn't exist, needs to be added
        };
        
        if needs_update {
            let schema = target_schema.to_entity_schema()?;
            schema_requests.push(crate::Request::SchemaUpdate { 
                schema, 
                timestamp: None,
                originator: Some("restore-via-proxy".to_string()) 
            });
        }
        
        current_map.remove(&target_schema.entity_type);
    }
    
    // Note: We don't remove schemas that exist in current but not in target
    // as this could be destructive. Only add/update schemas.
    
    if !schema_requests.is_empty() {
        store.perform_mut(&mut schema_requests).await?;
    }
    
    Ok(())
}

/// Apply entity differences between current and target entity trees
async fn apply_entity_diff(
    store: &mut crate::StoreProxy,
    current_tree: &JsonEntity,
    target_tree: &JsonEntity,
) -> Result<()> {
    // This is a simplified diff implementation
    // In a full implementation, this would compute a more sophisticated diff
    // For now, we'll do a simple recursive comparison and update approach
    
    apply_entity_diff_recursive(store, Some(current_tree), target_tree, None).await?;
    Ok(())
}

/// Recursively apply entity differences
fn apply_entity_diff_recursive<'a>(
    store: &'a mut crate::StoreProxy,
    current_entity: Option<&'a JsonEntity>,
    target_entity: &'a JsonEntity,
    parent_id: Option<crate::EntityId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::EntityId>> + 'a>> {
    Box::pin(async move {
    // Find if this entity already exists by name and type
    let entity_id = if let Some(current) = current_entity {
        if current.entity_type == target_entity.entity_type {
            // Entity exists, find its ID by searching
            let entity_type = crate::EntityType::from(target_entity.entity_type.clone());
            let entities = store.find_entities(&entity_type, None).await?;
            
            // Try to find by name
            let target_name = target_entity.fields.get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");
                
            let mut found_entity_id = None;
            for entity_id in &entities {
                let mut read_requests = vec![crate::Request::Read {
                    entity_id: entity_id.clone(),
                    field_type: crate::FieldType::from("Name".to_string()),
                    value: None,
                    write_time: None,
                    writer_id: None,
                }];
                
                if store.perform_mut(&mut read_requests).await.is_ok() {
                    if let Some(crate::Request::Read { value: Some(crate::Value::String(name)), .. }) = read_requests.first() {
                        if name == target_name {
                            found_entity_id = Some(entity_id.clone());
                            break;
                        }
                    }
                }
            }
            
            found_entity_id.unwrap_or_else(|| {
                // If not found, we'll create it (fall through to creation logic)
                entities.first().cloned().unwrap_or_else(|| crate::EntityId::new(target_entity.entity_type.clone(), 0))
            })
        } else {
            // Different type, create new entity
            create_entity_from_json(store, target_entity, parent_id.clone()).await?
        }
    } else {
        // Entity doesn't exist, create it
        create_entity_from_json(store, target_entity, parent_id.clone()).await?
    };
    
    // Update entity fields (only configuration fields)
    let entity_type = crate::EntityType::from(target_entity.entity_type.clone());
    let complete_schema = store.get_complete_entity_schema(&entity_type).await?;
    
    let mut write_requests = Vec::new();
    for (field_name, json_value) in &target_entity.fields {
        if field_name == "Children" {
            continue; // Handle children separately
        }
        
        let field_type = crate::FieldType::from(field_name.clone());
        if let Some(field_schema) = complete_schema.fields.get(&field_type) {
            // Only update configuration fields
            if matches!(field_schema.storage_scope(), crate::data::StorageScope::Configuration) {
                if let Ok(value) = json_value_to_value(json_value, field_schema) {
                    write_requests.push(crate::Request::Write {
                        entity_id: entity_id.clone(),
                        field_type,
                        value: Some(value),
                        push_condition: crate::PushCondition::Always,
                        adjust_behavior: crate::AdjustBehavior::Set,
                        write_time: None,
                        writer_id: None,
                        originator: Some("restore-via-proxy".to_string()),
                    });
                }
            }
        }
    }
    
    if !write_requests.is_empty() {
        store.perform_mut(&mut write_requests).await?;
    }
    
    // Handle children recursively
    if let Some(target_children_json) = target_entity.fields.get("Children") {
        if let Some(target_children_array) = target_children_json.as_array() {
            let mut child_ids = Vec::new();
            
            // Get current children for comparison
            let current_children = if let Some(current) = current_entity {
                current.fields.get("Children")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            
            for (i, child_json) in target_children_array.iter().enumerate() {
                if let Ok(child_entity) = serde_json::from_value::<JsonEntity>(child_json.clone()) {
                    let current_child_entity = current_children.get(i)
                        .and_then(|v| serde_json::from_value::<JsonEntity>((*v).clone()).ok());
                    
                    let child_id = apply_entity_diff_recursive(
                        store, 
                        current_child_entity.as_ref(), 
                        &child_entity, 
                        Some(entity_id.clone())
                    ).await?;
                    child_ids.push(child_id);
                }
            }
            
            // Update the Children field
            if !child_ids.is_empty() {
                let mut children_write_requests = vec![crate::Request::Write {
                    entity_id: entity_id.clone(),
                    field_type: crate::FieldType::from("Children".to_string()),
                    value: Some(crate::Value::EntityList(child_ids)),
                    push_condition: crate::PushCondition::Always,
                    adjust_behavior: crate::AdjustBehavior::Set,
                    write_time: None,
                    writer_id: None,
                    originator: Some("restore-via-proxy".to_string()),
                }];
                store.perform_mut(&mut children_write_requests).await?;
            }
        }
    }
    
    Ok(entity_id)
    })
}

/// Helper function to create a new entity from JSON data
async fn create_entity_from_json(
    store: &mut crate::StoreProxy,
    json_entity: &JsonEntity,
    parent_id: Option<crate::EntityId>,
) -> Result<crate::EntityId> {
    let name = json_entity.fields.get("Name")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();

    let mut create_requests = vec![crate::Request::Create {
        entity_type: crate::EntityType::from(json_entity.entity_type.clone()),
        parent_id,
        name,
        created_entity_id: None,
        timestamp: None,
        originator: Some("restore-via-proxy".to_string()),
    }];
    
    store.perform_mut(&mut create_requests).await?;

    if let Some(crate::Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
        Ok(id.clone())
    } else {
        Err(crate::Error::EntityNotFound(crate::EntityId::new(json_entity.entity_type.clone(), 0)))
    }
}
