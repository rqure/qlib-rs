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
    #[serde(skip_serializing_if = "Option::is_none", rename = "inheritsFrom")]
    pub inherits_from: Option<String>,
    pub fields: Vec<JsonFieldSchema>,
}

/// JSON-friendly representation of an entity with its data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntity {
    #[serde(rename = "entityType")]
    pub entity_type: String,
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
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
                ("Timestamp".to_string(), serde_json::to_value(default_value).unwrap_or(JsonValue::Null), None)
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
                let default_value = serde_json::from_value(self.default.clone())
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

        let inherits_from = schema.inherit.as_ref().map(|t| t.as_ref().to_string());

        Self {
            entity_type: schema.entity_type.as_ref().to_string(),
            inherits_from,
            fields,
        }
    }

    /// Convert to internal EntitySchema
    pub fn to_entity_schema(&self) -> Result<EntitySchema<Single>> {
        let mut schema = EntitySchema::<Single>::new(
            self.entity_type.clone(),
            self.inherits_from.as_ref()
                .map(|s| EntityType::from(s.clone()))
        );

        for field in &self.fields {
            let field_schema = field.to_field_schema()?;
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
        Value::Timestamp(v) => serde_json::to_value(v).unwrap_or(JsonValue::Null),
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
        Value::Timestamp(v) => serde_json::to_value(v).unwrap_or(JsonValue::Null),
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
            Ok(Value::String(string_val.to_string()))
        },
        FieldSchema::Timestamp { .. } => {
            let timestamp = serde_json::from_value(json_value.clone())
                .map_err(|_| Error::InvalidFieldValue("Invalid timestamp data".to_string()))?;
            Ok(Value::Timestamp(timestamp))
        },
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
    
    let mut fields = std::collections::HashMap::new();

    // Read all field values for this entity using perform()
    for (field_type, field_schema) in &complete_schema.fields {
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
                        fields.insert("Children".to_string(), serde_json::Value::Array(children));
                    } else {
                        fields.insert("Children".to_string(), serde_json::Value::Array(vec![]));
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
                    fields.insert(field_type.as_ref().to_string(), json_value);
                }
            }
        }
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
        if let Some(ref inherits) = a.inherits_from {
            if inherits == &b.entity_type {
                return std::cmp::Ordering::Greater;
            }
        }
        // If b inherits from a, a should come first  
        if let Some(ref inherits) = b.inherits_from {
            if inherits == &a.entity_type {
                return std::cmp::Ordering::Less;
            }
        }
        // Otherwise sort alphabetically for consistency
        a.entity_type.cmp(&b.entity_type)
    });

    // First, restore schemas in dependency order
    let mut schema_requests = Vec::new();
    for json_schema in &sorted_schemas {
        let schema = json_schema.to_entity_schema()?;
        schema_requests.push(crate::Request::SchemaUpdate { 
            schema, 
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

    // Set field values (except Children - we'll handle that last)
    let mut write_requests = Vec::new();
    for (field_name, json_value) in &json_entity.fields {
        if field_name == "Children" {
            continue; // Handle children separately
        }

        let field_type = crate::FieldType::from(field_name.clone());
        if let Some(field_schema) = complete_schema.fields.get(&field_type) {
            match crate::json_value_to_value(json_value, field_schema) {
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
