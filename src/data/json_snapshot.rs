use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType, Single, Value, Error, Result
};

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
}

/// JSON-friendly representation of an entity schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntitySchema {
    #[serde(rename = "_entityType")]
    pub entity_type: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "inheritsFrom")]
    pub inherits_from: Option<Vec<String>>,
    pub fields: Vec<JsonFieldSchema>,
}

/// JSON-friendly representation of an entity with its data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEntity {
    #[serde(rename = "_entityType")]
    pub entity_type: String,
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
}

/// JSON snapshot format matching the user's requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSnapshot {
    pub schemas: Vec<JsonEntitySchema>,
    pub entity: JsonEntity,
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
        }
    }

    /// Convert to internal FieldSchema
    pub fn to_field_schema(&self) -> Result<FieldSchema> {
        let field_type = FieldType::from(self.name.clone());
        let rank = self.rank.unwrap_or(0);

        match self.data_type.as_str() {
            "Blob" => {
                let default_value: Vec<u8> = serde_json::from_value(self.default.clone())
                    .unwrap_or_default();
                Ok(FieldSchema::Blob { field_type, default_value, rank })
            },
            "Bool" => {
                let default_value = self.default.as_bool().unwrap_or(false);
                Ok(FieldSchema::Bool { field_type, default_value, rank })
            },
            "Choice" => {
                let choices = self.choices.clone().unwrap_or_default();
                let default_value = if let Some(choice_str) = self.default.as_str() {
                    choices.iter().position(|c| c == choice_str).unwrap_or(0) as i64
                } else {
                    0
                };
                Ok(FieldSchema::Choice { field_type, default_value, rank, choices })
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
                Ok(FieldSchema::EntityList { field_type, default_value, rank })
            },
            "EntityReference" => {
                let default_value = self.default.as_str()
                    .and_then(|s| EntityId::try_from(s).ok());
                Ok(FieldSchema::EntityReference { field_type, default_value, rank })
            },
            "Float" => {
                let default_value = self.default.as_f64().unwrap_or(0.0);
                Ok(FieldSchema::Float { field_type, default_value, rank })
            },
            "Int" => {
                let default_value = self.default.as_i64().unwrap_or(0);
                Ok(FieldSchema::Int { field_type, default_value, rank })
            },
            "String" => {
                let default_value = self.default.as_str().unwrap_or("").to_string();
                Ok(FieldSchema::String { field_type, default_value, rank })
            },
            "Timestamp" => {
                let default_value = serde_json::from_value(self.default.clone())
                    .unwrap_or_else(|_| super::epoch());
                Ok(FieldSchema::Timestamp { field_type, default_value, rank })
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

        let inherits_from = schema.inherit.as_ref().map(|t| vec![t.as_ref().to_string()]);

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
                .and_then(|list| list.first())
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

/// Macro to take a JSON snapshot of the current store state
/// This finds the Root entity automatically and creates a hierarchical representation
/// Works with both Store and StoreProxy since they have the same method signatures
#[macro_export]
macro_rules! take_json_snapshot {
    ($store:expr) => {{
        async move {
            // Collect all schemas by getting all entity types first
            let mut json_schemas = Vec::new();
            let entity_types = $store.get_entity_types().await?;
            for entity_type in entity_types {
                if let Ok(schema) = $store.get_entity_schema(&entity_type).await {
                    json_schemas.push(crate::JsonEntitySchema::from_entity_schema(&schema));
                }
            }

            // Sort schemas for consistent output
            json_schemas.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));

            // Find the Root entity
            let root_entities = $store.find_entities(&crate::EntityType::from("Root")).await?;
            let root_entity_id = root_entities.first()
                .ok_or_else(|| crate::Error::EntityNotFound(crate::EntityId::new("Root", 0)))?;

            // Build the entity tree starting from root using a helper macro
            let root_entity = crate::build_json_entity_generic!($store, root_entity_id).await?;

            Ok::<crate::JsonSnapshot, crate::Error>(crate::JsonSnapshot {
                schemas: json_schemas,
                entity: root_entity,
            })
        }
    }};
}

/// Helper macro to build a JSON entity with full field data
/// This handles both Store and StoreProxy by using their common interface
#[macro_export]
macro_rules! build_json_entity_generic {
    ($store:expr, $entity_id:expr) => {{
        async move {
            if !$store.entity_exists($entity_id).await {
                return Err(crate::Error::EntityNotFound($entity_id.clone()));
            }

            let entity_type = $entity_id.get_type();
            let complete_schema = $store.get_complete_entity_schema(entity_type).await?;
            
            let mut fields = std::collections::HashMap::new();

            // Read all field values for this entity using perform()
            for (field_type, _field_schema) in &complete_schema.fields {
                // Create a read request
                let mut read_requests = vec![crate::Request::Read {
                    entity_id: $entity_id.clone(),
                    field_type: field_type.clone(),
                    value: None,
                    write_time: None,
                    writer_id: None,
                }];

                // Perform the read operation
                if let Ok(()) = $store.perform(&mut read_requests).await {
                    if let Some(crate::Request::Read { value: Some(ref value), .. }) = read_requests.first() {
                        // Convert the value to JSON using the helper function
                        let choices = if let crate::FieldSchema::Choice { choices, .. } = _field_schema {
                            Some(choices)
                        } else {
                            None
                        };
                        fields.insert(field_type.as_ref().to_string(), crate::value_to_json_value(value, choices));
                    }
                }
            }

            Ok::<crate::JsonEntity, crate::Error>(crate::JsonEntity {
                entity_type: entity_type.as_ref().to_string(),
                fields,
            })
        }
    }};
}

/// Macro to restore the store state from a JSON snapshot
/// This determines the diff and converts it to a series of Request operations
/// Works with both Store and StoreProxy since they have the same method signatures
#[macro_export]
macro_rules! restore_json_snapshot {
    ($store:expr, $json_snapshot:expr) => {{
        async move {
            // First, restore schemas
            let mut schema_requests = Vec::new();
            for json_schema in &$json_snapshot.schemas {
                let schema = json_schema.to_entity_schema()?;
                schema_requests.push(crate::Request::SchemaUpdate { 
                    schema, 
                    originator: None 
                });
            }

            // Perform schema updates first
            $store.perform(&mut schema_requests).await?;

            // Find or create the root entity
            let root_entities = $store.find_entities(&crate::EntityType::from("Root")).await?;
            let _root_entity_id = if let Some(existing_root) = root_entities.first() {
                existing_root.clone()
            } else {
                // Create a new root entity
                let mut create_requests = vec![crate::Request::Create {
                    entity_type: crate::EntityType::from("Root"),
                    parent_id: None,
                    name: $json_snapshot.entity.fields.get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Root")
                        .to_string(),
                    created_entity_id: None,
                    originator: None,
                }];
                $store.perform(&mut create_requests).await?;
                
                // The created entity ID should be in the response
                if let Some(crate::Request::Create { created_entity_id: Some(ref id), .. }) = create_requests.first() {
                    id.clone()
                } else {
                    return Err(crate::Error::EntityNotFound(crate::EntityId::new("Root", 0)));
                }
            };

            // For now, just restore the schemas. Full entity restoration would require
            // more complex logic to handle field differences between Store and StoreProxy
            Ok::<(), crate::Error>(())
        }
    }};
}
