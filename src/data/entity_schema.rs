use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::data::{EntityType, FieldSchema, FieldType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Single;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Complete;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntitySchema<T> {
    pub entity_type: EntityType,
    pub inherit: Option<EntityType>,
    pub fields: HashMap<FieldType, FieldSchema>,
    
    _marker: std::marker::PhantomData<T>,
}

impl EntitySchema<Single> {
    pub fn new(entity_type: impl Into<EntityType>, inherit: Option<EntityType>) -> Self {
        Self {
            entity_type: entity_type.into(),
            inherit,
            fields: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl EntitySchema<Complete> {
    pub fn new(entity_type: impl Into<EntityType>) -> Self {
        Self {
            entity_type: entity_type.into(),
            inherit: None,
            fields: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> EntitySchema<T> {
    pub fn diff(&self, other: &EntitySchema<T>) -> Vec<FieldSchema> {
        self.fields
            .values()
            .filter(|v| !other.fields.contains_key(&v.field_type))
            .cloned()
            .collect()
    }
}

impl From<EntitySchema<Single>> for EntitySchema<Complete> {
    fn from(schema: EntitySchema<Single>) -> Self {
        Self {
            entity_type: schema.entity_type,
            inherit: schema.inherit,
            fields: schema.fields,
            _marker: std::marker::PhantomData,
        }
    }
}