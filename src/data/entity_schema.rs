use std::hash::Hash;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::{data::{EntityType, FieldSchema, FieldType}, StoreTrait};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Single;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Complete;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntitySchema<T, ET: PartialEq=EntityType, FT: Eq + Hash=FieldType> {
    pub entity_type: ET,
    pub inherit: Vec<ET>,
    pub fields: FxHashMap<FT, FieldSchema<FT>>,

    _marker: std::marker::PhantomData<T>,
}

impl EntitySchema<Single, EntityType, FieldType> {
    pub fn new(entity_type: EntityType, inherit: Vec<EntityType>) -> Self {
        Self {
            entity_type,
            inherit,
            fields: FxHashMap::default(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl EntitySchema<Complete, EntityType, FieldType> {
    pub fn new(entity_type: EntityType) -> Self {
        Self {
            entity_type,
            inherit: Vec::new(),
            fields: FxHashMap::default(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl EntitySchema<Single, EntityType, FieldType> {
    pub fn diff(&self, other: &EntitySchema<Single, EntityType, FieldType>) -> Vec<FieldSchema> {
        self.fields
            .values()
            .filter(|v| other.fields.contains_key(&v.field_type()))
            .cloned()
            .collect()
    }
}

impl EntitySchema<Complete, EntityType, FieldType> {
    pub fn diff(&self, other: &EntitySchema<Complete, EntityType, FieldType>) -> Vec<FieldSchema> {
        self.fields
            .values()
            .filter(|v| other.fields.contains_key(&v.field_type()))
            .cloned()
            .collect()
    }
}

impl From<EntitySchema<Single, EntityType, FieldType>> for EntitySchema<Complete, EntityType, FieldType> {
    fn from(schema: EntitySchema<Single, EntityType, FieldType>) -> Self {
        Self {
            entity_type: schema.entity_type,
            inherit: schema.inherit,
            fields: schema.fields,
            _marker: std::marker::PhantomData,
        }
    }
}

impl EntitySchema<Single, EntityType, FieldType> {
    pub fn from_string_schema(schema: EntitySchema<Single, String, String>, store: &impl StoreTrait) -> Self {
        Self {
            entity_type: store.get_entity_type(schema.entity_type.as_str()).expect("Entity type not found"),
            inherit: schema.inherit.into_iter().map(|et| store.get_entity_type(et.as_str()).expect("Entity type not found")).collect(),
            fields: schema
                .fields
                .into_iter()
                .map(|(k, v)| (store.get_field_type(k.as_str()).expect("Field type not found"), FieldSchema::from_string_schema(v, store)))
                .collect(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn to_string_schema(&self, store: &impl StoreTrait) -> EntitySchema<Single, String, String> {
        EntitySchema {
            entity_type: store.resolve_entity_type(self.entity_type.clone()).expect("Entity type does not exist"),
            inherit: self.inherit.iter().map(|et| store.resolve_entity_type(et.clone()).expect("Entity type does not exist")).collect(),
            fields: self
                .fields
                .iter()
                .map(|(k, v)| (store.resolve_field_type(k.clone()).expect("Field type does not exist"), v.to_string_schema(store)))
                .collect(),
            _marker: std::marker::PhantomData,
        }
    }
}