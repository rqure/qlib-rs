use std::hash::Hash;

use qlib_rs_derive::{RespDecode, RespEncode};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::{data::{resp::RespDecode as RespDecodeT, EntityType, FieldSchema, FieldType}, StoreTrait, Value};

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

impl<ET: PartialEq, FT: Eq + Hash> EntitySchema<Single, ET, FT> {
    pub fn new(entity_type: ET, inherit: Vec<ET>) -> Self {
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

#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct EntitySchemaResp {
    pub entity_type: String,
    pub inherit: Vec<String>,
    pub fields: Vec<FieldSchemaResp>,
}

impl EntitySchemaResp {
    /// Convert from EntitySchemaResp to EntitySchema<Single, String, String>
    pub fn to_entity_schema(self, _store: &impl StoreTrait) -> crate::Result<EntitySchema<Single, String, String>> {
        let fields = self.fields
            .into_iter()
            .map(|field_resp| {
                let field_type = field_resp.field_type.clone();
                let field_schema = field_resp.to_field_schema();
                Ok((field_type, field_schema))
            })
            .collect::<Result<rustc_hash::FxHashMap<String, FieldSchema<String>>, crate::Error>>()?;

        Ok(EntitySchema {
            entity_type: self.entity_type,
            inherit: self.inherit,
            fields,
            _marker: std::marker::PhantomData,
        })
    }
}

#[derive(Debug, Clone, RespEncode, RespDecode)]
pub struct FieldSchemaResp {
    pub field_type: String,
    pub rank: i64,
    pub default_value: Value,
}

impl FieldSchemaResp {
    /// Convert from FieldSchemaResp to FieldSchema<String>
    pub fn to_field_schema(self) -> FieldSchema<String> {
        // Determine the field schema variant based on the default value type
        match self.default_value {
            Value::Blob(data) => FieldSchema::Blob {
                field_type: self.field_type,
                default_value: data,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::Bool(val) => FieldSchema::Bool {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::Choice(val) => FieldSchema::Choice {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                choices: Vec::new(), // TODO: Consider adding choices to FieldSchemaResp
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::EntityList(val) => FieldSchema::EntityList {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::EntityReference(val) => FieldSchema::EntityReference {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::Float(val) => FieldSchema::Float {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::Int(val) => FieldSchema::Int {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::String(val) => FieldSchema::String {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
            Value::Timestamp(val) => FieldSchema::Timestamp {
                field_type: self.field_type,
                default_value: val,
                rank: self.rank,
                storage_scope: crate::data::field_schema::StorageScope::Runtime,
            },
        }
    }
}