use std::{collections::HashMap, error};

use crate::{data::{EntityType, FieldType}, Entity, EntityId, EntitySchema, Field, Request, Result, Snowflake};

#[derive(Debug, Clone)]
pub struct EntityExists(EntityId);
impl error::Error for EntityExists {}
impl std::fmt::Display for EntityExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity already exists: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EntityTypeNotFound(EntityType);
impl error::Error for EntityTypeNotFound {}
impl std::fmt::Display for EntityTypeNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown entity type: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EntityNotFound(EntityId);
impl error::Error for EntityNotFound {}
impl std::fmt::Display for EntityNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity not found: {}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct FieldNotFound(EntityId, FieldType);
impl error::Error for FieldNotFound {}
impl std::fmt::Display for FieldNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) ->
        std::fmt::Result {
        write!(f, "Field not found for entity {}: {}", self.0, self.1)
    }
}

pub struct StoreContext {
}

pub struct MapStore {
    schema: HashMap<EntityType, EntitySchema>,
    entity: HashMap<EntityType, Vec<EntityId>>,
    types: Vec<EntityType>,
    field: HashMap<EntityId, HashMap<FieldType, Field>>,
    snowflake: Snowflake
}

impl MapStore {
    pub fn new() -> Self {
        MapStore {
            schema: HashMap::new(),
            entity: HashMap::new(),
            types: Vec::new(),
            field: HashMap::new(),
            snowflake: Snowflake::new(),
        }
    }

    pub fn create_entity(
        &mut self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity>
    {
        if !self.schema.contains_key(&entity_type) {
            return Err(EntityTypeNotFound(entity_type.clone()).into());
        }
        
        if let Some(parent) = parent_id {
            if !self.entity_exists(&parent) {
                return Err(EntityNotFound(parent).into());
            }
        }

        let entity_id = EntityId::new(&entity_type, self.snowflake.generate());
        if self.field.contains_key(&entity_id) {
            return Err(EntityExists(entity_id).into());
        }

        {
            let entities = self.entity.entry(entity_type.clone()).or_insert_with(Vec::new);
            entities.push(entity_id.clone());
        }

        {
            self.field.entry(entity_id.clone()).or_insert_with(HashMap::new);
        }

        {
            let requests = self.schema.get(&entity_type)
                .map(|s| &s.fields)
                .into_iter()
                .flat_map(|fields| fields.iter())
                .map(|(field_type, field_schema)| {
                    Request::new(&entity_id, field_type, Some(field_schema.default_value.clone().into()))
                })
                .collect::<Vec<Request>>();
        }

        Ok(Entity::new(entity_id))
    }

    pub fn get_entity_schema(&self, entity_type: &EntityType) -> Result<&EntitySchema> {
        self.schema.get(entity_type).ok_or_else(|| EntityTypeNotFound(entity_type.clone()).into())
    }

    pub fn entity_exists(&self, entity_id: &EntityId) -> bool {
        self.field.contains_key(entity_id)
    }

    pub fn field_exists(&self, entity_type: &EntityType, field_type: &FieldType) -> bool {
        self.schema.get(entity_type)
            .map(|schema| schema.fields.contains_key(field_type))
            .unwrap_or(false)
    }
}