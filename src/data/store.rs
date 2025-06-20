use std::{collections::HashMap, error, mem::discriminant};

use crate::{data::{now, request::WriteOption, EntityType, FieldType, Shared, Timestamp}, Entity, EntityId, EntitySchema, Field, Request, Result, Snowflake, Value};

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

#[derive(Debug, Clone)]
pub struct ValueTypeMismatch(EntityId, FieldType, Value, Value);
impl error::Error for ValueTypeMismatch {}
impl std::fmt::Display for ValueTypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value type mismatch for entity {} field {}: expected {:?}, got {:?}", self.0, self.1, self.2, self.3)
    }
}

pub struct Context {
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

    pub async fn create_entity(
        &mut self,
        context: &Context,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity>
    {
        if !self.schema.contains_key(&entity_type) {
            return Err(EntityTypeNotFound(entity_type.clone()).into());
        }
        
        if let Some(parent) = parent_id {
            if !self.entity_exists(context, &parent).await {
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
                .map(|(field_type, _)| {
                    Request::Write{
                        entity_id: entity_id.clone(),
                        field_type: field_type.clone(),
                        value: None,
                        write_time: None,
                        writer_id: None,
                        write_option: WriteOption::Normal
                    }
                })
                .collect::<Vec<Request>>();
        }

        Ok(Entity::new(entity_id))
    }

    pub async fn get_entity_schema(&self, _: &Context, entity_type: &EntityType) -> Result<&EntitySchema> {
        self.schema.get(entity_type).ok_or_else(|| EntityTypeNotFound(entity_type.clone()).into())
    }

    pub async fn entity_exists(&self, _: &Context, entity_id: &EntityId) -> bool {
        self.field.contains_key(entity_id)
    }

    pub async fn field_exists(&self, _: &Context, entity_type: &EntityType, field_type: &FieldType) -> bool {
        self.schema.get(entity_type)
            .map(|schema| schema.fields.contains_key(field_type))
            .unwrap_or(false)
    }

    pub async fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
        for request in requests {
            match request {
                Request::Read { entity_id, field_type , value, write_time, writer_id } => {
                    self.read(ctx, entity_id, field_type, value, write_time, writer_id).await?;
                }
                Request::Write { entity_id, field_type, value, write_time, writer_id, write_option } => {
                    self.write(ctx, entity_id, field_type, value, write_time, writer_id, write_option).await?;
                }
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        _: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Shared<Option<Value>>,
        write_time: &Shared<Option<Timestamp>>,
        writer_id: &Shared<Option<EntityId>>,
    ) -> Result<()> {
        let field = self.field
            .get(entity_id)
            .and_then(|fields| fields.get(field_type));

        if let Some(field) = field {
            value.set(field.get_value().await).await;
            write_time.set(field.get_write_time().await).await;
            writer_id.set(field.get_writer_id().await).await;
        } else {
            return Err(FieldNotFound(entity_id.clone(), field_type.clone()).into());
        }

        Ok(())
    }

    async fn write(
        &mut self,
        _: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Option<Value>,
        write_time: &Option<Timestamp>,
        writer_id: &Option<EntityId>,
        write_option: &WriteOption,
    ) -> Result<()> {
        let field_schema = self.schema.get(entity_id.get_type())
            .and_then(|schema| schema.fields.get(field_type))
            .ok_or_else(|| FieldNotFound(entity_id.clone(), field_type.clone()))?;

        let fields = self.field.entry(entity_id.clone())
            .or_insert_with(HashMap::new);

        let field = fields.entry(field_type.clone())
            .or_insert_with(|| Field::new(entity_id.clone(), field_type.clone()));

        let mut new_value: Option<Value> = value.clone();
        // Check that the value being written is the same type as the field schema
        // If the value is None, use the default value from the schema
        if let Some(value) = value {
            if discriminant(value) != discriminant(&field_schema.default_value) {
                return Err(ValueTypeMismatch(entity_id.clone(), field_type.clone(), field_schema.default_value.clone(), value.clone()).into());
            }
        } else {
            new_value = Some(field_schema.default_value.clone().into());
        }

        match write_option {
            WriteOption::Normal => {
                // Normal write, overwrite existing value
                field.set_value(new_value).await;
                if let Some(write_time) = write_time {
                    field.set_write_time(Some(write_time.clone())).await;
                } else {
                    field.set_write_time(Some(now())).await;
                }
                if let Some(writer_id) = writer_id {
                    field.set_writer_id(Some(writer_id.clone())).await;
                } else {
                    field.set_writer_id(None).await;
                }
            }
            WriteOption::Changes => {
                // Changes write, only update if the value is different
                if field.get_value().await != new_value {
                    field.set_value(new_value).await;
                    if let Some(write_time) = write_time {
                        field.set_write_time(Some(write_time.clone())).await;
                    } else {
                        field.set_write_time(Some(now())).await;
                    }
                    if let Some(writer_id) = writer_id {
                        field.set_writer_id(Some(writer_id.clone())).await;
                    } else {
                        field.set_writer_id(None).await;
                    }
                }
            }
        }

        Ok(())
    }
}