use crate::{Complete, Context, Entity, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, NotificationCallback, NotifyConfig, PageOpts, PageResult, Request, Result, Single};

pub trait StoreTrait {
    async fn create_entity(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity>;

    async fn get_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>>;

    async fn get_complete_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>>;

    async fn set_entity_schema(
        &mut self,
        ctx: &Context,
        entity_schema: &EntitySchema<Single>,
    ) -> Result<()>;

    async fn get_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema>;

    async fn set_field_schema(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: FieldSchema,
    ) -> Result<()>;

    async fn entity_exists(&self, ctx: &Context, entity_id: &EntityId) -> bool;

    async fn field_exists(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool;

    async fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()>;

    async fn delete_entity(&mut self, ctx: &Context, entity_id: &EntityId) -> Result<()>;

    async fn find_entities_paginated(
        &self,
        _: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>>;

    async fn find_entities_exact(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>>;

    async fn find_entities(
        &self,
        ctx: &Context,
        entity_type: &EntityType
    ) -> Result<Vec<EntityId>>;

    async fn get_entity_types(
        &self,
        ctx: &Context,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>>;

    async fn register_notification(
        &mut self,
        ctx: &Context,
        config: NotifyConfig,
        callback: NotificationCallback,
    ) -> Result<()>;

    async fn unregister_notification(&mut self, target_config: &NotifyConfig) -> bool;

}