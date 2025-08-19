use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    Complete, Context, Entity, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, 
    NotificationSender, NotifyConfig, PageOpts, PageResult, Request, Result, Single, Store, StoreProxy
};

/// Enum-based store implementation that provides static dispatch
/// instead of using async traits which have limitations in Rust
#[derive(Debug, Clone)]
pub enum StoreInterface {
    SharedLocal(Arc<Mutex<Store>>),
    SharedProxy(Arc<Mutex<StoreProxy>>),
}

impl StoreInterface {
    pub fn new_shared_local(store: Store) -> Self {
        StoreInterface::SharedLocal(Arc::new(Mutex::new(store)))
    }

    pub fn new_shared_proxy(store_proxy: StoreProxy) -> Self {
        StoreInterface::SharedProxy(Arc::new(Mutex::new(store_proxy)))
    }

    pub async fn create_entity(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<Entity> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.create_entity(ctx, entity_type, parent_id, name).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.create_entity(ctx, entity_type, parent_id, name).await
            },
        }
    }

    pub async fn get_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Single>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.get_entity_schema(ctx, entity_type).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.get_entity_schema(ctx, entity_type).await
            },
        }
    }

    pub async fn get_complete_entity_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
    ) -> Result<EntitySchema<Complete>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.get_complete_entity_schema(ctx, entity_type).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.get_complete_entity_schema(ctx, entity_type).await
            },
        }
    }

    pub async fn set_entity_schema(
        &mut self,
        ctx: &Context,
        entity_schema: &EntitySchema<Single>,
    ) -> Result<()> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.set_entity_schema(ctx, entity_schema).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.set_entity_schema(ctx, entity_schema).await
            },
        }
    }

    pub async fn get_field_schema(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> Result<FieldSchema> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.get_field_schema(ctx, entity_type, field_type).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.get_field_schema(ctx, entity_type, field_type).await
            },
        }
    }

    pub async fn set_field_schema(
        &mut self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
        field_schema: FieldSchema,
    ) -> Result<()> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.set_field_schema(ctx, entity_type, field_type, field_schema).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.set_field_schema(ctx, entity_type, field_type, field_schema).await
            },
        }
    }

    pub async fn entity_exists(&self, ctx: &Context, entity_id: &EntityId) -> bool {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.entity_exists(ctx, entity_id).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.entity_exists(ctx, entity_id).await
            },
        }
    }

    pub async fn field_exists(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        field_type: &FieldType,
    ) -> bool {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.field_exists(ctx, entity_type, field_type).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.field_exists(ctx, entity_type, field_type).await
            },
        }
    }

    pub async fn perform(&mut self, ctx: &Context, requests: &mut Vec<Request>) -> Result<()> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.perform(ctx, requests).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.perform(ctx, requests).await
            },
        }
    }

    pub async fn delete_entity(&mut self, ctx: &Context, entity_id: &EntityId) -> Result<()> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.delete_entity(ctx, entity_id).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.delete_entity(ctx, entity_id).await
            },
        }
    }

    pub async fn find_entities_paginated(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.find_entities_paginated(ctx, entity_type, page_opts).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.find_entities_paginated(ctx, entity_type, page_opts).await
            },
        }
    }

    pub async fn find_entities_exact(
        &self,
        ctx: &Context,
        entity_type: &EntityType,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityId>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.find_entities_exact(ctx, entity_type, page_opts).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.find_entities_exact(ctx, entity_type, page_opts).await
            },
        }
    }

    pub async fn find_entities(
        &self,
        ctx: &Context,
        entity_type: &EntityType
    ) -> Result<Vec<EntityId>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.find_entities(ctx, entity_type).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.find_entities(ctx, entity_type).await
            },
        }
    }

    pub async fn get_entity_types(
        &self,
        ctx: &Context,
        page_opts: Option<PageOpts>,
    ) -> Result<PageResult<EntityType>> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let local = shared_local.lock().await;
                local.get_entity_types(ctx, page_opts).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let proxy = shared_proxy.lock().await;
                proxy.get_entity_types(ctx, page_opts).await
            },
        }
    }

    /// Register a notification configuration with a provided sender
    /// The sender will be added to the list of senders for this notification config
    pub async fn register_notification(
        &mut self,
        ctx: &Context,
        config: NotifyConfig,
        sender: NotificationSender,
    ) -> Result<()> {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.register_notification(ctx, config, sender).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.register_notification(ctx, config, sender).await
            },
        }
    }

    /// Unregister a notification by removing a specific sender
    /// Returns true if the sender was found and removed
    pub async fn unregister_notification(&mut self, target_config: &NotifyConfig, sender: &NotificationSender) -> bool {
        match self {
            StoreInterface::SharedLocal(shared_local) => {
                let mut local = shared_local.lock().await;
                local.unregister_notification(target_config, sender).await
            },
            StoreInterface::SharedProxy(shared_proxy) => {
                let mut proxy = shared_proxy.lock().await;
                proxy.unregister_notification(target_config, sender).await
            },
        }
    }
}
