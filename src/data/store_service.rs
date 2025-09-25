use tokio::sync::{mpsc, oneshot};
use std::cell::RefCell;
use std::rc::Rc;

use crate::{
    EntityId, EntitySchema, EntityType, FieldSchema, FieldType,
    NotificationQueue, NotifyConfig, PageOpts, PageResult, Requests, Result, Single,
    StoreProxy, StoreTrait,
};

/// Message types for the StoreService actor
#[derive(Debug)]
enum StoreServiceMessage {
    GetEntityType {
        name: String,
        respond_to: oneshot::Sender<Result<EntityType>>,
    },
    ResolveEntityType {
        entity_type: EntityType,
        respond_to: oneshot::Sender<Result<String>>,
    },
    GetFieldType {
        name: String,
        respond_to: oneshot::Sender<Result<FieldType>>,
    },
    ResolveFieldType {
        field_type: FieldType,
        respond_to: oneshot::Sender<Result<String>>,
    },
    GetEntitySchema {
        entity_type: EntityType,
        respond_to: oneshot::Sender<Result<EntitySchema<Single>>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: oneshot::Sender<Result<FieldSchema>>,
    },
    SetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        schema: FieldSchema,
        respond_to: oneshot::Sender<Result<()>>,
    },
    EntityExists {
        entity_id: EntityId,
        respond_to: oneshot::Sender<bool>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: oneshot::Sender<bool>,
    },
    ResolveIndirection {
        entity_id: EntityId,
        fields: Vec<FieldType>,
        respond_to: oneshot::Sender<Result<(EntityId, FieldType)>>,
    },
    Perform {
        requests: Requests,
        respond_to: oneshot::Sender<Result<Requests>>,
    },
    PerformMut {
        requests: Requests,
        respond_to: oneshot::Sender<Result<Requests>>,
    },
    FindEntitiesPaginated {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        respond_to: oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    FindEntitiesExact {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        respond_to: oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    FindEntities {
        entity_type: EntityType,
        filter: Option<String>,
        respond_to: oneshot::Sender<Result<Vec<EntityId>>>,
    },
    GetEntityTypes {
        respond_to: oneshot::Sender<Result<Vec<EntityType>>>,
    },
    GetEntityTypesPaginated {
        page_opts: Option<PageOpts>,
        respond_to: oneshot::Sender<Result<PageResult<EntityType>>>,
    },
    RegisterNotification {
        config: NotifyConfig,
        sender: NotificationQueue,
        respond_to: oneshot::Sender<Result<()>>,
    },
    UnregisterNotification {
        config: NotifyConfig,
        sender: NotificationQueue,
        respond_to: oneshot::Sender<bool>,
    },
}

/// StoreService that follows the actor pattern
/// Provides wrapper functionality over StoreProxy using async Rust with mpsc and oneshot channels
/// This is the actual actor that owns and manages the StoreProxy
pub struct StoreService {
    receiver: mpsc::UnboundedReceiver<StoreServiceMessage>,
    store_proxy: Rc<RefCell<StoreProxy>>,
}

/// StoreHandle is a cloneable handle used by other service actors to communicate with the StoreService
/// It provides a lightweight interface for sending messages to the StoreService actor
#[derive(Debug, Clone)]
pub struct StoreHandle {
    sender: mpsc::UnboundedSender<StoreServiceMessage>,
}

impl std::fmt::Debug for StoreService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreService")
            .field("receiver", &"<mpsc::UnboundedReceiver>")
            .field("store_proxy", &"<StoreProxy>")
            .finish()
    }
}

impl StoreService {
    /// Create a new StoreService with the given StoreProxy and return both the service and a handle
    /// The StoreService is the actor that owns the StoreProxy and processes messages
    pub fn new(store_proxy: StoreProxy) -> (Self, StoreHandle) {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        let service = Self {
            receiver,
            store_proxy: Rc::new(RefCell::new(store_proxy)),
        };
        
        let handle = StoreHandle {
            sender,
        };
        
        (service, handle)
    }

    /// Run the StoreService actor - this processes messages from the channel
    /// This should be called in an async context to handle incoming messages
    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            self.process_message(message);
        }
    }

    /// Process a single message by calling the appropriate StoreProxy method
    fn process_message(&self, message: StoreServiceMessage) {
        let mut proxy = self.store_proxy.borrow_mut();
        match message {
            StoreServiceMessage::GetEntityType { name, respond_to } => {
                let result = proxy.get_entity_type(&name);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::ResolveEntityType { entity_type, respond_to } => {
                let result = proxy.resolve_entity_type(entity_type);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::GetFieldType { name, respond_to } => {
                let result = proxy.get_field_type(&name);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::ResolveFieldType { field_type, respond_to } => {
                let result = proxy.resolve_field_type(field_type);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::GetEntitySchema { entity_type, respond_to } => {
                let result = proxy.get_entity_schema(entity_type);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::GetFieldSchema { entity_type, field_type, respond_to } => {
                let result = proxy.get_field_schema(entity_type, field_type);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::SetFieldSchema { entity_type, field_type, schema, respond_to } => {
                let result = proxy.set_field_schema(entity_type, field_type, schema);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::EntityExists { entity_id, respond_to } => {
                let result = proxy.entity_exists(entity_id);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::FieldExists { entity_type, field_type, respond_to } => {
                let result = proxy.field_exists(entity_type, field_type);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::ResolveIndirection { entity_id, fields, respond_to } => {
                let result = proxy.resolve_indirection(entity_id, &fields);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::Perform { requests, respond_to } => {
                let result = proxy.perform(requests);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::PerformMut { requests, respond_to } => {
                let result = proxy.perform_mut(requests);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::FindEntitiesPaginated { entity_type, page_opts, filter, respond_to } => {
                let result = proxy.find_entities_paginated(entity_type, page_opts.as_ref(), filter.as_deref());
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::FindEntitiesExact { entity_type, page_opts, filter, respond_to } => {
                let result = proxy.find_entities_exact(entity_type, page_opts.as_ref(), filter.as_deref());
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::FindEntities { entity_type, filter, respond_to } => {
                let result = proxy.find_entities(entity_type, filter.as_deref());
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::GetEntityTypes { respond_to } => {
                let result = proxy.get_entity_types();
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::GetEntityTypesPaginated { page_opts, respond_to } => {
                let result = proxy.get_entity_types_paginated(page_opts.as_ref());
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::RegisterNotification { config, sender, respond_to } => {
                let result = proxy.register_notification(config, sender);
                let _ = respond_to.send(result);
            }
            StoreServiceMessage::UnregisterNotification { config, sender, respond_to } => {
                let result = proxy.unregister_notification(&config, &sender);
                let _ = respond_to.send(result);
            }
        }
    }
}

impl StoreHandle {
    /// Helper method to send a message and get the response asynchronously
    async fn send_and_receive<T, F>(&self, message_builder: F) -> T
    where
        F: FnOnce(oneshot::Sender<T>) -> StoreServiceMessage,
    {
        let (tx, rx) = oneshot::channel();
        let message = message_builder(tx);
        
        // Send the message to the StoreService actor
        if self.sender.send(message).is_err() {
            panic!("StoreService actor has been dropped");
        }
        
        // Await the response
        rx.await.unwrap_or_else(|_| panic!("Actor response channel closed"))
    }

    /// Get entity type by name
    pub async fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetEntityType {
            name: name.to_string(),
            respond_to,
        }).await
    }

    /// Resolve entity type to name
    pub async fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        self.send_and_receive(|respond_to| StoreServiceMessage::ResolveEntityType {
            entity_type,
            respond_to,
        }).await
    }

    /// Get field type by name
    pub async fn get_field_type(&self, name: &str) -> Result<FieldType> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetFieldType {
            name: name.to_string(),
            respond_to,
        }).await
    }

    /// Resolve field type to name
    pub async fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        self.send_and_receive(|respond_to| StoreServiceMessage::ResolveFieldType {
            field_type,
            respond_to,
        }).await
    }

    /// Get entity schema
    pub async fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetEntitySchema {
            entity_type,
            respond_to,
        }).await
    }

    /// Get field schema
    pub async fn get_field_schema(&self, entity_type: EntityType, field_type: FieldType) -> Result<FieldSchema> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetFieldSchema {
            entity_type,
            field_type,
            respond_to,
        }).await
    }

    /// Set field schema
    pub async fn set_field_schema(&self, entity_type: EntityType, field_type: FieldType, schema: FieldSchema) -> Result<()> {
        self.send_and_receive(|respond_to| StoreServiceMessage::SetFieldSchema {
            entity_type,
            field_type,
            schema,
            respond_to,
        }).await
    }

    /// Check if entity exists
    pub async fn entity_exists(&self, entity_id: EntityId) -> bool {
        self.send_and_receive(|respond_to| StoreServiceMessage::EntityExists {
            entity_id,
            respond_to,
        }).await
    }

    /// Check if field exists for entity type
    pub async fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        self.send_and_receive(|respond_to| StoreServiceMessage::FieldExists {
            entity_type,
            field_type,
            respond_to,
        }).await
    }

    /// Resolve indirection
    pub async fn resolve_indirection(&self, entity_id: EntityId, fields: &[FieldType]) -> Result<(EntityId, FieldType)> {
        self.send_and_receive(|respond_to| StoreServiceMessage::ResolveIndirection {
            entity_id,
            fields: fields.to_vec(),
            respond_to,
        }).await
    }

    /// Perform operations
    pub async fn perform(&self, requests: Requests) -> Result<Requests> {
        self.send_and_receive(|respond_to| StoreServiceMessage::Perform {
            requests,
            respond_to,
        }).await
    }

    /// Perform mutable operations
    pub async fn perform_mut(&self, requests: Requests) -> Result<Requests> {
        self.send_and_receive(|respond_to| StoreServiceMessage::PerformMut {
            requests,
            respond_to,
        }).await
    }

    /// Find entities with pagination
    pub async fn find_entities_paginated(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::FindEntitiesPaginated {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            respond_to,
        }).await
    }

    /// Find entities exactly with pagination
    pub async fn find_entities_exact(&self, entity_type: EntityType, page_opts: Option<&PageOpts>, filter: Option<&str>) -> Result<PageResult<EntityId>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::FindEntitiesExact {
            entity_type,
            page_opts: page_opts.cloned(),
            filter: filter.map(|s| s.to_string()),
            respond_to,
        }).await
    }

    /// Find entities
    pub async fn find_entities(&self, entity_type: EntityType, filter: Option<&str>) -> Result<Vec<EntityId>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::FindEntities {
            entity_type,
            filter: filter.map(|s| s.to_string()),
            respond_to,
        }).await
    }

    /// Get all entity types
    pub async fn get_entity_types(&self) -> Result<Vec<EntityType>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetEntityTypes {
            respond_to,
        }).await
    }

    /// Get entity types with pagination
    pub async fn get_entity_types_paginated(&self, page_opts: Option<&PageOpts>) -> Result<PageResult<EntityType>> {
        self.send_and_receive(|respond_to| StoreServiceMessage::GetEntityTypesPaginated {
            page_opts: page_opts.cloned(),
            respond_to,
        }).await
    }

    /// Register notification
    pub async fn register_notification(&self, config: NotifyConfig, sender: NotificationQueue) -> Result<()> {
        self.send_and_receive(|respond_to| StoreServiceMessage::RegisterNotification {
            config,
            sender,
            respond_to,
        }).await
    }

    /// Unregister notification
    pub async fn unregister_notification(&self, config: &NotifyConfig, sender: &NotificationQueue) -> bool {
        self.send_and_receive(|respond_to| StoreServiceMessage::UnregisterNotification {
            config: config.clone(),
            sender: sender.clone(),
            respond_to,
        }).await
    }
}