use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    data::StoreTrait, EntityId, EntityType, FieldType, NotificationReceiver, NotificationSender, Request, Value
};

#[derive(Debug)]
pub struct Cache<T: StoreTrait + Send + Sync + 'static> {
    pub store: Arc<RwLock<T>>,

    pub entity_type: EntityType,
    pub index_fields: Vec<FieldType>,
    pub other_fields: Vec<FieldType>,

    // Mapping from index values to entity IDs
    // This can be a one to many mapping.
    // For instance, two different entities can have the same Name field type value.
    pub entity_ids_by_index_fields: HashMap<Vec<Value>, Vec<EntityId>>,

    // Mapping from entity ID to acquire other (context) fields.
    // For instance, an entity with a specific ID may have different values for the other fields.
    pub fields_by_entity_id: HashMap<EntityId, HashMap<FieldType, Value>>,

    // Notification for keeping the cache up to date
    pub notify_channel: (NotificationSender, NotificationReceiver),
}

impl<T: StoreTrait + Send + Sync + 'static> Cache<T> {
    pub async fn new(
        store: Arc<RwLock<T>>,
        entity_type: EntityType,
        index_fields: Vec<FieldType>,
        other_fields: Vec<FieldType>,
    ) -> crate::Result<Self> {
        let (sender, receiver) = crate::notification_channel();

        // Register notifications for all fields
        for field in index_fields.iter() {
            store
                .write()
                .await
                .register_notification(
                    crate::NotifyConfig::EntityType {
                        entity_type: entity_type.clone(),
                        field_type: field.clone(),
                        trigger_on_change: true,
                        context: vec![],
                    },
                    sender.clone(),
                ).await?;
        }

        for field in other_fields.iter() {
            store
                .write()
                .await
                .register_notification(
                    crate::NotifyConfig::EntityType {
                        entity_type: entity_type.clone(),
                        field_type: field.clone(),
                        trigger_on_change: true,
                        context: vec![],
                    },
                    sender.clone(),
                ).await?;
        }

        // Read initial values from the store
        let mut entity_ids_by_index_fields = HashMap::new();
        let mut fields_by_entity_id = HashMap::new();

        let entity_ids = store.read().await.find_entities(&entity_type, None).await?;
        for entity_id in entity_ids {
            let mut reqs = Vec::new();
            for field in index_fields.iter() {
                reqs.push(crate::sread!(entity_id.clone(), field.clone()));
            }
            
            for field in other_fields.iter() {
                reqs.push(crate::sread!(entity_id.clone(), field.clone()));
            }

            store.write().await.perform_mut(&mut reqs).await?;

            let index_key = reqs[..index_fields.len()]
                .iter()
                .filter_map(|req| req.value().cloned())
                .collect::<Vec<crate::Value>>();

            let all_fields = reqs[index_fields.len()..]
                .iter()
                .filter_map(|req| {
                    if let (Some(field_type), Some(value)) = (req.field_type(), req.value()) {
                        Some((field_type.clone(), value.clone()))
                    } else {
                        None
                    }
                })
                .chain(
                    reqs[..index_fields.len()]
                        .iter()
                        .filter_map(|req| {
                            if let (Some(field_type), Some(value)) = (req.field_type(), req.value()) {
                                Some((field_type.clone(), value.clone()))
                            } else {
                                None
                            }
                        }),
                )
                .collect::<HashMap<crate::FieldType, crate::Value>>();

            entity_ids_by_index_fields
                .entry(index_key)
                .or_insert_with(Vec::new)
                .push(entity_id.clone());

            fields_by_entity_id.insert(entity_id, all_fields);
        }

        Ok(Cache {
            entity_type,
            index_fields,
            other_fields,
            entity_ids_by_index_fields,
            fields_by_entity_id,
            notify_channel: (sender, receiver),
            store,
        })
    }
}

impl<T: StoreTrait + Send + Sync + 'static> Cache<T> {
    pub fn process_notifications(&mut self) {
        loop {
            match self.notify_channel.1.try_recv() {
                Ok(notification) => {
                    // Extract entity_id and field_type from the current request
                    if let Request::Read { entity_id, field_type, value: current_value, .. } = &notification.current {
                        if let Request::Read { value: previous_value, .. } = &notification.previous {
                            if let Some(curr_val) = current_value {
                                self.fields_by_entity_id
                                    .entry(entity_id.clone())
                                    .or_default()
                                    .insert(field_type.clone(), curr_val.clone());
                            }

                            // If the field type is one of the index fields, we need to update the index
                            if self.index_fields.contains(field_type) {
                                // Remove old entry if it exists
                                if let Some(prev_val) = previous_value {
                                    let old_index_key = self.make_index_key(entity_id, field_type, prev_val);
                                    self.entity_ids_by_index_fields.remove(&old_index_key);
                                }
                                
                                // Add new entry
                                if let Some(curr_val) = current_value {
                                    let new_index_key = self.make_index_key(entity_id, field_type, curr_val);
                                    self.entity_ids_by_index_fields
                                        .entry(new_index_key)
                                        .or_insert_with(Vec::new)
                                        .push(entity_id.clone());
                                }
                            }
                        }
                    }
                },
                Err(_) => {
                    /* No notification to process */
                    break;
                }
            }
        }
    }

    fn make_index_key(
        &self,
        entity_id: &EntityId,
        field_type: &FieldType,
        value: &Value,
    ) -> Vec<Value> {
        let mut index_key = Vec::new();

        for field in &self.index_fields {
            if field == field_type {
                index_key.push(value.clone());
            } else {
                let other_value = self
                    .fields_by_entity_id
                    .get(entity_id)
                    .and_then(|fields| fields.get(field));

                index_key.push(other_value.unwrap().clone());
            }
        }

        index_key
    }

    pub fn get(&self, index_key: Vec<Value>) -> Option<Vec<HashMap<FieldType, Value>>> {
        self.entity_ids_by_index_fields
            .get(&index_key)
            .map(|entity_ids| {
                entity_ids
                    .iter()
                    .filter_map(|entity_id| self.fields_by_entity_id.get(entity_id).cloned())
                    .collect()
            })
    }

    pub fn get_unique(&self, index_key: Vec<Value>) -> Option<HashMap<FieldType, Value>> {
        return self.get(index_key).and_then(|entities| {
            if entities.len() == 1 {
                Some(entities[0].clone())
            } else {
                None
            }
        });
    }
}

impl<T: StoreTrait + Send + Sync + 'static> Drop for Cache<T> {
    fn drop(&mut self) {
        // Clone the necessary data for the cleanup task
        let store = self.store.clone();
        let entity_type = self.entity_type.clone();
        let index_fields = self.index_fields.clone();
        let other_fields = self.other_fields.clone();
        let sender = self.notify_channel.0.clone();

        // Spawn a task to handle async cleanup
        // This is a best-effort cleanup - if the runtime is shutting down, this may not complete
        tokio::spawn(async move {
            let mut store_guard = store.write().await;

            // Unregister notifications for index fields
            for field in index_fields.iter() {
                let config = crate::NotifyConfig::EntityType {
                    entity_type: entity_type.clone(),
                    field_type: field.clone(),
                    trigger_on_change: true,
                    context: vec![],
                };
                store_guard.unregister_notification(&config, &sender).await;
            }

            // Unregister notifications for other fields
            for field in other_fields.iter() {
                let config = crate::NotifyConfig::EntityType {
                    entity_type: entity_type.clone(),
                    field_type: field.clone(),
                    trigger_on_change: true,
                    context: vec![],
                };
                store_guard.unregister_notification(&config, &sender).await;
            }
        });
    }
}
