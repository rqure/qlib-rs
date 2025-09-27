use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::{
    data::StoreTrait, EntityId, EntityType, FieldType, NotificationQueue, NotifyConfig, Request, Value
};

#[derive(Debug)]
pub struct Cache {
    pub entity_type: EntityType,
    pub index_fields: Vec<FieldType>,
    pub other_fields: Vec<FieldType>,

    // Mapping from index values to entity IDs
    // This can be a one to many mapping.
    // For instance, two different entities can have the same Name field type value.
    pub entity_ids_by_index_fields: HashMap<Vec<Value>, Vec<EntityId>>,

    // Mapping from entity ID to acquire other (context) fields.
    // For instance, an entity with a specific ID may have different values for the other fields.
    pub fields_by_entity_id: FxHashMap<EntityId, FxHashMap<FieldType, Value>>,

    pub notify_queue: NotificationQueue,
}

impl Cache {
    pub fn new(
        store: &mut impl StoreTrait,
        entity_type: EntityType,
        index_fields: Vec<FieldType>,
        other_fields: Vec<FieldType>,
    ) -> crate::Result<(Self, NotificationQueue)> {
        let queue = NotificationQueue::new();

        // Register notifications for all fields
        for field_type in index_fields.iter() {
            store.register_notification(
                NotifyConfig::EntityType {
                    entity_type,
                    field_type: *field_type,
                    trigger_on_change: true,
                    context: vec![],
                },
                queue.clone(),
            )?;
        }

        for field_type in other_fields.iter() {
            store.register_notification(
                NotifyConfig::EntityType {
                    entity_type: entity_type.clone(),
                    field_type: *field_type,
                    trigger_on_change: true,
                    context: vec![],
                },
                queue.clone(),
            )?;
        }

        // Read initial values from the store
        let mut entity_ids_by_index_fields = HashMap::new();
        let mut fields_by_entity_id = FxHashMap::default();

        let entity_ids = store.find_entities(entity_type, None)?;
        for entity_id in entity_ids {
            let reqs = crate::sreq![];
            for field in index_fields.iter() {
                reqs.push(crate::sread!(entity_id, crate::sfield![*field]));
            }
            
            for field in other_fields.iter() {
                reqs.push(crate::sread!(entity_id, crate::sfield![*field]));
            }

            let reqs = store.perform_mut(reqs)?;

            let index_key = reqs.read()[..index_fields.len()]
                .iter()
                .filter_map(|req| req.value().cloned())
                .collect::<Vec<crate::Value>>();

            let all_fields = reqs.read()[index_fields.len()..]
                .iter()
                .filter_map(|req| {
                    if let (Some(field_types), Some(value)) = (req.field_type(), req.value()) {
                        // Handle the case where field_types is Vec<FieldType> - take the first one
                        if let Some(field_type) = field_types.first() {
                            Some((*field_type, value.clone()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .chain(
                    reqs.read()[..index_fields.len()]
                        .iter()
                        .filter_map(|req| {
                            if let (Some(field_types), Some(value)) = (req.field_type(), req.value()) {
                                // Handle the case where field_types is Vec<FieldType> - take the first one
                                if let Some(field_type) = field_types.first() {
                                    Some((*field_type, value.clone()))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }),
                )
                .collect::<FxHashMap<FieldType, Value>>();

            entity_ids_by_index_fields
                .entry(index_key)
                .or_insert_with(Vec::new)
                .push(entity_id);

            fields_by_entity_id.insert(entity_id, all_fields);
        }

        Ok((Cache {
            entity_type,
            index_fields,
            other_fields,
            entity_ids_by_index_fields,
            fields_by_entity_id,
            notify_queue: queue.clone()
        }, queue))
    }
}

impl Cache {
    pub fn process_notifications(&mut self) {
        // Extract entity_id and field_type from the current request
        while let Some(notification) = self.notify_queue.pop() {
            if let Request::Read { entity_id, field_types, value: current_value, .. } = &notification.current {
                if let Request::Read { value: previous_value, .. } = &notification.previous {
                    // Handle the case where field_types is Vec<FieldType> - take the first one
                    if let Some(field_type) = field_types.first() {
                        if let Some(curr_val) = current_value {
                            self.fields_by_entity_id
                                .entry(*entity_id)
                                .or_default()
                                .insert(*field_type, curr_val.clone());
                        }

                        // If the field type is one of the index fields, we need to update the index
                        if self.index_fields.contains(field_type) {
                            // Remove old entry if it exists
                            if let Some(prev_val) = previous_value {
                                let old_index_key = self.make_index_key(*entity_id, *field_type, prev_val);
                                self.entity_ids_by_index_fields.remove(&old_index_key);
                            }
                            
                            // Add new entry
                            if let Some(curr_val) = current_value {
                                let new_index_key = self.make_index_key(*entity_id, *field_type, curr_val);
                                self.entity_ids_by_index_fields
                                    .entry(new_index_key)
                                    .or_insert_with(Vec::new)
                                    .push(*entity_id);
                            }
                        }
                    }
                }
            }
        }
    }

    fn make_index_key(
        &self,
        entity_id: EntityId,
        field_type: FieldType,
        value: &Value,
    ) -> Vec<Value> {
        let mut index_key = Vec::new();

        for field in &self.index_fields {
            if *field == field_type {
                index_key.push(value.clone());
            } else {
                let other_value = self
                    .fields_by_entity_id
                    .get(&entity_id)
                    .and_then(|fields| fields.get(field));

                index_key.push(other_value.unwrap().clone());
            }
        }

        index_key
    }

    pub fn get(&self, index_key: Vec<Value>) -> Option<Vec<FxHashMap<FieldType, Value>>> {
        self.entity_ids_by_index_fields
            .get(&index_key)
            .map(|entity_ids| {
                entity_ids
                    .iter()
                    .filter_map(|entity_id| self.fields_by_entity_id.get(entity_id).cloned())
                    .collect()
            })
    }

    pub fn get_unique(&self, index_key: Vec<Value>) -> Option<FxHashMap<FieldType, Value>> {
        return self.get(index_key).and_then(|entities| {
            if entities.len() == 1 {
                Some(entities[0].clone())
            } else {
                None
            }
        });
    }

    pub fn get_config_sender(&self) -> (Vec<NotifyConfig>, Option<NotificationQueue>) {
        let sender = &self.notify_queue;
        let mut configs = Vec::new();

        // Unregister notifications for index fields
        for field in self.index_fields.iter() {
            let config = crate::NotifyConfig::EntityType {
                entity_type: self.entity_type,
                field_type: *field,
                trigger_on_change: true,
                context: vec![],
            };
            configs.push(config);
        }

        // Unregister notifications for other fields
        for field in self.other_fields.iter() {
            let config = crate::NotifyConfig::EntityType {
                entity_type: self.entity_type,
                field_type: *field,
                trigger_on_change: true,
                context: vec![],
            };
            configs.push(config);
        }

        (configs, Some(sender.clone()))
    }
}
