use std::collections::HashMap;

use crossbeam::channel::{Receiver, Sender};
use rustc_hash::FxHashMap;

use crate::{
    EntityId, EntityType, FieldType, Notification, NotifyConfig, NotifyInfo, StoreProxy, Value
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

    notify_receiver: Receiver<Notification>,
    notify_sender: Sender<Notification>,
}

impl Cache {
    pub fn new(
        store: &StoreProxy,
        entity_type: EntityType,
        index_fields: Vec<FieldType>,
        other_fields: Vec<FieldType>,
    ) -> crate::Result<(Self, Receiver<Notification>)> {
        let (sender, receiver) = crossbeam::channel::unbounded();

        // Register notifications for all fields
        for field_type in index_fields.iter() {
            store.register_notification(
                NotifyConfig::EntityType {
                    entity_type,
                    field_type: *field_type,
                    trigger_on_change: true,
                    context: vec![],
                },
                sender.clone(),
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
                sender.clone(),
            )?;
        }

        // Read initial values from the store
        let mut entity_ids_by_index_fields = HashMap::new();
        let mut fields_by_entity_id = FxHashMap::default();

        let entity_ids = store.find_entities(entity_type, None)?;
        for entity_id in entity_ids {
            let mut index_values = Vec::new();
            let mut other_values = Vec::new();

            // Read index fields
            for field in index_fields.iter() {
                let (value, _, _) = store.read(entity_id, &[*field])?;
                index_values.push(value);
            }
            
            // Read other fields
            for field in other_fields.iter() {
                let (value, _, _) = store.read(entity_id, &[*field])?;
                other_values.push(value);
            }

            let index_key = index_values.clone();

            let all_fields = index_fields.iter().cloned()
                .zip(index_values.into_iter())
                .chain(other_fields.iter().cloned().zip(other_values.into_iter()))
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
            notify_receiver: receiver.clone(),
            notify_sender: sender.clone(),
        }, receiver))
    }
}

impl Cache {
    pub fn process_notifications(&mut self) {
        // Extract entity_id and field_type from the current request
        while let Ok(notification) = self.notify_receiver.try_recv() {
            let NotifyInfo { entity_id, field_path: field_type, value: current_value, .. } = &notification.current;
            let NotifyInfo { value: previous_value, .. } = &notification.previous;
            // Handle the case where field_type is Vec<FieldType> - take the first one
            if let Some(field_type) = field_type.first() {
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

    pub fn get_config_sender(&self) -> (Vec<NotifyConfig>, Sender<Notification>) {
        let mut configs = Vec::new();

        // Collect notification configs for index fields
        for field in self.index_fields.iter() {
            let config = crate::NotifyConfig::EntityType {
                entity_type: self.entity_type,
                field_type: *field,
                trigger_on_change: true,
                context: vec![],
            };
            configs.push(config);
        }

        // Collect notification configs for other fields
        for field in self.other_fields.iter() {
            let config = crate::NotifyConfig::EntityType {
                entity_type: self.entity_type,
                field_type: *field,
                trigger_on_change: true,
                context: vec![],
            };
            configs.push(config);
        }

        (configs, self.notify_sender.clone())
    }
}
