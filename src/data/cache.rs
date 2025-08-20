use std::collections::HashMap;

use crate::{
    EntityId, EntityType, FieldType, NotificationReceiver,
    NotificationSender, Value,
};

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
    pub fields_by_entity_id: HashMap<EntityId, HashMap<FieldType, Value>>,

    // Notification for keeping the cache up to date
    pub notify_channel: (NotificationSender, NotificationReceiver),
}

#[macro_export]
macro_rules! scache {
    ($ctx:expr, $store:expr, $entity_type:expr, $index_fields:expr, $other_fields:expr) => {{
        async {
            let (sender, receiver) = crate::notification_channel();

            // Register notifications for all fields
            for field in $index_fields.iter() {
                $store
                    .register_notification(
                        &$ctx,
                        crate::NotifyConfig::EntityType {
                            entity_type: $entity_type.clone(),
                            field_type: field.clone(),
                            trigger_on_change: true,
                            context: vec![],
                        },
                        sender.clone(),
                    )
                    .await?;
            }

            for field in $other_fields.iter() {
                $store
                    .register_notification(
                        &$ctx,
                        crate::NotifyConfig::EntityType {
                            entity_type: $entity_type.clone(),
                            field_type: field.clone(),
                            trigger_on_change: true,
                            context: vec![],
                        },
                        sender.clone(),
                    )
                    .await?;
            }

            // Read initial values from the store
            let mut entity_ids_by_index_fields = HashMap::new();
            let mut fields_by_entity_id = HashMap::new();

            for entity_id in $store.find_entities(&$ctx, &$entity_type).await? {
                let mut reqs = Vec::new();
                for field in index_fields.iter() {
                    reqs.push(crate::sread!(entity_id.clone(), field.clone()));
                }
                
                for field in $other_fields.iter() {
                    reqs.push(crate::sread!(entity_id.clone(), field.clone()));
                }

                $store.perform(&$ctx, &mut reqs).await?;

                let index_key = reqs[..$index_fields.len()]
                    .iter()
                    .map(|req| req.value().unwrap().clone())
                    .collect::<Vec<crate::Value>>();

                let all_fields = reqs[$index_fields.len()..]
                    .iter()
                    .map(|req| (req.field_type().clone(), req.value().unwrap().clone()))
                    .chain(
                        reqs[..$index_fields.len()]
                            .iter()
                            .map(|req| (req.field_type().clone(), req.value().unwrap().clone())),
                    )
                    .collect::<HashMap<crate::FieldType, crate::Value>>();

                entity_ids_by_index_fields
                    .entry(index_key)
                    .or_insert_with(Vec::new)
                    .push(entity_id.clone());

                fields_by_entity_id.insert(entity_id, all_fields);
            }

            Ok(Cache {
                entity_type: $entity_type,
                index_fields: $index_fields,
                other_fields: $other_fields,
                entity_ids_by_index_fields,
                fields_by_entity_id,
                notify_channel: (sender, receiver),
            })
        }
    }};
}

impl Cache {
    pub async fn process_notifications(&mut self) {
        while let Some(notification) = self.notify_channel.1.recv().await {
            let entity_id = notification.entity_id;
            let field_type = notification.field_type;

            self.fields_by_entity_id
                .entry(entity_id.clone())
                .or_default()
                .insert(field_type.clone(), notification.current_value.clone());

            // If the field type is one of the index fields, we need to update the index
            if self.index_fields.contains(&field_type) {
                // Remove old entry if it exists
                let old_index_key =
                    self.make_index_key(&entity_id, &field_type, &notification.previous_value);
                let new_index_key =
                    self.make_index_key(&entity_id, &field_type, &notification.current_value);
                self.entity_ids_by_index_fields.remove(&old_index_key);

                // Add new entry
                self.entity_ids_by_index_fields
                    .entry(new_index_key)
                    .or_insert_with(Vec::new)
                    .push(entity_id.clone());
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
