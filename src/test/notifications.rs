#[cfg(test)]
mod tests {
    use crate::*;
    use std::sync::{Arc, Mutex};
    #[test]
    fn test_register_notification_entity_id() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            },
        );
        store.set_entity_schema(&ctx, &user_schema)?;
        // Create an entity
        let user = store.create_entity(&ctx, &et_user, None, "TestUser")?;
        // Set up notification tracking
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        // Register notification for specific entity and field
        let config = NotifyConfig::EntityId {
            entity_id: user.entity_id.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: false,
            context: vec![],
        };
        let notification_token = store.register_notification(&ctx, config.clone(), callback)?;
        // Perform a write operation
        let mut requests = vec![swrite!(
            user.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("John Doe")
        )];
        store.perform(&ctx, &mut requests)?;
        // Check that notification was triggered
        let triggered_notifications = notifications.lock().unwrap();
        assert_eq!(triggered_notifications.len(), 1);
        let notification = &triggered_notifications[0];
        assert_eq!(notification.entity_id, user.entity_id);
        assert_eq!(notification.field_type, FieldType::from("Name"));
        assert_eq!(notification.current_value, Value::String("John Doe".to_string()));
        assert_eq!(notification.previous_value, Value::String("TestUser".to_string()));
        // Unregister the notification by token
        assert!(store.unregister_notification_by_token(&notification_token));
        assert!(!store.unregister_notification_by_token(&notification_token)); // Should return false for second attempt
        Ok(())
    }
    #[test]
    fn test_register_notification_entity_type() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("email"),
            FieldSchema::String {
                field_type: FieldType::from("email"),
                default_value: "".to_string(),
                rank: 0,
            },
        );
        store.set_entity_schema(&ctx, &user_schema)?;
        // Set up notification tracking
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        // Register notification for entity type
        let config = NotifyConfig::EntityType {
            entity_type: EntityType::from("User"),
            field_type: FieldType::from("email"),
            trigger_on_change: true, // Only trigger on actual changes
            context: vec![],
        };
        store.register_notification(&ctx, config, callback)?;
        // Create entities
        let user1 = store.create_entity(&ctx, &et_user, None, "User1")?;
        let user2 = store.create_entity(&ctx, &et_user, None, "User2")?;
        // Perform write operations
        let mut requests = vec![
            swrite!(user1.entity_id.clone(), FieldType::from("email"), sstr!("user1@example.com")),
            swrite!(user2.entity_id.clone(), FieldType::from("email"), sstr!("user2@example.com")),
            // Write the same value again - this should not trigger notification due to trigger_on_change=true
            swrite!(user1.entity_id.clone(), FieldType::from("email"), sstr!("user1@example.com")),
        ];
        store.perform(&ctx, &mut requests)?;
        // Check that only 2 notifications were triggered (not 3, due to trigger_on_change)
        let triggered_notifications = notifications.lock().unwrap();
        assert_eq!(triggered_notifications.len(), 2);
        // Verify the notifications
        assert!(triggered_notifications.iter().any(|n| n.entity_id == user1.entity_id));
        assert!(triggered_notifications.iter().any(|n| n.entity_id == user2.entity_id));
        Ok(())
    }
    #[test]
    fn test_notification_with_context_fields() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("status"),
            FieldSchema::String {
                field_type: FieldType::from("status"),
                default_value: "inactive".to_string(),
                rank: 0,
            },
        );
        user_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 1,
            },
        );
        store.set_entity_schema(&ctx, &user_schema)?;
        // Create an entity
        let user = store.create_entity(&ctx, &et_user, None, "TestUser")?;
        // Set up notification tracking
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        // Register notification with context fields
        let config = NotifyConfig::EntityId {
            entity_id: user.entity_id.clone(),
            field_type: FieldType::from("status"),
            trigger_on_change: true,
            context: vec![FieldType::from("Name")], // Include name as context
        };
        store.register_notification(&ctx, config, callback)?;
        // Perform a write operation
        let mut requests = vec![swrite!(
            user.entity_id.clone(),
            FieldType::from("status"),
            sstr!("active")
        )];
        store.perform(&ctx, &mut requests)?;
        // Check that notification was triggered with context
        let triggered_notifications = notifications.lock().unwrap();
        assert_eq!(triggered_notifications.len(), 1);
        let notification = &triggered_notifications[0];
        assert_eq!(notification.entity_id, user.entity_id);
        assert_eq!(notification.field_type, FieldType::from("status"));
        assert_eq!(notification.current_value, Value::String("active".to_string()));
        assert_eq!(notification.previous_value, Value::String("inactive".to_string()));
        // Check context field
        assert!(notification.context.contains_key(&FieldType::from("Name")));
        assert_eq!(
            notification.context.get(&FieldType::from("Name")).unwrap(),
            &Some(Value::String("TestUser".to_string()))
        );
        Ok(())
    }
    #[test]
    fn test_cannot_register_notification_on_indirect_field() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        let callback = Box::new(|_: &Notification| {});
        // Try to register notification on indirect field - should fail
        let config = NotifyConfig::EntityId {
            entity_id: EntityId::new("User", 1),
            field_type: FieldType::from("parent->Name"), // This contains indirection
            trigger_on_change: false,
            context: vec![],
        };
        let result = store.register_notification(&ctx, config, callback);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot register notifications on indirect fields"));
        Ok(())
    }
    #[test]
    fn test_get_notification_configs() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        let callback1 = Box::new(|_: &Notification| {});
        let callback2 = Box::new(|_: &Notification| {});
        let config1 = NotifyConfig::EntityId {
            entity_id: EntityId::new("User", 1),
            field_type: FieldType::from("Name"),
            trigger_on_change: false,
            context: vec![],
        };
        let config2 = NotifyConfig::EntityType {
            entity_type: EntityType::from("User"),
            field_type: FieldType::from("email"),
            trigger_on_change: true,
            context: vec![],
        };
        let _id1 = store.register_notification(&ctx, config1.clone(), callback1)?;
        let _id2 = store.register_notification(&ctx, config2.clone(), callback2)?;
        // Test entity-specific configs
        let entity_configs = store.get_id_notification_configs(&EntityId::new("User", 1));
        assert_eq!(entity_configs.len(), 1);
        assert_eq!(*entity_configs[0], config1);
        // Test type-specific configs
        let type_configs = store.get_type_notification_configs(&EntityType::from("User"));
        assert_eq!(type_configs.len(), 1);
        assert_eq!(*type_configs[0], config2);
        Ok(())
    }
    #[test]
    fn test_unregister_notification() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create entity schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("email"),
            FieldSchema::String {
                field_type: FieldType::from("email"),
                default_value: "".to_string(),
                rank: 0,
            },
        );
        store.set_entity_schema(&ctx, &user_schema)?;
        // Create test entity
        let user = store.create_entity(&ctx, &et_user, None, "TestUser")?;
        // Track notifications
        let triggered_notifications = Arc::new(Mutex::new(Vec::<Notification>::new()));
        let notifications_clone = triggered_notifications.clone();
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        // Register notification
        let config = NotifyConfig::EntityType {
            entity_type: EntityType::from("User"),
            field_type: FieldType::from("email"),
            trigger_on_change: false,
            context: vec![],
        };
        let _notification_id = store.register_notification(&ctx, config.clone(), callback)?;
        // Perform a write operation - should trigger notification
        let mut requests = vec![swrite!(
            user.entity_id.clone(),
            FieldType::from("email"),
            sstr!("john@example.com")
        )];
        store.perform(&ctx, &mut requests)?;
        // Check that notification was triggered
        assert_eq!(triggered_notifications.lock().unwrap().len(), 1);
        // Unregister the notification
        assert!(store.unregister_notification(&config));
        // Clear previous notifications
        triggered_notifications.lock().unwrap().clear();
        // Perform another write operation - should NOT trigger notification
        let mut requests = vec![swrite!(
            user.entity_id.clone(),
            FieldType::from("email"),
            sstr!("jane@example.com")
        )];
        store.perform(&ctx, &mut requests)?;
        // Check that no notification was triggered
        assert_eq!(triggered_notifications.lock().unwrap().len(), 0);
        Ok(())
    }
    #[test]
    fn test_notification_inheritance_parent_type() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create inheritance hierarchy: Animal -> Mammal -> Dog
        let et_animal = EntityType::from("Animal");
        let et_mammal = EntityType::from("Mammal");
        let et_dog = EntityType::from("Dog");
        // Animal schema (base type)
        let mut animal_schema = EntitySchema::<Single>::new(et_animal.clone(), None);
        animal_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            }
        );
        store.set_entity_schema(&ctx, &animal_schema)?;
        // Mammal schema (inherits from Animal)
        let mammal_schema = EntitySchema::<Single>::new(et_mammal.clone(), Some(et_animal.clone()));
        store.set_entity_schema(&ctx, &mammal_schema)?;
        // Dog schema (inherits from Mammal)
        let dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_mammal.clone()));
        store.set_entity_schema(&ctx, &dog_schema)?;
        // Create a dog entity
        let dog = store.create_entity(&ctx, &et_dog, None, "Buddy")?;
        // Track notifications
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        // Register notification on Animal type (parent type) for Name field
        let animal_config = NotifyConfig::EntityType {
            entity_type: et_animal.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        let token = store.register_notification(&ctx, animal_config, callback)?;
        // Write to dog's Name field - should trigger notification on Animal type
        let mut requests = vec![swrite!(
            dog.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Rex")
        )];
        store.perform(&ctx, &mut requests)?;
        // Verify notification was triggered
        let captured_notifications = notifications.lock().unwrap();
        assert_eq!(captured_notifications.len(), 1);
        let notification = &captured_notifications[0];
        assert_eq!(notification.entity_id, dog.entity_id);
        assert_eq!(notification.field_type, FieldType::from("Name"));
        assert_eq!(notification.current_value, Value::String("Rex".to_string()));
        assert_eq!(notification.previous_value, Value::String("Buddy".to_string()));
        // Clean up
        let success = store.unregister_notification_by_token(&token);
        assert!(success);
        Ok(())
    }
    #[test]
    fn test_notification_inheritance_multiple_levels() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create inheritance hierarchy: Object -> Vehicle -> Car -> Sedan
        let et_object = EntityType::from("Object");
        let et_vehicle = EntityType::from("Vehicle");
        let et_car = EntityType::from("Car");
        let et_sedan = EntityType::from("Sedan");
        // Object schema (base type)
        let mut object_schema = EntitySchema::<Single>::new(et_object.clone(), None);
        object_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            }
        );
        store.set_entity_schema(&ctx, &object_schema)?;
        // Vehicle schema (inherits from Object)
        let vehicle_schema = EntitySchema::<Single>::new(et_vehicle.clone(), Some(et_object.clone()));
        store.set_entity_schema(&ctx, &vehicle_schema)?;
        // Car schema (inherits from Vehicle)
        let car_schema = EntitySchema::<Single>::new(et_car.clone(), Some(et_vehicle.clone()));
        store.set_entity_schema(&ctx, &car_schema)?;
        // Sedan schema (inherits from Car)
        let sedan_schema = EntitySchema::<Single>::new(et_sedan.clone(), Some(et_car.clone()));
        store.set_entity_schema(&ctx, &sedan_schema)?;
        // Create a sedan entity
        let sedan = store.create_entity(&ctx, &et_sedan, None, "Toyota Camry")?;
        // Track notifications for each level
        let object_notifications = Arc::new(Mutex::new(Vec::new()));
        let vehicle_notifications = Arc::new(Mutex::new(Vec::new()));
        let car_notifications = Arc::new(Mutex::new(Vec::new()));
        // Register notifications at different inheritance levels
        let object_config = NotifyConfig::EntityType {
            entity_type: et_object.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let vehicle_config = NotifyConfig::EntityType {
            entity_type: et_vehicle.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let car_config = NotifyConfig::EntityType {
            entity_type: et_car.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let object_notifications_clone = object_notifications.clone();
        let vehicle_notifications_clone = vehicle_notifications.clone();
        let car_notifications_clone = car_notifications.clone();
        let object_token = store.register_notification(
            &ctx,
            object_config,
            Box::new(move |notification: &Notification| {
                object_notifications_clone.lock().unwrap().push(notification.clone());
            })
        )?;
        let vehicle_token = store.register_notification(
            &ctx,
            vehicle_config,
            Box::new(move |notification: &Notification| {
                vehicle_notifications_clone.lock().unwrap().push(notification.clone());
            })
        )?;
        let car_token = store.register_notification(
            &ctx,
            car_config,
            Box::new(move |notification: &Notification| {
                car_notifications_clone.lock().unwrap().push(notification.clone());
            })
        )?;
        // Write to sedan's Name field - should trigger all parent notifications
        let mut requests = vec![swrite!(
            sedan.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Honda Accord")
        )];
        store.perform(&ctx, &mut requests)?;
        // Verify all parent type notifications were triggered
        let object_captured = object_notifications.lock().unwrap();
        let vehicle_captured = vehicle_notifications.lock().unwrap();
        let car_captured = car_notifications.lock().unwrap();
        assert_eq!(object_captured.len(), 1);
        assert_eq!(vehicle_captured.len(), 1);
        assert_eq!(car_captured.len(), 1);
        // All notifications should have the same entity and field info
        for notifications in [&*object_captured, &*vehicle_captured, &*car_captured] {
            let notification = &notifications[0];
            assert_eq!(notification.entity_id, sedan.entity_id);
            assert_eq!(notification.field_type, FieldType::from("Name"));
            assert_eq!(notification.current_value, Value::String("Honda Accord".to_string()));
            assert_eq!(notification.previous_value, Value::String("Toyota Camry".to_string()));
        }
        // Clean up
        assert!(store.unregister_notification_by_token(&object_token));
        assert!(store.unregister_notification_by_token(&vehicle_token));
        assert!(store.unregister_notification_by_token(&car_token));
        Ok(())
    }
    #[test]
    fn test_notification_inheritance_no_duplicate_triggers() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create inheritance hierarchy: Animal -> Dog
        let et_animal = EntityType::from("Animal");
        let et_dog = EntityType::from("Dog");
        // Animal schema (base type)
        let mut animal_schema = EntitySchema::<Single>::new(et_animal.clone(), None);
        animal_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            }
        );
        store.set_entity_schema(&ctx, &animal_schema)?;
        // Dog schema (inherits from Animal)
        let dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_animal.clone()));
        store.set_entity_schema(&ctx, &dog_schema)?;
        // Create a dog entity
        let dog = store.create_entity(&ctx, &et_dog, None, "Buddy")?;
        // Track notifications
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        // Register notification on both Animal (parent) and Dog (exact) types
        let animal_config = NotifyConfig::EntityType {
            entity_type: et_animal.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let dog_config = NotifyConfig::EntityType {
            entity_type: et_dog.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        let animal_token = store.register_notification(&ctx, animal_config, callback)?;
        let notifications_clone2 = notifications.clone();
        let callback2 = Box::new(move |notification: &Notification| {
            notifications_clone2.lock().unwrap().push(notification.clone());
        });
        let dog_token = store.register_notification(&ctx, dog_config, callback2)?;
        // Write to dog's Name field - should trigger both notifications
        let mut requests = vec![swrite!(
            dog.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Rex")
        )];
        store.perform(&ctx, &mut requests)?;
        // Verify both notifications were triggered (one for exact type, one for parent type)
        let captured_notifications = notifications.lock().unwrap();
        assert_eq!(captured_notifications.len(), 2);
        // Both notifications should have the same entity and field info
        for notification in captured_notifications.iter() {
            assert_eq!(notification.entity_id, dog.entity_id);
            assert_eq!(notification.field_type, FieldType::from("Name"));
            assert_eq!(notification.current_value, Value::String("Rex".to_string()));
            assert_eq!(notification.previous_value, Value::String("Buddy".to_string()));
        }
        // Clean up
        assert!(store.unregister_notification_by_token(&animal_token));
        assert!(store.unregister_notification_by_token(&dog_token));
        Ok(())
    }
    #[test]
    fn test_notification_inheritance_context_fields() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create inheritance hierarchy with parent-child relationships
        let et_object = EntityType::from("Object");
        let et_folder = EntityType::from("Folder");
        // Object schema (base type)
        let mut object_schema = EntitySchema::<Single>::new(et_object.clone(), None);
        object_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            }
        );
        object_schema.fields.insert(
            FieldType::from("Parent"),
            FieldSchema::EntityReference {
                field_type: FieldType::from("Parent"),
                default_value: None,
                rank: 1,
            }
        );
        object_schema.fields.insert(
            FieldType::from("Children"),
            FieldSchema::EntityList {
                field_type: FieldType::from("Children"),
                default_value: Vec::new(),
                rank: 2,
            }
        );
        store.set_entity_schema(&ctx, &object_schema)?;
        // Folder schema (inherits from Object)
        let folder_schema = EntitySchema::<Single>::new(et_folder.clone(), Some(et_object.clone()));
        store.set_entity_schema(&ctx, &folder_schema)?;
        // Create parent and child folders
        let parent_folder = store.create_entity(&ctx, &et_folder, None, "Parent Folder")?;
        let child_folder = store.create_entity(&ctx, &et_folder, Some(parent_folder.entity_id.clone()), "Child Folder")?;
        // Track notifications
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let notifications_clone = notifications.clone();
        // Register notification on Object type (parent) with context including parent name
        let config = NotifyConfig::EntityType {
            entity_type: et_object.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![
                FieldType::from("Parent"),
                FieldType::from("Parent->Name"),  // Indirect field for parent's name
            ],
        };
        let callback = Box::new(move |notification: &Notification| {
            notifications_clone.lock().unwrap().push(notification.clone());
        });
        let token = store.register_notification(&ctx, config, callback)?;
        // Write to child folder's Name field - should trigger notification with context
        let mut requests = vec![swrite!(
            child_folder.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Updated Child Folder")
        )];
        store.perform(&ctx, &mut requests)?;
        // Verify notification was triggered with proper context
        let captured_notifications = notifications.lock().unwrap();
        assert_eq!(captured_notifications.len(), 1);
        let notification = &captured_notifications[0];
        assert_eq!(notification.entity_id, child_folder.entity_id);
        assert_eq!(notification.field_type, FieldType::from("Name"));
        assert_eq!(notification.current_value, Value::String("Updated Child Folder".to_string()));
        // Check context fields
        assert_eq!(notification.context.len(), 2);
        // Parent field should contain the parent entity reference
        let parent_context = notification.context.get(&FieldType::from("Parent")).unwrap();
        assert_eq!(*parent_context, Some(Value::EntityReference(Some(parent_folder.entity_id.clone()))));
        // Parent->Name should contain the parent's name
        let parent_name_context = notification.context.get(&FieldType::from("Parent->Name")).unwrap();
        assert_eq!(*parent_name_context, Some(Value::String("Parent Folder".to_string())));
        // Clean up
        assert!(store.unregister_notification_by_token(&token));
        Ok(())
    }
    #[test]
    fn test_notification_inheritance_mixed_entity_and_type() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context::new();
        // Create inheritance hierarchy: Animal -> Dog
        let et_animal = EntityType::from("Animal");
        let et_dog = EntityType::from("Dog");
        // Animal schema (base type)
        let mut animal_schema = EntitySchema::<Single>::new(et_animal.clone(), None);
        animal_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
            }
        );
        store.set_entity_schema(&ctx, &animal_schema)?;
        // Dog schema (inherits from Animal)
        let dog_schema = EntitySchema::<Single>::new(et_dog.clone(), Some(et_animal.clone()));
        store.set_entity_schema(&ctx, &dog_schema)?;
        // Create specific dog entities
        let dog1 = store.create_entity(&ctx, &et_dog, None, "Buddy")?;
        let dog2 = store.create_entity(&ctx, &et_dog, None, "Rex")?;
        // Track notifications
        let entity_notifications = Arc::new(Mutex::new(Vec::new()));
        let type_notifications = Arc::new(Mutex::new(Vec::new()));
        // Register entity-specific notification for dog1
        let entity_config = NotifyConfig::EntityId {
            entity_id: dog1.entity_id.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        // Register type notification for Animal type (should catch both dogs due to inheritance)
        let type_config = NotifyConfig::EntityType {
            entity_type: et_animal.clone(),
            field_type: FieldType::from("Name"),
            trigger_on_change: true,
            context: vec![],
        };
        let entity_notifications_clone = entity_notifications.clone();
        let type_notifications_clone = type_notifications.clone();
        let entity_token = store.register_notification(
            &ctx,
            entity_config,
            Box::new(move |notification: &Notification| {
                entity_notifications_clone.lock().unwrap().push(notification.clone());
            })
        )?;
        let type_token = store.register_notification(
            &ctx,
            type_config,
            Box::new(move |notification: &Notification| {
                type_notifications_clone.lock().unwrap().push(notification.clone());
            })
        )?;
        // Write to dog1's Name field - should trigger both entity and type notifications
        let mut requests = vec![swrite!(
            dog1.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Buddy Updated")
        )];
        store.perform(&ctx, &mut requests)?;
        // Write to dog2's Name field - should trigger only type notification
        let mut requests = vec![swrite!(
            dog2.entity_id.clone(),
            FieldType::from("Name"),
            sstr!("Rex Updated")
        )];
        store.perform(&ctx, &mut requests)?;
        // Verify notifications
        let entity_captured = entity_notifications.lock().unwrap();
        let type_captured = type_notifications.lock().unwrap();
        // Entity notification should only trigger for dog1
        assert_eq!(entity_captured.len(), 1);
        assert_eq!(entity_captured[0].entity_id, dog1.entity_id);
        assert_eq!(entity_captured[0].current_value, Value::String("Buddy Updated".to_string()));
        // Type notification should trigger for both dogs (due to inheritance)
        assert_eq!(type_captured.len(), 2);
        // Find notifications for each dog
        let dog1_type_notification = type_captured.iter()
            .find(|n| n.entity_id == dog1.entity_id)
            .expect("Should have type notification for dog1");
        let dog2_type_notification = type_captured.iter()
            .find(|n| n.entity_id == dog2.entity_id)
            .expect("Should have type notification for dog2");
        assert_eq!(dog1_type_notification.current_value, Value::String("Buddy Updated".to_string()));
        assert_eq!(dog2_type_notification.current_value, Value::String("Rex Updated".to_string()));
        // Clean up
        assert!(store.unregister_notification_by_token(&entity_token));
        assert!(store.unregister_notification_by_token(&type_token));
        Ok(())
    }
}
