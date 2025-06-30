#[cfg(test)]
mod tests {
    use crate::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_register_notification_entity_id() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context {};

        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 0,
                read_permission: None,
                write_permission: None,
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

        let notification_id = store.register_notification(&ctx, config, callback)?;

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

        // Unregister the notification
        assert!(store.unregister_notification(&notification_id));
        assert!(!store.unregister_notification(&notification_id)); // Should return false for second attempt

        Ok(())
    }

    #[test]
    fn test_register_notification_entity_type() -> Result<()> {
        let mut store = Store::new(Arc::new(Snowflake::new()));
        let ctx = Context {};

        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("email"),
            FieldSchema::String {
                field_type: FieldType::from("email"),
                default_value: "".to_string(),
                rank: 0,
                read_permission: None,
                write_permission: None,
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
            entity_type: "User".to_string(),
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
        let ctx = Context {};

        // Create entity type and schema
        let et_user = EntityType::from("User");
        let mut user_schema = EntitySchema::<Single>::new(et_user.clone(), None);
        user_schema.fields.insert(
            FieldType::from("status"),
            FieldSchema::String {
                field_type: FieldType::from("status"),
                default_value: "inactive".to_string(),
                rank: 0,
                read_permission: None,
                write_permission: None,
            },
        );
        user_schema.fields.insert(
            FieldType::from("Name"),
            FieldSchema::String {
                field_type: FieldType::from("Name"),
                default_value: "".to_string(),
                rank: 1,
                read_permission: None,
                write_permission: None,
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
        let ctx = Context {};

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
        let ctx = Context {};

        let callback1 = Box::new(|_: &Notification| {});
        let callback2 = Box::new(|_: &Notification| {});

        let config1 = NotifyConfig::EntityId {
            entity_id: EntityId::new("User", 1),
            field_type: FieldType::from("Name"),
            trigger_on_change: false,
            context: vec![],
        };

        let config2 = NotifyConfig::EntityType {
            entity_type: "User".to_string(),
            field_type: FieldType::from("email"),
            trigger_on_change: true,
            context: vec![],
        };

        let id1 = store.register_notification(&ctx, config1.clone(), callback1)?;
        let id2 = store.register_notification(&ctx, config2.clone(), callback2)?;

        // Get all notification configs
        let configs = store.get_notification_configs();
        assert_eq!(configs.len(), 2);
        assert!(configs.contains_key(&id1));
        assert!(configs.contains_key(&id2));
        assert_eq!(**configs.get(&id1).unwrap(), config1);
        assert_eq!(**configs.get(&id2).unwrap(), config2);

        Ok(())
    }
}
