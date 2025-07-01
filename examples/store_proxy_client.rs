use qlib_rs::{StoreProxy, Context, EntityType, FieldType, FieldSchema, EntitySchema, Single};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connect to a qcore-rs WebSocket server
    let store_proxy = StoreProxy::connect("ws://localhost:8080").await?;
    let ctx = Context {};

    // Set up a simple entity schema
    let entity_type = EntityType::from("Person");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    
    // Add a name field
    let name_field = FieldType::from("Name");
    schema.fields.insert(name_field.clone(), FieldSchema::String {
        field_type: name_field.clone(),
        default_value: "".to_string(),
        rank: 0,
        read_permission: None,
        write_permission: None,
    });
    
    // Set the schema on the remote store
    store_proxy.set_entity_schema(&ctx, &schema).await?;
    
    // Create a new entity
    let entity = store_proxy.create_entity(&ctx, &entity_type, None, "John Doe").await?;
    println!("Created entity: {:?}", entity);
    
    // Subscribe to notifications
    let mut notifications = store_proxy.subscribe_notifications().await;
    
    // Spawn a task to handle notifications
    tokio::spawn(async move {
        while let Some(notification) = notifications.recv().await {
            println!("Received notification: {:?}", notification);
        }
    });
    
    println!("StoreProxy client example completed successfully!");
    Ok(())
}
