#[cfg(test)]
mod tests {
    use crate::scripting::{compile_wat, execute_wasm, compile_wat_to_wasm};
    use crate::data::{EntityId, EntityType, FieldType, Entity, FieldSchema};
    use crate::{Result, Error};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio;

    #[tokio::test]
    async fn test_compile_wat() {
        let wat_code = r#"
            (module
                (memory (export "memory") 1)
                (func (export "main") (result i32)
                    i32.const 42
                )
            )
        "#;

        let result = compile_wat(wat_code);
        assert!(result.is_ok(), "Failed to compile WAT: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_execute_wasm_basic() {
        let wat_code = r#"
            (module
                (memory (export "memory") 1)
                (func (export "main") (result i32)
                    i32.const 42
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_host_function_entity_exists() {
        let wat_code = r#"
            (module
                (import "env" "entity_exists" (func $entity_exists (param i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "{\"typ\":\"TestType\",\"id\":1}")
                (func (export "main") (result i32)
                    i32.const 0
                    i32.const 30
                    call $entity_exists
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM with entity_exists: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_host_function_get_entity_types() {
        let wat_code = r#"
            (module
                (import "env" "get_entity_types" (func $get_entity_types (param i32 i32) (result i32)))
                (memory (export "memory") 1)
                (func (export "main") (result i32)
                    i32.const 0
                    i32.const 1000
                    call $get_entity_types
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM with get_entity_types: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_host_function_find_entities() {
        let wat_code = r#"
            (module
                (import "env" "find_entities" (func $find_entities (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "TestType")
                (func (export "main") (result i32)
                    i32.const 0
                    i32.const 8
                    i32.const 100
                    i32.const 1000
                    call $find_entities
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM with find_entities: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_host_function_field_exists() {
        let wat_code = r#"
            (module
                (import "env" "field_exists" (func $field_exists (param i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "{\"entity_type\":\"TestType\",\"field_type\":\"TestField\"}")
                (func (export "main") (result i32)
                    i32.const 0
                    i32.const 54
                    call $field_exists
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM with field_exists: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_host_function_get_field_schema() {
        let wat_code = r#"
            (module
                (import "env" "get_field_schema" (func $get_field_schema (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 0) "{\"entity_type\":\"TestType\",\"field_type\":\"TestField\"}")
                (func (export "main") (result i32)
                    i32.const 0
                    i32.const 54
                    i32.const 100
                    i32.const 1000
                    call $get_field_schema
                )
            )
        "#;

        let bytecode = compile_wat_to_wasm(wat_code).expect("Failed to compile WAT");
        let store = Arc::new(RwLock::new(create_mock_store().await));
        
        let result = execute_wasm(&bytecode, store, serde_json::json!({}), None).await;
        assert!(result.is_ok(), "Failed to execute WASM with get_field_schema: {:?}", result.err());
    }

    // Helper function to create a mock store for testing
    async fn create_mock_store() -> MockStore {
        MockStore::new()
    }

    // Mock store implementation for testing
    pub struct MockStore {
        entities: HashMap<EntityId, Entity>,
        entity_types: Vec<EntityType>,
        schemas: HashMap<EntityType, FieldSchema>,
    }

    impl MockStore {
        pub fn new() -> Self {
            let mut store = MockStore {
                entities: HashMap::new(),
                entity_types: Vec::new(),
                schemas: HashMap::new(),
            };

            // Add some test data
            let test_entity_type = EntityType::from("TestType");
            let test_entity_id = EntityId::new(test_entity_type.clone(), 1);
            let test_entity = Entity::new(test_entity_id.clone());

            store.entities.insert(test_entity_id, test_entity);
            store.entity_types.push(test_entity_type.clone());

            store
        }
    }

    #[async_trait::async_trait]
    impl crate::data::StoreTrait for MockStore {
        async fn get_entity_schema(&self, _entity_type: &EntityType) -> Result<crate::EntitySchema<crate::Single>> {
            Err(Error::Scripting("Not implemented for mock".to_string()))
        }

        async fn get_complete_entity_schema(&self, _entity_type: &EntityType) -> Result<crate::EntitySchema<crate::Complete>> {
            Err(Error::Scripting("Not implemented for mock".to_string()))
        }

        async fn get_field_schema(&self, entity_type: &EntityType, _field_type: &FieldType) -> Result<FieldSchema> {
            if let Some(schema) = self.schemas.get(entity_type) {
                Ok(schema.clone())
            } else {
                Err(Error::Scripting("Schema not found".to_string()))
            }
        }

        async fn set_field_schema(&mut self, _entity_type: &EntityType, _field_type: &FieldType, _schema: FieldSchema) -> Result<()> {
            Ok(())
        }

        async fn entity_exists(&self, entity_id: &EntityId) -> bool {
            self.entities.contains_key(entity_id)
        }

        async fn field_exists(&self, _entity_type: &EntityType, _field_type: &FieldType) -> bool {
            true
        }

        async fn perform(&mut self, _requests: &mut Vec<crate::Request>) -> Result<()> {
            Ok(())
        }

        async fn find_entities_paginated(&self, _entity_type: &EntityType, _page_opts: Option<crate::PageOpts>) -> Result<crate::PageResult<EntityId>> {
            Err(Error::Scripting("Not implemented for mock".to_string()))
        }

        async fn find_entities_exact(&self, _entity_type: &EntityType, _page_opts: Option<crate::PageOpts>) -> Result<crate::PageResult<EntityId>> {
            Err(Error::Scripting("Not implemented for mock".to_string()))
        }

        async fn find_entities(&self, entity_type: &EntityType) -> Result<Vec<EntityId>> {
            Ok(self.entities.keys()
                .filter(|id| id.get_type() == entity_type)
                .cloned()
                .collect())
        }

        async fn get_entity_types(&self) -> Result<Vec<EntityType>> {
            Ok(self.entity_types.clone())
        }

        async fn get_entity_types_paginated(&self, _page_opts: Option<crate::PageOpts>) -> Result<crate::PageResult<EntityType>> {
            Err(Error::Scripting("Not implemented for mock".to_string()))
        }

        async fn register_notification(&mut self, _config: crate::NotifyConfig, _sender: crate::NotificationSender) -> Result<()> {
            Ok(())
        }

        async fn unregister_notification(&mut self, _config: &crate::NotifyConfig, _sender: &crate::NotificationSender) -> bool {
            false
        }
    }
}
