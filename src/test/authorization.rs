use crate::*;
use crate::auth::*;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_store() -> Store {
        Store::new(Arc::new(Snowflake::new()))
    }

    fn create_test_context() -> Context {
        Context::new()
    }

    #[test]
    fn test_authorization_manager_initialization() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthorizationManager::new();

        // Initialize schemas
        let result = auth_manager.initialize_schemas(&mut store, &ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_subject() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        assert!(!subject_id.to_string().is_empty());
    }

    #[test]
    fn test_create_permission() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create permission with simple script
        let permission_id = auth_manager
            .create_permission(&mut store, &ctx, "admin_permission", "true")
            .unwrap();

        assert!(!permission_id.to_string().is_empty());
    }

    #[test]
    fn test_create_authorization_rule() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create permission first
        let permission_id = auth_manager
            .create_permission(&mut store, &ctx, "read_permission", "true")
            .unwrap();

        // Create authorization rule
        let rule_id = auth_manager
            .create_authorization_rule(
                &mut store,
                &ctx,
                "user_read_rule",
                AuthorizationScope::ReadOnly,
                "User",
                "Name",
                &permission_id,
            )
            .unwrap();

        assert!(!rule_id.to_string().is_empty());
    }

    #[test]
    fn test_authorization_scope_conversion() {
        // Test choice value conversion
        assert_eq!(AuthorizationScope::ReadOnly.as_choice_value(), 0);
        assert_eq!(AuthorizationScope::ReadAndWrite.as_choice_value(), 1);

        // Test from choice value
        assert_eq!(AuthorizationScope::from_choice_value(0).unwrap(), AuthorizationScope::ReadOnly);
        assert_eq!(AuthorizationScope::from_choice_value(1).unwrap(), AuthorizationScope::ReadAndWrite);
        assert!(AuthorizationScope::from_choice_value(99).is_err());

        // Test access checking
        assert!(AuthorizationScope::ReadOnly.allows_read());
        assert!(!AuthorizationScope::ReadOnly.allows_write());
        assert!(AuthorizationScope::ReadAndWrite.allows_read());
        assert!(AuthorizationScope::ReadAndWrite.allows_write());
    }

    #[test]
    fn test_check_permission_no_rules() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let mut auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        // Create TestEntity schema
        let test_entity_schema = EntitySchema::<Single>::new(EntityType::from("TestEntity"), Some(EntityType::from("Object")));
        store.set_entity_schema(&ctx, &test_entity_schema).unwrap();

        // Create some test entity
        let test_entity = store
            .create_entity(&ctx, &EntityType::from("TestEntity"), None, "test")
            .unwrap();

        // Check permission with no rules - should use default policy
        let can_read = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &test_entity.entity_id,
                &FieldType::from("Name"),
                AccessType::Read,
            )
            .unwrap();

        let can_write = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &test_entity.entity_id,
                &FieldType::from("Name"),
                AccessType::Write,
            )
            .unwrap();

        // Default config allows both read and write
        assert!(can_read);
        assert!(can_write);
    }

    #[test]
    fn test_check_permission_with_rules() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let mut auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        // Create permission that allows access
        let allow_permission_id = auth_manager
            .create_permission(&mut store, &ctx, "allow_permission", "true")
            .unwrap();

        // Create authorization rule for read-only access
        let _rule_id = auth_manager
            .create_authorization_rule(
                &mut store,
                &ctx,
                "test_read_rule",
                AuthorizationScope::ReadOnly,
                "Subject",
                "Name",
                &allow_permission_id,
            )
            .unwrap();

        // Check permission on subject entity
        let can_read = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &subject_id,
                &FieldType::from("Name"),
                AccessType::Read,
            )
            .unwrap();

        let can_write = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &subject_id,
                &FieldType::from("Name"),
                AccessType::Write,
            )
            .unwrap();

        // Read should be allowed (scope allows it and permission script returns true)
        assert!(can_read);
        // Write should be denied (scope doesn't allow it)
        assert!(!can_write);
    }

    #[test]
    fn test_check_permission_with_deny_script() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let mut auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        // Create permission that denies access
        let deny_permission_id = auth_manager
            .create_permission(&mut store, &ctx, "deny_permission", "false")
            .unwrap();

        // Create authorization rule
        let _rule_id = auth_manager
            .create_authorization_rule(
                &mut store,
                &ctx,
                "test_deny_rule",
                AuthorizationScope::ReadAndWrite,
                "Subject",
                "Name",
                &deny_permission_id,
            )
            .unwrap();

        // Check permission on subject entity
        let can_read = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &subject_id,
                &FieldType::from("Name"),
                AccessType::Read,
            )
            .unwrap();

        let can_write = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &subject_id,
                &FieldType::from("Name"),
                AccessType::Write,
            )
            .unwrap();

        // Both should be denied (permission script returns false)
        assert!(!can_read);
        assert!(!can_write);
    }

    #[test]
    fn test_custom_authorization_config() {
        let config = AuthorizationConfig {
            default_allow_read: false,
            default_allow_write: false,
        };
        let mut auth_manager = AuthorizationManager::with_config(config);
        let mut store = create_test_store();
        let ctx = create_test_context();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        // Create TestEntity schema
        let test_entity_schema = EntitySchema::<Single>::new(EntityType::from("TestEntity"), Some(EntityType::from("Object")));
        store.set_entity_schema(&ctx, &test_entity_schema).unwrap();

        // Create test entity
        let test_entity = store
            .create_entity(&ctx, &EntityType::from("TestEntity"), None, "test")
            .unwrap();

        // Check permission with no rules and restrictive default policy
        let can_read = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &test_entity.entity_id,
                &FieldType::from("Name"),
                AccessType::Read,
            )
            .unwrap();

        let can_write = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &test_entity.entity_id,
                &FieldType::from("Name"),
                AccessType::Write,
            )
            .unwrap();

        // Both should be denied due to restrictive default policy
        assert!(!can_read);
        assert!(!can_write);
    }

    #[test]
    fn test_multiple_authorization_rules() {
        let mut store = create_test_store();
        let ctx = create_test_context();
        let mut auth_manager = AuthorizationManager::new();

        // Initialize schemas
        auth_manager.initialize_schemas(&mut store, &ctx).unwrap();

        // Create subject
        let subject_id = auth_manager
            .create_subject(&mut store, &ctx, "test_user")
            .unwrap();

        // Create permission that allows access
        let allow_permission_id = auth_manager
            .create_permission(&mut store, &ctx, "allow_permission", "true")
            .unwrap();

        // Create permission that denies access
        let deny_permission_id = auth_manager
            .create_permission(&mut store, &ctx, "deny_permission", "false")
            .unwrap();

        // Create multiple rules for the same resource
        let _rule1_id = auth_manager
            .create_authorization_rule(
                &mut store,
                &ctx,
                "deny_rule",
                AuthorizationScope::ReadAndWrite,
                "Subject",
                "Name",
                &deny_permission_id,
            )
            .unwrap();

        let _rule2_id = auth_manager
            .create_authorization_rule(
                &mut store,
                &ctx,
                "allow_rule",
                AuthorizationScope::ReadAndWrite,
                "Subject",
                "Name",
                &allow_permission_id,
            )
            .unwrap();

        // Check permission - should be allowed if any rule allows
        let can_read = auth_manager
            .check_permission(
                &mut store,
                &ctx,
                &subject_id,
                &subject_id,
                &FieldType::from("Name"),
                AccessType::Read,
            )
            .unwrap();

        // Should be allowed because one of the rules (allow_rule) permits access
        assert!(can_read);
    }
}
