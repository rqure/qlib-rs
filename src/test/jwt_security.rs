use crate::*;
use crate::auth::*;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_store() -> Store {
        Store::new(Arc::new(Snowflake::new()))
    }

    fn create_test_jwt_manager() -> JwtManager {
        let secret = b"test_secret_key_that_is_long_enough_for_testing_purposes";
        JwtManager::new(secret).with_expiry_hours(1)
    }

    #[test]
    fn test_context_with_security() {
        let security_context = SecurityContext::with_subject(EntityId::try_from("User$1").unwrap());
        let ctx = Context::with_security(security_context);

        assert!(ctx.is_authenticated());
        // Note: EntityId.to_string() includes type and ID, so we compare the type part
        assert_eq!(ctx.get_subject_id().unwrap().get_type().to_string(), "admin_user");
    }

    #[test]
    fn test_jwt_token_generation_and_validation() {
        let jwt_manager = create_test_jwt_manager();
        let subject_id = EntityId::try_from("User$1").unwrap();

        // Generate token
        let token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        assert!(!token.is_empty());

        // Validate token
        let claims = jwt_manager
            .validate_token(&token)
            .expect("Should validate token");

        // Note: The subject in JWT is the full EntityId string representation
        assert!(claims.sub.contains("test_user"));
        assert_eq!(claims.name, "Test User");
        assert!(claims.exp > claims.iat);
    }

    #[test]
    fn test_security_context_from_jwt() {
        let jwt_manager = create_test_jwt_manager();
        let subject_id = EntityId::try_from("User$1").unwrap();

        let token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        let security_context = jwt_manager.create_security_context(&token).unwrap();

        assert!(security_context.is_authenticated());
        // The subject ID should have the correct type
        assert_eq!(security_context.get_subject_id().unwrap().get_type().to_string(), "User");
    }

    #[test]
    fn test_invalid_jwt_token() {
        let jwt_manager = create_test_jwt_manager();

        let result = jwt_manager.validate_token("invalid_token");
        assert!(result.is_err());

        let security_context = jwt_manager.create_security_context("invalid_token").unwrap();
        assert!(!security_context.is_authenticated());
        assert!(security_context.get_subject_id().is_none());
    }

    #[test]
    fn test_store_operations_with_admin_context() {
        let mut store = create_test_store();
        let jwt_manager = create_test_jwt_manager();

        // Create a JWT context with admin user
        let admin_subject_id = EntityId::try_from("User$1").unwrap();

        let admin_token = jwt_manager
            .generate_token(&admin_subject_id, "Admin User")
            .expect("Should generate token");

        let admin_ctx = Context::with_security(
            jwt_manager.create_security_context(&admin_token).unwrap()
        );

        // Create an entity first
        let entity_type = EntityType::from("test_entity");
        let field_type = FieldType::from("test_field");
        
        let entity = store.create_entity(&admin_ctx, &entity_type, None, "test").expect("Should create entity");

        // Should be able to write due to QOS default-allow policy
        let mut write_reqs = vec![swrite!(
            entity.entity_id.clone(),
            field_type.clone(),
            sstr!("test_value")
        )];
        assert!(store.perform(&admin_ctx, &mut write_reqs).is_ok());

        // Should be able to read due to QOS default-allow policy
        let mut read_reqs = vec![sread!(
            entity.entity_id.clone(),
            field_type.clone()
        )];
        assert!(store.perform(&admin_ctx, &mut read_reqs).is_ok());
    }

    #[test]
    fn test_store_operations_with_no_security_context() {
        let mut store = create_test_store();

        // Context without security - should work due to backward compatibility
        let ctx = Context::new();

        // Create an entity first
        let entity_type = EntityType::from("test_entity");
        let field_type = FieldType::from("test_field");
        
        let entity = store.create_entity(&ctx, &entity_type, None, "test").expect("Should create entity");

        // Should be able to write due to QOS default-allow policy
        let mut write_reqs = vec![swrite!(
            entity.entity_id.clone(),
            field_type.clone(),
            sstr!("test_value")
        )];
        assert!(store.perform(&ctx, &mut write_reqs).is_ok());

        // Should be able to read due to QOS default-allow policy
        let mut read_reqs = vec![sread!(
            entity.entity_id.clone(),
            field_type.clone()
        )];
        assert!(store.perform(&ctx, &mut read_reqs).is_ok());
    }

    #[test]
    fn test_store_operations_with_unauthenticated_context() {
        let mut store = create_test_store();

        // Anonymous context
        let ctx = Context::with_security(SecurityContext::anonymous());

        // Create an entity first
        let entity_type = EntityType::from("test_entity");
        let field_type = FieldType::from("test_field");
        
        let entity = store.create_entity(&ctx, &entity_type, None, "test").expect("Should create entity");

        // Should be able to write due to QOS default-allow policy
        let mut write_reqs = vec![swrite!(
            entity.entity_id.clone(),
            field_type.clone(),
            sstr!("test_value")
        )];
        assert!(store.perform(&ctx, &mut write_reqs).is_ok());

        // Should be able to read due to QOS default-allow policy
        let mut read_reqs = vec![sread!(
            entity.entity_id.clone(),
            field_type.clone()
        )];
        assert!(store.perform(&ctx, &mut read_reqs).is_ok());
    }

    #[test]
    fn test_store_operations_with_jwt_permissions() {
        let mut store = create_test_store();
        let jwt_manager = create_test_jwt_manager();

        // Create a JWT context for a regular user
        let subject_id = EntityId::try_from("User$1").unwrap();

        let user_token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        let user_ctx = Context::with_security(
            jwt_manager.create_security_context(&user_token).unwrap()
        );

        // Create an entity first
        let entity_type = EntityType::from("test_entity");
        let field_type = FieldType::from("test_field");
        
        let entity = store.create_entity(&user_ctx, &entity_type, None, "test").expect("Should create entity");

        // Should be able to write due to QOS default-allow policy
        let mut write_reqs = vec![swrite!(
            entity.entity_id.clone(),
            field_type.clone(),
            sstr!("test_value")
        )];
        assert!(store.perform(&user_ctx, &mut write_reqs).is_ok());

        // Should be able to read due to QOS default-allow policy
        let mut read_reqs = vec![sread!(
            entity.entity_id.clone(),
            field_type.clone()
        )];
        assert!(store.perform(&user_ctx, &mut read_reqs).is_ok());
    }

    #[test]
    fn test_jwt_token_refresh() {
        let jwt_manager = create_test_jwt_manager();
        let subject_id = EntityId::try_from("User$1").unwrap();

        let token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        let security_context = jwt_manager.create_security_context(&token).unwrap();

        // Test refresh with a low threshold (should not refresh because token is fresh)
        let refresh_result = jwt_manager.refresh_token_if_needed(&security_context, 0);
        assert!(refresh_result.is_ok());
        assert!(refresh_result.unwrap().is_none()); // Should not refresh because threshold is 0

        // Test refresh with a high threshold (should refresh because 1hr token is less than 48hr threshold)
        let refresh_result = jwt_manager.refresh_token_if_needed(&security_context, 48);
        assert!(refresh_result.is_ok());
        assert!(refresh_result.unwrap().is_some()); // Should refresh
    }

    #[test]
    fn test_resource_types() {
        let entity_id = EntityId::try_from("Entity$1").unwrap();
        let field_type = FieldType::from("TestField");

        let entity_resource = Resource::entity(entity_id.clone());
        assert_eq!(entity_resource.entity_id, Some(entity_id.clone()));
        assert!(entity_resource.entity_type.is_none());
        assert!(entity_resource.field_type.is_none());

        let field_resource = Resource::field(entity_id.clone(), field_type.clone());
        assert_eq!(field_resource.entity_id, Some(entity_id));
        assert_eq!(field_resource.field_type, Some(field_type));
        assert!(field_resource.entity_type.is_none());
    }
}
