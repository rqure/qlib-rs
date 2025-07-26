use crate::{Context, EntityId, FieldType, Store};
use crate::auth::{AuthorizationManager, AccessType};

/// Permission checker for store operations
pub struct PermissionChecker {
    /// Authorization manager for evaluating permissions
    authorization_manager: Option<AuthorizationManager>,
}

impl Default for PermissionChecker {
    fn default() -> Self {
        Self {
            authorization_manager: None,
        }
    }
}

impl PermissionChecker {
    /// Create a new permission checker
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a permission checker with an authorization manager
    pub fn with_authorization_manager(authorization_manager: AuthorizationManager) -> Self {
        Self {
            authorization_manager: Some(authorization_manager),
        }
    }

    /// Check if the context has permission to read from an entity field
    pub fn can_read(
        &mut self,
        store: &mut Store,
        ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // If no security context, allow by default (backward compatibility)
        let security_context = match ctx.get_security_context() {
            Some(sc) => sc,
            None => return Ok(true),
        };

        // Use authorization manager if available and user is authenticated
        if let Some(auth_manager) = &mut self.authorization_manager {
            if let Some(subject_id) = security_context.get_subject_id() {
                return auth_manager.check_permission(
                    store,
                    ctx,
                    subject_id,
                    entity_id,
                    field_type,
                    AccessType::Read,
                ).map_err(|e| e.into());
            }
        }

        // QOS Default policy: If no AuthorizationRules exist, allow access
        // This implements the QOS specification: "By default, if a resource does not have an 
        // AuthorizationRule associated to it, anyone can read or write to it"
        Ok(true)
    }

    /// Check if the context has permission to write to an entity field
    pub fn can_write(
        &mut self,
        store: &mut Store,
        ctx: &Context,
        entity_id: &EntityId,
        field_type: &FieldType,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // If no security context, allow by default (backward compatibility)
        let security_context = match ctx.get_security_context() {
            Some(sc) => sc,
            None => return Ok(true),
        };

        // Use authorization manager if available and user is authenticated
        if let Some(auth_manager) = &mut self.authorization_manager {
            if let Some(subject_id) = security_context.get_subject_id() {
                return auth_manager.check_permission(
                    store,
                    ctx,
                    subject_id,
                    entity_id,
                    field_type,
                    AccessType::Write,
                ).map_err(|e| e.into());
            }
        }

        // QOS Default policy: If no AuthorizationRules exist, allow access
        // This implements the QOS specification: "By default, if a resource does not have an 
        // AuthorizationRule associated to it, anyone can read or write to it"
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SecurityContext;

    fn create_test_store() -> crate::Store {
        crate::Store::new(std::sync::Arc::new(crate::Snowflake::new()))
    }

    #[test]
    fn test_permission_checker_anonymous() {
        let mut checker = PermissionChecker::new();
        let mut store = create_test_store();
        let ctx = Context::new(); // No security context
        let entity_id = EntityId::try_from("Entity$1").unwrap();
        let field_type = FieldType::from("TestField");

        // Anonymous context should allow by default (backward compatibility)
        assert!(checker.can_read(&mut store, &ctx, &entity_id, &field_type).unwrap());
        assert!(checker.can_write(&mut store, &ctx, &entity_id, &field_type).unwrap());
    }

    #[test]
    fn test_permission_checker_admin() {
        let mut checker = PermissionChecker::new();
        let mut store = create_test_store();
        let admin_ctx = Context::with_security(SecurityContext::with_subject(
            EntityId::try_from("User$1").unwrap()
        ));
        let entity_id = EntityId::try_from("Entity$1").unwrap();
        let field_type = FieldType::from("TestField");

        // Admin should have all permissions
        assert!(checker.can_read(&mut store, &admin_ctx, &entity_id, &field_type).unwrap());
        assert!(checker.can_write(&mut store, &admin_ctx, &entity_id, &field_type).unwrap());
    }

    #[test]
    fn test_permission_checker_unauthenticated() {
        let mut checker = PermissionChecker::new();
        let mut store = create_test_store();
        let unauth_ctx = Context::with_security(SecurityContext::anonymous());
        let entity_id = EntityId::try_from("Entity$1").unwrap();
        let field_type = FieldType::from("TestField");

        // QOS Default Policy: Unauthenticated context should be allowed access when no AuthorizationRules exist
        // This implements the QOS specification: "By default, if a resource does not have an 
        // AuthorizationRule associated to it, anyone can read or write to it"
        assert!(checker.can_read(&mut store, &unauth_ctx, &entity_id, &field_type).unwrap());
        assert!(checker.can_write(&mut store, &unauth_ctx, &entity_id, &field_type).unwrap());
    }
}
