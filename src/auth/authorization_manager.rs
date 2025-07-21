use crate::{
    auth::{
        error::{AuthError, AuthResult},
        *,
    },
    Context, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, Single, Store, Value,
    AdjustBehavior, PushCondition, Request,
};

/// Scope of authorization - defines what level of access is granted
#[derive(Debug, Clone, PartialEq)]
pub enum AuthorizationScope {
    ReadOnly,
    ReadAndWrite,
}

impl AuthorizationScope {
    pub fn as_choice_value(&self) -> i64 {
        match self {
            AuthorizationScope::ReadOnly => 0,
            AuthorizationScope::ReadAndWrite => 1,
        }
    }

    pub fn from_choice_value(value: i64) -> AuthResult<Self> {
        match value {
            0 => Ok(AuthorizationScope::ReadOnly),
            1 => Ok(AuthorizationScope::ReadAndWrite),
            _ => Err(AuthError::StoreError(format!("Invalid authorization scope value: {}", value))),
        }
    }

    pub fn allows_read(&self) -> bool {
        true // Both scopes allow read
    }

    pub fn allows_write(&self) -> bool {
        match self {
            AuthorizationScope::ReadOnly => false,
            AuthorizationScope::ReadAndWrite => true,
        }
    }
}

/// Type of access being requested
#[derive(Debug, Clone, PartialEq)]
pub enum AccessType {
    Read,
    Write,
}

/// Configuration for authorization behavior
#[derive(Debug, Clone)]
pub struct AuthorizationConfig {
    /// Default access when no authorization rules exist
    pub default_allow_read: bool,
    pub default_allow_write: bool,
}

impl Default for AuthorizationConfig {
    fn default() -> Self {
        Self {
            default_allow_read: true,
            default_allow_write: true,
        }
    }
}

/// Authorization manager for handling resource access control
pub struct AuthorizationManager {
    config: AuthorizationConfig,
}

impl AuthorizationManager {
    /// Create a new authorization manager
    pub fn new() -> Self {
        Self::with_config(AuthorizationConfig::default())
    }

    /// Create a new authorization manager with custom configuration
    pub fn with_config(config: AuthorizationConfig) -> Self {
        Self {
            config,
        }
    }

    /// Initialize the authorization-related entity schemas
    pub fn initialize_schemas(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        // Initialize Subject schema
        self.initialize_subject_schema(store, ctx)?;
        
        // Initialize Permission schema
        self.initialize_permission_schema(store, ctx)?;
        
        // Initialize AuthorizationRule schema
        self.initialize_authorization_rule_schema(store, ctx)?;
        
        Ok(())
    }

    /// Check if a subject has permission to access a resource
    pub fn check_permission(
        &mut self,
        store: &mut Store,
        ctx: &Context,
        subject_id: &EntityId,
        resource_entity_id: &EntityId,
        resource_field: &FieldType,
        access_type: AccessType,
    ) -> AuthResult<bool> {
        // Get the resource entity type
        let resource_entity_type = resource_entity_id.get_type();

        // Find applicable authorization rules
        let rules = self.find_authorization_rules(store, ctx, resource_entity_type, resource_field)?;

        if rules.is_empty() {
            // No rules found, use default policy
            return Ok(match access_type {
                AccessType::Read => self.config.default_allow_read,
                AccessType::Write => self.config.default_allow_write,
            });
        }

        // Check each rule
        for rule_id in rules {
            let (scope, permission_id) = self.get_rule_details(store, ctx, &rule_id)?;
            
            // Check if scope allows the requested access type
            if !scope.allows_read() && access_type == AccessType::Read {
                continue;
            }
            if !scope.allows_write() && access_type == AccessType::Write {
                continue;
            }

            // Evaluate the permission script
            if self.evaluate_permission(store, ctx, &permission_id, subject_id, resource_entity_id, resource_field)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Create a new Subject entity
    pub fn create_subject(&self, store: &mut Store, ctx: &Context, name: &str) -> AuthResult<EntityId> {
        let subject = store
            .create_entity(ctx, &subject_entity_type(), None, name)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(subject.entity_id)
    }

    /// Create a new Permission entity with a test script
    pub fn create_permission(
        &self,
        store: &mut Store,
        ctx: &Context,
        name: &str,
        test_script: &str,
    ) -> AuthResult<EntityId> {
        let permission = store
            .create_entity(ctx, &permission_entity_type(), None, name)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        // Set the TestFn field
        let mut requests = vec![Request::Write {
            entity_id: permission.entity_id.clone(),
            field_type: test_fn_field(),
            value: Some(Value::String(test_script.to_string())),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(permission.entity_id)
    }

    /// Create a new AuthorizationRule
    pub fn create_authorization_rule(
        &self,
        store: &mut Store,
        ctx: &Context,
        name: &str,
        scope: AuthorizationScope,
        resource_type: &str,
        resource_field: &str,
        permission_id: &EntityId,
    ) -> AuthResult<EntityId> {
        let rule = store
            .create_entity(ctx, &authorization_rule_entity_type(), None, name)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        // Set all the rule fields
        let mut requests = vec![
            Request::Write {
                entity_id: rule.entity_id.clone(),
                field_type: scope_field(),
                value: Some(Value::Choice(scope.as_choice_value())),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: rule.entity_id.clone(),
                field_type: resource_type_field(),
                value: Some(Value::String(resource_type.to_string())),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: rule.entity_id.clone(),
                field_type: resource_field_field(),
                value: Some(Value::String(resource_field.to_string())),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
            Request::Write {
                entity_id: rule.entity_id.clone(),
                field_type: permission_field(),
                value: Some(Value::EntityReference(Some(permission_id.clone()))),
                push_condition: PushCondition::Always,
                adjust_behavior: AdjustBehavior::Set,
                write_time: None,
                writer_id: None,
            },
        ];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(rule.entity_id)
    }

    // Private helper methods

    /// Initialize Subject entity schema
    fn initialize_subject_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        if store.get_entity_schema(ctx, &subject_entity_type()).is_ok() {
            return Ok(());
        }

        // Ensure Object schema exists first
        self.initialize_object_schema(store, ctx)?;

        // Subject inherits from Object
        let subject_schema = EntitySchema::<Single>::new(subject_entity_type(), Some(EntityType::from("Object")));

        store
            .set_entity_schema(ctx, &subject_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Initialize Permission entity schema
    fn initialize_permission_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        if store.get_entity_schema(ctx, &permission_entity_type()).is_ok() {
            return Ok(());
        }

        // Ensure Object schema exists first
        self.initialize_object_schema(store, ctx)?;

        let mut permission_schema = EntitySchema::<Single>::new(permission_entity_type(), Some(EntityType::from("Object")));

        // Add TestFn field
        permission_schema.fields.insert(
            test_fn_field(),
            FieldSchema::String {
                field_type: test_fn_field(),
                default_value: String::new(),
                rank: 0,
            },
        );

        store
            .set_entity_schema(ctx, &permission_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Initialize AuthorizationRule entity schema
    fn initialize_authorization_rule_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        if store.get_entity_schema(ctx, &authorization_rule_entity_type()).is_ok() {
            return Ok(());
        }

        // Ensure Object schema exists first
        self.initialize_object_schema(store, ctx)?;

        let mut rule_schema = EntitySchema::<Single>::new(authorization_rule_entity_type(), Some(EntityType::from("Object")));

        // Add authorization rule fields
        rule_schema.fields.insert(
            scope_field(),
            FieldSchema::Choice {
                field_type: scope_field(),
                default_value: 0, // ReadOnly
                rank: 0,
                choices: vec!["ReadOnly".to_string(), "ReadAndWrite".to_string()],
            },
        );

        rule_schema.fields.insert(
            resource_type_field(),
            FieldSchema::String {
                field_type: resource_type_field(),
                default_value: String::new(),
                rank: 1,
            },
        );

        rule_schema.fields.insert(
            resource_field_field(),
            FieldSchema::String {
                field_type: resource_field_field(),
                default_value: String::new(),
                rank: 2,
            },
        );

        rule_schema.fields.insert(
            permission_field(),
            FieldSchema::EntityReference {
                field_type: permission_field(),
                default_value: None,
                rank: 3,
            },
        );

        store
            .set_entity_schema(ctx, &rule_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Initialize Object schema if it doesn't exist
    fn initialize_object_schema(&self, store: &mut Store, ctx: &Context) -> AuthResult<()> {
        if store.get_entity_schema(ctx, &EntityType::from("Object")).is_ok() {
            return Ok(());
        }

        let mut object_schema = EntitySchema::<Single>::new(EntityType::from("Object"), None);

        // Add base Object fields
        object_schema.fields.insert(
            name_field(),
            FieldSchema::String {
                field_type: name_field(),
                default_value: String::new(),
                rank: 0,
            },
        );

        object_schema.fields.insert(
            FieldType::from("Parent"),
            FieldSchema::EntityReference {
                field_type: FieldType::from("Parent"),
                default_value: None,
                rank: 1,
            },
        );

        object_schema.fields.insert(
            FieldType::from("Children"),
            FieldSchema::EntityList {
                field_type: FieldType::from("Children"),
                default_value: Vec::new(),
                rank: 2,
            },
        );

        store
            .set_entity_schema(ctx, &object_schema)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        Ok(())
    }

    /// Find authorization rules that apply to a resource
    fn find_authorization_rules(
        &self,
        store: &mut Store,
        ctx: &Context,
        resource_type: &EntityType,
        resource_field: &FieldType,
    ) -> AuthResult<Vec<EntityId>> {
        let entities = store
            .find_entities(ctx, &authorization_rule_entity_type(), None)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        let mut matching_rules = Vec::new();

        for entity_id in entities.items {
            // Read the rule's resource type and field
            let mut requests = vec![
                Request::Read {
                    entity_id: entity_id.clone(),
                    field_type: resource_type_field(),
                    value: None,
                    write_time: None,
                    writer_id: None,
                },
                Request::Read {
                    entity_id: entity_id.clone(),
                    field_type: resource_field_field(),
                    value: None,
                    write_time: None,
                    writer_id: None,
                },
            ];

            store
                .perform(ctx, &mut requests)
                .map_err(|e| AuthError::StoreError(e.to_string()))?;

            let rule_resource_type = if let Some(Request::Read { value: Some(Value::String(rt)), .. }) = requests.get(0) {
                rt
            } else {
                continue;
            };

            let rule_resource_field = if let Some(Request::Read { value: Some(Value::String(rf)), .. }) = requests.get(1) {
                rf
            } else {
                continue;
            };

            // Check if this rule matches the resource
            if rule_resource_type == &resource_type.to_string() && rule_resource_field == &resource_field.to_string() {
                matching_rules.push(entity_id);
            }
        }

        Ok(matching_rules)
    }

    /// Get rule details (scope and permission ID)
    fn get_rule_details(
        &self,
        store: &mut Store,
        ctx: &Context,
        rule_id: &EntityId,
    ) -> AuthResult<(AuthorizationScope, EntityId)> {
        let mut requests = vec![
            Request::Read {
                entity_id: rule_id.clone(),
                field_type: scope_field(),
                value: None,
                write_time: None,
                writer_id: None,
            },
            Request::Read {
                entity_id: rule_id.clone(),
                field_type: permission_field(),
                value: None,
                write_time: None,
                writer_id: None,
            },
        ];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        let scope = if let Some(Request::Read { value: Some(Value::Choice(s)), .. }) = requests.get(0) {
            AuthorizationScope::from_choice_value(*s)?
        } else {
            return Err(AuthError::StoreError("Failed to read scope".to_string()));
        };

        let permission_id = if let Some(Request::Read { value: Some(Value::EntityReference(Some(pid))), .. }) = requests.get(1) {
            pid.clone()
        } else {
            return Err(AuthError::StoreError("Failed to read permission ID".to_string()));
        };

        Ok((scope, permission_id))
    }

    /// Evaluate a permission script
    fn evaluate_permission(
        &mut self,
        store: &mut Store,
        ctx: &Context,
        permission_id: &EntityId,
        _subject_id: &EntityId,
        _resource_entity_id: &EntityId,
        _resource_field: &FieldType,
    ) -> AuthResult<bool> {
        // Get the permission's test script
        let mut requests = vec![Request::Read {
            entity_id: permission_id.clone(),
            field_type: test_fn_field(),
            value: None,
            write_time: None,
            writer_id: None,
        }];

        store
            .perform(ctx, &mut requests)
            .map_err(|e| AuthError::StoreError(e.to_string()))?;

        let test_script = if let Some(Request::Read { value: Some(Value::String(script)), .. }) = requests.get(0) {
            script
        } else {
            return Err(AuthError::StoreError("Failed to read permission test script".to_string()));
        };

        // For now, implement a simple script evaluation
        // TODO: Integrate with proper scripting engine when available
        if test_script.trim().is_empty() {
            return Ok(false);
        }

        // Simple script evaluation - if script contains "true", return true
        // This is a placeholder implementation
        if test_script.contains("true") {
            Ok(true)
        } else if test_script.contains("false") {
            Ok(false)
        } else {
            // For more complex scripts, we would need the scripting engine
            // For now, default to allowing access
            Ok(true)
        }
    }
}
