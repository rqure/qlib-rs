use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{EntityId, StoreInterface};

#[derive(Debug, Clone)]
pub struct Context {
    // Reference to store
    pub store_interface: Arc<Mutex<StoreInterface>>,

    /// Optional security context for JWT-based authorization
    pub security_context: Option<crate::auth::SecurityContext>,
}

impl Context {
    /// Create a new context without security
    pub fn new(store_interface: Arc<Mutex<StoreInterface>>) -> Self {
        Self {
            store_interface,
            security_context: None,
        }
    }

    /// Create a new context with a security context
    pub fn with_security(store_interface: Arc<Mutex<StoreInterface>>, security_context: crate::auth::SecurityContext) -> Self {
        Self {
            store_interface,
            security_context: Some(security_context),
        }
    }

    /// Get the security context if present
    pub fn get_security_context(&self) -> Option<&crate::auth::SecurityContext> {
        self.security_context.as_ref()
    }

    /// Check if the context is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.security_context
            .as_ref()
            .map(|sc| sc.is_authenticated())
            .unwrap_or(false)
    }

    /// Get the subject ID if authenticated
    pub fn get_subject_id(&self) -> Option<&EntityId> {
        self.security_context
            .as_ref()
            .and_then(|sc| sc.get_subject_id())
    }
}
