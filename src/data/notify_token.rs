use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A unique token for a notification registration
/// This allows users to unregister specific notification callbacks
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NotifyToken(Uuid);

impl NotifyToken {
    /// Create a new random notification token
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    /// Get the inner UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for NotifyToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NotifyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for NotifyToken {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<NotifyToken> for Uuid {
    fn from(token: NotifyToken) -> Self {
        token.0
    }
}
