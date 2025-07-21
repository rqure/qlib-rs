use std::fmt;

/// Authentication error types
#[derive(Debug, Clone)]
pub enum AuthError {
    /// Invalid credentials provided
    InvalidCredentials,
    /// User account is disabled
    AccountDisabled,
    /// User account is locked due to too many failed attempts
    AccountLocked,
    /// User not found
    UserNotFound,
    /// Password hashing error
    PasswordHashError(String),
    /// Store operation error
    StoreError(String),
    /// Invalid name format (empty, whitespace only, etc.)
    InvalidName,
    /// Password validation error (too weak, etc.)
    InvalidPassword(String),
    /// User already exists
    UserAlreadyExists,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
            AuthError::AccountDisabled => write!(f, "Account is disabled"),
            AuthError::AccountLocked => write!(f, "Account is locked due to too many failed attempts"),
            AuthError::UserNotFound => write!(f, "User not found"),
            AuthError::PasswordHashError(msg) => write!(f, "Password hashing error: {}", msg),
            AuthError::StoreError(msg) => write!(f, "Store error: {}", msg),
            AuthError::InvalidName => write!(f, "Invalid name format"),
            AuthError::InvalidPassword(msg) => write!(f, "Invalid password: {}", msg),
            AuthError::UserAlreadyExists => write!(f, "User already exists"),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<Box<dyn std::error::Error>> for AuthError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        AuthError::StoreError(err.to_string())
    }
}

/// Result type for authentication operations
pub type AuthResult<T> = Result<T, AuthError>;
