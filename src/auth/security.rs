use crate::{EntityId, FieldType};
use serde::{Deserialize, Serialize};
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use chrono::{Duration, Utc};

/// JWT claims structure for authorization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject ID (user entity ID)
    pub sub: String,
    /// Issued at (timestamp)
    pub iat: i64,
    /// Expiration time (timestamp)
    pub exp: i64,
    /// User name
    pub name: String,
}

/// Security context containing JWT information and subject details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityContext {
    /// JWT token (optional, if not provided no authorization checks are performed)
    pub token: Option<String>,
    /// Decoded JWT claims (if token is provided and valid)
    pub claims: Option<JwtClaims>,
    /// Subject ID for authorization (if authenticated)
    pub subject_id: Option<EntityId>,
}

impl Default for SecurityContext {
    fn default() -> Self {
        Self {
            token: None,
            claims: None,
            subject_id: None,
        }
    }
}

impl SecurityContext {
    /// Create a new security context without authentication (anonymous)
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// Create a new security context with a JWT token
    pub fn with_token(token: String) -> Self {
        Self {
            token: Some(token),
            claims: None,
            subject_id: None,
        }
    }

    /// Create a new security context with validated claims
    pub fn with_claims(token: String, claims: JwtClaims) -> Option<Self> {
        EntityId::try_from(claims.sub.as_str())
            .and_then(|subject_id| {
                Ok(Self {
                    token: Some(token),
                    claims: Some(claims),
                    subject_id: Some(subject_id),
                })
            })
            .ok()
    }

    /// Create a security context with a subject ID (for testing purposes)
    pub fn with_subject(subject_id: EntityId) -> Self {
        Self {
            token: None,
            claims: None,
            subject_id: Some(subject_id),
        }
    }

    /// Check if the context is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.claims.is_some() || self.subject_id.is_some()
    }

    /// Get the subject ID if authenticated
    pub fn get_subject_id(&self) -> Option<&EntityId> {
        self.subject_id.as_ref()
    }
}

/// JWT token manager for creating and validating tokens
pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    algorithm: Algorithm,
    token_expiry_hours: i64,
}

impl JwtManager {
    /// Create a new JWT manager with a secret key
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret),
            decoding_key: DecodingKey::from_secret(secret),
            algorithm: Algorithm::HS256,
            token_expiry_hours: 24, // Default 24 hours
        }
    }

    /// Set token expiry in hours
    pub fn with_expiry_hours(mut self, hours: i64) -> Self {
        self.token_expiry_hours = hours;
        self
    }

    /// Generate a JWT token for a subject
    pub fn generate_token(
        &self,
        subject_id: &EntityId,
        name: &str,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let now = Utc::now();
        let expiry = now + Duration::hours(self.token_expiry_hours);

        let claims = JwtClaims {
            sub: subject_id.to_string(),
            iat: now.timestamp(),
            exp: expiry.timestamp(),
            name: name.to_string(),
        };

        encode(&Header::new(self.algorithm), &claims, &self.encoding_key)
    }

    /// Validate and decode a JWT token
    pub fn validate_token(&self, token: &str) -> Result<JwtClaims, jsonwebtoken::errors::Error> {
        let mut validation = Validation::new(self.algorithm);
        validation.validate_exp = true;

        match decode::<JwtClaims>(token, &self.decoding_key, &validation) {
            Ok(token_data) => Ok(token_data.claims),
            Err(e) => Err(e),
        }
    }

    /// Create a security context from a JWT token
    pub fn create_security_context(&self, token: &str) -> Option<SecurityContext> {
        match self.validate_token(token) {
            Ok(claims) => SecurityContext::with_claims(token.to_string(), claims),
            Err(_) => Some(SecurityContext::with_token(token.to_string())),
        }
    }

    /// Refresh a token if it's close to expiry
    pub fn refresh_token_if_needed(
        &self,
        security_context: &SecurityContext,
        refresh_threshold_hours: i64,
    ) -> Result<Option<String>, jsonwebtoken::errors::Error> {
        if let Some(claims) = &security_context.claims {
            let now = Utc::now().timestamp();
            let time_to_expiry = claims.exp - now;
            let threshold_seconds = refresh_threshold_hours * 3600;

            if time_to_expiry < threshold_seconds {
                let subject_id = EntityId::try_from(claims.sub.as_str()).ok();
                if let Some(subject_id) = subject_id {
                    let new_token = self.generate_token(
                        &subject_id,
                        &claims.name,
                    )?;
                    return Ok(Some(new_token));
                } else {
                    return Err(jsonwebtoken::errors::Error::from(
                        jsonwebtoken::errors::ErrorKind::InvalidToken,
                    ));
                };
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_context_creation() {
        let ctx = SecurityContext::anonymous();
        assert!(!ctx.is_authenticated());
        assert!(ctx.get_subject_id().is_none());

        let admin_ctx = SecurityContext::with_subject(EntityId::try_from("User$1").unwrap());
        assert!(admin_ctx.is_authenticated());
        assert!(admin_ctx.get_subject_id().is_some());
    }

    #[test]
    fn test_jwt_manager() {
        let secret = b"test_secret_key_that_is_long_enough";
        let jwt_manager = JwtManager::new(secret).with_expiry_hours(1);

        let subject_id = EntityId::try_from("User$1").unwrap();

        let token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        let claims = jwt_manager
            .validate_token(&token)
            .expect("Should validate token");

        assert!(claims.sub.contains("test_user")); // The full EntityId string contains the type
        assert_eq!(claims.name, "Test User");
    }

    #[test]
    fn test_security_context_with_jwt() {
        let secret = b"test_secret_key_that_is_long_enough";
        let jwt_manager = JwtManager::new(secret);

        let subject_id = EntityId::try_from("User$1").unwrap();

        let token = jwt_manager
            .generate_token(&subject_id, "Test User")
            .expect("Should generate token");

        let security_context = jwt_manager.create_security_context(&token).unwrap();

        assert!(security_context.is_authenticated());
        assert_eq!(
            security_context.get_subject_id().unwrap().get_type().to_string(),
            "test_user"
        );
    }

    #[test]
    fn test_invalid_jwt_token() {
        let secret = b"test_secret_key_that_is_long_enough";
        let jwt_manager = JwtManager::new(secret);

        let security_context = jwt_manager.create_security_context("invalid_token").unwrap();

        assert!(!security_context.is_authenticated());
        assert!(security_context.get_subject_id().is_none());
    }
}
