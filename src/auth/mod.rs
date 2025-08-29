mod authentication;
mod authorization;

pub use authentication::{
    AuthConfig, AuthMethod, authenticate, change_password, find_user_by_name,
    is_user_active, is_user_locked, get_user_auth_method, get_user_secret,
    hash_password, verify_password, validate_password, increment_failed_attempts,
    reset_failed_attempts, update_last_login, create_user, set_user_password,
    set_user_auth_method, authenticate_native, authenticate_ldap, authenticate_openid_connect,
    // Service authentication functions
    authenticate_service, find_service_by_name, is_service_active,
    get_service_secret, set_service_secret, authenticate_subject,
};
pub use authorization::{
    AuthorizationScope,
};

