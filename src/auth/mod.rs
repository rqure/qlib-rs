mod authentication;
mod authorization;

pub use authentication::{
    authenticate_ldap,
    authenticate_native,
    authenticate_openid_connect,
    // Service authentication functions
    authenticate_service,
    authenticate_subject,
    authenticate_user,
    change_password,
    create_user,
    find_subject_by_name,
    find_user_by_name,
    get_service_secret,
    get_user_auth_method,
    get_user_secret,
    hash_password,
    increment_failed_attempts,
    is_service_active,
    is_user_active,
    is_user_locked,
    reset_failed_attempts,
    set_service_secret,
    set_user_auth_method,
    set_user_password,
    update_last_login,
    validate_password,
    verify_password,
    AuthConfig,
    AuthMethod,
};
pub use authorization::{get_scope, AuthorizationScope};
