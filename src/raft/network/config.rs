//! Network transport configuration for Raft.

use std::net::SocketAddr;
use rustls::{Certificate, PrivateKey};
use std::time::Duration;

/// Generates a self-signed certificate for testing
fn generate_self_signed_cert() -> (Certificate, PrivateKey) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = PrivateKey(cert.serialize_private_key_der());
    let cert = Certificate(cert.serialize_der().unwrap());
    (cert, key)
}

/// Configuration for a QUIC transport endpoint
#[derive(Clone)]
pub struct NetworkConfig {
    /// Local address to bind to
    pub addr: SocketAddr,
    
    /// Server certificate and private key
    pub server_cert: Option<(Certificate, PrivateKey)>,
    
    /// Client certificates for verification
    pub client_certs: Option<Vec<Certificate>>,
    
    /// Whether to verify certificates (false for development)
    pub verify_certs: bool,
    
    /// Idle timeout for connections
    pub idle_timeout: Duration,
    
    /// Keep alive interval
    pub keep_alive: Option<Duration>,
}

impl NetworkConfig {
    /// Create a new NetworkConfig with default values
    pub fn new(addr: SocketAddr) -> Self {
        NetworkConfig {
            addr,
            server_cert: None,
            client_certs: None,
            verify_certs: false, // Default to insecure for development
            idle_timeout: Duration::from_secs(30),
            keep_alive: Some(Duration::from_secs(5)),
        }
    }

    /// Create a NetworkConfig with a self-signed certificate
    pub fn with_self_signed_cert(addr: SocketAddr) -> Self {
        let cert = generate_self_signed_cert();
        NetworkConfig {
            addr,
            server_cert: Some(cert),
            client_certs: None,
            verify_certs: false,
            idle_timeout: Duration::from_secs(30),
            keep_alive: Some(Duration::from_secs(5)),
        }
    }
    
    /// Enable certificate verification (for production use)
    pub fn with_cert_verification(mut self) -> Self {
        self.verify_certs = true;
        self
    }
    
    /// Set custom idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }
    
    /// Set custom keep-alive interval
    pub fn with_keep_alive(mut self, interval: Option<Duration>) -> Self {
        self.keep_alive = interval;
        self
    }
}
