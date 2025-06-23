//! Network transport implementation for the Raft consensus protocol.

mod config;
mod quic;

pub use config::NetworkConfig;
pub use quic::QuicTransport;
