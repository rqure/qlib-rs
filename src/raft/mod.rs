//! Raft consensus implementation for distributed systems.
//!
//! This module provides a Raft consensus implementation that can be used to build
//! distributed systems with strong consistency guarantees.

mod client;
mod error;
mod node;
mod types;
mod storage;
mod network;

// Re-export core types for easier access
pub use types::{ClientRequest, ClientResponse, NodeId, RaftCommand};
pub use error::RaftError;
pub use node::RaftNode;
pub use storage::RaftStore;
pub use network::{NetworkConfig, QuicTransport};
