//! Error types for the Raft implementation.

use thiserror::Error;

use crate::raft::types::NodeId;

/// Comprehensive error type for Raft operations
#[derive(Error, Debug)]
pub enum RaftError {
    /// The current node is not the leader
    #[error("Not the leader: current leader is {0:?}")]
    NotLeader(Option<NodeId>),
    
    /// An error from the underlying Raft consensus library
    #[error("Raft consensus error: {0}")]
    Consensus(String),
    
    /// An error with the Raft storage implementation
    #[error("Storage error: {0}")]
    Storage(#[from] Box<dyn std::error::Error + Send + Sync>),
    
    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    /// Client-related error
    #[error("Client error: {0}")]
    Client(String),

    /// Network-related error
    #[error("Network error: {0}")]
    Network(String),
    
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// Timeout error
    #[error("Operation timed out after {0}ms")]
    Timeout(u64),
    
    /// Node operation error
    #[error("Node operation error: {0}")]
    Operation(String),
    
    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

impl From<async_raft::error::ClientWriteError<RaftCommand>> for RaftError {
    fn from(error: async_raft::error::ClientWriteError<RaftCommand>) -> Self {
        match error {
            async_raft::error::ClientWriteError::ForwardToLeader { leader_id, .. } => {
                RaftError::NotLeader(leader_id)
            }
            _ => RaftError::Consensus(format!("{:?}", error)),
        }
    }
}

impl From<async_raft::RaftError> for RaftError {
    fn from(error: async_raft::RaftError) -> Self {
        RaftError::Consensus(format!("{:?}", error))
    }
}

impl From<std::io::Error> for RaftError {
    fn from(error: std::io::Error) -> Self {
        RaftError::Storage(Box::new(error))
    }
}
