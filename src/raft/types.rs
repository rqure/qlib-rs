use async_raft::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

use crate::{Context, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, PageOpts, Request};

/// Unique identifier for a Raft node
pub type NodeId = async_raft::NodeId;

/// Error types for Raft operations
#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Not the leader: current leader is {0:?}")]
    NotLeader(Option<NodeId>),
    
    #[error("Raft error: {0}")]
    RaftError(#[from] async_raft::error::RaftError),
    
    #[error("Storage error: {0}")]
    StorageError(#[from] Box<dyn std::error::Error + Send + Sync>),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("Client error: {0}")]
    ClientError(String),
}

/// Commands that can be replicated through Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Performs one or more read/write operations
    PerformRequests(Vec<Request>),
    
    /// Creates a new entity
    CreateEntity {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
    },
    
    /// Deletes an entity
    DeleteEntity(EntityId),
    
    /// Sets entity schema
    SetEntitySchema(EntitySchema),
    
    /// Sets field schema
    SetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        field_schema: FieldSchema,
    },
}

impl fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftCommand::PerformRequests(requests) => write!(f, "PerformRequests({})", requests.len()),
            RaftCommand::CreateEntity { entity_type, .. } => write!(f, "CreateEntity({})", entity_type),
            RaftCommand::DeleteEntity(id) => write!(f, "DeleteEntity({})", id),
            RaftCommand::SetEntitySchema(schema) => write!(f, "SetEntitySchema({})", schema.entity_type),
            RaftCommand::SetFieldSchema { entity_type, field_type, .. } => {
                write!(f, "SetFieldSchema({}, {})", entity_type, field_type)
            }
        }
    }
}

/// Client request to the Raft cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    pub command: RaftCommand,
    pub context: Context,
}

/// Response from the Raft cluster to a client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    // Read responses
    EntityExists(bool),
    EntitySchema(EntitySchema),
    FieldSchema(FieldSchema),
    FieldExists(bool),
    FindEntities(Vec<EntityId>, usize, Option<String>), // items, total, next_cursor
    EntityTypes(Vec<EntityType>, usize, Option<String>), // items, total, next_cursor
    
    // Write responses
    Success,
    EntityCreated(EntityId),
    Error(String),
}
