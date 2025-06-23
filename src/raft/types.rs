//! Core type definitions for the Raft implementation.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{EntityId, EntitySchema, FieldSchema};

/// Unique identifier for a Raft node
pub type NodeId = u64;

/// Commands that can be replicated through the Raft consensus protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Creates a new entity
    CreateEntity {
        entity_type: String,
        parent_id: Option<EntityId>,
        name: String,
    },
    
    /// Deletes an entity
    DeleteEntity(EntityId),
    
    /// Sets entity schema
    SetEntitySchema(EntitySchema),
    
    /// Sets field schema for an entity type
    SetFieldSchema {
        entity_type: String,
        field_type: String,
        field_schema: FieldSchema,
    },
}

impl fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftCommand::CreateEntity { entity_type, .. } => write!(f, "CreateEntity({})", entity_type),
            RaftCommand::DeleteEntity(id) => write!(f, "DeleteEntity({})", id),
            RaftCommand::SetEntitySchema(schema) => write!(f, "SetEntitySchema({})", schema.entity_type),
            RaftCommand::SetFieldSchema { entity_type, field_type, .. } => {
                write!(f, "SetFieldSchema({}.{})", entity_type, field_type)
            },
        }
    }
}

/// Client request to the Raft cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    /// The command to execute
    pub command: RaftCommand,
    /// Client request ID (for deduplication)
    pub request_id: Option<String>,
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
    EntityTypes(Vec<String>, usize, Option<String>), // items, total, next_cursor
    
    // Write responses
    Success,
    EntityCreated(EntityId),
    Error(String),
}
