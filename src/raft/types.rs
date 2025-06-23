//! Core type definitions for the Raft implementation.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::io;

use async_raft::AppData;
use async_raft::AppDataResponse;
use crate::{Context, EntityId, EntitySchema, FieldSchema, Request};

/// Unique identifier for a Raft node
pub type NodeId = u64;

/// Configuration for Raft types
#[derive(Debug, Clone)]
pub struct RaftTypesConfig;

impl async_raft::RaftTypeConfig for RaftTypesConfig {
    type D = RaftCommand;
    type R = RaftCommand;
    type NodeId = NodeId;
    type Node = ();
    type Entry = async_raft::raft::Entry<RaftCommand>;
    type SnapshotData = io::Cursor<Vec<u8>>;
}

/// Commands that can be replicated through the Raft consensus protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Performs one or more read/write operations
    PerformRequests(Vec<Request>),
    
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

// Implement AppData trait for RaftCommand
impl AppData for RaftCommand {
    type Entry = Self;
}

// Implement AppDataResponse trait for RaftCommand
impl AppDataResponse for RaftCommand {}

impl fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftCommand::PerformRequests(requests) => write!(f, "PerformRequests({})", requests.len()),
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
    /// Request context
    pub context: Context,
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
