//! Storage implementation for the Raft consensus protocol.

use std::{sync::Arc, io};
use anyhow::Result;
use tokio::sync::RwLock;
use async_raft::{
    storage::{CurrentSnapshotData, HardState, InitialState},
    raft::{Entry, EntryPayload, MembershipConfig},
    RaftStorage
};
use async_trait::async_trait;
use log::info;

use crate::{MapStore, Context, Request};
use crate::raft::{
    types::{RaftCommand, NodeId, ClientResponse},
    error::RaftError,
    storage::state::RaftState,
};

/// Storage implementation for the Raft log
#[derive(Clone)]
pub struct RaftStore {
    /// The ID of this Raft node
    node_id: NodeId,
    
    /// The Raft state
    state: Arc<RwLock<RaftState>>,
    
    /// The MapStore for applying commands
    map_store: Arc<RwLock<MapStore>>,
}

impl RaftStore {
    /// Create a new RaftStore with the given node ID and MapStore
    pub fn new(node_id: NodeId, map_store: MapStore) -> Self {
        Self {
            node_id,
            state: Arc::new(RwLock::new(RaftState::new())),
            map_store: Arc::new(RwLock::new(map_store)),
        }
    }

    /// Get the MapStore
    pub fn get_map_store(&self) -> Arc<RwLock<MapStore>> {
        self.map_store.clone()
    }

    /// Apply a command directly to the MapStore (for read operations that don't need consensus)
    pub async fn apply_read_command(
        &self,
        command: &RaftCommand,
        context: &Context,
    ) -> Result<ClientResponse, RaftError> {
        
        let mut store = self.map_store.write().await;
        
        match command {
            RaftCommand::PerformRequests(requests) => {
                // For read operations, we can directly apply to the store
                // Check that all requests are reads
                if requests.iter().all(|req| matches!(req, Request::Read { .. })) {
                    let mut req_copy = requests.clone();
                    store.perform(context, &mut req_copy).await
                        .map_err(|e| RaftError::Client(e.to_string()))?;
                    Ok(ClientResponse::Success)
                } else {
                    Err(RaftError::Client("Cannot process write requests directly".into()))
                }
            },
            RaftCommand::CreateEntity { .. } => {
                Err(RaftError::Client("Create entity operations require consensus".into()))
            },
            RaftCommand::DeleteEntity(_) => {
                Err(RaftError::Client("Delete entity operations require consensus".into()))
            },
            RaftCommand::SetEntitySchema(_) => {
                Err(RaftError::Client("Schema operations require consensus".into()))
            },
            RaftCommand::SetFieldSchema { .. } => {
                Err(RaftError::Client("Schema operations require consensus".into()))
            },
        }
    }

    /// Apply a command to the store after it has gone through consensus
    async fn apply_command(
        &self,
        command: &RaftCommand,
        context: &Context,
    ) -> Result<ClientResponse, Box<dyn std::error::Error + Send + Sync>> {
        
        let mut store = self.map_store.write().await;
        info!("Applying command: {}", command);
        
        match command {
            RaftCommand::PerformRequests(requests) => {
                let mut req_copy = requests.clone();
                match store.perform(context, &mut req_copy).await {
                    Ok(_) => Ok(ClientResponse::Success),
                    Err(e) => Err(Box::new(anyhow::Error::msg(e.to_string())))
                }
            },
            RaftCommand::CreateEntity { entity_type, parent_id, name } => {
                match store.create_entity(context, entity_type.clone(), parent_id.clone(), name).await {
                    Ok(entity) => Ok(ClientResponse::EntityCreated(entity.entity_id)),
                    Err(e) => Err(Box::new(anyhow::Error::msg(e.to_string())))
                }
            },
            RaftCommand::DeleteEntity(entity_id) => {
                match store.delete_entity(context, entity_id).await {
                    Ok(_) => Ok(ClientResponse::Success),
                    Err(e) => Err(Box::new(anyhow::Error::msg(e.to_string())))
                }
            },
            RaftCommand::SetEntitySchema(schema) => {
                match store.set_entity_schema(context, schema).await {
                    Ok(_) => Ok(ClientResponse::Success),
                    Err(e) => Err(Box::new(anyhow::Error::msg(e.to_string())))
                }
            },
            RaftCommand::SetFieldSchema { entity_type, field_type, field_schema } => {
                match store.set_field_schema(context, entity_type, field_type, field_schema).await {
                    Ok(_) => Ok(ClientResponse::Success),
                    Err(e) => Err(Box::new(anyhow::Error::msg(e.to_string())))
                }
            }
        }
    }
}

#[async_trait]
impl RaftStorage<RaftCommand, RaftCommand> for RaftStore {
    type Snapshot = io::Cursor<Vec<u8>>;
    type ShutdownError = io::Error;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let state = self.state.read().await;
        Ok(state.membership.clone())
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let state = self.state.read().await;
        
        // Get the last log index and term
        let (last_log_index, last_log_term) = state.get_last_log_info();
        
        // Get the hard state or create an initial one
        let hard_state = state.hard_state.clone().unwrap_or(HardState {
            current_term: 0,
            voted_for: None,
        });
        
        Ok(InitialState {
            last_log_index,
            last_log_term,
            hard_state,
            membership: state.membership.clone(),
            last_applied_log: state.last_applied_log,
        })
    }

    async fn save_hard_state(&self, hard_state: &HardState) -> Result<()> {
        let mut state = self.state.write().await;
        state.hard_state = Some(hard_state.clone());
        Ok(())
    }

    async fn get_log_entries(
        &self,
        start: u64,
        stop: u64,
    ) -> Result<Vec<Entry<RaftCommand>>> {
        let state = self.state.read().await;
        let entries = state
            .log
            .iter()
            .filter(|entry| entry.index >= start && entry.index < stop)
            .cloned()
            .collect();
        Ok(entries)
    }

    async fn delete_logs_from(
        &self,
        start: u64,
        stop: Option<u64>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        let stop = stop.unwrap_or(u64::MAX);
        
        state.log.retain(|entry| entry.index < start || entry.index >= stop);
        Ok(())
    }

    async fn append_entry_to_log(
        &self,
        entry: &Entry<RaftCommand>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        state.log.push(entry.clone());
        
        // If this is a configuration entry, update membership
        if let EntryPayload::ConfigChange(cfg) = &entry.payload {
            state.membership = cfg.membership.clone();
        } else if let EntryPayload::SnapshotPointer(_) = &entry.payload {
            // Handle snapshot pointer entries if needed
        }
        
        Ok(())
    }

    async fn replicate_to_log(
        &self,
        entries: &[Entry<RaftCommand>],
    ) -> Result<()> {
        let mut state = self.state.write().await;
        
        // Add all entries to the log
        for entry in entries {
            // Check if we already have this entry
            if let Some(existing) = state.log.iter().find(|e| e.index == entry.index) {
                // If the terms are different, we need to remove this and all subsequent entries
                if existing.term != entry.term {
                    state.log.retain(|e| e.index < entry.index);
                    state.log.push(entry.clone());
                }
            } else {
                state.log.push(entry.clone());
            }
            
            // Update membership if this is a configuration entry
            if let EntryPayload::ConfigChange(cfg) = &entry.payload {
                state.membership = cfg.membership.clone();
            }
        }
        
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: u64,
        data: &RaftCommand,
    ) -> Result<()> {
        // Apply the command to our MapStore
        let context = Context {};
        match self.apply_command(data, &context).await {
            Ok(_) => {
                // Update last applied log
                let mut state = self.state.write().await;
                // Get the term from logs - look up by index
                let term = state.log.iter()
                    .find(|entry| entry.index == index)
                    .map(|entry| entry.term)
                    .unwrap_or(0); // Default to 0 if not found
                
                state.update_applied(index, term);
                Ok(())
            },
            Err(e) => {
                anyhow::bail!("Failed to apply entry: {}", e)
            }
        }
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(u64, &RaftCommand)],
    ) -> Result<()> {
        let context = Context {};
        
        for (index, command) in entries {
            match self.apply_command(command, &context).await {
                Ok(_) => {
                    // Update last applied log
                    let mut state = self.state.write().await;
                    
                    // Get the term from logs directly
                    let term = state.log.iter()
                        .find(|entry| entry.index == *index)
                        .map(|entry| entry.term)
                        .unwrap_or(0); // Default to 0 if not found
                    
                    state.update_applied(*index, term);
                },
                Err(e) => {
                    anyhow::bail!("Failed to apply entry: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        // Create a snapshot of the MapStore
        let state = self.state.read().await;
        let store = self.map_store.read().await;
        
        // For simplicity, we're using bincode to serialize the entire store
        let data = bincode::serialize(&*store)
            .map_err(|e| anyhow::anyhow!("Serialization error: {}", e))?;
        
        let (last_index, last_term) = state.last_applied_log
            .ok_or_else(|| anyhow::anyhow!("No logs applied yet"))?;
        
        let snapshot = CurrentSnapshotData {
            index: last_index,
            term: last_term,
            membership: state.membership.clone(),
            snapshot: io::Cursor::new(data),
        };
        
        // Save the snapshot to our state
        let mut state = self.state.write().await;
        state.current_snapshot = Some(snapshot.clone());
        
        Ok(snapshot)
    }

    async fn create_snapshot(&self) -> Result<(Self::Snapshot, u64)> {
        // Create a snapshot
        let snapshot_data = self.do_log_compaction().await?;
        
        // Return the snapshot data and its index
        Ok((snapshot_data.snapshot, snapshot_data.index))
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        let state = self.state.read().await;
        Ok(state.current_snapshot.clone())
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        // Install a snapshot from another node
        let mut state = self.state.write().await;
        
        let membership = state.membership.clone(); // Use the current membership
        
        // Update the current snapshot
        state.current_snapshot = Some(CurrentSnapshotData {
            index,
            term,
            membership: membership.clone(),
            snapshot: *snapshot.clone(),
        });
        
        // Update the last applied log index
        state.last_applied_log = Some((index, term));
        
        // Remove log entries up to the snapshot if specified
        if let Some(delete_through) = delete_through {
            state.log.retain(|entry| entry.index > delete_through);
        }
        
        // Deserialize the MapStore from the snapshot
        let bytes = snapshot.get_ref();
        let new_store: MapStore = bincode::deserialize(bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;
        
        // Update our MapStore
        let mut store = self.map_store.write().await;
        *store = new_store;
        
        Ok(())
    }
}
