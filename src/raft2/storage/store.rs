//! Storage implementation for the Raft consensus protocol.

use std::{sync::Arc, io};
use tokio::sync::RwLock;
use async_raft::{
    storage::{CurrentSnapshotData, HardState, InitialState},
    RaftStorage, raft::{Entry, EntryPayload, MembershipConfig}
};
use async_trait::async_trait;
use log::{info, error};

use crate::{MapStore, Context};
use crate::raft2::{
    types::{RaftCommand, RaftTypesConfig, NodeId},
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
    ) -> Result<crate::raft2::ClientResponse, RaftError> {
        use crate::raft2::ClientResponse;
        
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
    ) -> Result<crate::raft2::ClientResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::raft2::ClientResponse;
        
        let mut store = self.map_store.write().await;
        info!("Applying command: {}", command);
        
        match command {
            RaftCommand::PerformRequests(requests) => {
                let mut req_copy = requests.clone();
                store.perform(context, &mut req_copy).await?;
                Ok(ClientResponse::Success)
            },
            RaftCommand::CreateEntity { entity_type, parent_id, name } => {
                let entity = store.create_entity(context, entity_type.clone(), parent_id.clone(), name).await?;
                Ok(ClientResponse::EntityCreated(entity.entity_id))
            },
            RaftCommand::DeleteEntity(entity_id) => {
                store.delete_entity(context, entity_id).await?;
                Ok(ClientResponse::Success)
            },
            RaftCommand::SetEntitySchema(schema) => {
                store.set_entity_schema(context, schema).await?;
                Ok(ClientResponse::Success)
            },
            RaftCommand::SetFieldSchema { entity_type, field_type, field_schema } => {
                store.set_field_schema(context, entity_type, field_type, field_schema).await?;
                Ok(ClientResponse::Success)
            }
        }
    }
}

#[async_trait]
impl RaftStorage<RaftTypesConfig> for RaftStore {
    type Snapshot = io::Cursor<Vec<u8>>;
    type ShutdownError = io::Error;

    async fn get_membership_config(&self) -> Result<MembershipConfig, async_raft::StorageError> {
        let state = self.state.read().await;
        Ok(state.membership.clone())
    }

    async fn get_initial_state(&self) -> Result<InitialState, async_raft::StorageError> {
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
        })
    }

    async fn save_hard_state(&self, hard_state: &HardState) -> Result<(), async_raft::StorageError> {
        let mut state = self.state.write().await;
        state.hard_state = Some(hard_state.clone());
        Ok(())
    }

    async fn get_log_entries(
        &self,
        start: u64,
        stop: u64,
    ) -> Result<Vec<Entry<RaftCommand>>, async_raft::StorageError> {
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
    ) -> Result<(), async_raft::StorageError> {
        let mut state = self.state.write().await;
        let stop = stop.unwrap_or(u64::MAX);
        
        state.log.retain(|entry| entry.index < start || entry.index >= stop);
        Ok(())
    }

    async fn append_entry_to_log(
        &self,
        entry: &Entry<RaftCommand>,
    ) -> Result<(), async_raft::StorageError> {
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
    ) -> Result<(), async_raft::StorageError> {
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
        term: u64,
    ) -> Result<(), async_raft::StorageError> {
        // Apply the command to our MapStore
        let context = Context {};
        match self.apply_command(data, &context).await {
            Ok(_) => {
                // Update last applied log
                let mut state = self.state.write().await;
                state.update_applied(index, term);
                Ok(())
            },
            Err(e) => {
                Err(async_raft::StorageError::Other { 
                    error: format!("Failed to apply entry: {}", e).into() 
                })
            }
        }
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(&Entry<RaftCommand>, &RaftCommand)],
    ) -> Result<(), async_raft::StorageError> {
        let context = Context {};
        
        for (entry, command) in entries {
            match self.apply_command(command, &context).await {
                Ok(_) => {
                    // Update last applied log
                    let mut state = self.state.write().await;
                    state.update_applied(entry.index, entry.term);
                },
                Err(e) => {
                    return Err(async_raft::StorageError::Other { 
                        error: format!("Failed to apply entry: {}", e).into() 
                    });
                }
            }
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>, async_raft::StorageError> {
        // Create a snapshot of the MapStore
        let state = self.state.read().await;
        let store = self.map_store.read().await;
        
        // For simplicity, we're using bincode to serialize the entire store
        let data = bincode::serialize(&*store).map_err(|e| async_raft::StorageError::Other {
            error: format!("Serialization error: {}", e).into(),
        })?;
        
        let (last_index, last_term) = state.last_applied_log.ok_or_else(|| async_raft::StorageError::Other {
            error: "No logs applied yet".into(),
        })?;
        
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

    async fn create_snapshot(&self) -> Result<(Self::Snapshot, u64), async_raft::StorageError> {
        // Create a snapshot
        let snapshot_data = self.do_log_compaction().await?;
        
        // Return the snapshot data and its index
        Ok((snapshot_data.snapshot, snapshot_data.index))
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>, async_raft::StorageError> {
        let state = self.state.read().await;
        Ok(state.current_snapshot.clone())
    }

    async fn install_snapshot(
        &self,
        meta: &CurrentSnapshotData<Self::Snapshot>,
        snapshot: Self::Snapshot,
    ) -> Result<(), async_raft::StorageError> {
        // Install a snapshot from another node
        let mut state = self.state.write().await;
        
        // Update the current snapshot
        state.current_snapshot = Some(CurrentSnapshotData {
            index: meta.index,
            term: meta.term,
            membership: meta.membership.clone(),
            snapshot: snapshot.clone(),
        });
        
        // Update the membership
        state.membership = meta.membership.clone();
        
        // Update the last applied log index
        state.last_applied_log = Some((meta.index, meta.term));
        
        // Remove log entries up to the snapshot
        state.log.retain(|entry| entry.index > meta.index);
        
        // Deserialize the MapStore from the snapshot
        let bytes = snapshot.get_ref();
        let new_store: MapStore = bincode::deserialize(bytes).map_err(|e| {
            async_raft::StorageError::Other {
                error: format!("Deserialization error: {}", e).into(),
            }
        })?;
        
        // Update our MapStore
        let mut store = self.map_store.write().await;
        *store = new_store;
        
        Ok(())
    }
}
