use std::{collections::HashMap, io, sync::Arc};
use tokio::sync::RwLock;
use async_raft::{
    storage::{CurrentSnapshotData, HardState, InitialState},
    raft::{Entry, EntryPayload, MembershipConfig},
    NodeId, RaftStorage, RaftTypeConfig,
};
use serde::{Deserialize, Serialize};
use log::info;

use super::types::{RaftCommand, RaftError};
use crate::MapStore;

// This defines the specific Raft configuration types we'll use
pub struct RaftTypesConfig;

impl RaftTypeConfig for RaftTypesConfig {
    type NodeId = NodeId;
    type Entry = Entry<RaftCommand>;
    type SnapshotData = Cursor<Vec<u8>>;
    type R = RaftCommand;
    type NID = NodeId;
}

/// Storage implementation for the raft log
#[derive(Clone)]
pub struct RaftStore {
    node_id: NodeId,
    state: Arc<RwLock<RaftState>>,
    map_store: Arc<RwLock<MapStore>>,
}

/// Internal state stored for Raft
#[derive(Serialize, Deserialize, Debug, Default)]
struct RaftState {
    // Raft log entries
    log: Vec<Entry<RaftCommand>>,
    // Hard state (voted for, current term, etc.)
    hard_state: Option<HardState>,
    // Current snapshot (if any)
    current_snapshot: Option<CurrentSnapshotData<Cursor<Vec<u8>>>>,
    // Current membership configuration
    membership: MembershipConfig,
}

/// Simple cursor implementation for snapshot data
pub struct Cursor<T> {
    inner: T,
    pos: usize,
}

impl<T> Cursor<T> {
    fn new(inner: T) -> Self {
        Self { inner, pos: 0 }
    }
}

impl Cursor<Vec<u8>> {
    fn get_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl io::Read for Cursor<Vec<u8>> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.inner.len() {
            return Ok(0);
        }

        let n = std::cmp::min(buf.len(), self.inner.len() - self.pos);
        buf[..n].copy_from_slice(&self.inner[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl io::Write for Cursor<Vec<u8>> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for b in buf {
            self.inner.push(*b);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Seek for Cursor<Vec<u8>> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match pos {
            io::SeekFrom::Start(n) => {
                self.pos = n as usize;
                Ok(self.pos as u64)
            }
            io::SeekFrom::End(n) => {
                if n > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Cannot seek past end",
                    ));
                }
                let end = self.inner.len() as i64;
                let pos = end + n;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Cannot seek before start",
                    ));
                }
                self.pos = pos as usize;
                Ok(self.pos as u64)
            }
            io::SeekFrom::Current(n) => {
                let pos = self.pos as i64 + n;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Cannot seek before start",
                    ));
                }
                self.pos = pos as usize;
                Ok(self.pos as u64)
            }
        }
    }
}

impl RaftStore {
    pub fn new(node_id: NodeId, map_store: MapStore) -> Self {
        Self {
            node_id,
            state: Arc::new(RwLock::new(RaftState::default())),
            map_store: Arc::new(RwLock::new(map_store)),
        }
    }

    pub fn get_map_store(&self) -> Arc<RwLock<MapStore>> {
        self.map_store.clone()
    }

    /// Apply a command directly to the MapStore (for read operations that don't need consensus)
    pub async fn apply_read_command(
        &self,
        command: &RaftCommand,
        context: &crate::Context,
    ) -> Result<super::types::ClientResponse, RaftError> {
        use super::types::ClientResponse;
        use crate::{Entity, PageResult};
        
        let mut store = self.map_store.write().await;
        
        match command {
            RaftCommand::PerformRequests(requests) => {
                // For read operations, we can directly apply to the store
                // Check that all requests are reads
                if requests.iter().all(|req| matches!(req, Request::Read { .. })) {
                    let mut req_copy = requests.clone();
                    store.perform(context, &mut req_copy).await
                        .map_err(|e| RaftError::ClientError(e.to_string()))?;
                    Ok(ClientResponse::Success)
                } else {
                    Err(RaftError::ClientError("Cannot process write requests directly".into()))
                }
            },
            _ => Err(RaftError::ClientError("Command requires consensus".into())),
        }
    }

    /// Apply a command to the store after it has gone through consensus
    async fn apply_command(
        &self,
        command: &RaftCommand,
        context: &crate::Context,
    ) -> Result<super::types::ClientResponse, Box<dyn std::error::Error + Send + Sync>> {
        use super::types::ClientResponse;
        
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

#[async_trait::async_trait]
impl RaftStorage<RaftTypesConfig> for RaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_membership_config(&self) -> Result<MembershipConfig, async_raft::error::StorageError> {
        let state = self.state.read().await;
        Ok(state.membership.clone())
    }

    async fn get_initial_state(&self) -> Result<InitialState, async_raft::error::StorageError> {
        let state = self.state.read().await;
        
        // Get the last log index and term
        let (last_log_index, last_log_term) = state.log.last().map_or((0, 0), |entry| {
            (entry.index, entry.term)
        });
        
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

    async fn save_hard_state(&self, hard_state: &HardState) -> Result<(), async_raft::error::StorageError> {
        let mut state = self.state.write().await;
        state.hard_state = Some(hard_state.clone());
        Ok(())
    }

    async fn get_log_entries(
        &self,
        start: u64,
        stop: u64,
    ) -> Result<Vec<Entry<RaftCommand>>, async_raft::error::StorageError> {
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
    ) -> Result<(), async_raft::error::StorageError> {
        let mut state = self.state.write().await;
        let stop = stop.unwrap_or(u64::MAX);
        
        state.log.retain(|entry| entry.index < start || entry.index >= stop);
        Ok(())
    }

    async fn append_entry_to_log(
        &self,
        entry: &Entry<RaftCommand>,
    ) -> Result<(), async_raft::error::StorageError> {
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
    ) -> Result<(), async_raft::error::StorageError> {
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
    ) -> Result<(), async_raft::error::StorageError> {
        // Apply the command to our MapStore
        let context = crate::Context {};
        self.apply_command(data, &context)
            .await
            .map_err(|e| async_raft::error::StorageError::Other {
                error: Box::new(io::Error::new(io::ErrorKind::Other, e.to_string())),
            })?;
        
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<async_raft::storage::CurrentSnapshotData<Cursor<Vec<u8>>>>, async_raft::error::StorageError> {
        let state = self.state.read().await;
        Ok(state.current_snapshot.clone())
    }

    async fn create_snapshot(
        &self,
    ) -> Result<
        async_raft::storage::CurrentSnapshotData<Cursor<Vec<u8>>>,
        async_raft::error::StorageError,
    > {
        // Create a snapshot of the MapStore
        let state = self.state.read().await;
        let store = self.map_store.read().await;
        
        // For simplicity, we're using bincode to serialize the entire store
        // In a real implementation, you'd want a more efficient snapshot mechanism
        let data = bincode::serialize(&*store).map_err(|e| async_raft::error::StorageError::Other {
            error: Box::new(io::Error::new(io::ErrorKind::Other, e.to_string())),
        })?;
        
        let last_applied_log = state.log.last().ok_or_else(|| async_raft::error::StorageError::Other {
            error: Box::new(io::Error::new(io::ErrorKind::NotFound, "No log entries to create snapshot from")),
        })?;
        
        let snapshot = CurrentSnapshotData {
            index: last_applied_log.index,
            term: last_applied_log.term,
            membership: state.membership.clone(),
            data: Cursor::new(data),
        };
        
        // Save the snapshot to our state
        let mut state = self.state.write().await;
        state.current_snapshot = Some(snapshot.clone());
        
        Ok(snapshot)
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        membership: MembershipConfig,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), async_raft::error::StorageError> {
        // Install a snapshot from another node
        let mut state = self.state.write().await;
        
        // Update the current snapshot
        state.current_snapshot = Some(CurrentSnapshotData {
            index,
            term,
            membership: membership.clone(),
            data: snapshot.clone(),
        });
        
        // Update the membership
        state.membership = membership;
        
        // Remove log entries up to the snapshot
        state.log.retain(|entry| entry.index > index);
        
        // Deserialize the MapStore from the snapshot
        let data = snapshot.get_ref();
        let new_store: MapStore = bincode::deserialize(data).map_err(|e| {
            async_raft::error::StorageError::Other {
                error: Box::new(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }
        })?;
        
        // Update our MapStore
        let mut store = self.map_store.write().await;
        *store = new_store;
        
        Ok(())
    }
}

// Implement LogReader trait
#[async_trait::async_trait]
impl async_raft::storage::LogReader<RaftTypesConfig> for RaftStore {
    async fn get_log_entries(
        &mut self,
        start: u64,
        stop: u64,
    ) -> Result<Vec<Entry<RaftCommand>>, async_raft::error::StorageError> {
        RaftStorage::get_log_entries(self, start, stop).await
    }
}

// Implement SnapshotBuilder trait
#[async_trait::async_trait]
impl async_raft::storage::SnapshotBuilder<RaftTypesConfig> for RaftStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<CurrentSnapshotData<Cursor<Vec<u8>>>, async_raft::error::StorageError> {
        self.create_snapshot().await
    }
}
