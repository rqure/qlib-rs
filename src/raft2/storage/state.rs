//! Raft state implementation.

use std::io;
use serde::{Deserialize, Serialize};
use async_raft::{
    storage::CurrentSnapshotData,
    raft::{Entry, HardState, MembershipConfig},
};

use crate::raft2::types::RaftCommand;

/// State stored for Raft consensus
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RaftState {
    /// Raft log entries
    pub log: Vec<Entry<RaftCommand>>,
    
    /// Hard state (voted for, current term, etc.)
    pub hard_state: Option<HardState>,
    
    /// Current snapshot (if any)
    pub current_snapshot: Option<CurrentSnapshotData<io::Cursor<Vec<u8>>>>,
    
    /// Current membership configuration
    pub membership: MembershipConfig,
    
    /// Last applied log (required by Raft impl)
    pub last_applied_log: Option<(u64, u64)>, // (index, term)
}

impl RaftState {
    /// Create a new empty RaftState
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Get the highest log index and term
    pub fn get_last_log_info(&self) -> (u64, u64) {
        self.log.last().map_or((0, 0), |entry| {
            (entry.index, entry.term)
        })
    }
    
    /// Update the last applied log
    pub fn update_applied(&mut self, index: u64, term: u64) {
        self.last_applied_log = Some((index, term));
    }
}
