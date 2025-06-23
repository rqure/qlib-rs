//! Storage implementation for the Raft consensus protocol.

mod state;
mod store;

pub use state::RaftState;
pub use store::RaftStore;
