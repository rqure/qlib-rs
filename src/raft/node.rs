use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_raft::{Raft, RaftNetwork};
use log::{info, error};

use crate::MapStore;
use super::storage::{RaftStore, RaftTypesConfig};
use super::types::{ClientRequest, ClientResponse, NodeId, RaftCommand, RaftError};

// Network implementation stub for Raft cluster communication
struct RaftNetworkImpl {
    // In a real implementation, this would contain network client instances
    // to communicate with other nodes in the cluster
}

#[async_trait::async_trait]
impl RaftNetwork<RaftTypesConfig> for RaftNetworkImpl {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: async_raft::raft::AppendEntriesRequest<RaftCommand>,
    ) -> Result<async_raft::raft::AppendEntriesResponse, async_raft::error::NetworkError> {
        // In a real implementation, this would send a network request to another node
        Err(async_raft::error::NetworkError::Other {
            error: "Network implementation not provided".into(),
        })
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: async_raft::raft::InstallSnapshotRequest,
    ) -> Result<async_raft::raft::InstallSnapshotResponse, async_raft::error::NetworkError> {
        // In a real implementation, this would send a network request to another node
        Err(async_raft::error::NetworkError::Other {
            error: "Network implementation not provided".into(),
        })
    }

    async fn vote(
        &self,
        target: NodeId,
        rpc: async_raft::raft::VoteRequest,
    ) -> Result<async_raft::raft::VoteResponse, async_raft::error::NetworkError> {
        // In a real implementation, this would send a network request to another node
        Err(async_raft::error::NetworkError::Other {
            error: "Network implementation not provided".into(),
        })
    }
}

/// RaftNode wraps a MapStore and integrates it with Raft consensus
pub struct RaftNode {
    node_id: NodeId,
    raft: Arc<Raft<RaftTypesConfig, RaftStore, RaftNetworkImpl>>,
    store: Arc<RwLock<RaftStore>>,
}

impl RaftNode {
    /// Creates a new RaftNode with the given node ID and cluster configuration
    pub async fn new(
        node_id: NodeId,
        map_store: MapStore,
        nodes: Vec<NodeId>,
    ) -> Result<Self, RaftError> {
        // Create the Raft storage
        let store = RaftStore::new(node_id, map_store);
        
        // Configure the initial cluster membership
        let mut config = HashMap::new();
        for id in nodes {
            config.insert(id, "".to_string()); // In a real impl, this would be the node's address
        }
        
        // Create the network implementation
        let network = RaftNetworkImpl {};
        
        // Create the Raft node
        let config = async_raft::Config::build("qlib-raft-cluster".into())
            .heartbeat_interval(200)
            .election_timeout_min(600)
            .election_timeout_max(1200)
            .validate()
            .map_err(RaftError::RaftError)?;
            
        let raft = Raft::new(node_id, config, Box::new(store.clone()), Box::new(network))
            .await
            .map_err(RaftError::RaftError)?;
            
        // Initialize the Raft cluster if this is a new cluster
        if nodes.len() == 1 && nodes[0] == node_id {
            // Single-node cluster, bootstrap it
            raft.initialize(nodes).await.map_err(RaftError::RaftError)?;
            info!("Initialized single-node Raft cluster");
        }

        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            store: Arc::new(RwLock::new(store)),
        })
    }

    /// Get the underlying MapStore
    pub fn get_map_store(&self) -> Arc<RwLock<MapStore>> {
        self.store.read().blocking_lock().get_map_store()
    }

    /// Process a client request, deciding whether to use consensus or direct read
    pub async fn process_request(&self, request: ClientRequest) -> Result<ClientResponse, RaftError> {
        match &request.command {
            RaftCommand::PerformRequests(requests) => {
                // If all requests are reads, we can bypass Raft consensus
                if requests.iter().all(|req| matches!(req, crate::Request::Read { .. })) {
                    info!("Processing read-only request directly");
                    self.store.read().await.apply_read_command(&request.command, &request.context).await
                } else {
                    info!("Processing write request through consensus");
                    self.process_write_request(request).await
                }
            },
            // All other commands are processed through consensus
            _ => self.process_write_request(request).await,
        }
    }

    /// Process a write request through Raft consensus
    async fn process_write_request(&self, request: ClientRequest) -> Result<ClientResponse, RaftError> {
        // Check if we're the leader
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state != async_raft::State::Leader {
            return Err(RaftError::NotLeader(metrics.current_leader));
        }
        
        // Client request is processed through the Raft consensus protocol
        info!("Processing consensus request: {:?}", request.command);
        let result = self.raft.client_write(request.command).await;
        
        match result {
            Ok(_idx) => Ok(ClientResponse::Success),
            Err(e) => {
                error!("Raft client_write error: {:?}", e);
                match e {
                    async_raft::error::ClientWriteError::ForwardToLeader { leader_id, .. } => 
                        Err(RaftError::NotLeader(leader_id)),
                    _ => Err(RaftError::RaftError(async_raft::error::RaftError::from(e))),
                }
            }
        }
    }
    
    /// Get current leader of the cluster
    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }
    
    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == async_raft::State::Leader
    }
    
    /// Get the current Raft node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}
