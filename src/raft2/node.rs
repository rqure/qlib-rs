//! Raft node implementation.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use async_raft::{Raft, Config};
use log::{info, error};

use crate::MapStore;
use crate::raft2::{
    error::RaftError,
    storage::RaftStore,
    network::{NetworkConfig, QuicTransport},
    types::{ClientRequest, ClientResponse, NodeId, RaftCommand, RaftTypesConfig},
};

/// RaftNode wraps a MapStore and integrates it with Raft consensus
pub struct RaftNode {
    /// The ID of this Raft node
    node_id: NodeId,
    
    /// The Raft instance
    raft: Arc<Raft<RaftTypesConfig>>,
    
    /// The RaftStore for storage
    store: Arc<RwLock<RaftStore>>,
    
    /// The network transport
    network: Arc<QuicTransport>,
}

impl RaftNode {
    /// Creates a new RaftNode with the given node ID and cluster configuration
    pub async fn new(
        node_id: NodeId,
        map_store: MapStore,
        addr: SocketAddr,
        nodes: HashMap<NodeId, SocketAddr>,
    ) -> Result<Self, RaftError> {
        // Create the Raft storage
        let store = RaftStore::new(node_id, map_store);
        let store_arc = Arc::new(RwLock::new(store));
        
        // Create the QUIC network implementation
        let quic_config = NetworkConfig::with_self_signed_cert(addr);
        let network = QuicTransport::new(node_id, quic_config).await?;
        
        // Register all known nodes
        for (id, addr) in &nodes {
            network.register_node(*id, *addr).await;
        }
        
        // Start the network server
        network.start_server().await?;
        
        let network_arc = Arc::new(network);
        
        // Create the Raft configuration
        let config = Config::build("qlib-raft-cluster".into())
            .heartbeat_interval(200)
            .election_timeout_min(600)
            .election_timeout_max(1200)
            .validate()
            .map_err(RaftError::Consensus)?;
            
        // Create the Raft instance
        let raft = Raft::new(
            node_id, 
            Arc::new(config), 
            store_arc.clone(), 
            network_arc.clone()
        ).map_err(RaftError::Consensus)?;
        
        // Initialize the Raft cluster if this is a single-node setup
        if nodes.len() == 1 && nodes.contains_key(&node_id) {
            // Single-node cluster, bootstrap it
            raft.initialize(vec![node_id]).await.map_err(RaftError::Consensus)?;
            info!("Initialized single-node Raft cluster");
        }

        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            store: store_arc,
            network: network_arc,
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
                Err(e.into())
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
    
    /// Add a new node to the cluster
    pub async fn add_node(&self, node_id: NodeId, addr: SocketAddr) -> Result<(), RaftError> {
        // Register the node in the network transport
        self.network.register_node(node_id, addr).await;
        
        // If we're the leader, update the cluster configuration
        if self.is_leader() {
            let mut membership = self.raft.membership().await;
            
            // If the node is already in the config, do nothing
            if membership.contains(&node_id) {
                return Ok(());
            }
            
            // Add the node to the membership
            membership.push(node_id);
            
            // Apply the membership change
            self.raft.change_membership(membership).await
                .map_err(RaftError::Consensus)?;
                
            info!("Added node {} to cluster", node_id);
            Ok(())
        } else {
            // Only the leader can change membership
            Err(RaftError::NotLeader(self.current_leader()))
        }
    }
    
    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), RaftError> {
        // Cannot remove ourselves
        if node_id == self.node_id {
            return Err(RaftError::Operation("Cannot remove self from cluster".into()));
        }
        
        // Must be the leader to remove a node
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.current_leader()));
        }
        
        let mut membership = self.raft.membership().await;
        
        // If the node is not in the config, do nothing
        if !membership.contains(&node_id) {
            return Ok(());
        }
        
        // Remove the node from membership
        membership.retain(|&id| id != node_id);
        
        // Apply the membership change
        self.raft.change_membership(membership).await
            .map_err(RaftError::Consensus)?;
            
        info!("Removed node {} from cluster", node_id);
        Ok(())
    }
    
    /// Transfer leadership to another node
    pub async fn transfer_leadership(&self, target_node: NodeId) -> Result<(), RaftError> {
        // Must be the leader to transfer leadership
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.current_leader()));
        }
        
        // Check if target node is in the cluster
        let membership = self.raft.membership().await;
        if !membership.contains(&target_node) {
            return Err(RaftError::Operation(format!("Node {} is not in the cluster", target_node)));
        }
        
        // Initiate leadership transfer
        self.raft.trigger_leadership_transfer(Some(target_node))
            .map_err(|e| RaftError::Operation(format!("Leadership transfer failed: {}", e)))?;
            
        info!("Leadership transfer initiated to node {}", target_node);
        Ok(())
    }
}

impl Drop for RaftNode {
    fn drop(&mut self) {
        info!("Shutting down RaftNode {}", self.node_id);
    }
}
