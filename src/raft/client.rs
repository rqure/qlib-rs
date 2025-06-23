//! Client handling for the Raft implementation.

use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use crate::raft::{
    error::RaftError,
    types::{ClientRequest, ClientResponse, RaftCommand},
    node::RaftNode,
};

/// Options for client requests
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Timeout for the request
    pub timeout: Duration,
    
    /// Whether to retry on leader change
    pub retry_on_leader_change: bool,
    
    /// Client request ID for deduplication
    pub request_id: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            retry_on_leader_change: true,
            request_id: None,
        }
    }
}

/// Client for interacting with the Raft cluster
pub struct RaftClient {
    /// The Raft node
    node: Arc<RaftNode>,
    
    /// Default options for requests
    options: ClientOptions,
}

impl RaftClient {
    /// Create a new RaftClient
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self {
            node,
            options: ClientOptions::default(),
        }
    }
    
    /// Set default options for this client
    pub fn with_options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self
    }
    
    /// Set the default timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = timeout;
        self
    }
    
    /// Send a request with default options
    pub async fn send(&self, command: RaftCommand) -> Result<ClientResponse, RaftError> {
        self.send_with_options(command, &self.options).await
    }
    
    /// Send a request with custom options
    pub async fn send_with_options(
        &self, 
        command: RaftCommand,
        options: &ClientOptions
    ) -> Result<ClientResponse, RaftError> {
        let request = ClientRequest {
            command,
            request_id: options.request_id.clone(),
        };
        
        // Create a timeout for the operation
        let timeout = tokio::time::timeout(
            options.timeout,
            self._send_request(request.clone(), options.retry_on_leader_change)
        ).await;
        
        match timeout {
            Ok(result) => result,
            Err(_) => Err(RaftError::Timeout(options.timeout.as_millis() as u64)),
        }
    }
    
    async fn _send_request(
        &self, 
        request: ClientRequest, 
        retry_on_leader_change: bool
    ) -> Result<ClientResponse, RaftError> {
        match self.node.process_request(request.clone()).await {
            Ok(response) => Ok(response),
            Err(RaftError::NotLeader(Some(leader_id))) if retry_on_leader_change => {
                // We know who the leader is, but our node isn't it
                // In a real implementation, we would forward the request to the leader
                Err(RaftError::NotLeader(Some(leader_id)))
            },
            Err(e) => Err(e),
        }
    }
}
