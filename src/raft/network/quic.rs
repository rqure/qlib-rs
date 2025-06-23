//! QUIC-based transport implementation for Raft.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use anyhow::Result;
use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    RaftNetwork,
};
use async_trait::async_trait;
use log::{debug, info};
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig};
use rustls::{ServerConfig as RustlsServerConfig, ClientConfig as RustlsClientConfig};
use tokio::sync::RwLock;

use crate::raft::{
    types::{NodeId, RaftTypesConfig, RaftCommand},
    error::RaftError,
    network::config::NetworkConfig,
};

/// Network implementation using QUIC for Raft communication
pub struct QuicTransport {
    node_id: NodeId,
    endpoint: Endpoint,
    node_addrs: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

impl QuicTransport {
    /// Create a new QuicTransport with the given configuration
    pub async fn new(node_id: NodeId, config: NetworkConfig) -> Result<Self, RaftError> {
        // Configure QUIC transport
        let mut transport_config = TransportConfig::default();
        transport_config.max_idle_timeout(Some(config.idle_timeout.try_into().unwrap()));
        if let Some(keep_alive) = config.keep_alive {
            transport_config.keep_alive_interval(Some(keep_alive.try_into().unwrap()));
        }
        
        // Configure server
        let mut server_config = if let Some((cert, key)) = config.server_cert {
            let mut server_crypto = RustlsServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .map_err(|e| RaftError::Network(format!("TLS error: {}", e)))?;
            
            // Enable QUIC support
            server_crypto.alpn_protocols = vec![b"h3".to_vec()];
            
            ServerConfig::with_crypto(Arc::new(server_crypto))
        } else {
            // For testing only - dangerous in production
            ServerConfig::with_crypto(Arc::new(RustlsServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(
                    vec![config.server_cert.unwrap_or_else(|| {
                        use crate::raft::network::config::generate_self_signed_cert;
                        generate_self_signed_cert()
                    }).0], 
                    config.server_cert.unwrap_or_else(|| {
                        use crate::raft::network::config::generate_self_signed_cert;
                        generate_self_signed_cert()
                    }).1)
                .unwrap()))
        };
        server_config.transport = Arc::new(transport_config.clone());
        
        // Configure client
        let mut client_crypto = RustlsClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        
        // Enable QUIC support
        client_crypto.alpn_protocols = vec![b"h3".to_vec()];
        
        let mut client_config = ClientConfig::new(Arc::new(client_crypto));
        client_config.transport = Arc::new(transport_config);
        
        // Create the endpoint
        let mut endpoint = Endpoint::server(server_config, config.addr)
            .map_err(|e| RaftError::Network(format!("Failed to create endpoint: {}", e)))?;
        endpoint.set_default_client_config(client_config);
        
        info!("QUIC endpoint created at {}", config.addr);
        
        Ok(Self {
            node_id,
            endpoint,
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Register a node's address for communication
    pub async fn register_node(&self, node_id: NodeId, addr: SocketAddr) {
        let mut nodes = self.node_addrs.write().await;
        nodes.insert(node_id, addr);
        info!("Registered node {} at {}", node_id, addr);
    }
    
    /// Start listening for incoming connections
    pub async fn start_server(&self) -> Result<(), RaftError> {
        let endpoint = self.endpoint.clone();
        let node_id = self.node_id;
        
        tokio::spawn(async move {
            info!("QUIC server started for node {}", node_id);
            
            while let Some(conn) = endpoint.accept().await {
                let connection = match conn.await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        continue;
                    }
                };
                
                debug!("New connection from: {:?}", connection.remote_address());
                
                tokio::spawn(async move {
                    // Handle the connection (receive Raft requests)
                    // Implementation would deserialize requests and process them
                    
                    while let Ok(stream) = connection.accept_bi().await {
                        let (mut send, mut recv) = stream;
                        
                        // Read request data
                        use tokio::io::AsyncReadExt;
                        let mut len_bytes = [0u8; 2];
                        if let Err(e) = recv.read_exact(&mut len_bytes).await {
                            error!("Failed to read message type length: {}", e);
                            continue;
                        }
                        let type_len = u16::from_be_bytes(len_bytes) as usize;
                        
                        let mut type_bytes = vec![0u8; type_len];
                        if let Err(e) = recv.read_exact(&mut type_bytes).await {
                            error!("Failed to read message type: {}", e);
                            continue;
                        }
                        
                        let req_type = String::from_utf8_lossy(&type_bytes);
                        
                        // Read the rest of the message
                        let mut data = Vec::new();
                        if let Err(e) = recv.read_to_end(&mut data).await {
                            error!("Failed to read message data: {}", e);
                            continue;
                        }
                        
                        // Process based on request type
                        match req_type.as_ref() {
                            "append_entries" => {
                                // Handle AppendEntries request
                                match bincode::deserialize::<AppendEntriesRequest<RaftCommand>>(&data) {
                                    Ok(req) => {
                                        // Process append entries request
                                    },
                                    Err(e) => {
                                        error!("Failed to deserialize AppendEntries request: {}", e);
                                    }
                                }
                            },
                            "install_snapshot" => {
                                // Handle InstallSnapshot request
                            },
                            "vote" => {
                                // Handle Vote request
                            },
                            _ => {
                                error!("Unknown request type: {}", req_type);
                            }
                        }
                    }
                });
            }
        });
        
        Ok(())
    }
    
    /// Send a request to another node and get response
    async fn send_request<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self, 
        target: NodeId, 
        req_type: &str,
        request: T
    ) -> Result<R, RaftError> {
        // Find the node address
        let nodes = self.node_addrs.read().await;
        let addr = nodes.get(&target).ok_or_else(|| 
            RaftError::Network(format!("Unknown node: {}", target)))?;
            
        // Connect to the target node
        let connection = self.endpoint.connect(*addr, "localhost")
            .map_err(|e| RaftError::Network(format!("Failed to connect to node {}: {}", target, e)))?
            .await
            .map_err(|e| RaftError::Network(format!("Connection to node {} failed: {}", target, e)))?;
            
        // Open a bidirectional stream
        let (mut send, mut recv) = connection.open_bi()
            .await
            .map_err(|e| RaftError::Network(format!("Failed to open stream: {}", e)))?;
            
        // Serialize the request type and data
        let req_type_bytes = req_type.as_bytes();
        let req_data = bincode::serialize(&request)
            .map_err(|e| RaftError::Serialization(e))?;
            
        // Send request type length + type + request data
        let type_len = req_type_bytes.len() as u16;
        let mut buf = Vec::with_capacity(2 + type_len as usize + req_data.len());
        buf.extend_from_slice(&type_len.to_be_bytes());
        buf.extend_from_slice(req_type_bytes);
        buf.extend_from_slice(&req_data);
        
        // Write the request
        use tokio::io::AsyncWriteExt;
        send.write_all(&buf).await
            .map_err(|e| RaftError::Network(format!("Failed to send request: {}", e)))?;
        send.finish().await
            .map_err(|e| RaftError::Network(format!("Failed to finish stream: {}", e)))?;
            
        // Read the response
        use tokio::io::AsyncReadExt;
        let mut response_data = Vec::new();
        recv.read_to_end(&mut response_data).await
            .map_err(|e| RaftError::Network(format!("Failed to read response: {}", e)))?;
            
        // Deserialize the response
        bincode::deserialize(&response_data)
            .map_err(|e| RaftError::Serialization(e))
    }
}

#[async_trait]
impl RaftNetwork<RaftTypesConfig> for QuicTransport {
    async fn append_entries(
        &self,
        target: NodeId,
        request: AppendEntriesRequest<RaftCommand>,
    ) -> Result<AppendEntriesResponse, async_raft::RaftError> {
        debug!("Sending AppendEntries to node {}", target);
        
        self.send_request(target, "append_entries", request)
            .await
            .map_err(|e| async_raft::RaftError::RaftNetwork(format!("Network error: {}", e).into()))
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, async_raft::RaftError> {
        debug!("Sending InstallSnapshot to node {}", target);
        
        self.send_request(target, "install_snapshot", request)
            .await
            .map_err(|e| async_raft::RaftError::RaftNetwork(format!("Network error: {}", e).into()))
    }

    async fn vote(
        &self,
        target: NodeId,
        request: VoteRequest,
    ) -> Result<VoteResponse, async_raft::RaftError> {
        debug!("Sending Vote to node {}", target);
        
        self.send_request(target, "vote", request)
            .await
            .map_err(|e| async_raft::RaftError::RaftNetwork(format!("Network error: {}", e).into()))
    }
}
