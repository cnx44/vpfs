use iroh::Endpoint;
use iroh::PublicKey;
use iroh::endpoint::Connection;
use iroh::endpoint::RecvStream;
use iroh::endpoint::SendStream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use anyhow::Result;

use std::sync::{Arc, Mutex};

use crate::protocol::VPFSProtocol;
use crate::messages::{Hello, HelloResponse};

use crate::state::DaemonState;
use crate::messages::{DaemonRequest, DaemonResponse, VPFSNode};

pub async fn send_message<T: serde::Serialize>(send: &mut SendStream, msg: T) -> Result<()> {
    // Serialize message
    let buf = serde_bare::to_vec(&msg)?;

    // Write length
    send.write_all(&(buf.len() as u64).to_be_bytes()).await?;
    // Write payload
    send.write_all(&buf).await?;
    // send.finish()?;

    Ok(())
}

pub async fn receive_message<T: DeserializeOwned>(recv: &mut RecvStream) ->  Result<T> {
    // Read length
    let mut len_buf = [0u8; 8];
    recv.read_exact(&mut len_buf).await?;
    let len = u64::from_be_bytes(len_buf) as usize;

    // Read payload
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;

    // Deserialize message
    let msg = serde_bare::from_slice(&buf)?;
    Ok(msg)
}

pub async fn send_and_receive <T: Serialize, U: DeserializeOwned> (node_name: &String, message: T, state: &Arc<DaemonState>) -> Result<U, anyhow::Error> {
    if let Some(node_connection) = get_connection(node_name, state).await {
        if let Ok((mut send, mut recv)) = node_connection.open_bi().await {
            send_message(&mut send, message).await;
            return receive_message(&mut recv).await;
        }
        
    }
    Err(anyhow::Error::msg("Could not connect"))
    
}


/// Join p2p network
pub async fn connect_to_network(endpoint: &Endpoint, remote_endpoint_id: PublicKey, state: &Arc<DaemonState>) -> Option<Connection> {
    println!("Connecting to root node: {}", remote_endpoint_id);
    // connect to the other endpoint
    let endpoint_addr = iroh::EndpointAddr::new(remote_endpoint_id);
    match endpoint.connect(endpoint_addr, VPFSProtocol::ALPN).await {
        Ok(conn) => {
            println!("Connected to root node: {remote_endpoint_id}");
            match conn.open_bi().await {
                Ok((mut send, mut recv)) => {
                    println!("Opened bi-directional stream to root node: {}", remote_endpoint_id);
                    
                    let local_known_nodes = {
                        let mut nodes = state.known_nodes.lock().unwrap().clone();
                        nodes.insert(state.local.name.clone(), state.local.endpoint_id);
                        nodes
                    };

                    let msg = Hello::InitHello(local_known_nodes);
                    send_message(&mut send, msg).await;

                    println!("Sent init to root node, waiting for response...");
                    
                    if let Ok(HelloResponse::InitHello(host_names)) = receive_message(&mut recv).await {
                        let mut known_nodes = state.known_nodes.lock().unwrap();
                        known_nodes.extend(host_names);
                        known_nodes.remove(&state.local.name);
                        // println!("{:?}", known_nodes);
                    } else {
                        eprintln!("Failed to deserialize response from root node");
                    }

                    
                }
                Err(e) => eprintln!("Error opening bi-directional stream: {}", e),
            }            
            return Some(conn);

        }
        Err(e) => {
            eprintln!("Failed to connect to root node: {}", e);
            eprintln!("Error details: {:?}", e);
        }
    }

    None
}


/// Connect to a single node
async fn establish_connection(endpoint: &Endpoint, node: &VPFSNode, cur_node: &VPFSNode) -> Option<Connection> {
    let remote_id = node.endpoint_id;
    println!("Connecting to root node: {}", remote_id);
    // connect to the other endpoint
    let endpoint_addr = iroh::EndpointAddr::new(remote_id);
    match endpoint.connect(endpoint_addr, VPFSProtocol::ALPN).await {
        Ok(conn) => {
            println!("Connected to root node: {remote_id}");
            match conn.open_bi().await {
                Ok((mut send, mut recv)) => {
                    println!("Opened bi-directional stream to root node: {}", remote_id);

                    send_message(&mut send, Hello::DaemonHello(cur_node.clone())).await;
                    println!("Sent hello to root node, waiting for response...");
                    receive_message::<HelloResponse>(&mut recv).await.expect("Got bad hello response");

                    
                    return Some(conn);
                }
                Err(e) => {
                    eprintln!("Error opening bi-directional stream: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to node: {}", e);
        }
    }

    None
}

/// Open connection to all known hosts and update hashmap
pub async fn establish_connections(state: &Arc<DaemonState>){
    let nodes_to_connect: Vec<(String, iroh::PublicKey)> = {
        let known_nodes = state.known_nodes.lock().unwrap();
        known_nodes.iter()
            .filter(|(_, id)| *id != &state.local.endpoint_id)
            .map(|(name, id)| (name.clone(), *id))
            .collect()
    };

    for (node_name, node_id) in nodes_to_connect {
        let node = VPFSNode{name: node_name.clone(), endpoint_id: node_id};
        if let Some(conn) = establish_connection(&state.endpoint, &node, &state.local).await{
            let conn = Arc::new(conn);
            state.connections.lock().unwrap().insert(node_name.clone(), conn.clone());
            // Spawn a daemon loop so the remote node can open streams back to us
            let protocol = VPFSProtocol { state: state.clone() };
            tokio::spawn(async move { protocol.handle_daemon(conn).await; });
        } else {
            eprintln!("Failed to establish connection to node: {}", node_name);
        }
    }
}

/// Get a connection to a node, if it doesn't exist, try to establish it
pub async fn get_connection(node_name: &String, state: &Arc<DaemonState>) -> Option<Arc<Connection>> {
    println!("Getting connection to node: {}", node_name);
    
    // check hashmap for existing connection
    // if exists and already closed remove from hashmap
    let mut connections = state.connections.lock().unwrap();
    if let Some(connection) = connections.get(node_name) {
        println!("Found existing connection to node: {}, close reason: {:?}", node_name, connection.close_reason());
        if connection.close_reason().is_some() {
            connections.remove(node_name);
        } else {
            return Some(connection.clone());
        }
    }
    println!("None");
    return None;
    

    // use endpoint id from known nodes hashmap to connect
    // let known_nodes = state.known_nodes.lock().unwrap();
    // if let Some(remote_id) = known_nodes.get(node_name) {
    //     if let Some(conn) = establish_connection(&state.endpoint, &VPFSNode{name: node_name.clone(), endpoint_id:remote_id.clone()}).await {
    //         let conn = Arc::new(conn);
    //         let mut connections = state.connections.lock().unwrap();
    //         connections.insert(node_name.clone(), conn.clone());
    //         return Some(conn);
    //     }
    // }

    // TODO: ask network for node's endpoint_id if not in known_nodes
    // if let Some(root_node) = state.root.read().unwrap().as_ref() {
    //     if state.local == *root_node {
    //         return None;
    //     }
    //     if let Some(root_connection) = connections.get(&root_node.name) {
    //         let mut root_connection = root_connection.lock().unwrap();
    //         match root_connection.open_bi().await {
    //             Ok((mut send, mut recv)) => {
    //                 println!("Opened bi-directional stream to root node: {}", root_node.endpoint_id);
                    
    //                 send_message(&mut send, DaemonRequest::AddressFor(node_name.clone())).await;
    //                 match receive_message(&mut recv).await {
    //                     Ok(DaemonResponse::AddressFor(Some(remote_id))) => {
    //                         drop(root_connection);
    //                         if let Some(conn) = establish_connection(&state.endpoint, &VPFSNode{name: node_name.clone(), endpoint_id:remote_id}).await {
    //                             let conn = Arc::new(Mutex::new(conn));
    //                             connections.insert(node_name.clone(), conn.clone());
    //                             return Some(conn);
    //                         }
    //                     },
    //                     _ => return None
    //                 }
                    
    //             }
    //             Err(e) => eprintln!("Error opening bi-directional stream: {}", e),
    //         }
            
    //     }
    // }
    None
}