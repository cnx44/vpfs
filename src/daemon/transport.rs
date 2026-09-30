//! Delivery of messages to other nodes, by node name.
//!
//! Two primitives: `fetch` asks one node for something and waits for the
//! answer; `broadcast` hands events to every connected node. Who the nodes
//! are and how connections are established is membership.rs's business.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use iroh::endpoint::Connection;

use vpfs::framing::{recv_msg, send_msg};
use vpfs::messages::{DaemonRequest, DaemonResponse, LogEntry, VPFSError};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Transport: Send + Sync {
    /// Send `req` to `node` and wait for its response. `NotAccessible` if the node cannot be reached.
    fn fetch<'a>(&'a self, node: &'a str, req: DaemonRequest) -> BoxFuture<'a, Result<DaemonResponse, VPFSError>>;

    /// Deliver `events` to every connected node and wait until they are applied,
    /// giving up on a node after a timeout. Never fails: unreachable nodes catch up when they rejoin.
    fn broadcast<'a>(&'a self, events: Vec<LogEntry>) -> BoxFuture<'a, ()>;
}

/// How long `broadcast` waits for one node before moving on.
const BROADCAST_TIMEOUT: Duration = Duration::from_secs(3);

/// Iroh (QUIC) transport: one connection per node, one bi-directional stream per request.
#[derive(Debug, Default)]
pub struct IrohTransport {
    /// Node name -> connection adopted by peer_handler.rs. These nodes are the broadcast targets.
    connections: Mutex<HashMap<String, Connection>>,
}

impl IrohTransport {
    /// Called by `PeerHandler::adopt`; replaces any previous connection to `node`.
    pub fn register(&self, node: &str, conn: Connection) {
        self.connections.lock().unwrap().insert(node.to_string(), conn);
    }

    /// Live connection to `node`; closed ones are forgotten.
    fn connection(&self, node: &str) -> Option<Connection> {
        let mut connections = self.connections.lock().unwrap();
        match connections.get(node) {
            Some(conn) if conn.close_reason().is_none() => Some(conn.clone()),
            Some(_) => {
                connections.remove(node);
                None
            }
            None => None,
        }
    }

    /// One request on a fresh bi-directional stream, then its one response.
    async fn exchange(conn: &Connection, req: &DaemonRequest) -> anyhow::Result<DaemonResponse> {
        let (mut send, mut recv) = conn.open_bi().await?;
        send_msg(&mut send, req).await?;
        send.finish()?;
        recv_msg(&mut recv).await
    }
}

impl Transport for IrohTransport {
    fn fetch<'a>(&'a self, node: &'a str, req: DaemonRequest) -> BoxFuture<'a, Result<DaemonResponse, VPFSError>> {
        Box::pin(async move {
            let conn = self.connection(node).ok_or(VPFSError::NotAccessible)?;
            Self::exchange(&conn, &req).await.map_err(|e| {
                eprintln!("Request to {node} failed: {e}");
                VPFSError::NotAccessible
            })
        })
    }

    /// Nodes are contacted one after the other, so the worst case is
    /// `BROADCAST_TIMEOUT` per unresponsive node.
    fn broadcast<'a>(&'a self, events: Vec<LogEntry>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let nodes: Vec<String> = self.connections.lock().unwrap().keys().cloned().collect();
            let req = DaemonRequest::Events(events);
            for node in nodes {
                let Some(conn) = self.connection(&node) else { continue };
                match tokio::time::timeout(BROADCAST_TIMEOUT, Self::exchange(&conn, &req)).await {
                    Ok(Ok(DaemonResponse::Ack)) => {}
                    Ok(Ok(other)) => eprintln!("Unexpected response to events from {node}: {other:?}"),
                    Ok(Err(e)) => eprintln!("Could not send events to {node}: {e}"),
                    Err(_) => eprintln!("Node {node} did not acknowledge events in time"),
                }
            }
        })
    }
}
