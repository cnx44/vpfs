//! Who is in the network, and the handshakes that connect nodes.
//!
//! * `join`: a new or returning node contacts one member (`InitHello`) and
//!   they exchange the nodes each one knows.
//! * `dial`: open the long-lived connection to a known node (`DaemonHello`).
//! * `on_hello`: the receiving side of both.

use std::collections::HashMap;
use std::sync::Mutex;

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey};

use vpfs::framing::{recv_msg, send_msg};
use vpfs::messages::{Hello, HelloResponse, VPFSNode};

pub const ALPN: &[u8] = b"uic/vpfs";

#[derive(Debug)]
pub struct Membership {
    pub local: VPFSNode,
    known: Mutex<HashMap<String, PublicKey>>, // node name -> endpoint id
}

impl Membership {
    pub fn new(local: VPFSNode) -> Membership {
        Membership { local, known: Mutex::new(HashMap::new()) }
    }

    /// Known nodes other than this one.
    pub fn others(&self) -> Vec<(String, PublicKey)> {
        self.known.lock().unwrap().iter()
            .filter(|(_, id)| **id != self.local.endpoint_id)
            .map(|(name, id)| (name.clone(), *id))
            .collect()
    }

    /// Join the network through `remote`. Returns `remote`'s node name.
    pub async fn join(&self, endpoint: &Endpoint, remote: PublicKey) -> Option<String> {
        println!("Connecting to root node: {}", remote);
        let conn = match endpoint.connect(EndpointAddr::new(remote), ALPN).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("Failed to connect to root node: {}", e);
                eprintln!("Error details: {:?}", e);
                return None;
            }
        };
        println!("Connected to root node: {remote}");
        let mut ours = self.known.lock().unwrap().clone();
        ours.insert(self.local.name.clone(), self.local.endpoint_id);
        let theirs = match Self::handshake(&conn, Hello::InitHello(ours)).await {
            Some(HelloResponse::InitHello(theirs)) => theirs,
            _ => {
                eprintln!("Failed to deserialize response from root node");
                return None;
            }
        };
        let root_name = theirs.iter().find(|(_, id)| **id == remote).map(|(name, _)| name.clone());
        let mut known = self.known.lock().unwrap();
        known.extend(theirs);
        known.remove(&self.local.name);
        root_name
    }

    /// Open the connection used for all traffic with `name`.
    pub async fn dial(&self, endpoint: &Endpoint, name: &str, id: PublicKey) -> Option<Connection> {
        println!("Connecting to node: {} ({})", name, id);
        let conn = endpoint.connect(EndpointAddr::new(id), ALPN).await
            .map_err(|e| eprintln!("Failed to connect to node: {}", e)).ok()?;
        match Self::handshake(&conn, Hello::DaemonHello(self.local.clone())).await {
            Some(HelloResponse::DaemonHello) => Some(conn),
            _ => {
                eprintln!("Got bad hello response from {name}");
                None
            }
        }
    }

    async fn handshake(conn: &Connection, hello: Hello) -> Option<HelloResponse> {
        let (mut send, mut recv) = conn.open_bi().await
            .map_err(|e| eprintln!("Error opening bi-directional stream: {}", e)).ok()?;
        send_msg(&mut send, &hello).await.ok()?;
        recv_msg(&mut recv).await.ok()
    }

    /// Answer the hello on an incoming connection. Returns the node name if the
    /// connection is a `dial` from that node (so it can carry our requests too).
    pub async fn on_hello(&self, conn: &Connection) -> Result<Option<String>, ()> {
        let remote_id = conn.remote_id();
        let (mut send, mut recv): (SendStream, RecvStream) = conn.accept_bi().await.map_err(|_| ())?;
        match recv_msg::<Hello>(&mut recv).await {
            Ok(Hello::DaemonHello(node)) => {
                println!("Received DaemonHello from node: {}, endpoint_id: {}", node.name, node.endpoint_id);
                self.known.lock().unwrap().insert(node.name.clone(), node.endpoint_id);
                let _ = send_msg(&mut send, &HelloResponse::DaemonHello).await;
                Ok(Some(node.name))
            }
            Ok(Hello::InitHello(new_nodes)) => {
                println!("Received InitHello from node: {}, new nodes: {:?}", remote_id, new_nodes);
                let snapshot = {
                    let mut known = self.known.lock().unwrap();
                    let mut snapshot = known.clone();
                    known.extend(new_nodes);
                    snapshot.insert(self.local.name.clone(), self.local.endpoint_id);
                    snapshot
                };
                let _ = send_msg(&mut send, &HelloResponse::InitHello(snapshot)).await;
                Ok(None)
            }
            Ok(Hello::ClientHello) => {
                eprintln!("Unexpected message from {remote_id}");
                Err(())
            }
            Err(e) => {
                eprintln!("Error receiving message from {remote_id}: {:?}", e);
                Err(())
            }
        }
    }
}
