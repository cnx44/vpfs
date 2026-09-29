//! Receiving side of daemon-to-daemon traffic: decodes requests and hands them
//! to the service. No logic of its own.

use std::sync::Arc;

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::Endpoint;

use super::membership::Membership;
use super::service::Service;
use super::transport::IrohTransport;
use vpfs::framing::{recv_msg, send_msg};
use vpfs::messages::DaemonRequest;

#[derive(Clone)]
pub struct PeerHandler {
    pub service: Arc<Service>,
    pub membership: Arc<Membership>,
    pub transport: Arc<IrohTransport>,
}

impl std::fmt::Debug for PeerHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerHandler")
    }
}

impl PeerHandler {
    /// Dial every known node and start serving what they send on those connections.
    pub async fn connect_all(&self, endpoint: &Endpoint) {
        for (name, id) in self.membership.others() {
            match self.membership.dial(endpoint, &name, id).await {
                Some(conn) => self.adopt(&name, conn),
                None => eprintln!("Failed to establish connection to node: {}", name),
            }
        }
    }

    /// Use `conn` for our requests to `node`, and serve the requests it brings.
    fn adopt(&self, node: &str, conn: Connection) {
        self.transport.register(node, conn.clone());
        let this = self.clone();
        tokio::spawn(async move { this.serve(conn).await });
    }

    async fn serve(&self, conn: Connection) {
        while let Ok((send, recv)) = conn.accept_bi().await {
            let this = self.clone();
            tokio::spawn(async move { this.serve_stream(send, recv).await });
        }
    }

    async fn serve_stream(&self, mut send: SendStream, mut recv: RecvStream) {
        match recv_msg::<DaemonRequest>(&mut recv).await {
            Ok(req) => {
                let response = self.service.serve_peer(req).await;
                if let Err(e) = send_msg(&mut send, &response).await {
                    eprintln!("Could not answer peer: {e}");
                }
                let _ = send.finish();
            }
            Err(e) => eprintln!("Error receiving message from peer: {:?}", e),
        }
    }
}

impl ProtocolHandler for PeerHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        println!("Accepted connection from {}", conn.remote_id());
        match self.membership.on_hello(&conn).await {
            Ok(Some(node)) => self.adopt(&node, conn),
            Ok(None) => self.serve(conn).await,
            Err(()) => {}
        }
        Ok(())
    }
}
