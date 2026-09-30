//! VPFS daemon: wires the components together and starts them.
//!
//!   client programs ──TCP──> gateway ──┐
//!   other daemons ──iroh──> peer_handler ──> service ──> executor ──> state
//!   service ──> transport (to other daemons), human (conflict resolver)
//!
//! State lives in `./files`: the blobs plus `log`, `vector_clock`,
//! `file_system` and `cache`.

mod blobs;
mod cache;
mod conflict;
mod content;
mod executor;
mod gateway;
mod human;
mod logbook;
mod membership;
mod namespace;
mod peer_handler;
mod service;
mod state;
mod transport;

use std::fs;
use std::sync::{mpsc, Arc};

use anyhow::Result;
use clap::Parser;
use iroh::endpoint::TransportConfig;
use iroh::protocol::Router;
use iroh::{Endpoint, PublicKey};
use tokio::runtime::Handle;

use executor::Executor;
use membership::Membership;
use peer_handler::PeerHandler;
use service::Service;
use state::State;
use transport::IrohTransport;
use vpfs::messages::VPFSNode;

#[derive(Parser, Debug)]
#[command(name = "vpfs", about = "Virtual private file system iroh prototype.")]
struct Opt {
    // Iroh (QUIC) port for daemon-to-daemon traffic.
    #[arg(short, long, default_value_t = 8081)]
    port: u16,

    // TCP port for local client programs (gateway.rs).
    #[arg(short, long, default_value_t = 8082)]
    listen_port: u16,

    // TCP port of the local conflict resolver (human.rs).
    #[arg(short, long, default_value_t = 8083)]
    conflict_port: u16,

    // Endpoint id of any member of an existing network; omit to start a new one.
    #[arg(short, long)]
    remote_id: Option<PublicKey>,

    //Maximum cache size in bytes
    #[arg(short = 's', long, default_value_t = 1 << 16)]
    cache_size: usize,

    // Node name: unique in the network and stable across restarts (it keys the
    // vector clock and file ownership).
    #[arg(short, long)]
    name: String
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    // Iroh endpoint; no idle timeout, so peer connections stay open.
    let mut config = TransportConfig::default();
    config.max_idle_timeout(None);
    let endpoint = Endpoint::builder()
        .transport_config(config)
        .bind_addr_v4(format!("0.0.0.0:{}", opt.port).parse().unwrap())
        .bind()
        .await?;
    endpoint.online().await;
    println!("Endpoint Id: {}", endpoint.id());

    // VPFSNode reppresent this node (Name and EndpointID), Membership is the map of remote noded
    // known. We need ARC (Atomically Reference Counted) because we need the same Membership in
    // different part of the system. membership is type Arc<Membership> which means that only on
    // refcount going to zero we free the object
    let membership = Arc::new(Membership::new(VPFSNode { name: opt.name.clone(), endpoint_id: endpoint.id() }));

    // if remote_id is optional, in case of none it starts as the first node of the system, root
    // reppresent the node contacted to enter. 
    // In case of remote_id is provided we try to connect to the remote node and get its name. In
    // case of fail panic
    let root = match opt.remote_id {
        Some(remote_id) => {
            println!("Connecting to network with node {}", remote_id);
            let Some(root) = membership.join(&endpoint, remote_id).await else { panic!("Could not connect") };
            println!("Connected to network");
            Some(root)
        }
        None => {
            println!("Running as first node on vpfs");
            None
        }
    };
    
    // try to create files/ directory, in case of success new_node return true, false if alredy
    // exist error for any other case. new_node means if we are creating a new directory for the FS
    let dir = std::env::current_dir()?.join("files");
    let new_node = match fs::create_dir(&dir) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(e) => panic!("Could not create directory for storing files: {e}"),
    };
    
    // ARC because both Service and PeerHandelr use the same transport layer. 
    let transport = Arc::new(IrohTransport::default());
    // State is loaded from ./files and owned by the executor thread from now on.
    let executor = Executor::spawn(State::open(&dir, &opt.name, opt.cache_size));
    // Unresolved conflicts flow from the service to the human resolver thread.
    let (conflicts, human_queue) = mpsc::channel();
    let service = Arc::new(Service::new(opt.name.clone(), executor, transport.clone(), conflicts));
    human::spawn(opt.conflict_port, human_queue, service.clone(), Handle::current());

    // Start accepting daemon connections.
    let peers = PeerHandler { service: service.clone(), membership, transport };
    let _router = Router::builder(endpoint.clone()).accept(membership::ALPN, peers.clone()).spawn();

    // Joining: connect to every node, copy the namespace if this node is new,
    // then exchange the log entries missed while apart.
    if let Some(root) = root {
        peers.connect_all(&endpoint).await;
        if new_node {
            service.bootstrap_from(&root).await;
        }
        service.sync_with(&root).await;
    }

    // Serve local clients until the process ends.
    let address = format!("0.0.0.0:{}", opt.listen_port);
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || gateway::serve(&address, service, rt)).await?;
    Ok(())
}
