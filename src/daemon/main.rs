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
    #[arg(short, long, default_value_t = 8081)]
    port: u16,

    #[arg(short, long, default_value_t = 8082)]
    listen_port: u16,

    #[arg(short, long, default_value_t = 8083)]
    conflict_port: u16,

    #[arg(short, long)]
    remote_id: Option<PublicKey>,

    //Maximum cache size in bytes
    #[arg(short = 's', long, default_value_t = 1 << 16)]
    cache_size: usize,

    #[arg(short, long)]
    name: String
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    let mut config = TransportConfig::default();
    config.max_idle_timeout(None);
    let endpoint = Endpoint::builder()
        .transport_config(config)
        .bind_addr_v4(format!("0.0.0.0:{}", opt.port).parse().unwrap())
        .bind()
        .await?;
    endpoint.online().await;
    println!("Endpoint Id: {}", endpoint.id());

    let membership = Arc::new(Membership::new(VPFSNode { name: opt.name.clone(), endpoint_id: endpoint.id() }));
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

    let dir = std::env::current_dir()?.join("files");
    let new_node = match fs::create_dir(&dir) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(e) => panic!("Could not create directory for storing files: {e}"),
    };

    let transport = Arc::new(IrohTransport::default());
    let executor = Executor::spawn(State::open(&dir, &opt.name, opt.cache_size));
    let (conflicts, human_queue) = mpsc::channel();
    let service = Arc::new(Service::new(opt.name.clone(), executor, transport.clone(), conflicts));
    human::spawn(opt.conflict_port, human_queue, service.clone(), Handle::current());

    let peers = PeerHandler { service: service.clone(), membership, transport };
    let _router = Router::builder(endpoint.clone()).accept(membership::ALPN, peers.clone()).spawn();

    if let Some(root) = root {
        peers.connect_all(&endpoint).await;
        if new_node {
            service.bootstrap_from(&root).await;
        }
        service.sync_with(&root).await;
    }

    let address = format!("0.0.0.0:{}", opt.listen_port);
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || gateway::serve(&address, service, rt)).await?;
    Ok(())
}
