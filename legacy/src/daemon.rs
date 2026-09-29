use clap::Parser;
use iroh::{Endpoint, PublicKey, protocol::Router, endpoint::TransportConfig};
use serde::de::DeserializeOwned;
use serde::{Serialize};
use lru::LruCache;
use tokio::runtime::Handle;
use anyhow::{Error, Result};

use std::hash::Hash;
use std::os::linux::net::TcpStreamExt;
use std::thread;
use std::net::{TcpListener, TcpStream};
use std::fs;
use std::io;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};

mod protocol;
use crate::protocol::VPFSProtocol;

mod state;
use crate::state::DaemonState;

mod messages;
use messages::*;

mod remote_communication;
use remote_communication::*;

mod file_system;
use file_system::*;

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

/// Send a message to a TcpStream
fn send_message_tcp <T: Serialize>(stream: &mut TcpStream, message: T) {
    // Serialize message
    let buf = serde_bare::to_vec(&message).unwrap();

    // Write length
    stream.write_all(&(buf.len() as u64).to_be_bytes()).unwrap();
    // Write payload
    stream.write_all(&buf).unwrap();
}

/// Receive a message from a TcpStream
fn receive_message_tcp <T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T, serde_bare::error::Error> {
    // Read length
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u64::from_be_bytes(len_buf) as usize;
    
    // Read payload
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).unwrap();

    // Deserialize message
    let msg = serde_bare::from_slice(&buf);
    msg
}

fn send_buf_tcp(stream: &mut TcpStream, buf: &Vec<u8>) {
    stream.write_all(&buf).unwrap();
}

fn receive_buf_tcp(stream: &mut TcpStream, len: usize) -> Result<Vec<u8>, Error> {
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// Handle client ListFiles request
async fn handle_client_list_files(stream: &mut TcpStream, dir: &str, state: &Arc<DaemonState>) {
    send_message_tcp(stream, ClientResponse::ListFiles(list_files(dir, state)));
}

/// Handle client Find request
async fn handle_client_find(stream: &mut TcpStream, file: &str, state: &Arc<DaemonState>) {
    println!("handle client find for file: {}", file);

    send_message_tcp(stream, ClientResponse::Find(find(file, state)));
}

/// Handle client Place request
async fn handle_client_place(stream: &mut TcpStream, file: &str, node_name: String, state: &Arc<DaemonState>) {
    println!("handle client place for file: {}", file);

    send_message_tcp(stream, ClientResponse::Place(place_file(file, &node_name, state).await));
}

async fn handle_client_open_file(stream: &mut TcpStream, file: FileEntry, state: &Arc<DaemonState>) {
    println!("handle client open for file: {:?}", file);

    send_message_tcp(stream, ClientResponse::Open(open_file(file, state).await));    
}

/// Handle client Read request
/// <br>
async fn handle_client_read(stream: &mut TcpStream, file: FileEntry, state: &Arc<DaemonState>) {
    println!("handle client read for file: {:?}", file);

    // if file is local, read locally, else read remotely and send response back through stream
    if file.owner == state.local.name {
        println!("local read {}", file.uri);
        if let Ok(buf) = read_local(&file.uri, &state.file_system) {
            send_message_tcp(stream, ClientResponse::Read(Ok(buf.len())));
            send_buf_tcp(stream, &buf);
        } else {
            send_message_tcp(stream, ClientResponse::Read(Err(VPFSError::DoesNotExist)));
        }
    } else  {
        match read_remote(&file, state).await {
            Ok(buf) => {
                send_message_tcp(stream, ClientResponse::Read(Ok(buf.len())));                    
                send_buf_tcp(stream, &buf);
            }
            Err(error) => {
                send_message_tcp(stream, ClientResponse::Read(Err(error)));
            }
        }
    }
}

async fn handle_client_read_fd(stream: &mut TcpStream, file: FileEntry, fd: i32, len: usize, state: &Arc<DaemonState>) {
    match read_fd(&file, fd, len, state).await {
        Ok(buf) => {
            send_message_tcp(stream, ClientResponse::ReadFd(Ok(buf.len())));                    
            send_buf_tcp(stream, &buf);
        }
        Err(error) => {
            send_message_tcp(stream, ClientResponse::ReadFd(Err(error)));
        }
    }
}

async fn handle_client_read_line_fd(stream: &mut TcpStream, file: FileEntry, fd: i32, state: &Arc<DaemonState>) {
    match read_line_fd(&file, fd, state).await {
        Ok(buf) => {
            send_message_tcp(stream, ClientResponse::ReadLineFd(Ok(buf.len())));                    
            send_buf_tcp(stream, &buf);            
        }
        Err(error) => {
            send_message_tcp(stream, ClientResponse::ReadLineFd(Err(error)));
        }
    }
}

async fn handle_client_close_file(stream: &mut TcpStream, node_name: String, fd:i32, state: &Arc<DaemonState>) {
    send_message_tcp(stream, ClientResponse::Close(close_file(&node_name, fd, state).await));    
}

/// Handle client Write request
async fn handle_client_write(stream: &mut TcpStream, file: FileEntry, file_len: usize, state: &Arc<DaemonState>) {
    println!("handle client write for file: {:?}", file);

    if file.owner == state.local.name {

        // Evict cache entry if it exists; only then generate a new URI
        let write_uri = {
            let mut cache = state.cache.lock().unwrap();
            if let Some(evicted) = cache.pop(&file.name) {
                let file_size = fs::metadata(&evicted.uri).map(|m| m.len()).unwrap_or(0);
                fs::remove_file(&evicted.uri).ok();
                *state.used_cache_bytes.write().unwrap() -= file_size as usize;
                let new_uri = create_file_with_random_uri();
                new_uri
            } else {
                file.uri.clone()
            }
        };
        
        let mut file = file;
        file.uri = write_uri.clone();

        place_file_in_memory(&state.file_system, &file.name, file.clone());

        let buf = receive_buf_tcp(stream, file_len).unwrap();
        if write_local(&file.uri, &buf, &state.file_system).is_ok() {
            append_log_entry(LogOp::Modify(file.clone()), &state).await;
            send_message_tcp(stream, ClientResponse::Write(Ok(file_len)));
        } else {
            send_message_tcp(stream, ClientResponse::Write(Err(VPFSError::DoesNotExist)));
        }
    } else if let Some(file_owner_connection) = get_connection(&file.owner, &state).await {
        match file_owner_connection.open_bi().await {
            Ok((mut send, mut recv)) => {
                
                let buf = receive_buf_tcp(stream, file_len).unwrap();

                send_message(&mut send, DaemonRequest::Write(file.uri)).await;
                send_message(&mut send, buf).await;
                if let Ok(DaemonResponse::Write(write_result)) = receive_message(&mut recv).await {
                    send_message_tcp(stream, ClientResponse::Write(write_result));
                }
                
            }
            Err(e) => {
                eprintln!("Error opening bi-directional stream: {}", e);
                receive_buf_tcp(stream, file_len).ok();
                let cache_entry_file = {
                    let cache = state.cache.lock().unwrap();
                    cache.peek(&file.name).map(|e| FileEntry {
                        owner: state.local.name.clone(),
                        uri: e.uri.clone(),
                        name: file.name.clone(),
                    })
                };
                if cache_entry_file.is_some() {
                    send_message_tcp(stream, ClientResponse::Write(Err(VPFSError::OnlyInCache(cache_entry_file.unwrap()))));
                } else {
                    send_message_tcp(stream, ClientResponse::Write(Err(VPFSError::NotAccessible)));
                }
            }
        }
    } else {
        receive_buf_tcp(stream, file_len).ok();
        let cache_entry_file = {
            let cache = state.cache.lock().unwrap();
            cache.peek(&file.name).map(|e| FileEntry {
                owner: state.local.name.clone(),
                uri: e.uri.clone(),
                name: file.name.clone(),
            })
        };

        if cache_entry_file.is_some() {
            send_message_tcp(stream, ClientResponse::Write(Err(VPFSError::OnlyInCache(cache_entry_file.unwrap()))));
        } else {
            send_message_tcp(stream, ClientResponse::Write(Err(VPFSError::NotAccessible)));
        }
    }
}

/// Handle requests from connected client program
fn handle_client(mut stream: TcpStream, state: Arc<DaemonState>, rt_handle: &Handle) {
    rt_handle.block_on(async {
        println!("handle client");
        loop {

            match receive_message_tcp(&mut stream) {
                Ok(ClientRequest::ListFiles(dir)) => {
                    handle_client_list_files(&mut stream, &dir, &state).await;
                }
                Ok(ClientRequest::Find(file)) => {
                    handle_client_find(&mut stream, &file, &state).await;
                }
                Ok(ClientRequest::Place(file, node_name )) => {
                    handle_client_place(&mut stream, &file, node_name,  &state).await;
                }
                Ok(ClientRequest::Open(file)) => {
                    handle_client_open_file(&mut stream, file, &state).await;
                }
                Ok(ClientRequest::ReadFd(file, fd, len)) => {
                    handle_client_read_fd(&mut stream, file, fd, len, & state).await;
                }
                Ok(ClientRequest::ReadLineFd(file, fd)) => {
                    handle_client_read_line_fd(&mut stream, file, fd, & state).await;
                }
                Ok(ClientRequest::Close(node_name, fd)) => {
                    handle_client_close_file(&mut stream, node_name, fd, &state).await;
                }
                Ok(ClientRequest::Read(file)) => {
                    handle_client_read(&mut stream, file, &state).await;
                }
                Ok(ClientRequest::Write(file,len)) => {
                    handle_client_write(&mut stream, file, len, &state).await;
                }
                Err(_) => {
                    println!("Client diconnected");
                    break;
                }
            }
        }
    });
}

/// Handle incoming connection from client program
fn handle_connection(mut stream: TcpStream, state: Arc<DaemonState>, rt_handle: Handle) {
    stream.set_nodelay(true);
    stream.set_quickack(true);

    println!("handle connection");
    match receive_message_tcp(&mut stream) {
        Ok(Hello::ClientHello) => {
            println!("User process connected");
            send_message_tcp(&mut stream, HelloResponse::ClientHello(state.local.name.clone()));
            handle_client(stream, state, &rt_handle);
        },
        Ok(_) => eprintln!("Unexpected hello message"),
        Err(_) => eprintln!("Did not receive proper hello message"),
    }
}

/// Start TCP server to accept connections from client programs
fn start_server(address: &str, state: Arc<DaemonState>, rt_handle: Handle) {
    let listener = TcpListener::bind(address).unwrap();
    println!("Listening for client connections");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Incoming connections");
                let state_clone = state.clone();
                let rt_handle_clone = rt_handle.clone();
                thread::spawn(move || {
                    handle_connection(stream, state_clone, rt_handle_clone); 
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    
    // initialize iroh endpoint and wait for it to be online
    let address = format!("0.0.0.0:{}", opt.port);
    let mut config = TransportConfig::default();
    config.max_idle_timeout(None);
    let endpoint: Endpoint = Endpoint::builder()
        .transport_config(config)
        .bind_addr_v4(address.parse().unwrap())
        .bind()
        .await?;
    
    endpoint.online().await;
    
    let endpoint_id = endpoint.id();
    println!("Endpoint Id: {endpoint_id}");

    // initialize daemon state
    let mut state = DaemonState {
        endpoint: endpoint.clone(),
        local: VPFSNode{name: opt.name.clone(), endpoint_id},
        connections: Mutex::new(HashMap::new()),
        known_nodes: Mutex::new(HashMap::new()),
        cache: Mutex::new(LruCache::unbounded()),
        max_cache_size: opt.cache_size,
        used_cache_bytes: RwLock::new(0),
        file_system: RwLock::new(HashMap::new()),
        vector_clock: Mutex::new(HashMap::from([(opt.name.clone(), 0u64)])),
        log: Mutex::new(Vec::new()),
        open_files: Mutex::new(HashMap::new())
    };

    let state = Arc::new(state);

    // Initialize protocol router
    let router = Router::builder(endpoint)
        .accept(VPFSProtocol::ALPN, protocol::VPFSProtocol{ state:state.clone() })
        .spawn();

    if opt.remote_id.is_some() {
        // remote_id is provided, connect to remote node, send hello and populate known hosts
        let remote_id = opt.remote_id.unwrap();
        println!("Connecting to network with node {}", remote_id);

        let connection = connect_to_network(&router.endpoint(), remote_id, &state).await;
        if connection.is_none() {
            panic!("Could not connect")
        }
        let connection = connection.unwrap();
        println!("Connected to network");
        let new_node = setup_files_dir();
        if new_node {
            build_file_system(&connection, &state).await;
        } else {
            restore_vector_clock(&state);
            restore_file_system(&state);
            restore_log(&state);
        }

        let conflict_address = format!("127.0.0.1:{}", opt.conflict_port);
        println!("Connecting to conflict resolution client on {}", conflict_address);
        let mut conflict_stream = TcpStream::connect(&conflict_address).unwrap();
        println!("Connected to conflict resolution client");

        //TODO: propagate conflict resolution to entire network
        check_conflicts(conflict_stream, &connection, &state).await;

        establish_connections(&state).await;

        for (name, remote_id) in state.known_nodes.lock().unwrap().iter() {
            println!("Known node: {}, {}", name, remote_id);
        }

        for (name, connection) in state.connections.lock().unwrap().iter() {
            println!("connection: {}, {:?}", name, connection.close_reason());
        }

    } else {
        // current node is the initial node of network
        println!("Running as first node on vpfs");

        let new_node = setup_files_dir();
        if !new_node {
            restore_vector_clock(&state);
            restore_file_system(&state);
            restore_log(&state);
        }
    }

    restore_cache(&state);

    let client_address = format!("0.0.0.0:{}",opt.listen_port);
    let rt_handle = Handle::current();
    start_server(&client_address, state.clone(), rt_handle);

    Ok(())

}