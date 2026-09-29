use anyhow::{Result};
use iroh::{
    endpoint::{Connection}, protocol::{ProtocolHandler}
};

use std::sync::Arc;
use std::fs;
use std::io::{Read, Seek, SeekFrom};

use crate::state::DaemonState;
use crate::messages::*;
use crate::file_system::*;
use crate::remote_communication::*;

#[derive(Debug, Clone)]
pub struct VPFSProtocol {
    pub state: Arc<DaemonState>
}

impl VPFSProtocol {
    pub const ALPN: &'static [u8] = b"uic/vpfs";

    /// Handle daemon requests
    pub async fn handle_daemon(&self, conn: Arc<Connection>) {
        let remote_id = conn.remote_id();

        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
            match receive_message(&mut recv).await {
                Ok(DaemonRequest::Place)  => {
                    println!("Received Place request for node: {}", remote_id);
                    let response = DaemonResponse::Place(create_file_with_random_uri());
                    send_message(&mut send, response).await;
                }
                Ok(DaemonRequest::Open(uri)) => {
                    println!("Received Open request for node: {}", remote_id);

                    match open_file_local(&uri, &self.state.open_files) {
                        Ok(daemon_fd) => {
                            send_message(&mut send, DaemonResponse::Open(Ok(daemon_fd))).await;
                        }
                        Err(_) => {
                            send_message(&mut send, DaemonResponse::Open(Err(VPFSError::DoesNotExist))).await;
                        }
                    }
                }
                Ok(DaemonRequest::ReadFd(fd, len)) => {
                    match read_fd_local(fd, len, &self.state.open_files) {
                        Ok(buf) => {
                            send_message(&mut send, DaemonResponse::ReadFd(Ok(()))).await;
                            send_message(&mut send, buf).await;
                        }
                        Err(_) => {
                            send_message(&mut send, DaemonResponse::ReadFd(Err(VPFSError::FileNotOpen))).await;
                        }
                    }
                }
                Ok(DaemonRequest::ReadLineFd(fd)) => {
                    match read_line_fd_local(fd, &self.state.open_files) {
                        Ok(buf) => {
                            send_message(&mut send, DaemonResponse::ReadLineFd(Ok(()))).await;
                            send_message(&mut send, buf).await;
                        }
                        Err(_) => {
                            send_message(&mut send, DaemonResponse::ReadLineFd(Err(VPFSError::FileNotOpen))).await;
                        }
                    }
                }
                Ok(DaemonRequest::Close(fd)) => {
                    match close_file_local(fd, &self.state.open_files) {
                        Ok(()) => {
                            send_message(&mut send, DaemonResponse::Close(Ok(()))).await;
                        }
                        Err(_) => {
                            send_message(&mut send, DaemonResponse::Close(Err(VPFSError::FileNotOpen))).await;
                        }
                    }
                }
                Ok(DaemonRequest::Read( uri, last_modified )) => {
                    println!("Received Read request for node: {}", remote_id);

                    let should_send = {
                        if let Some(remote_last_modified) = last_modified {
                            let _fs_lock = self.state.file_system.read().unwrap();
                            if let Ok(file_data) = fs::metadata(&uri) {
                                if let Ok(local_last_modified) = file_data.modified() {
                                    local_last_modified != remote_last_modified
                                } else { true }
                            } else { true }
                        } else {
                            true
                        }
                    };

                    if !should_send {
                        send_message(&mut send, DaemonResponse::Read(Err(VPFSError::NotModified))).await;
                        let _ = send.finish();
                        continue;
                    }

                    match read_local(&uri, &self.state.file_system) {
                        Ok(buf) => {
                            send_message(&mut send, DaemonResponse::Read(Ok(()))).await;
                            send_message(&mut send, buf).await;
                        }
                        Err(_) => {
                            send_message(&mut send, DaemonResponse::Read(Err(VPFSError::DoesNotExist))).await;
                        }
                    }
                }
                Ok(DaemonRequest::Write(uri)) => {
                    println!("Received Write request for node: {}", remote_id);

                    let buf=receive_message::<Vec<u8>>(&mut recv).await.unwrap();
                    if write_local(&uri, &buf, &self.state.file_system).is_ok() {
                        // Log the modification — read guard must be dropped before awaiting
                        let file_entry: Option<FileEntry> = {
                            self.state.file_system.read().unwrap()
                                .values().find(|e| e.uri == uri).cloned()
                        };
                        if let Some(file_entry) = file_entry {
                            append_log_entry(LogOp::Modify(file_entry), &self.state).await;
                        }
                        send_message(&mut send, DaemonResponse::Write(Ok(buf.len()))).await;
                    } else {
                        send_message(&mut send, DaemonResponse::Write(Err(VPFSError::DoesNotExist))).await;
                    }
                }
                Ok(DaemonRequest::Remove(uri)) => {
                    let file_entry = self.state.file_system.read().unwrap()
                        .values().find(|e| e.uri == uri).cloned();
                    let result = fs::remove_file(&uri).is_ok();

                    if result {
                        if let Some(entry) = file_entry {
                            {
                                let mut fs = self.state.file_system.write().unwrap();
                                fs.remove(&entry.name);
                                save_file_system(&fs);
                            } // write guard dropped here before await
                            append_log_entry(LogOp::Remove(entry), &self.state).await;
                        }
                        send_message(&mut send, DaemonResponse::Remove(Ok(()))).await;
                    } else {
                        send_message(&mut send, DaemonResponse::Remove(Err(VPFSError::DoesNotExist))).await;
                    }
                }
                Ok(DaemonRequest::AddEntry(path, file_entry)) => {
                    println!("Received AddEntry request for node: {}", remote_id);

                    place_file_in_memory(&self.state.file_system, &path, file_entry);
                    send_message(&mut send, DaemonResponse::AddEntry(Ok(()))).await;
                }
                Ok(DaemonRequest::AddressFor(node_name)) => {
                    let addr = {
                        let known_nodes = self.state.known_nodes.lock().unwrap();
                        known_nodes.get(&node_name).cloned()
                    };

                    send_message(&mut send, DaemonResponse::AddressFor(addr)).await;
                }
                Ok(DaemonRequest::FileSystem) => {
                    println!("Received FileSystem request for node: {}", remote_id);

                    let data = {
                        let file_system = self.state.file_system.read().unwrap();
                        file_system.clone()
                    };
                    send_message(&mut send, DaemonResponse::FileSystem(data)).await;
                }
                Ok(DaemonRequest::UpdatedFiles(updated_files)) => {
                    let mut file_system = self.state.file_system.write().unwrap();
                    for entry in updated_files {
                        let mut cache = self.state.cache.lock().unwrap();
                        if let Some(evicted) = cache.pop(&entry.name) {
                            let size = fs::metadata(&evicted.uri).map(|m| m.len() as usize).unwrap_or(0);
                            fs::remove_file(&evicted.uri).ok();
                            *self.state.used_cache_bytes.write().unwrap() -= size;
                        }
                        drop(cache);
                        file_system.insert(entry.name.clone(), entry);
                    }
                    save_file_system(&file_system);
                }
                Ok(DaemonRequest::LogSince(their_clock)) => {
                    let (partial, our_vc) = {
                        let log = self.state.log.lock().unwrap();
                        let partial = partial_log_since(&log, &their_clock);
                        let our_vc = self.state.vector_clock.lock().unwrap().clone();
                        (partial, our_vc)
                    };
                    send_message(&mut send, DaemonResponse::Log(partial, our_vc)).await;
                }
                Ok(DaemonRequest::UpdateLog(entries)) => {
                    {
                        let mut vc = self.state.vector_clock.lock().unwrap();
                        let mut log = self.state.log.lock().unwrap();
                        for entry in entries {
                            if log.contains(&entry) { continue; } // dedup
                            for (node, &val) in &entry.clock {
                                let cur = vc.entry(node.clone()).or_insert(0);
                                if val > *cur { *cur = val; }
                            }
                            log.push(entry);
                        }
                        save_log(&log);
                        save_vector_clock(&vc);
                    }
                    send_message(&mut send, DaemonResponse::UpdateLog).await;
                }
                Ok(DaemonRequest::ResolveConflict(_path, add)) => {
                    {
                        let mut vc = self.state.vector_clock.lock().unwrap();
                        let mut log = self.state.log.lock().unwrap();
                        for (node, &val) in &add.clock {
                            let cur = vc.entry(node.clone()).or_insert(0);
                            if val > *cur { *cur = val; }
                        }
                        if !log.contains(&add) {
                            log.push(add);
                        }
                        save_log(&log);
                        save_vector_clock(&vc);
                    }
                    send_message(&mut send, DaemonResponse::ResolveConflict).await;
                }
                Ok(_) => eprintln!("Unexpected message from {remote_id}"),
                Err(e) => eprintln!("Error receiving message from {remote_id}: {:?}", e),
            }
            let _ = send.finish();
        }
    }

    /// Handle an incoming iroh connection
    pub async fn handle_connection(&self, conn: Connection) {
        let conn = Arc::new(conn);
        let remote_id = conn.remote_id();
        println!("Accepted connection from {remote_id}");

        if let Ok((mut send, mut recv)) = conn.accept_bi().await {
            println!("Opened bi-directional stream, endpoint id: {}", remote_id);

            match receive_message(&mut recv).await {
                Ok(Hello::DaemonHello(node)) => {
                    println!("Received DaemonHello from node: {}, endpoint_id: {}", node.name, node.endpoint_id);
                    {    
                        let mut known_nodes = self.state.known_nodes.lock().unwrap();
                        known_nodes.insert(node.name.clone(), node.endpoint_id.clone());
                        let mut connections = self.state.connections.lock().unwrap();
                        connections.insert(node.name.clone(), conn.clone());
                    }
                    for (name, endpoint_id) in self.state.known_nodes.lock().unwrap().iter() {
                        println!("known node: {}, endpoint_id: {}", name, endpoint_id);
                    }
                    for (name, connection) in self.state.connections.lock().unwrap().iter() {
                        println!("connection: {}, {:?}", name, connection.close_reason());
                    }
                    send_message(&mut send, HelloResponse::DaemonHello).await;
                    self.handle_daemon(conn).await;
                }
                Ok(Hello::InitHello(new_nodes)) => {
                    println!("Received InitHello from node: {}, new nodes: {:?}", remote_id, new_nodes);

                    let known_nodes_snapshot = {
                        let mut known_nodes = self.state.known_nodes.lock().unwrap();
                        let mut known_nodes_snapshot = known_nodes.clone();
                        known_nodes.extend(new_nodes);
                        known_nodes_snapshot.insert(self.state.local.name.clone(), self.state.local.endpoint_id.clone());
                        known_nodes_snapshot
                    };

                    for (name, id) in known_nodes_snapshot.iter() {
                        println!("Sending known node: {}, endpoint_id: {}", name, id);
                    }

                    send_message(&mut send, HelloResponse::InitHello(known_nodes_snapshot)).await;
                    self.handle_daemon(conn).await;
                }
                Ok(_) => eprintln!("Unexpected message from {remote_id}"),
                Err(e) => eprintln!("Error receiving message from {remote_id}: {:?}", e),
            }
                
        }
    }
}

impl ProtocolHandler for VPFSProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        self.handle_connection(conn).await;
        Ok(())
    }
}

// impl ProtocolHandler for VPFSProtocol {
//     fn accept(&self, conn: Connection) -> impl Future<Output = Result<(), AcceptError>> + Send {
//         Box::pin(async move {
//             self.handle_connection(conn).await;
//             Ok(())
//         })
//     }
// }