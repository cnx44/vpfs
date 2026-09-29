use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, IntoRawFd};
use std::result;
use std::{fs, io::{Read, Write}};
use std::sync::{Mutex, RwLock};
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;
use iroh::endpoint::Connection;
use rand::Rng;
use lru::LruCache;
use rand::rand_core::le;

use std::sync::MutexGuard;

use crate::{file_system, messages::*, receive_message_tcp, send_message_tcp};

use crate::state::DaemonState;

use crate::remote_communication::*;

/// Increment this node's entry in the clock and return a snapshot of the full clock.
fn tick_vector_clock(node: &str, vc: &mut HashMap<String, u64>) -> HashMap<String, u64> {
    *vc.entry(node.to_string()).or_insert(0) += 1;
    vc.clone()
}

/// Returns true if every component of `a` is ≤ the corresponding component of `b`,
/// and `b` is strictly greater in at least one component.
pub fn happens_before(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> bool {
    let all_le = a.iter().all(|(k, v)| *v <= *b.get(k).unwrap_or(&0));
    let b_strictly_greater = b.iter().any(|(k, v)| *v > *a.get(k).unwrap_or(&0));
    all_le && b_strictly_greater
}

/// Returns true if neither clock happens-before the other (and they differ).
pub fn are_concurrent(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> bool {
    !happens_before(a, b) && !happens_before(b, a) && a != b
}

/// Returns a new clock that is the component-wise maximum of `a` and `b`.
pub fn merge_clocks(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> HashMap<String, u64> {
    let mut result = a.clone();
    for (k, v) in b {
        let cur = result.entry(k.clone()).or_insert(0);
        if *v > *cur { *cur = *v; }
    }
    result
}

/// Extract the file path from any log operation.
pub fn entry_path(op: &LogOp) -> String {
    match op {
        LogOp::Create(f) | LogOp::Modify(f) | LogOp::Remove(f) => f.name.clone(),
    }
}

/// Extract the FileEntry reference from any log operation.
pub fn entry_file(op: &LogOp) -> &FileEntry {
    match op {
        LogOp::Create(f) | LogOp::Modify(f) | LogOp::Remove(f) => f,
    }
}

/// Return the subset of `log` that the `since` clock has not yet observed.
/// An entry is "unseen" if its creator's own clock value exceeds what `since` records for that node.
pub fn partial_log_since(log: &[LogEntry], since: &HashMap<String, u64>) -> Vec<LogEntry> {
    log.iter()
        .filter(|e| {
            e.clock.get(&e.node).copied().unwrap_or(0)
                > since.get(&e.node).copied().unwrap_or(0)
        })
        .cloned()
        .collect()
}

/// Tick the clock, build a LogEntry, append to the log, persist it, and push it to all peers.
pub async fn append_log_entry(op: LogOp, state: &Arc<DaemonState>) {
    let clock_snapshot = {
        let mut vc = state.vector_clock.lock().unwrap();
        let snapshot = tick_vector_clock(&state.local.name, &mut vc);
        save_vector_clock(&vc);
        snapshot
    };
    let entry = LogEntry { clock: clock_snapshot, node: state.local.name.clone(), op };
    {
        let mut log = state.log.lock().unwrap();
        log.push(entry.clone());
        save_log(&log);
    }

    // Fan out the new entry to every active peer connection.
    let connections: Vec<Arc<Connection>> = {
        let conns = state.connections.lock().unwrap();
        conns.values().filter(|c| c.close_reason().is_none()).cloned().collect()
    };
    for conn in connections {
        if let Ok((mut send, mut recv)) = conn.open_bi().await {
            let _ = send_message(&mut send, DaemonRequest::UpdateLog(vec![entry.clone()])).await;
            let _ = receive_message::<DaemonResponse>(&mut recv).await;
        }
    }
}

pub fn save_log(log: &[LogEntry]) {
    let log_file = fs::File::create("log").expect("Failed to create log file");
    serde_bare::to_writer(&log_file, log).expect("Failed to write log");
}

pub fn save_vector_clock(vc: &HashMap<String, u64>) {
    let vc_file = fs::File::create("vector_clock").expect("Failed to create vector_clock file");
    serde_bare::to_writer(&vc_file, vc).expect("Failed to write vector_clock");
}

pub fn restore_vector_clock(state: &Arc<DaemonState>) {
    if let Ok(vc_file) = fs::File::open("vector_clock") {
        if let Ok(saved_vc) = serde_bare::from_reader::<_, HashMap<String, u64>>(&vc_file) {
            let mut vc = state.vector_clock.lock().unwrap();
            for (node, val) in saved_vc {
                let cur = vc.entry(node).or_insert(0);
                if val > *cur { *cur = val; }
            }
        }
    }
}

/// Restore the log
pub fn restore_log(state: &Arc<DaemonState>) {
    if let Ok(log_file) = fs::File::open("log") {
        if let Ok(log) = serde_bare::from_reader::<_, Vec<LogEntry>>(&log_file) {
            let mut vc = state.vector_clock.lock().unwrap();
            for entry in &log {
                for (node, &val) in &entry.clock {
                    let cur = vc.entry(node.clone()).or_insert(0);
                    if val > *cur {
                        *cur = val;
                    }
                }
            }
            *state.log.lock().unwrap() = log;
        }
    }
}

/// Create ./files and go to it. Panic if it cannot be created or cd'ed into.
pub fn setup_files_dir() -> bool {
    if let Err(err) = fs::create_dir("./files") {
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            std::env::set_current_dir("./files").expect("Could not cd into ./files directory");
            return false;
        }
        panic!("Could not create directory for storing files");
    }
    std::env::set_current_dir("./files").expect("Could not cd into ./files directory");
    true
}

pub fn add_cache_entry(file: &FileEntry, data: &[u8], cache: &mut MutexGuard<LruCache<String, CacheEntry>>, state: &Arc<DaemonState>) {
    let old_size = if let Some(existing) = cache.peek(&file.name) {
        fs::metadata(&existing.uri).map(|m| m.len() as usize).unwrap_or(0)
    } else {
        0
    };
    let new_cache_entry = CacheEntry { uri: file.uri.clone() };
    fs::write(&new_cache_entry.uri, &data);
    cache.put(file.name.clone(), new_cache_entry);

    let mut used_cache = state.used_cache_bytes.write().unwrap();
    *used_cache = used_cache.saturating_sub(old_size);
    *used_cache += data.len();
    // Evict elements to make room in cache
    while *used_cache > state.max_cache_size {
        if let Some((_, lru_entry)) = cache.pop_lru() {
            let file_size = fs::metadata(&lru_entry.uri).expect("Cache entry missing backing file").len();
            fs::remove_file(&lru_entry.uri).unwrap();
            *used_cache -= file_size as usize;
        }
        else {
            break;
        }
    }
    let cache_file = fs::File::create("cache").expect("Failed to create cache file");
    serde_bare::to_writer(&cache_file, &*used_cache).expect("Failed to save cahce size to file");
    for (key, value) in cache.iter() {
        serde_bare::to_writer(&cache_file, key).expect("Could not write cache entry to file");
        serde_bare::to_writer(&cache_file, value).expect("Could not write cache entry to file");
    }
}


/// Restore cache from ./cache file if it exists
pub fn restore_cache(state: &Arc<DaemonState>) {
    if let Ok(cache_file) = fs::File::open("cache") {
        let mut cache = state.cache.lock().unwrap();
        *state.used_cache_bytes.write().unwrap() = serde_bare::from_reader(&cache_file).expect("Failed to readed from cache file");
        while let Ok(key) = serde_bare::from_reader::<_, String>(&cache_file) {
            let value = serde_bare::from_reader(&cache_file).unwrap();
            cache.put(key, value);
        }
    }
}

/// Restore file system from ./file_system file if it exists
pub fn restore_file_system(state: &Arc<DaemonState>) {
    if let Ok(fs_file) = fs::File::open("file_system") {
        let mut file_system = state.file_system.write().unwrap();
        while let Ok(path) = serde_bare::from_reader::<_, String>(&fs_file) {
            let entry: FileEntry = serde_bare::from_reader(&fs_file).unwrap();
            file_system.insert(path, entry);
        }
    }
}

pub async fn check_conflicts(mut stream: TcpStream, connection: &Connection, state: &Arc<DaemonState>) {
    // Get remote node's partial log from where we last synced
    let our_vc = state.vector_clock.lock().unwrap().clone();

    let (remote_entries, remote_vc) = match connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            send_message(&mut send, DaemonRequest::LogSince(our_vc)).await;
            match receive_message::<DaemonResponse>(&mut recv).await {
                Ok(DaemonResponse::Log(entries, vc)) => (entries, vc),
                Ok(_) => { eprintln!("Unexpected response to LogSince"); return; }
                Err(e) => { eprintln!("Error receiving log: {}", e); return; }
            }
        }
        Err(e) => { eprintln!("Error opening stream for LogSince: {}", e); return; }
    };

    // each file's last log entry
    let mut remote_last: HashMap<String, LogEntry> = HashMap::new();
    for entry in &remote_entries {
        remote_last.insert(entry_path(&entry.op), entry.clone());
    }

    let local_unseen: Vec<LogEntry> = {
        let local_log = state.log.lock().unwrap();
        partial_log_since(&local_log, &remote_vc)
    };
    let mut local_last: HashMap<String, LogEntry> = HashMap::new();
    for entry in &local_unseen {
        local_last.insert(entry_path(&entry.op), entry.clone());
    }

    // Compare and resolve
    let mut send_remote: Vec<FileEntry> = Vec::new();
    let local_file_system = state.file_system.read().unwrap().clone();

    // Track paths resolved via conflict resolution; all log entries for these paths are
    // purged from both sides and replaced by a single resolution entry.
    let mut resolved_paths: Vec<String> = Vec::new();
    let mut resolve_msgs: Vec<(String, LogEntry)> = Vec::new();

    for (path, remote_entry) in &remote_last {
        let remote_file = entry_file(&remote_entry.op).clone();

        if let Some(local_entry) = local_last.get(path) {
            let local_file = entry_file(&local_entry.op).clone();

            if are_concurrent(&local_entry.clock, &remote_entry.clock) {
                println!("Conflict (concurrent) for file: {}", path);
                let to_send = vec![local_file.clone(), remote_file.clone()];
                send_message_tcp(&mut stream, ConflictResolutionRequest::Versions(to_send));
                if let Ok(ConflictResolutionResponse::FinalVersion(final_entry)) = receive_message_tcp(&mut stream) {
                    println!("Resolved file {}: {:?}", path, final_entry);

                    // Build a resolution log entry whose clock supersedes both sides
                    let mut resolved_clock = merge_clocks(&local_entry.clock, &remote_entry.clock);
                    *resolved_clock.entry(state.local.name.clone()).or_insert(0) += 1;
                    let resolved_log_entry = LogEntry {
                        clock: resolved_clock.clone(),
                        node: state.local.name.clone(),
                        op: LogOp::Modify(final_entry.clone()),
                    };

                    // Append the resolution entry (keep existing history, just avoid duplicates)
                    {
                        let mut vc = state.vector_clock.lock().unwrap();
                        let mut log = state.log.lock().unwrap();
                        if !log.contains(&resolved_log_entry) {
                            log.push(resolved_log_entry.clone());
                        }
                        for (k, v) in &resolved_clock {
                            let cur = vc.entry(k.clone()).or_insert(0);
                            if *v > *cur { *cur = *v; }
                        }
                        save_log(&log);
                        save_vector_clock(&vc);
                    }

                    // Skip any remote log entry for this path during the merge step below
                    resolved_paths.push(path.clone());
                    // Queue a ResolveConflict message so the remote mirrors this change
                    resolve_msgs.push((path.clone(), resolved_log_entry));

                    if final_entry.uri != local_file.uri {
                        // Remote version won — update local file system and evict cache
                        state.file_system.write().unwrap().insert(path.clone(), final_entry);
                        let mut cache = state.cache.lock().unwrap();
                        if let Some(evicted) = cache.pop(path) {
                            let file_size = fs::metadata(&evicted.uri).map(|m| m.len()).unwrap_or(0);
                            fs::remove_file(&evicted.uri).ok();
                            *state.used_cache_bytes.write().unwrap() -= file_size as usize;
                        }
                    } else {
                        // Local version won, remote needs to update its file system
                        send_remote.push(final_entry);
                    }
                }
            } else if happens_before(&local_entry.clock, &remote_entry.clock) {
                // Remote is strictly newer, accept it
                println!("Remote newer for file: {}", path);
                state.file_system.write().unwrap().insert(path.clone(), remote_file);
                let mut cache = state.cache.lock().unwrap();
                if let Some(evicted) = cache.pop(path) {
                    let file_size = fs::metadata(&evicted.uri).map(|m| m.len()).unwrap_or(0);
                    fs::remove_file(&evicted.uri).ok();
                    *state.used_cache_bytes.write().unwrap() -= file_size as usize;
                }
            } else {
                // Local is newer (or equal), send to remote
                if let Some(local_fs_entry) = local_file_system.get(path) {
                    send_remote.push(local_fs_entry.clone());
                }
            }
        } else if !matches!(&remote_entry.op, LogOp::Remove(_)) {
            // File only exists on remote, add it locally
            println!("New remote file: {}", path);
            state.file_system.write().unwrap().insert(path.clone(), remote_file);
        }
    }

    // Files only in local log, send to remote
    for (path, _) in &local_last {
        if !remote_last.contains_key(path) {
            if let Some(entry) = local_file_system.get(path) {
                send_remote.push(entry.clone());
            }
        }
    }

    save_file_system(&state.file_system.read().unwrap());

    // Merge remote log entries into local, skipping any entry whose path was conflict-resolved
    {
        let mut vc = state.vector_clock.lock().unwrap();
        let mut log = state.log.lock().unwrap();
        for entry in &remote_entries {
            let entry_p = entry_path(&entry.op);
            if resolved_paths.iter().any(|p| p == &entry_p) {
                continue;
            }
            if log.contains(entry) {
                continue;
            }
            for (node, &val) in &entry.clock {
                let cur = vc.entry(node.clone()).or_insert(0);
                if val > *cur { *cur = val; }
            }
            log.push(entry.clone());
        }
        save_log(&log);
        save_vector_clock(&vc);
    }

    // Push new partial log to remote
    let our_partial = {
        let log = state.log.lock().unwrap();
        partial_log_since(&log, &remote_vc)
    };
    if let Ok((mut send, mut recv)) = connection.open_bi().await {
        send_message(&mut send, DaemonRequest::UpdateLog(our_partial)).await;
        let _ = receive_message::<DaemonResponse>(&mut recv).await;
    } else {
        eprintln!("Error opening stream for UpdateLog");
    }

    // Inform remote about each conflict resolution: drop all entries for `path`, add the resolved one
    for (path, add_entry) in &resolve_msgs {
        if let Ok((mut send, mut recv)) = connection.open_bi().await {
            send_message(&mut send, DaemonRequest::ResolveConflict(
                path.clone(),
                add_entry.clone(),
            )).await;
            let _ = receive_message::<DaemonResponse>(&mut recv).await;
        } else {
            eprintln!("Error opening stream for ResolveConflict");
        }
    }

    // Send file entries for updates
    if let Ok((mut send, _)) = connection.open_bi().await {
        send_message(&mut send, DaemonRequest::UpdatedFiles(send_remote)).await;
    } else {
        eprintln!("Error opening stream for UpdatedFiles");
    }
}

pub fn read_local(uri: &str, file_system: &RwLock<HashMap<String, FileEntry>>) -> io::Result<Vec<u8>> {
    fs::read(uri).map_err(|_| io::Error::from(io::ErrorKind::NotFound))
}

pub fn write_local(uri: &str, data: &Vec<u8>, file_system: &RwLock<HashMap<String, FileEntry>>) -> io::Result<()> {
    let outer = file_system.read().unwrap();
    if outer.values().any(|e| e.uri == uri) {
        if fs::exists(uri)? {
            fs::write(uri, data)
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    } else {
        Err(io::Error::from(io::ErrorKind::NotFound))
    }
}

pub fn create_file_with_random_uri() -> String {
    let mut rng = rand::rng();
    let mut uri = format!("{:x}", rng.random::<u64>());
    loop {
        if let Err(error) = fs::File::create_new(&uri) {
            if error.kind() != io::ErrorKind::AlreadyExists {
                panic!("Could not create file"); // TODO better error handleing
            }
            uri = format!("{:x}", rng.random::<u64>());
        }
        else {
            break;
        }
    }
    uri
}

pub async fn build_file_system(connection: &Connection, state: &Arc<DaemonState>) {
    println!("Building file system from connection: {}", connection.remote_id());
    match connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            let msg = DaemonRequest::FileSystem;
            send_message(&mut send, msg).await;

            match receive_message::<DaemonResponse>(&mut recv).await {
                Ok(DaemonResponse::FileSystem(data)) => {
                    let mut file_system = state.file_system.write().unwrap();
                    for (path, entry) in data {
                        file_system.entry(path).or_insert(entry);
                    }
                    save_file_system(&file_system);

                },
                Ok(_) => {
                    eprintln!("Unexpected response");
                }
                Err(e) => { eprintln!("Error: {}", e); }
            }

        }
        Err(e) => eprintln!("Error opening bi-directional stream: {}", e),
    }

}

pub async fn read_remote(file: &FileEntry, state: &Arc<DaemonState>) -> Result<Vec<u8>, VPFSError> {
    println!("Read remote file: {}, owner: {}, uri: {}", file.name, file.owner, file.uri);
    // Collect what we need from the cache and release the lock before any async work.
    let (cache_last_update_time, cached_uri) = {
        let cache = state.cache.lock().unwrap();
        let entry = cache.peek(&file.name);
        let mtime = entry.and_then(|e| fs::metadata(&e.uri).ok())
            .and_then(|m| m.modified().ok());
        let uri = entry.map(|e| e.uri.clone());
        (mtime, uri)
    };

    if let Some(file_owner_connection) = get_connection(&file.owner, state).await {
        match file_owner_connection.open_bi().await {
            Ok((mut send, mut recv)) => {
                send_message(&mut send, DaemonRequest::Read(file.uri.clone(), cache_last_update_time)).await;

                match receive_message(&mut recv).await {
                    Ok(DaemonResponse::Read(Ok(()))) => {
                        let buf = receive_message::<Vec<u8>>(&mut recv).await.unwrap();
                        let mut cache = state.cache.lock().unwrap();
                        add_cache_entry(&file, &buf, &mut cache, state);
                        return Ok(buf)
                    },
                    Ok(DaemonResponse::Read(Err(VPFSError::NotModified))) => {
                        let uri = cached_uri.expect("NotModified response but no cache entry");
                        return Ok(fs::read(&uri).expect("Missing file for cache entry"))
                    }
                    Ok(DaemonResponse::Read(Err(error))) => {
                        return Err(error)
                    },
                    Ok(_) => panic!("Bad response"),
                    Err(_) => {
                        todo!("Check if error came from bad response, or from connection closing")
                    }
                }
            }
            Err(e) => {
                eprintln!("Error opening bi-directional stream: {}", e);
                return Err(VPFSError::NotAccessible);
            }
        }
    }
    else {
        if let Some(uri) = cached_uri {
            let cache_entry_file = FileEntry {
                owner: state.local.name.clone(),
                uri,
                name: file.name.clone()
            };
            Err(VPFSError::OnlyInCache(cache_entry_file))
        }
        else {
            Err(VPFSError::NotAccessible)
        }
    }
}

pub fn save_file_system(file_system: &HashMap<String, FileEntry>) {
    let fs_file = fs::File::create("file_system").expect("Failed to create file_system file");
    for (path, entry) in file_system.iter() {
        serde_bare::to_writer(&fs_file, path).expect("Failed to write path to file_system file");
        serde_bare::to_writer(&fs_file, entry).expect("Failed to write entry to file_system file");
    }
}

pub fn place_file_in_memory(file_system: &RwLock<HashMap<String, FileEntry>>, path: &str, new_file: FileEntry) {
    println!("Placing file in memory at path: {}, with uri: {}, owner: {}", path, new_file.uri, new_file.owner);
    let mut fs = file_system.write().unwrap();
    fs.insert(path.to_string(), new_file);
    save_file_system(&fs);
}

pub async fn place_file(path: &str, at: &String, state: &Arc<DaemonState>) -> Result<FileEntry, VPFSError>{
    let find_result = find(path, state);
    if find_result.is_ok() {
        return Err(VPFSError::AlreadyExists(find_result.unwrap()));
    }
    let uri = if *at == state.local.name {
        create_file_with_random_uri()
    }
    else if let Ok(DaemonResponse::Place(uri)) = send_and_receive(at, DaemonRequest::Place, state).await {
        uri
    }
    else {
        return Err(VPFSError::NotAccessible);
    };
    let new_file = FileEntry {
        owner: at.clone(),
        uri: uri,
        name: path.to_string(),
    };
    place_file_in_memory(&state.file_system, path, new_file.clone());
    append_log_entry(LogOp::Create(new_file.clone()), state).await;

    let connections: Vec<Arc<Connection>> = {
        let conns = state.connections.lock().unwrap();
        conns.values()
            .filter(|c| c.close_reason().is_none())
            .cloned()
            .collect()
    };
    println!("Notifying {} other nodes of new file", connections.len());
    for conn in connections {
        if let Ok((mut send, mut recv)) = conn.open_bi().await {
            let _ = send_message(&mut send, DaemonRequest::AddEntry(path.to_string(), new_file.clone())).await;
            let _ = receive_message::<DaemonResponse>(&mut recv).await;
        }
    }

    Ok(new_file)
}


pub fn list_files(dir: &str, state: &Arc<DaemonState>) -> Result<Vec<FileEntry>, VPFSError> {
    // TODO
    // for now lists all files (as directories are not supported)
    let outer = state.file_system.read().unwrap();
    let entries = outer.iter()
        .map(|(_, entry)| entry.clone())
        .collect();
    Ok(entries)
}

pub fn find(file: &str, state: &Arc<DaemonState>) -> Result<FileEntry, VPFSError> {
    let outer = state.file_system.read().unwrap();
    println!("Finding file: {}, file system: {:?}", file, *outer);
    outer.get(file)
        .map(|e| e.clone())
        .ok_or(VPFSError::DoesNotExist)
}

pub fn open_file_local(uri: &str, open_files: &Mutex<HashMap<i32,File>>) -> io::Result<i32> {
    // fs_lock.read().unwrap();
    let file = File::open(uri);
    match file {
        Ok(file) => {
            let mut open_files = open_files.lock().unwrap();
            let fd = file.as_raw_fd();
            open_files.insert(fd, file);
            Ok(fd)
        },
        Err(e) => Err(e),
    }
}

pub async fn open_file(file: FileEntry, state: &Arc<DaemonState>) -> Result<i32, VPFSError> {
    if file.owner == state.local.name {
        if let Ok(fd) = open_file_local(&file.uri, &state.open_files) {
            return Ok(fd);
        }
        return Err(VPFSError::DoesNotExist);
    }
    let file_owner_connection = get_connection(&file.owner, state).await;
    if file_owner_connection.is_none() {
        return Err(VPFSError::NotAccessible);
    }
    let file_owner_connection = file_owner_connection.unwrap();
    match file_owner_connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            send_message(&mut send, DaemonRequest::Open(file.uri.clone())).await;
            
            match receive_message(&mut recv).await {
                Ok(DaemonResponse::Open(fd_result)) => {
                    return fd_result;
                },
                Ok(_) => panic!("Bad response"),
                Err(_) => {
                    todo!("Check if error came from bad response, or from connection closing")
                }
            }                
        }
        Err(e) => {
            eprintln!("Error opening bi-directional stream: {}", e);
            return Err(VPFSError::NotAccessible);
        }
        
    }
}

pub fn read_fd_local(fd: i32, len:usize, open_files: &Mutex<HashMap<i32,File>>) -> io::Result<Vec<u8>>{
    let mut open_files = open_files.lock().unwrap();
    let file = open_files
        .get_mut(&fd)
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

    // let mut reader = BufReader::new(file);
    // let mut buf = Vec::new();

    // reader.take(len as u64)         
    //     .read_to_end(&mut buf)?;

    // Ok(buf)

    let mut buf = vec![0u8; len];
    let n = file.read(&mut buf)?;

    buf.truncate(n);
    Ok(buf)
}

pub fn read_line_fd_local(fd: i32, open_files: &Mutex<HashMap<i32,File>>) -> io::Result<Vec<u8>>{
    let mut open_files = open_files.lock().unwrap();
    let file = open_files
        .get_mut(&fd)
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;

    // let mut reader = BufReader::new(file);
    // let mut line = String::new();

    // reader.read_line(&mut line)?;

    // Ok(line.into_bytes())

    let mut buf = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        let n = file.read(&mut byte)?;
        if n == 0 {
            break; // EOF
        }

        buf.push(byte[0]);
        
        if byte[0] == b'\n' {
            break;
        }
    }

    Ok(buf)
}

pub async fn read_fd(file: &FileEntry, fd:i32, len:usize, state: &Arc<DaemonState>) -> Result<Vec<u8>, VPFSError> {
    if file.owner == state.local.name {
        if let Ok(fd) = read_fd_local(fd, len, &state.open_files) {
            return Ok(fd);
        }
        return Err(VPFSError::FileNotOpen);
    }
    let file_owner_connection = get_connection(&file.owner, state).await;
    if file_owner_connection.is_none() {
        return Err(VPFSError::NotAccessible);
    }
    let file_owner_connection = file_owner_connection.unwrap();
    match file_owner_connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            send_message(&mut send, DaemonRequest::ReadFd(fd, len)).await;
            
            match receive_message(&mut recv).await {
                Ok(DaemonResponse::ReadFd(Ok(()))) => {
                    let buf = receive_message::<Vec<u8>>(&mut recv).await.unwrap();
                    return Ok(buf)
                },
                Ok(DaemonResponse::ReadFd(Err(error))) => {
                    return Err(error)
                },
                Ok(_) => panic!("Bad response"),
                Err(_) => {
                    todo!("Check if error came from bad response, or from connection closing")
                }
            }
        }
        Err(e) => {
            eprintln!("Error opening bi-directional stream: {}", e);
            return Err(VPFSError::NotAccessible);
        }
        
    }
}

pub async fn read_line_fd(file: &FileEntry, fd:i32, state: &Arc<DaemonState>) -> Result<Vec<u8>, VPFSError> {
    if file.owner == state.local.name {
        if let Ok(fd) = read_line_fd_local(fd, &state.open_files) {
            return Ok(fd);
        }
        return Err(VPFSError::FileNotOpen);
    }
    let file_owner_connection = get_connection(&file.owner, state).await;
    if file_owner_connection.is_none() {
        return Err(VPFSError::NotAccessible);
    }
    let file_owner_connection = file_owner_connection.unwrap();
    match file_owner_connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            send_message(&mut send, DaemonRequest::ReadLineFd(fd)).await;
            
            match receive_message(&mut recv).await {
                Ok(DaemonResponse::ReadLineFd(Ok(()))) => {
                    let buf = receive_message::<Vec<u8>>(&mut recv).await.unwrap();
                    return Ok(buf)
                },
                Ok(DaemonResponse::ReadLineFd(Err(error))) => {
                    return Err(error)
                },
                Ok(_) => panic!("Bad response"),
                Err(_) => {
                    todo!("Check if error came from bad response, or from connection closing")
                }
            }
        }
        Err(e) => {
            eprintln!("Error opening bi-directional stream: {}", e);
            return Err(VPFSError::NotAccessible);
        }
        
    }
}

pub fn close_file_local(fd: i32, open_files: &Mutex<HashMap<i32,File>>) -> io::Result<()> {
    let mut open_files = open_files.lock().unwrap();
    if !open_files.contains_key(&fd) {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    open_files.remove(&fd);
    Ok(())
}

pub async fn close_file(node_name: &String, fd: i32, state: &Arc<DaemonState>) -> Result<(), VPFSError> {
    if *node_name == state.local.name {
        if let Ok(()) = close_file_local(fd, &state.open_files) {
            return Ok(());
        }
        return Err(VPFSError::FileNotOpen);
    }
    let file_owner_connection = get_connection(node_name, state).await;
    if file_owner_connection.is_none() {
        return Err(VPFSError::NotAccessible);
    }
    let file_owner_connection = file_owner_connection.unwrap();
    match file_owner_connection.open_bi().await {
        Ok((mut send, mut recv)) => {
            send_message(&mut send, DaemonRequest::Close(fd)).await;
            
            match receive_message(&mut recv).await {
                Ok(DaemonResponse::Close(close_result)) => {
                    return close_result;
                },
                Ok(_) => panic!("Bad response"),
                Err(_) => {
                    todo!("Check if error came from bad response, or from connection closing")
                }
            }                
        }
        Err(e) => {
            eprintln!("Error opening bi-directional stream: {}", e);
            return Err(VPFSError::NotAccessible);
        }
        
    }
}