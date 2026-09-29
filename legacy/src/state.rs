use iroh::{Endpoint, PublicKey};
use iroh::endpoint::Connection;
use lru::LruCache;
use std::fs::File;
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::messages::{VPFSNode, CacheEntry, FileEntry, LogEntry};

#[derive(Debug)]
pub(crate) struct DaemonState {
    pub endpoint: Endpoint,
    pub local: VPFSNode,
    pub connections: Mutex<HashMap<String, Arc<Connection>>>, // name of node -> connection
    pub known_nodes: Mutex<HashMap<String, PublicKey>>,  // name of node -> public key
    pub cache: Mutex<LruCache<String, CacheEntry>>,
    pub max_cache_size: usize,
    pub used_cache_bytes: RwLock<usize>,
    pub file_system: RwLock<HashMap<String, FileEntry>>, // path -> file entry
    pub vector_clock: Mutex<HashMap<String, u64>>,       // logical clock per node
    pub log: Mutex<Vec<LogEntry>>,
    pub open_files: Mutex<HashMap<i32, File>>,
}
