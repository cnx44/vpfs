//! Every type that crosses a process boundary: client <-> daemon (TCP),
//! daemon <-> daemon (iroh), daemon <-> conflict resolver (TCP), plus the
//! records the daemon persists on disk.

use serde::{Deserialize, Serialize};
use iroh::PublicKey;

use std::collections::HashMap;
use std::time::SystemTime;

/// Logical clock: node name -> number of operations seen from that node.
pub type Clock = HashMap<String, u64>;

#[derive(Serialize,Deserialize,Clone,Hash,Debug,PartialEq,Eq)]
pub struct VPFSNode {
    pub name: String,
    pub endpoint_id: PublicKey
}

/// How a file's content is interpreted; selects the write policy (see daemon/content.rs).
#[derive(Serialize,Deserialize,Clone,Copy,Debug,Default,PartialEq,Eq,Hash)]
pub enum FileKind {
    #[default]
    Blob,
    Text,
}

/// Metadata of a file in the VPFS namespace.
#[derive(Debug,Clone,Eq,Hash,PartialEq,Serialize,Deserialize)]
pub struct FileEntry {
    /// Node that stores the content.
    pub owner: String,
    /// Name of the content blob on the owner.
    pub uri: String,
    /// VPFS path.
    pub name: String,
    pub kind: FileKind,
}

/// A change to a file's content. Which ones are accepted depends on the file's kind.
#[derive(Serialize,Deserialize,Clone,Debug,PartialEq,Eq)]
pub enum Mutation {
    /// Replace the whole content.
    Replace(Vec<u8>),
    /// Insert `data` at `pos` (for text: `pos` counts characters).
    InsertAt { pos: u64, data: Vec<u8> },
    /// Delete `len` units starting at `pos` (for text: characters).
    DeleteAt { pos: u64, len: u64 },
}

#[derive(Serialize,Deserialize,Clone,Eq,PartialEq,Debug)]
pub enum LogOp {
    Create(FileEntry),
    Modify(FileEntry),
    Remove(FileEntry),
}

impl LogOp {
    pub fn file(&self) -> &FileEntry {
        match self {
            LogOp::Create(f) | LogOp::Modify(f) | LogOp::Remove(f) => f,
        }
    }

    pub fn path(&self) -> &str {
        &self.file().name
    }
}

#[derive(Serialize,Deserialize,Clone,Debug,Eq,PartialEq)]
pub struct LogEntry {
    pub clock: Clock,
    pub node: String,
    pub op: LogOp,
}

#[derive(Serialize,Deserialize,Clone,Eq,Hash,PartialEq,Debug)]
pub struct CacheEntry {
    pub uri: String
}

/// Hello messages
#[derive(Serialize,Deserialize)]
pub enum Hello {
    ClientHello,
    DaemonHello(VPFSNode),
    InitHello(HashMap<String, PublicKey>),
}

/// Responses to Hello messages
#[derive(Serialize,Deserialize)]
pub enum HelloResponse {
    /// node_name
    ClientHello(String),
    DaemonHello,
    /// node, knownhosts
    InitHello(HashMap<String, PublicKey>),
}

#[derive(Serialize,Deserialize,Debug,Eq,PartialEq,Clone)]
pub enum VPFSError {
    OnlyInCache(FileEntry),
    NotModified,
    DoesNotExist,  // We can verify that the file does not exist
    NotFound,      // We can not find the file. File may or may not exist
    NotAccessible, // We can not access the node need to complete request
    NotADirectory,
    AlreadyExists(FileEntry),
    FileNotOpen,
    Other(String),
    /// The path is in an unresolved conflict: changes need a human decision first.
    Conflicted(String),
    /// The mutation is not supported by the file's kind.
    Unsupported(FileKind),
}

/// Requests from a daemon to another daemon. Each one travels on its own stream
/// and gets exactly one `DaemonResponse`.
#[derive(Serialize,Deserialize,Debug)]
pub enum DaemonRequest {
    /// Allocate an empty blob on the receiver; answers `Allocated`.
    Allocate,
    /// Whole namespace, used to bootstrap a new node.
    Snapshot,
    /// Log entries the given clock has not seen.
    LogSince(Clock),
    /// Blob content, unless it did not change since the given time.
    Read { uri: String, if_modified_since: Option<SystemTime> },
    Write { entry: FileEntry, mutations: Vec<Mutation> },
    Open(String),
    ReadFd(i32, usize),
    ReadLineFd(i32),
    Close(i32),
    /// Operations recorded by the sender. Answered with `Ack` once applied.
    Events(Vec<LogEntry>),
}

#[derive(Serialize,Deserialize,Debug)]
pub enum DaemonResponse {
    Allocated(String),
    Snapshot(HashMap<String, FileEntry>),
    /// Missing log entries + sender's current clock
    Log(Vec<LogEntry>, Clock),
    /// `None` means not modified
    Read(Result<Option<Vec<u8>>, VPFSError>),
    /// Size of the content after the write
    Write(Result<usize, VPFSError>),
    Open(Result<i32, VPFSError>),
    Data(Result<Vec<u8>, VPFSError>),
    Close(Result<(), VPFSError>),
    Ack,
}

/// Requests from client to daemon
#[derive(Serialize,Deserialize)]
pub enum ClientRequest {
    ListFiles(String),
    Find(String),
    /// path, owner node, kind
    Place(String, String, FileKind),
    Open(FileEntry),
    ReadFd(FileEntry, i32, usize),
    ReadLineFd(FileEntry, i32),
    /// owner node, fd
    Close(String, i32),
    Read(FileEntry),
    /// `FileEntry`, number of bytes to write (sent right after the request)
    Write(FileEntry, usize),
    Mutate(FileEntry, Vec<Mutation>),
}

/// Response to client requests
#[derive(Serialize,Deserialize)]
pub enum ClientResponse {
    ListFiles(Result<Vec<FileEntry>, VPFSError>),
    Find(Result<FileEntry, VPFSError>),
    Place(Result<FileEntry, VPFSError>),
    Open(Result<i32,VPFSError>),
    ReadFd(Result<usize, VPFSError>),
    ReadLineFd(Result<usize, VPFSError>),
    Close(Result<(), VPFSError>),
    /// usize is number of bytes read (sent right after the response)
    Read(Result<usize, VPFSError>),
    /// usize is number of bytes written (for `Write`) or the new size (for `Mutate`)
    Write(Result<usize, VPFSError>),
}

#[derive(Serialize,Deserialize)]
pub enum ConflictResolutionRequest {
    /// [local, remote]
    Versions(Vec<FileEntry>)
}

#[derive(Serialize,Deserialize)]
pub enum ConflictResolutionResponse {
    FinalVersion(FileEntry)
}
