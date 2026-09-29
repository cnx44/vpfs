use serde::{Deserialize, Serialize};
use iroh::PublicKey;

use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Serialize,Deserialize,Clone,Hash,Debug,PartialEq,Eq)]
pub struct VPFSNode {
    pub name: String,
    pub endpoint_id: PublicKey
}

#[derive(Debug,Clone,Eq,Hash,PartialEq,Serialize,Deserialize)]
pub struct FileEntry {
    pub owner: String,
    pub uri: String,
    pub name: String,
}

#[derive(Serialize,Deserialize,Clone,Eq,PartialEq,Debug)]
pub enum LogOp {
    Create(FileEntry),
    Modify(FileEntry),
    Remove(FileEntry),
}

#[derive(Serialize,Deserialize,Clone,Debug,Eq,PartialEq)]
pub struct LogEntry {
    pub clock: HashMap<String, u64>,
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

#[derive(Serialize,Deserialize,Debug,Eq,PartialEq)]
pub enum VPFSError {
    OnlyInCache(FileEntry),
    // CacheNeededForTraversal(DirectoryEntry),
    NotModified,
    DoesNotExist,  // We can verify that the file does not exist
    NotFound,      // We can not find the file. File may or may not exist
    NotAccessible, // We can not access the node need to complete request
    NotADirectory,
    AlreadyExists(FileEntry),
    FileNotOpen,
    Other(String),
}

/// Requests to a daemon from a daemon
#[derive(Serialize,Deserialize)]
pub enum DaemonRequest {
    Place,
    FileSystem,
    UpdatedFiles(Vec<FileEntry>),
    AddEntry(String, FileEntry),
    Open(String),
    Read(String, Option<SystemTime>),
    ReadFd(i32, usize),
    ReadLineFd(i32),
    Close(i32),
    Write(String),
    Remove(String),
    /// to request for endpoint_id of node given node_name
    AddressFor(String),
    /// Request log entries newer than the given vector clock
    LogSince(HashMap<String, u64>),
    /// Push log entries to remote for merging
    UpdateLog(Vec<LogEntry>),
    /// Tell remote to drop all log entries for `path` and adopt the resolved entry
    ResolveConflict(String, LogEntry), // (path, resolved entry)
}

/// Responses to a daemon from a daemon for requests
#[derive(Serialize,Deserialize)]
pub enum DaemonResponse {
    Place(String),
    FileSystem(HashMap<String, FileEntry>),
    AddEntry(Result<(), VPFSError>),
    Open(Result<i32, VPFSError>),
    Read(Result<(), VPFSError>),
    ReadFd(Result<(), VPFSError>),
    ReadLineFd(Result<(), VPFSError>),
    Close(Result<(), VPFSError>),
    /// usize is number of bytes written
    Write(Result<usize, VPFSError>),
    Remove(Result<(), VPFSError>),
    /// `endpoint_id` for node given name
    AddressFor(Option<PublicKey>),
    /// Partial log entries + remote node's current vector clock
    Log(Vec<LogEntry>, HashMap<String, u64>),
    UpdateLog,
    ResolveConflict,
}

/// Requests from client to daemon
#[derive(Serialize,Deserialize)]
pub enum ClientRequest {
    ListFiles(String),
    Find(String),
    /// parent dir uri, name
    Place(String, String),
    /// parent dir uri, name
    Open(FileEntry),
    ReadFd(FileEntry, i32, usize),
    ReadLineFd(FileEntry, i32),
    Close(String, i32),
    Read(FileEntry),
    /// `FileEntry`, number of bytes to write
    Write(FileEntry, usize),
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
    /// usize is number of bytes read
    Read(Result<usize, VPFSError>),
    /// usize is number of bytes written
    Write(Result<usize, VPFSError>),
}

#[derive(Serialize,Deserialize)]
pub enum ConflictResolutionRequest {
    Versions(Vec<FileEntry>)
}

#[derive(Serialize,Deserialize)]
pub enum ConflictResolutionResponse {
    FinalVersion(FileEntry)
}