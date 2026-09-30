//! The operations a node offers, to its clients and to its peers.
//!
//! This layer holds no state. For each operation it decides *where* it runs:
//! on this node's state (through the executor) when this node owns the file,
//! otherwise on the owner (through the transport), falling back to the local
//! cached copy when the owner is unreachable. After every state task it
//! delivers the task's effects: events to the peers, conflicts to the human.

use std::sync::mpsc;
use std::sync::Arc;

use super::conflict::Conflict;
use super::executor::Executor;
use super::state::State;
use super::transport::Transport;
use vpfs::messages::*;

pub struct Service {
    /// This node's name.
    pub me: String,
    /// The only way to reach `State`.
    executor: Executor,
    /// Requests and broadcasts to other nodes.
    transport: Arc<dyn Transport>,
    /// Conflicts for the human resolver thread (human.rs).
    human: mpsc::Sender<Conflict>,
}

fn unexpected<T>(response: DaemonResponse) -> Result<T, VPFSError> {
    Err(VPFSError::Other(format!("unexpected response from peer: {response:?}")))
}

impl Service {
    pub fn new(me: String, executor: Executor, transport: Arc<dyn Transport>, human: mpsc::Sender<Conflict>) -> Service {
        Service { me, executor, transport, human }
    }

    /// Run a task on the state, then deliver its effects.
    /// The broadcast is awaited, so the caller answers its client only after the
    /// peers acked or timed out (see `BROADCAST_TIMEOUT` in transport.rs).
    async fn exec<R: Send + 'static>(&self, f: impl FnOnce(&mut State) -> R + Send + 'static) -> R {
        let (result, effects) = self.executor.run(f).await;
        if !effects.events.is_empty() {
            self.transport.broadcast(effects.events).await;
        }
        for conflict in effects.conflicts {
            let _ = self.human.send(conflict);
        }
        result
    }

    /// The error to report when `path`'s owner is unreachable: our cached copy if we have one.
    async fn unreachable(&self, path: String) -> VPFSError {
        match self.exec(move |s| s.cached(&path)).await {
            Some((copy, _)) => VPFSError::OnlyInCache(copy),
            None => VPFSError::NotAccessible,
        }
    }

    // ---- namespace ---------------------------------------------------------------

    /// Resolve `path` in the local namespace. Never uses the network: every node
    /// holds the whole namespace.
    pub async fn find(&self, path: String) -> Result<FileEntry, VPFSError> {
        self.exec(move |s| s.find(&path)).await
    }

    /// The whole namespace (there is no directory filter).
    pub async fn list(&self) -> Vec<FileEntry> {
        self.exec(|s| s.list()).await
    }

    /// Create a file at `path` whose content lives on `owner` (possibly another node):
    /// check the path is free, allocate an empty blob on the owner (`Allocate` if
    /// remote), then record the `Create` here and broadcast it. The entry is created
    /// by the node the client talks to, not by the owner. If the final `create`
    /// fails (path taken meanwhile), the allocated blob is left unused.
    pub async fn place(&self, path: String, owner: String, kind: FileKind) -> Result<FileEntry, VPFSError> {
        let p = path.clone();
        if let Ok(existing) = self.exec(move |s| s.find(&p)).await {
            return Err(VPFSError::AlreadyExists(existing));
        }
        let uri = if owner == self.me {
            self.exec(|s| s.allocate()).await
        } else {
            match self.transport.fetch(&owner, DaemonRequest::Allocate).await? {
                DaemonResponse::Allocated(uri) => uri,
                other => return unexpected(other),
            }
        };
        let entry = FileEntry { owner, uri, name: path, kind };
        self.exec(move |s| s.create(entry)).await
    }

    // ---- content -----------------------------------------------------------------

    /// Whole content of `file`. Owned files are read from disk. Remote ones are
    /// fetched from the owner (sending our copy's time as `if_modified_since`) and
    /// cached. If the owner is unreachable: `OnlyInCache(copy)`, which the client
    /// can read or write locally.
    pub async fn read(&self, file: FileEntry) -> Result<Vec<u8>, VPFSError> {
        if file.owner == self.me {
            return self.exec(move |s| s.read(&file.uri, None)).await.map(Option::unwrap_or_default);
        }
        let name = file.name.clone();
        let (cached_uri, cached_at) = match self.exec(move |s| s.cached(&name)).await {
            Some((copy, at)) => (Some(copy.uri), at),
            None => (None, None),
        };
        let req = DaemonRequest::Read { uri: file.uri.clone(), if_modified_since: cached_at };
        match self.transport.fetch(&file.owner, req).await {
            Ok(DaemonResponse::Read(Ok(Some(data)))) => {
                let stored = data.clone();
                self.exec(move |s| s.cache_store(&file, &stored)).await;
                Ok(data)
            }
            Ok(DaemonResponse::Read(Ok(None))) => {
                let uri = cached_uri.expect("NotModified response but no cache entry");
                self.exec(move |s| s.read(&uri, None)).await.map(Option::unwrap_or_default)
            }
            Ok(DaemonResponse::Read(Err(e))) => Err(e),
            Ok(other) => unexpected(other),
            Err(_) => Err(self.unreachable(file.name).await),
        }
    }

    /// Change the content of `file`; runs on the owner. Returns the new size.
    /// If the owner is unreachable: `OnlyInCache(copy)`. Writing that copy (its
    /// owner is this node) makes this node the owner, see `State::write`.
    pub async fn write(&self, file: FileEntry, mutations: Vec<Mutation>) -> Result<usize, VPFSError> {
        if file.owner == self.me {
            return self.exec(move |s| s.write(file, &mutations)).await;
        }
        let owner = file.owner.clone();
        let name = file.name.clone();
        match self.transport.fetch(&owner, DaemonRequest::Write { entry: file, mutations }).await {
            Ok(DaemonResponse::Write(result)) => result,
            Ok(other) => unexpected(other),
            Err(_) => Err(self.unreachable(name).await),
        }
    }

    // ---- fd api: the fd lives on the owner ------------------------------------------

    /// Open `file` on its owner; returns the owner's fd, which the client pairs
    /// with the owner's name. No cache fallback.
    pub async fn open(&self, file: FileEntry) -> Result<i32, VPFSError> {
        if file.owner == self.me {
            return self.exec(move |s| s.open_fd(&file.uri)).await;
        }
        match self.transport.fetch(&file.owner, DaemonRequest::Open(file.uri)).await? {
            DaemonResponse::Open(result) => result,
            other => unexpected(other),
        }
    }

    /// Up to `len` bytes from an fd opened on `owner`; empty at end of file.
    pub async fn read_fd(&self, owner: String, fd: i32, len: usize) -> Result<Vec<u8>, VPFSError> {
        if owner == self.me {
            return self.exec(move |s| s.read_fd(fd, len)).await;
        }
        match self.transport.fetch(&owner, DaemonRequest::ReadFd(fd, len)).await? {
            DaemonResponse::Data(result) => result,
            other => unexpected(other),
        }
    }

    /// Next line from an fd opened on `owner`.
    pub async fn read_line_fd(&self, owner: String, fd: i32) -> Result<Vec<u8>, VPFSError> {
        if owner == self.me {
            return self.exec(move |s| s.read_line_fd(fd)).await;
        }
        match self.transport.fetch(&owner, DaemonRequest::ReadLineFd(fd)).await? {
            DaemonResponse::Data(result) => result,
            other => unexpected(other),
        }
    }

    /// Close an fd opened on `owner`.
    pub async fn close(&self, owner: String, fd: i32) -> Result<(), VPFSError> {
        if owner == self.me {
            return self.exec(move |s| s.close_fd(fd)).await;
        }
        match self.transport.fetch(&owner, DaemonRequest::Close(fd)).await? {
            DaemonResponse::Close(result) => result,
            other => unexpected(other),
        }
    }

    // ---- replication -------------------------------------------------------------

    /// Answer a request from another node (peer_handler.rs).
    /// `Write` goes through `self.write`, so it is forwarded if we are not the owner.
    pub async fn serve_peer(&self, req: DaemonRequest) -> DaemonResponse {
        match req {
            DaemonRequest::Allocate => DaemonResponse::Allocated(self.exec(|s| s.allocate()).await),
            DaemonRequest::Snapshot => DaemonResponse::Snapshot(self.exec(|s| s.snapshot()).await),
            DaemonRequest::LogSince(clock) => {
                let (entries, clock) = self.exec(move |s| s.log_since(&clock)).await;
                DaemonResponse::Log(entries, clock)
            }
            DaemonRequest::Read { uri, if_modified_since } => {
                DaemonResponse::Read(self.exec(move |s| s.read(&uri, if_modified_since)).await)
            }
            DaemonRequest::Write { entry, mutations } => DaemonResponse::Write(self.write(entry, mutations).await),
            DaemonRequest::Open(uri) => DaemonResponse::Open(self.exec(move |s| s.open_fd(&uri)).await),
            DaemonRequest::ReadFd(fd, len) => DaemonResponse::Data(self.exec(move |s| s.read_fd(fd, len)).await),
            DaemonRequest::ReadLineFd(fd) => DaemonResponse::Data(self.exec(move |s| s.read_line_fd(fd)).await),
            DaemonRequest::Close(fd) => DaemonResponse::Close(self.exec(move |s| s.close_fd(fd)).await),
            DaemonRequest::Events(entries) => {
                self.exec(move |s| s.apply_remote(entries)).await;
                DaemonResponse::Ack
            }
        }
    }

    /// First join of a new node: take the namespace as `node` sees it.
    /// Called in `main` only when `./files` was just created.
    pub async fn bootstrap_from(&self, node: &str) {
        match self.transport.fetch(node, DaemonRequest::Snapshot).await {
            Ok(DaemonResponse::Snapshot(snapshot)) => self.exec(move |s| s.merge_snapshot(snapshot)).await,
            other => eprintln!("Could not get file system from {node}: {other:?}"),
        }
    }

    /// Exchange the log entries each side missed while apart. Our side goes
    /// through the same path as live events; conflicts are resolved later.
    /// Called in `main` on every start with `--remote-id`, only with that node.
    pub async fn sync_with(&self, node: &str) {
        let ours = self.exec(|s| s.clock()).await;
        let (theirs, their_clock) = match self.transport.fetch(node, DaemonRequest::LogSince(ours)).await {
            Ok(DaemonResponse::Log(entries, clock)) => (entries, clock),
            other => return eprintln!("Could not get log from {node}: {other:?}"),
        };
        self.exec(move |s| s.apply_remote(theirs)).await;
        let missing = self.exec(move |s| s.log_since(&their_clock).0).await;
        if let Err(e) = self.transport.fetch(node, DaemonRequest::Events(missing)).await {
            eprintln!("Could not push log to {node}: {e:?}");
        }
    }

    /// A human settled a conflict (called by human.rs).
    pub async fn resolve(&self, path: String, chosen: FileEntry) {
        self.exec(move |s| s.resolve(&path, chosen)).await
    }
}
