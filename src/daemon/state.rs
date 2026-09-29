//! Everything a node knows, and the only code that changes it.
//!
//! `State` is owned by the executor (see executor.rs): nothing else holds a
//! reference to it, so every change to namespace, log, cache and blobs goes
//! through these methods. They are synchronous and never talk to the network;
//! what must be told to other nodes or to a human is collected in `Effects`
//! and handled by the caller (service.rs).
//!
//! The file state changes in exactly two ways:
//!   * a local operation (`create`, `write`, `resolve`) is recorded in the log
//!     and applied, and its entry is queued for broadcast;
//!   * a remote entry (`apply_remote`) is applied if it is newer than what
//!     we have, or becomes a conflict if it is concurrent.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::SystemTime;

use super::blobs::Blobs;
use super::cache::Cache;
use super::conflict::{designated_resolver, heuristics_for, Conflict};
use super::content::policy_for;
use super::logbook::{happens_before, Arrival, Logbook};
use super::namespace::Namespace;
use vpfs::messages::*;

/// Side effects produced while handling one task.
#[derive(Default, Debug)]
pub struct Effects {
    /// Entries recorded locally, to broadcast to the other nodes.
    pub events: Vec<LogEntry>,
    /// Conflicts this node must hand to a human.
    pub conflicts: Vec<Conflict>,
}

pub struct State {
    pub me: String,
    blobs: Blobs,
    namespace: Namespace,
    logbook: Logbook,
    cache: Cache,
    open_files: HashMap<i32, File>,
    /// Paths in an unresolved conflict -> the concurrent entries competing with the head.
    quarantine: HashMap<String, Vec<LogEntry>>,
    effects: Effects,
}

impl State {
    /// Load the node's state from `dir` (empty if the files are missing).
    pub fn open(dir: &Path, me: &str, max_cache_size: usize) -> State {
        State {
            me: me.to_string(),
            blobs: Blobs::new(dir.to_path_buf()),
            namespace: Namespace::open(dir),
            logbook: Logbook::open(dir, me),
            cache: Cache::open(dir, max_cache_size),
            open_files: HashMap::new(),
            quarantine: HashMap::new(),
            effects: Effects::default(),
        }
    }

    pub fn take_effects(&mut self) -> Effects {
        std::mem::take(&mut self.effects)
    }

    // ---- namespace queries ---------------------------------------------------

    pub fn find(&self, path: &str) -> Result<FileEntry, VPFSError> {
        self.namespace.get(path).cloned().ok_or(VPFSError::DoesNotExist)
    }

    /// Every entry: directories are not modelled, so `dir` is ignored.
    pub fn list(&self) -> Vec<FileEntry> {
        self.namespace.list()
    }

    pub fn snapshot(&self) -> HashMap<String, FileEntry> {
        self.namespace.snapshot()
    }

    pub fn merge_snapshot(&mut self, snapshot: HashMap<String, FileEntry>) {
        self.namespace.merge_snapshot(snapshot);
    }

    pub fn clock(&self) -> Clock {
        self.logbook.clock().clone()
    }

    pub fn log_since(&self, clock: &Clock) -> (Vec<LogEntry>, Clock) {
        (self.logbook.since(clock), self.logbook.clock().clone())
    }

    // ---- local operations ------------------------------------------------------

    /// Reserve an empty blob that a new file will point to.
    pub fn allocate(&mut self) -> String {
        self.blobs.create()
    }

    pub fn create(&mut self, entry: FileEntry) -> Result<FileEntry, VPFSError> {
        if let Some(existing) = self.namespace.get(&entry.name) {
            return Err(VPFSError::AlreadyExists(existing.clone()));
        }
        self.commit(LogOp::Create(entry.clone()));
        Ok(entry)
    }

    /// The single write path: every change to the content of a file this node
    /// owns ends here, whether it came from a local client or from a peer.
    /// Returns the size of the new content.
    pub fn write(&mut self, mut entry: FileEntry, mutations: &[Mutation]) -> Result<usize, VPFSError> {
        if self.quarantine.contains_key(&entry.name) {
            return Err(VPFSError::Conflicted(entry.name));
        }
        let current = if let Some(copy) = self.cache.peek(&entry.name).cloned() {
            // Writing our cached copy of a file owned by an unreachable node:
            // this node takes ownership, with the copy moved to a fresh blob.
            let content = self.blobs.read(&copy.uri).unwrap_or_default();
            self.cache.invalidate(&self.blobs, &entry.name);
            entry.uri = self.blobs.create();
            content
        } else if self.namespace.get(&entry.name) == Some(&entry) {
            self.blobs.read(&entry.uri).map_err(|_| VPFSError::DoesNotExist)?
        } else {
            return Err(VPFSError::DoesNotExist);
        };
        let content = policy_for(entry.kind).apply(current, mutations)?;
        self.blobs.write(&entry.uri, &content).map_err(|_| VPFSError::DoesNotExist)?;
        self.commit(LogOp::Modify(entry));
        Ok(content.len())
    }

    /// Content of a local blob (owned or cached); `None` if unchanged since `if_modified_since`.
    pub fn read(&self, uri: &str, if_modified_since: Option<SystemTime>) -> Result<Option<Vec<u8>>, VPFSError> {
        if if_modified_since.is_some() && self.blobs.modified(uri) == if_modified_since {
            return Ok(None);
        }
        self.blobs.read(uri).map(Some).map_err(|_| VPFSError::DoesNotExist)
    }

    // ---- cache -----------------------------------------------------------------

    /// Our copy of a remote file, as an entry readable locally, and when it was stored.
    pub fn cached(&self, path: &str) -> Option<(FileEntry, Option<SystemTime>)> {
        let copy = self.cache.peek(path)?;
        let kind = self.namespace.get(path).map(|e| e.kind).unwrap_or_default();
        let entry = FileEntry { owner: self.me.clone(), uri: copy.uri.clone(), name: path.to_string(), kind };
        Some((entry, self.blobs.modified(&copy.uri)))
    }

    pub fn cache_store(&mut self, file: &FileEntry, data: &[u8]) {
        self.cache.store(&self.blobs, file, data);
    }

    // ---- open files (fd api) ---------------------------------------------------

    pub fn open_fd(&mut self, uri: &str) -> Result<i32, VPFSError> {
        let file = self.blobs.open(uri).map_err(|_| VPFSError::DoesNotExist)?;
        let fd = file.as_raw_fd();
        self.open_files.insert(fd, file);
        Ok(fd)
    }

    /// Up to `len` bytes; empty at end of file.
    pub fn read_fd(&mut self, fd: i32, len: usize) -> Result<Vec<u8>, VPFSError> {
        let file = self.open_files.get_mut(&fd).ok_or(VPFSError::FileNotOpen)?;
        let mut buf = vec![0u8; len];
        let n = file.read(&mut buf).map_err(|_| VPFSError::FileNotOpen)?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Next line including its '\n' (the tail without it at end of file; empty after).
    pub fn read_line_fd(&mut self, fd: i32) -> Result<Vec<u8>, VPFSError> {
        let file = self.open_files.get_mut(&fd).ok_or(VPFSError::FileNotOpen)?;
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        // Byte by byte, so the file offset stays right after the line.
        while file.read(&mut byte).map_err(|_| VPFSError::FileNotOpen)? == 1 {
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        Ok(line)
    }

    pub fn close_fd(&mut self, fd: i32) -> Result<(), VPFSError> {
        self.open_files.remove(&fd).map(|_| ()).ok_or(VPFSError::FileNotOpen)
    }

    // ---- replication -----------------------------------------------------------

    /// Apply entries recorded by other nodes.
    pub fn apply_remote(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            match self.arrival(&entry) {
                Arrival::Known => {}
                Arrival::Stale => self.logbook.merge(entry, false),
                Arrival::Newer => {
                    self.logbook.merge(entry.clone(), true);
                    self.quarantine.remove(entry.op.path());
                    self.apply(&entry.op);
                }
                Arrival::Concurrent(head) => {
                    self.logbook.merge(entry.clone(), false);
                    self.on_conflict(Conflict { path: entry.op.path().to_string(), local: head, remote: entry });
                }
            }
        }
    }

    /// A human chose `chosen` for a quarantined path. Ignored if the conflict
    /// was meanwhile settled by a newer entry.
    pub fn resolve(&mut self, path: &str, chosen: FileEntry) {
        if self.quarantine.remove(path).is_some() {
            self.commit(LogOp::Modify(chosen));
        }
    }

    /// Like `Logbook::classify`, but a quarantined path is only superseded by
    /// an entry newer than *all* the competing versions.
    fn arrival(&self, entry: &LogEntry) -> Arrival {
        let arrival = self.logbook.classify(entry);
        if arrival == Arrival::Newer {
            let competing = self.quarantine.get(entry.op.path()).into_iter().flatten();
            if let Some(other) = competing.into_iter().find(|c| !happens_before(&c.clock, &entry.clock)) {
                return Arrival::Concurrent(other.clone());
            }
        }
        arrival
    }

    fn on_conflict(&mut self, conflict: Conflict) {
        println!("Conflict (concurrent) for file: {}", conflict.path);
        if let Some(competing) = self.quarantine.get_mut(&conflict.path) {
            competing.push(conflict.remote);
            return;
        }
        if designated_resolver(&conflict) == self.me {
            let kind = conflict.local.op.file().kind;
            if let Some(chosen) = heuristics_for(kind).iter().find_map(|h| h.resolve(&conflict)) {
                self.commit(LogOp::Modify(chosen));
                return;
            }
            self.effects.conflicts.push(conflict.clone());
        }
        self.quarantine.insert(conflict.path, vec![conflict.remote]);
    }

    /// Record a local operation, apply it, and queue it for broadcast.
    fn commit(&mut self, op: LogOp) {
        let entry = self.logbook.record(op);
        self.apply(&entry.op);
        self.effects.events.push(entry);
    }

    /// Reflect an operation in the namespace. A cached copy of a different blob is stale.
    fn apply(&mut self, op: &LogOp) {
        let file = op.file();
        if self.cache.peek(&file.name).is_some_and(|c| c.uri != file.uri || matches!(op, LogOp::Remove(_))) {
            self.cache.invalidate(&self.blobs, &file.name);
        }
        match op {
            LogOp::Create(f) | LogOp::Modify(f) => self.namespace.bind(f.clone()),
            LogOp::Remove(f) => self.namespace.remove(&f.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A node's state in a fresh directory, removed on drop.
    struct Node {
        state: State,
        dir: PathBuf,
    }

    impl Node {
        fn new(name: &str) -> Node {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!("vpfs-state-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
            std::fs::create_dir_all(&dir).unwrap();
            Node { state: State::open(&dir, name, 1 << 16), dir }
        }

        fn place(&mut self, path: &str, kind: FileKind) -> FileEntry {
            let uri = self.state.allocate();
            let entry = FileEntry { owner: self.state.me.clone(), uri, name: path.into(), kind };
            self.state.create(entry).unwrap()
        }

        /// Deliver everything `self` recorded since the last call to `to`.
        fn send_to(&mut self, to: &mut Node) -> Effects {
            to.state.apply_remote(self.state.take_effects().events);
            to.state.take_effects()
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn write_is_validated_before_anything_changes() {
        let mut a = Node::new("a");
        let f = a.place("f", FileKind::Blob);
        let bogus = FileEntry { uri: "deadbeef".into(), ..f.clone() };
        assert_eq!(a.state.write(bogus, &[Mutation::Replace(b"x".to_vec())]), Err(VPFSError::DoesNotExist));
        let escape = FileEntry { uri: "../x".into(), ..f.clone() };
        assert_eq!(a.state.write(escape, &[Mutation::Replace(b"x".to_vec())]), Err(VPFSError::DoesNotExist));
        assert_eq!(a.state.find("f"), Ok(f.clone()));
        assert_eq!(a.state.write(f.clone(), &[Mutation::Replace(b"ok".to_vec())]), Ok(2));
        assert_eq!(a.state.read(&f.uri, None), Ok(Some(b"ok".to_vec())));
    }

    #[test]
    fn write_uses_the_kind_policy_and_rejected_mutations_change_nothing() {
        let mut a = Node::new("a");
        let t = a.place("t", FileKind::Text);
        a.state.write(t.clone(), &[Mutation::Replace(b"hello".to_vec())]).unwrap();
        a.state.take_effects();
        a.state.write(t.clone(), &[Mutation::InsertAt { pos: 5, data: b" world".to_vec() }]).unwrap();
        assert_eq!(a.state.read(&t.uri, None), Ok(Some(b"hello world".to_vec())));
        assert_eq!(a.state.take_effects().events.len(), 1, "one Modify per write");

        let b = a.place("b", FileKind::Blob);
        a.state.take_effects();
        let insert = Mutation::InsertAt { pos: 0, data: b"x".to_vec() };
        assert_eq!(a.state.write(b, &[insert]), Err(VPFSError::Unsupported(FileKind::Blob)));
        assert!(a.state.take_effects().events.is_empty(), "rejected write is not logged");
    }

    #[test]
    fn remote_entries_arriving_out_of_order_keep_the_newest() {
        let (mut a, mut b) = (Node::new("a"), Node::new("b"));
        let f = a.place("f", FileKind::Blob);
        let create = a.state.take_effects().events;
        let moved = FileEntry { uri: a.state.allocate(), ..f.clone() };
        a.state.commit(LogOp::Modify(moved.clone()));
        let modify = a.state.take_effects().events;

        b.state.apply_remote(modify);
        b.state.apply_remote(create);
        assert_eq!(b.state.find("f"), Ok(moved), "the late Create is stale");
        assert!(b.state.take_effects().conflicts.is_empty());
        assert_eq!(b.state.clock(), Clock::from([("a".into(), 2), ("b".into(), 0)]));
    }

    #[test]
    fn concurrent_changes_quarantine_until_the_designated_node_resolves() {
        let (mut a, mut b) = (Node::new("a"), Node::new("b"));
        let fa = a.place("f", FileKind::Blob);
        let fb = b.place("f", FileKind::Blob);
        let (from_a, from_b) = (a.state.take_effects().events, b.state.take_effects().events);

        // "b" > "a": b must resolve, and asks a human.
        b.state.apply_remote(from_a);
        let effects = b.state.take_effects();
        assert_eq!(effects.conflicts.len(), 1);
        assert_eq!((effects.conflicts[0].local.op.file(), effects.conflicts[0].remote.op.file()), (&fb, &fa));
        assert_eq!(b.state.write(fb.clone(), &[Mutation::Replace(vec![])]), Err(VPFSError::Conflicted("f".into())));

        // a is not designated: it quarantines and waits.
        a.state.apply_remote(from_b);
        assert!(a.state.take_effects().conflicts.is_empty());
        assert_eq!(a.state.write(fa.clone(), &[Mutation::Replace(vec![])]), Err(VPFSError::Conflicted("f".into())));

        b.state.resolve("f", fa.clone());
        assert_eq!(b.state.find("f"), Ok(fa.clone()));
        b.send_to(&mut a);
        assert_eq!(a.state.find("f"), Ok(fa.clone()));
        assert_eq!(a.state.write(fa, &[Mutation::Replace(b"free".to_vec())]), Ok(4), "quarantine lifted");
    }

    #[test]
    fn heuristics_settle_conflicts_without_a_human() {
        let (mut a, mut b) = (Node::new("a"), Node::new("b"));
        let f = a.place("f", FileKind::Blob);
        a.send_to(&mut b);
        // a and b record the same change concurrently.
        a.state.commit(LogOp::Modify(f.clone()));
        b.state.commit(LogOp::Modify(f.clone()));
        let (from_a, from_b) = (a.state.take_effects().events, b.state.take_effects().events);

        a.state.apply_remote(from_b);
        assert!(a.state.take_effects().conflicts.is_empty(), "a is not designated: waits");
        assert_eq!(a.state.write(f.clone(), &[]), Err(VPFSError::Conflicted("f".into())));

        b.state.apply_remote(from_a);
        let effects = b.state.take_effects();
        assert!(effects.conflicts.is_empty(), "SameVersion decided, no human");
        assert_eq!(effects.events.len(), 1, "resolution entry to broadcast");
        a.state.apply_remote(effects.events);
        assert_eq!(a.state.write(f, &[]), Ok(0), "quarantine lifted by the resolution");
    }

    #[test]
    fn restart_keeps_namespace_log_and_heads() {
        let mut a = Node::new("a");
        let f = a.place("f", FileKind::Text);
        let old = a.state.take_effects().events;
        a.state.write(f.clone(), &[Mutation::Replace(b"v2".to_vec())]).unwrap();
        a.state = State::open(&a.dir, "a", 1 << 16);
        assert_eq!(a.state.find("f"), Ok(f.clone()));
        assert_eq!(a.state.clock(), Clock::from([("a".into(), 2)]));
        a.state.apply_remote(old);
        assert_eq!(a.state.logbook.since(&Clock::new()).len(), 2, "known entry not duplicated");
    }
}
