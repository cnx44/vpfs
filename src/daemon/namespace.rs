//! The VPFS namespace: path -> file metadata. Flat for now; directories are
//! not modelled (a path is an opaque string).

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use vpfs::messages::FileEntry;

pub struct Namespace {
    /// Full VPFS path -> file metadata. The key always equals `FileEntry.name`.
    /// Holds only the current head of each path; history lives in the logbook.
    files: HashMap<String, FileEntry>,
    
    /// `./files/file_system`, next to the blobs (no clash: blob names are hex).
    file: PathBuf,
}

impl Namespace {
    /// Called once by `State::open` at startup. Reads a sequence of
    /// (path, FileEntry) pairs; empty if the file is missing, panics if truncated.
    /// The namespace is NOT rebuilt from the log: this file is the local source of truth.
    pub fn open(dir: &Path) -> Namespace {
        let file = dir.join("file_system");
        let mut files = HashMap::new();
        if let Ok(bytes) = fs::read(&file) {
            let mut cur = Cursor::new(bytes);
            while let Ok(path) = serde_bare::from_reader::<_, String>(&mut cur) {
                let entry: FileEntry = serde_bare::from_reader(&mut cur).expect("Corrupt file_system file");
                files.insert(path, entry);
            }
        }
        Namespace { files, file }
    }

    /// Exact lookup, no normalization ("a/b" != "/a/b"). Used by `State` for:
    /// - `find`: resolve a client path;
    /// - `create`: uniqueness check (`AlreadyExists`);
    /// - `write`: the client's entry must match exactly (owner, uri, name, kind),
    ///   otherwise `DoesNotExist`. This rejects stale or forged entries;
    /// - `cached`: get the `kind` of a file we only hold in cache.
    pub fn get(&self, path: &str) -> Option<&FileEntry> {
        self.files.get(path)
    }


    /// Every entry, in arbitrary order. Backs `ls`: the gateway drops the
    /// directory argument, so `ls` always returns the whole file system.
    pub fn list(&self) -> Vec<FileEntry> {
        self.files.values().cloned().collect()
    }

    /// Full copy of the map, sent to a peer on `DaemonRequest::Snapshot`.
    /// Its type is part of the wire protocol (`DaemonResponse::Snapshot`).
    pub fn snapshot(&self) -> HashMap<String, FileEntry> {
        self.files.clone()
    }

    /// Upsert keyed by `entry.name`, then save. Only called from `State::apply`
    /// for `Create` and `Modify`, local or remote. Uniqueness is checked earlier
    /// (`State::create`, local ops only). Cannot move: an entry with a new
    /// `name` adds a second key and leaves the old one.
    pub fn bind(&mut self, entry: FileEntry) {
        self.files.insert(entry.name.clone(), entry);
        self.save();
    }


    /// Drop the path, then save. The blob stays on disk. Only called from
    /// `State::apply` for `LogOp::Remove`, which nothing produces today
    /// (there is no delete request).
    pub fn remove(&mut self, path: &str) {
        self.files.remove(path);
        self.save();
    }

    /// Add the paths we don't have; on a clash ours wins (no clock comparison).
    /// Only used by `bootstrap_from` when a brand-new node joins, so the local
    /// map is empty. It bypasses the logbook; the `sync_with` call right after
    /// replays the log and fixes the heads.
    pub fn merge_snapshot(&mut self, snapshot: HashMap<String, FileEntry>) {
        for (path, entry) in snapshot {
            self.files.entry(path).or_insert(entry);
        }
        self.save();
    }

    /// Rewrites the whole file on every change. Not atomic, unlike
    /// `Blobs::write`: a crash mid-write can leave a truncated file, and
    /// `open` then panics on the next start.
    fn save(&self) {
        let mut out = Vec::new();
        for (path, entry) in &self.files {
            serde_bare::to_writer(&mut out, path).expect("Failed to encode file_system");
            serde_bare::to_writer(&mut out, entry).expect("Failed to encode file_system");
        }
        fs::write(&self.file, out).expect("Failed to write file_system");
    }
}
