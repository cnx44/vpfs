//! Local copies of files owned by other nodes, LRU-evicted by total size.
//! A cached copy is stored as a blob under the owner's uri.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use lru::LruCache;

use super::blobs::Blobs;
use vpfs::messages::{CacheEntry, FileEntry};

pub struct Cache {
    /// Path -> local copy. Recency is only updated by `store` (`peek` leaves it alone),
    /// so a copy is "used" when it is (re)fetched, not when it is served.
    lru: LruCache<String, CacheEntry>,
    /// Bytes taken by all cached blobs. Persisted, not recomputed at startup.
    used: usize,
    /// Limit from `--cache-size`.
    max: usize,
    /// `./files/cache`.
    file: PathBuf,
}

impl Cache {
    /// Load `cache` from `dir` if present: used bytes, then (path, CacheEntry) pairs from most to least recent.
    /// Called once by `State::open`.
    pub fn open(dir: &Path, max: usize) -> Cache {
        let mut cache = Cache { lru: LruCache::unbounded(), used: 0, max, file: dir.join("cache") };
        if let Ok(bytes) = fs::read(&cache.file) {
            let mut cur = Cursor::new(bytes);
            cache.used = serde_bare::from_reader(&mut cur).expect("Corrupt cache file");
            let mut entries = Vec::new();
            while let Ok(path) = serde_bare::from_reader::<_, String>(&mut cur) {
                entries.push((path, serde_bare::from_reader(&mut cur).expect("Corrupt cache file")));
            }
            for (path, entry) in entries.into_iter().rev() {
                cache.lru.put(path, entry);
            }
        }
        cache
    }

    /// Our copy of `path`, if any, without touching recency. Callers in `State`:
    /// `cached` (serve the copy when the owner is unreachable, or get its time
    /// for `if_modified_since`), `write` (writing a copy takes ownership) and
    /// `apply` (drop copies made stale by a new entry).
    pub fn peek(&self, path: &str) -> Option<&CacheEntry> {
        self.lru.peek(path)
    }

    /// Store a fresh copy of `file`, then evict least recently used copies until under the limit.
    /// Called by `Service::read` after the owner sent new content. The blob is
    /// written in our directory under the owner's uri. A copy bigger than `max`
    /// is evicted right away. If the blob write fails nothing changes.
    pub fn store(&mut self, blobs: &Blobs, file: &FileEntry, data: &[u8]) {
        let old_size = self.lru.peek(&file.name).map(|e| blobs.size(&e.uri)).unwrap_or(0);
        if blobs.write(&file.uri, data).is_err() {
            return;
        }
        self.lru.put(file.name.clone(), CacheEntry { uri: file.uri.clone() });
        self.used = self.used.saturating_sub(old_size) + data.len();
        while self.used > self.max {
            let Some((_, evicted)) = self.lru.pop_lru() else { break };
            self.used = self.used.saturating_sub(blobs.size(&evicted.uri));
            blobs.remove(&evicted.uri);
        }
        self.save();
    }

    /// Drop the copy of `path`, if any, and delete its blob. Called when a newer
    /// entry points to another blob or removes the file, and when this node
    /// takes ownership of the copy.
    pub fn invalidate(&mut self, blobs: &Blobs, path: &str) {
        if let Some(evicted) = self.lru.pop(path) {
            self.used = self.used.saturating_sub(blobs.size(&evicted.uri));
            blobs.remove(&evicted.uri);
            self.save();
        }
    }

    /// Rewrite the whole `cache` file: `used`, then pairs from most to least recent.
    /// Not atomic.
    fn save(&self) {
        let mut out = serde_bare::to_vec(&self.used).expect("Failed to encode cache");
        for (path, entry) in self.lru.iter() {
            serde_bare::to_writer(&mut out, path).expect("Failed to encode cache");
            serde_bare::to_writer(&mut out, entry).expect("Failed to encode cache");
        }
        fs::write(&self.file, out).expect("Failed to write cache");
    }
}
