//! Local copies of files owned by other nodes, LRU-evicted by total size.
//! A cached copy is stored as a blob under the owner's uri.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use lru::LruCache;

use super::blobs::Blobs;
use vpfs::messages::{CacheEntry, FileEntry};

pub struct Cache {
    lru: LruCache<String, CacheEntry>, // path -> copy
    used: usize,
    max: usize,
    file: PathBuf,
}

impl Cache {
    /// Load `cache` from `dir` if present: used bytes, then (path, CacheEntry) pairs from most to least recent.
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

    pub fn peek(&self, path: &str) -> Option<&CacheEntry> {
        self.lru.peek(path)
    }

    /// Store a fresh copy of `file`, then evict least recently used copies until under the limit.
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

    /// Drop the copy of `path`, if any.
    pub fn invalidate(&mut self, blobs: &Blobs, path: &str) {
        if let Some(evicted) = self.lru.pop(path) {
            self.used = self.used.saturating_sub(blobs.size(&evicted.uri));
            blobs.remove(&evicted.uri);
            self.save();
        }
    }

    fn save(&self) {
        let mut out = serde_bare::to_vec(&self.used).expect("Failed to encode cache");
        for (path, entry) in self.lru.iter() {
            serde_bare::to_writer(&mut out, path).expect("Failed to encode cache");
            serde_bare::to_writer(&mut out, entry).expect("Failed to encode cache");
        }
        fs::write(&self.file, out).expect("Failed to write cache");
    }
}
