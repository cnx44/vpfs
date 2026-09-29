//! The VPFS namespace: path -> file metadata. Flat for now; directories are
//! not modelled (a path is an opaque string).

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use vpfs::messages::FileEntry;

pub struct Namespace {
    files: HashMap<String, FileEntry>,
    file: PathBuf,
}

impl Namespace {
    /// Load `file_system` from `dir` if present: a sequence of (path, FileEntry) pairs.
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

    pub fn get(&self, path: &str) -> Option<&FileEntry> {
        self.files.get(path)
    }

    pub fn list(&self) -> Vec<FileEntry> {
        self.files.values().cloned().collect()
    }

    pub fn snapshot(&self) -> HashMap<String, FileEntry> {
        self.files.clone()
    }

    pub fn bind(&mut self, entry: FileEntry) {
        self.files.insert(entry.name.clone(), entry);
        self.save();
    }

    pub fn remove(&mut self, path: &str) {
        self.files.remove(path);
        self.save();
    }

    /// Add paths we do not know yet, keeping our own entries.
    pub fn merge_snapshot(&mut self, snapshot: HashMap<String, FileEntry>) {
        for (path, entry) in snapshot {
            self.files.entry(path).or_insert(entry);
        }
        self.save();
    }

    fn save(&self) {
        let mut out = Vec::new();
        for (path, entry) in &self.files {
            serde_bare::to_writer(&mut out, path).expect("Failed to encode file_system");
            serde_bare::to_writer(&mut out, entry).expect("Failed to encode file_system");
        }
        fs::write(&self.file, out).expect("Failed to write file_system");
    }
}
