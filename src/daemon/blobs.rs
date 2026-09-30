//! File contents on the local disk: one file per blob inside the node directory,
//! named by its uri. Knows nothing about paths, owners or kinds.

use std::fs::{self, File};
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

use rand::Rng;

pub struct Blobs {
    /// The node directory (`./files`). It also holds the daemon's state files
    /// (`log`, `vector_clock`, `file_system`, `cache`); they cannot clash with
    /// blobs because blob names are hex only (see `path`).
    dir: PathBuf,
}

impl Blobs {
    /// Called once by `State::open`. Does not touch the disk.
    pub fn new(dir: PathBuf) -> Blobs {
        Blobs { dir }
    }

    /// Uris are the hex names produced by `create`. Anything else could point
    /// outside the node directory (or at the daemon's own state files), so it is refused.
    fn path(&self, uri: &str) -> io::Result<PathBuf> {
        let valid = !uri.is_empty() && uri.len() <= 16 && uri.chars().all(|c| c.is_ascii_hexdigit());
        if valid {
            Ok(self.dir.join(uri))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, format!("invalid blob uri {uri:?}")))
        }
    }

    /// Create an empty blob with a fresh random uri.
    /// Callers: `State::allocate` (a new file placed on this node, possibly on
    /// behalf of a peer via `DaemonRequest::Allocate`) and `State::write` (taking
    /// ownership of a cached copy). The file exists on disk from now on, even if
    /// no namespace entry ever points to it (e.g. the following `create` fails).
    pub fn create(&self) -> String {
        let mut rng = rand::rng();
        loop {
            let uri = format!("{:x}", rng.random::<u64>());
            match File::create_new(self.dir.join(&uri)) {
                Ok(_) => return uri,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("Could not create blob: {e}"),
            }
        }
    }

    /// Whole content. Errors if the uri is invalid or the blob is missing.
    pub fn read(&self, uri: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(uri)?)
    }

    /// Replace the content atomically (write aside, then rename): a failed
    /// write never leaves a half-written blob.
    /// Creates the blob if missing: the cache relies on this to store a copy
    /// under the owner's uri.
    pub fn write(&self, uri: &str, data: &[u8]) -> io::Result<()> {
        let path = self.path(uri)?;
        let tmp = self.dir.join(format!(".{uri}.tmp"));
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)
    }

    /// Best effort: invalid uris and missing files are ignored.
    /// Only the cache removes blobs; a file removed from the namespace keeps its blob.
    pub fn remove(&self, uri: &str) {
        if let Ok(path) = self.path(uri) {
            let _ = fs::remove_file(path);
        }
    }

    /// Read-only handle for the fd api (`State::open_fd`); its offset is the read position.
    pub fn open(&self, uri: &str) -> io::Result<File> {
        File::open(self.path(uri)?)
    }

    /// Size in bytes, 0 if missing or invalid. Used for cache accounting.
    pub fn size(&self, uri: &str) -> usize {
        self.path(uri).and_then(fs::metadata).map(|m| m.len() as usize).unwrap_or(0)
    }

    /// Last modification time, used as the version of a cached copy: the reader
    /// sends its copy's time as `if_modified_since`, the owner compares it with
    /// its own blob's time (`State::read`).
    /// Note: the two times come from different files on different machines, so
    /// they almost never match and the owner resends the content.
    pub fn modified(&self, uri: &str) -> Option<SystemTime> {
        self.path(uri).and_then(fs::metadata).and_then(|m| m.modified()).ok()
    }
}
