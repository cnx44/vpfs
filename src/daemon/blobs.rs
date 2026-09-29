//! File contents on the local disk: one file per blob inside the node directory,
//! named by its uri. Knows nothing about paths, owners or kinds.

use std::fs::{self, File};
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

use rand::Rng;

pub struct Blobs {
    dir: PathBuf,
}

impl Blobs {
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

    pub fn read(&self, uri: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(uri)?)
    }

    /// Replace the content atomically (write aside, then rename): a failed
    /// write never leaves a half-written blob.
    pub fn write(&self, uri: &str, data: &[u8]) -> io::Result<()> {
        let path = self.path(uri)?;
        let tmp = self.dir.join(format!(".{uri}.tmp"));
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)
    }

    pub fn remove(&self, uri: &str) {
        if let Ok(path) = self.path(uri) {
            let _ = fs::remove_file(path);
        }
    }

    pub fn open(&self, uri: &str) -> io::Result<File> {
        File::open(self.path(uri)?)
    }

    pub fn size(&self, uri: &str) -> usize {
        self.path(uri).and_then(fs::metadata).map(|m| m.len() as usize).unwrap_or(0)
    }

    pub fn modified(&self, uri: &str) -> Option<SystemTime> {
        self.path(uri).and_then(fs::metadata).and_then(|m| m.modified()).ok()
    }
}
