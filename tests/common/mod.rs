//! Shared harness for the end-to-end tests: temp dirs, daemon processes,
//! a scriptable conflict resolver and parsers for the daemon's on-disk state.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use vpfs::messages::{
    CacheEntry, ConflictResolutionRequest, ConflictResolutionResponse, FileEntry, LogEntry,
};
use vpfs::VPFS;

pub const DAEMON: &str = env!("CARGO_BIN_EXE_daemon");
pub const SH: &str = env!("CARGO_BIN_EXE_sh");
pub const CAT: &str = env!("CARGO_BIN_EXE_cat");
pub const CAT2: &str = env!("CARGO_BIN_EXE_cat2");
pub const CONFLICT_RESOLVER: &str = env!("CARGO_BIN_EXE_conflict_resolver");

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// True when these tests are compiled against the frozen `legacy/` crate
/// (the oracle). Assertions on behaviour that the refactor fixes on purpose
/// branch on this flag.
pub const LEGACY: bool = cfg!(feature = "legacy");

/// Build a `FileEntry` independently of the fields each implementation has.
pub fn entry(owner: &str, uri: &str, name: &str) -> FileEntry {
    #[cfg(feature = "legacy")]
    return FileEntry { owner: owner.into(), uri: uri.into(), name: name.into() };
    #[cfg(not(feature = "legacy"))]
    FileEntry { owner: owner.into(), uri: uri.into(), name: name.into(), kind: Default::default() }
}

// ---------------------------------------------------------------------------
// Temp dirs
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Directory under `target/tmp` removed on drop (set `VPFS_KEEP_TMP=1` to keep it).
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "vpfs-{}-{}-{}-{:x}",
            tag,
            std::process::id(),
            n,
            rand::random::<u32>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn join(&self, p: &str) -> PathBuf {
        self.0.join(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::env::var_os("VPFS_KEEP_TMP").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// Daemon process
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct DaemonOpts {
    pub remote_id: Option<String>,
    pub conflict_port: Option<u16>,
    pub cache_size: Option<usize>,
}

pub struct Daemon {
    child: Option<Child>,
    pub name: String,
    pub dir: PathBuf,
    pub listen_port: u16,
    pub endpoint_id: String,
    output: Arc<Mutex<String>>,
}

impl Daemon {
    /// Start a daemon with `dir` as its working directory (it keeps state in `dir/files`).
    pub fn start(dir: &Path, name: &str, opts: DaemonOpts) -> Daemon {
        match Daemon::try_start(dir, name, opts) {
            Ok(d) => d,
            Err(out) => panic!("daemon {name} failed to start. Output:\n{out}"),
        }
    }

    /// Like `start`, but returns the captured output if the process exits before listening.
    ///
    /// A joining daemon finds the root only through iroh's DNS discovery, which
    /// can come back empty shortly after the root starts; the daemon then
    /// panics with "Could not connect" before touching its state directory.
    /// That specific failure is retried so tests exercise the behaviour past it.
    pub fn try_start(dir: &Path, name: &str, opts: DaemonOpts) -> Result<Daemon, String> {
        let mut attempt = 1;
        loop {
            match Daemon::try_start_once(dir, name, opts.clone()) {
                Err(out) if attempt < 6 && out.contains("Discovery produced no results") => {
                    attempt += 1;
                    thread::sleep(Duration::from_secs(2));
                }
                result => return result,
            }
        }
    }

    fn try_start_once(dir: &Path, name: &str, opts: DaemonOpts) -> Result<Daemon, String> {
        let listen_port = free_port();
        let mut cmd = Command::new(DAEMON);
        cmd.current_dir(dir)
            .args(["-n", name, "-p", "0", "-l", &listen_port.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(id) = &opts.remote_id {
            cmd.args(["--remote-id", id]);
        }
        if let Some(port) = opts.conflict_port {
            cmd.args(["-c", &port.to_string()]);
        }
        if let Some(size) = opts.cache_size {
            cmd.args(["-s", &size.to_string()]);
        }
        let mut child = cmd.spawn().expect("spawn daemon");

        let output = Arc::new(Mutex::new(String::new()));
        for stream in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>,
            Box::new(child.stderr.take().unwrap()) as Box<dyn Read + Send>,
        ] {
            let output = output.clone();
            thread::spawn(move || {
                for line in BufReader::new(stream).lines() {
                    let Ok(line) = line else { break };
                    let mut out = output.lock().unwrap();
                    out.push_str(&line);
                    out.push('\n');
                }
            });
        }

        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if output.lock().unwrap().contains("Listening for client connections") {
                break;
            }
            if let Ok(Some(_)) = child.try_wait() {
                // Give reader threads a moment to drain the pipes.
                thread::sleep(Duration::from_millis(200));
                return Err(output.lock().unwrap().clone());
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                return Err(format!("timed out\n{}", output.lock().unwrap()));
            }
            thread::sleep(Duration::from_millis(50));
        }

        let endpoint_id = output
            .lock()
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("Endpoint Id: ").map(str::to_string))
            .expect("endpoint id printed");

        Ok(Daemon {
            child: Some(child),
            name: name.to_string(),
            dir: dir.to_path_buf(),
            listen_port,
            endpoint_id,
            output,
        })
    }

    pub fn client(&self) -> VPFS {
        VPFS::connect(self.listen_port).expect("connect to daemon")
    }

    pub fn files_dir(&self) -> PathBuf {
        self.dir.join("files")
    }

    pub fn output(&self) -> String {
        self.output.lock().unwrap().clone()
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Poll `f` until it returns true or `timeout` elapses.
pub fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    f()
}

/// Run `f` on a helper thread; `None` if it did not finish within `timeout`.
/// The helper thread is leaked when it times out.
pub fn with_timeout<T: Send + 'static>(
    timeout: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).ok()
}

// ---------------------------------------------------------------------------
// Length-prefixed serde_bare framing used by all TCP peers
// ---------------------------------------------------------------------------

pub fn send_frame<T: serde::Serialize>(stream: &mut TcpStream, msg: &T) -> std::io::Result<()> {
    let buf = serde_bare::to_vec(msg).unwrap();
    stream.write_all(&(buf.len() as u64).to_be_bytes())?;
    stream.write_all(&buf)
}

pub fn recv_frame<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<T> {
    let mut len = [0u8; 8];
    stream.read_exact(&mut len)?;
    let mut buf = vec![0u8; u64::from_be_bytes(len) as usize];
    stream.read_exact(&mut buf)?;
    serde_bare::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

// ---------------------------------------------------------------------------
// Scriptable conflict resolver
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub enum Pick {
    Local,
    Remote,
}

/// In-process stand-in for the `conflict_resolver` binary. Records every
/// `Versions` request and answers with the configured pick.
pub struct FakeResolver {
    pub port: u16,
    pub requests: Arc<Mutex<Vec<Vec<FileEntry>>>>,
}

impl FakeResolver {
    pub fn start(pick: Pick) -> FakeResolver {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let recorded = recorded.clone();
                thread::spawn(move || {
                    while let Ok(ConflictResolutionRequest::Versions(v)) =
                        recv_frame::<ConflictResolutionRequest>(&mut stream)
                    {
                        let chosen = match pick {
                            Pick::Local => v[0].clone(),
                            Pick::Remote => v[1].clone(),
                        };
                        recorded.lock().unwrap().push(v);
                        if send_frame(&mut stream, &ConflictResolutionResponse::FinalVersion(chosen))
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        FakeResolver { port, requests }
    }

    pub fn requests(&self) -> Vec<Vec<FileEntry>> {
        self.requests.lock().unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// On-disk state parsers (formats written by the daemon into ./files)
// ---------------------------------------------------------------------------

pub fn read_log(files_dir: &Path) -> Vec<LogEntry> {
    let bytes = std::fs::read(files_dir.join("log")).expect("log file");
    serde_bare::from_slice(&bytes).expect("parse log")
}

pub fn read_vector_clock(files_dir: &Path) -> HashMap<String, u64> {
    let bytes = std::fs::read(files_dir.join("vector_clock")).expect("vector_clock file");
    serde_bare::from_slice(&bytes).expect("parse vector_clock")
}

/// `file_system` is a sequence of (path, FileEntry) pairs with no length prefix.
pub fn read_file_system(files_dir: &Path) -> HashMap<String, FileEntry> {
    let bytes = std::fs::read(files_dir.join("file_system")).expect("file_system file");
    let mut cur = Cursor::new(bytes);
    let mut out = HashMap::new();
    while (cur.position() as usize) < cur.get_ref().len() {
        let path: String = serde_bare::from_reader(&mut cur).unwrap();
        let entry: FileEntry = serde_bare::from_reader(&mut cur).unwrap();
        out.insert(path, entry);
    }
    out
}

/// `cache` is the used byte count followed by (name, CacheEntry) pairs in MRU→LRU order.
pub fn read_cache_state(files_dir: &Path) -> (u64, Vec<(String, CacheEntry)>) {
    let bytes = std::fs::read(files_dir.join("cache")).expect("cache file");
    let mut cur = Cursor::new(bytes);
    let used: u64 = serde_bare::from_reader(&mut cur).unwrap();
    let mut entries = Vec::new();
    while (cur.position() as usize) < cur.get_ref().len() {
        let key: String = serde_bare::from_reader(&mut cur).unwrap();
        let value: CacheEntry = serde_bare::from_reader(&mut cur).unwrap();
        entries.push((key, value));
    }
    (used, entries)
}

pub fn clock(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}
