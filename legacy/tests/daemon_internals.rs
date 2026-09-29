//! Unit-level tests for the daemon's internal modules.
//!
//! The daemon's modules are private to the `daemon` binary, so this test crate
//! compiles the unmodified source files directly with `#[path]`. The two TCP
//! helpers that `file_system.rs` imports from the daemon's crate root are
//! provided below with the same wire format.
//!
//! Many functions persist state relative to the current working directory, so
//! every test that touches the filesystem runs under `in_tmp_cwd`, which
//! serialises those tests and points the cwd at a fresh temp dir.
// Warnings come from the included daemon sources, which this suite must not modify.
#![allow(warnings)]

#[path = "../src/messages.rs"]
mod messages;
#[path = "../src/state.rs"]
mod state;
#[path = "../src/remote_communication.rs"]
mod remote_communication;
#[path = "../src/protocol.rs"]
mod protocol;
#[path = "../src/file_system.rs"]
mod file_system;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use iroh::{Endpoint, RelayMode};
use lru::LruCache;

use file_system::*;
use messages::*;
use state::DaemonState;

// Same framing as daemon.rs; required by file_system.rs via `crate::`.
fn send_message_tcp<T: serde::Serialize>(stream: &mut TcpStream, message: T) {
    let buf = serde_bare::to_vec(&message).unwrap();
    stream.write_all(&(buf.len() as u64).to_be_bytes()).unwrap();
    stream.write_all(&buf).unwrap();
}

fn receive_message_tcp<T: serde::de::DeserializeOwned>(
    stream: &mut TcpStream,
) -> Result<T, serde_bare::error::Error> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf).unwrap();
    let mut buf = vec![0u8; u64::from_be_bytes(len_buf) as usize];
    stream.read_exact(&mut buf).unwrap();
    serde_bare::from_slice(&buf)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static CWD_LOCK: Mutex<()> = Mutex::new(());

/// Run `f` with the process cwd set to a fresh temp dir (serialised across tests).
fn in_tmp_cwd<R>(f: impl FnOnce(PathBuf) -> R) -> R {
    let _guard = CWD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vpfs-internals-{}-{:x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let old = std::env::current_dir().unwrap();
    std::env::set_current_dir(&dir).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(dir.clone())));
    std::env::set_current_dir(old).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    match result {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

/// DaemonState with an offline iroh endpoint and no peers.
async fn make_state(name: &str, cache_size: usize) -> Arc<DaemonState> {
    let endpoint = Endpoint::empty_builder(RelayMode::Disabled).bind().await.unwrap();
    let endpoint_id = endpoint.id();
    Arc::new(DaemonState {
        endpoint,
        local: VPFSNode { name: name.to_string(), endpoint_id },
        connections: Mutex::new(HashMap::new()),
        known_nodes: Mutex::new(HashMap::new()),
        cache: Mutex::new(LruCache::unbounded()),
        max_cache_size: cache_size,
        used_cache_bytes: RwLock::new(0),
        file_system: RwLock::new(HashMap::new()),
        vector_clock: Mutex::new(HashMap::from([(name.to_string(), 0u64)])),
        log: Mutex::new(Vec::new()),
        open_files: Mutex::new(HashMap::new()),
    })
}

fn vc(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn fe(owner: &str, uri: &str, name: &str) -> FileEntry {
    FileEntry { owner: owner.into(), uri: uri.into(), name: name.into() }
}

fn log_entry(node: &str, clock: &[(&str, u64)], op: LogOp) -> LogEntry {
    LogEntry { clock: vc(clock), node: node.into(), op }
}

// ---------------------------------------------------------------------------
// Vector clocks
// ---------------------------------------------------------------------------

#[test]
fn happens_before_requires_all_le_and_one_strictly_greater() {
    assert!(happens_before(&vc(&[("a", 1)]), &vc(&[("a", 2)])));
    assert!(happens_before(&vc(&[("a", 1)]), &vc(&[("a", 1), ("b", 1)])));
    assert!(!happens_before(&vc(&[("a", 2)]), &vc(&[("a", 1)])));
    assert!(!happens_before(&vc(&[("a", 1)]), &vc(&[("a", 1)])), "equal clocks are not ordered");
    assert!(!happens_before(&vc(&[("a", 1)]), &vc(&[("b", 1)])));
    assert!(happens_before(&vc(&[]), &vc(&[("a", 1)])), "missing keys count as 0");
    assert!(!happens_before(&vc(&[]), &vc(&[])));
}

#[test]
fn are_concurrent_detects_incomparable_clocks() {
    assert!(are_concurrent(&vc(&[("a", 1)]), &vc(&[("b", 1)])));
    assert!(are_concurrent(&vc(&[("a", 2), ("b", 1)]), &vc(&[("a", 1), ("b", 2)])));
    assert!(!are_concurrent(&vc(&[("a", 1)]), &vc(&[("a", 2)])));
    assert!(!are_concurrent(&vc(&[("a", 1)]), &vc(&[("a", 1)])));
}

/// Current behaviour (suspected bug): clocks that only differ by an explicit
/// zero entry are logically equal, yet `are_concurrent` reports them as
/// concurrent because it falls back to map inequality.
#[test]
fn are_concurrent_treats_explicit_zero_entries_as_conflict() {
    let a = vc(&[("a", 1)]);
    let b = vc(&[("a", 1), ("b", 0)]);
    assert!(!happens_before(&a, &b));
    assert!(!happens_before(&b, &a));
    assert!(are_concurrent(&a, &b));
}

#[test]
fn merge_clocks_is_componentwise_max() {
    let merged = merge_clocks(&vc(&[("a", 3), ("b", 1)]), &vc(&[("a", 1), ("b", 5), ("c", 2)]));
    assert_eq!(merged, vc(&[("a", 3), ("b", 5), ("c", 2)]));
    assert_eq!(merge_clocks(&vc(&[]), &vc(&[])), vc(&[]));
}

// ---------------------------------------------------------------------------
// Log helpers
// ---------------------------------------------------------------------------

#[test]
fn entry_path_and_entry_file_extract_from_every_op() {
    let f = fe("a", "u1", "path/x");
    for op in [LogOp::Create(f.clone()), LogOp::Modify(f.clone()), LogOp::Remove(f.clone())] {
        assert_eq!(entry_path(&op), "path/x");
        assert_eq!(entry_file(&op), &f);
    }
}

#[test]
fn partial_log_since_filters_on_creator_component_only() {
    let log = vec![
        log_entry("a", &[("a", 1)], LogOp::Create(fe("a", "u1", "x"))),
        log_entry("a", &[("a", 2)], LogOp::Modify(fe("a", "u1", "x"))),
        log_entry("b", &[("a", 2), ("b", 1)], LogOp::Create(fe("b", "u2", "y"))),
    ];
    assert_eq!(partial_log_since(&log, &vc(&[])), log);
    assert_eq!(partial_log_since(&log, &vc(&[("a", 1)])), log[1..].to_vec());
    assert_eq!(partial_log_since(&log, &vc(&[("a", 2)])), log[2..].to_vec());
    assert_eq!(partial_log_since(&log, &vc(&[("a", 2), ("b", 1)])), vec![]);
    // Only the creator's component matters: a huge "a" value does not hide b's entry.
    assert_eq!(partial_log_since(&log, &vc(&[("a", 99)])), log[2..].to_vec());
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

#[test]
fn setup_files_dir_creates_then_reuses_and_chdirs() {
    in_tmp_cwd(|dir| {
        assert!(setup_files_dir(), "first call creates ./files");
        assert_eq!(std::env::current_dir().unwrap(), dir.join("files"));
        std::env::set_current_dir(&dir).unwrap();
        assert!(!setup_files_dir(), "second call reports existing dir");
        assert_eq!(std::env::current_dir().unwrap(), dir.join("files"));
    });
}

#[test]
fn create_file_with_random_uri_creates_empty_hex_named_files() {
    in_tmp_cwd(|dir| {
        let a = create_file_with_random_uri();
        let b = create_file_with_random_uri();
        assert_ne!(a, b);
        for uri in [&a, &b] {
            assert!(!uri.is_empty() && uri.len() <= 16);
            assert!(uri.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
            assert_eq!(std::fs::metadata(dir.join(uri)).unwrap().len(), 0);
        }
    });
}

#[test]
fn save_and_restore_file_system_roundtrip() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let mut fs_map = HashMap::new();
            fs_map.insert("x".to_string(), fe("a", "u1", "x"));
            fs_map.insert("dir/y".to_string(), fe("b", "u2", "dir/y"));
            save_file_system(&fs_map);

            let state = make_state("a", 1024).await;
            restore_file_system(&state);
            assert_eq!(*state.file_system.read().unwrap(), fs_map);
        })
    });
}

#[test]
fn restore_file_system_without_file_is_noop() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            restore_file_system(&state);
            assert!(state.file_system.read().unwrap().is_empty());
        })
    });
}

/// Current behaviour: a truncated `file_system` file (e.g. crash mid-write,
/// since saves are not atomic) makes restore panic instead of recovering.
#[test]
fn restore_file_system_panics_on_truncated_file() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let mut buf = serde_bare::to_vec(&"x".to_string()).unwrap();
            buf.extend(serde_bare::to_vec(&fe("a", "u1", "x")).unwrap());
            buf.extend(serde_bare::to_vec(&"y".to_string()).unwrap()); // path without entry
            std::fs::write("file_system", buf).unwrap();

            let state = make_state("a", 1024).await;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                restore_file_system(&state)
            }));
            assert!(result.is_err());
        })
    });
}

#[test]
fn restore_log_loads_entries_and_raises_vector_clock() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let log = vec![
                log_entry("a", &[("a", 1)], LogOp::Create(fe("a", "u1", "x"))),
                log_entry("b", &[("a", 1), ("b", 4)], LogOp::Modify(fe("a", "u1", "x"))),
            ];
            save_log(&log);
            let state = make_state("a", 1024).await;
            restore_log(&state);
            assert_eq!(*state.log.lock().unwrap(), log);
            assert_eq!(*state.vector_clock.lock().unwrap(), vc(&[("a", 1), ("b", 4)]));
        })
    });
}

#[test]
fn restore_vector_clock_takes_componentwise_max() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            save_vector_clock(&vc(&[("a", 3), ("b", 1)]));
            let state = make_state("a", 1024).await;
            state.vector_clock.lock().unwrap().insert("b".into(), 7);
            restore_vector_clock(&state);
            assert_eq!(*state.vector_clock.lock().unwrap(), vc(&[("a", 3), ("b", 7)]));
        })
    });
}

#[test]
fn restore_log_and_clock_ignore_missing_or_garbage_files() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            restore_log(&state);
            restore_vector_clock(&state);
            assert!(state.log.lock().unwrap().is_empty());
            assert_eq!(*state.vector_clock.lock().unwrap(), vc(&[("a", 0)]));

            std::fs::write("log", [0xff, 0xff, 0xff]).unwrap();
            std::fs::write("vector_clock", [0xff, 0xff, 0xff]).unwrap();
            restore_log(&state);
            restore_vector_clock(&state);
            assert!(state.log.lock().unwrap().is_empty());
            assert_eq!(*state.vector_clock.lock().unwrap(), vc(&[("a", 0)]));
        })
    });
}

// ---------------------------------------------------------------------------
// Log appends
// ---------------------------------------------------------------------------

#[test]
fn append_log_entry_ticks_clock_and_persists() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            append_log_entry(LogOp::Create(fe("a", "u1", "x")), &state).await;
            append_log_entry(LogOp::Modify(fe("a", "u1", "x")), &state).await;

            let expected = vec![
                log_entry("a", &[("a", 1)], LogOp::Create(fe("a", "u1", "x"))),
                log_entry("a", &[("a", 2)], LogOp::Modify(fe("a", "u1", "x"))),
            ];
            assert_eq!(*state.log.lock().unwrap(), expected);
            let on_disk: Vec<LogEntry> =
                serde_bare::from_slice(&std::fs::read("log").unwrap()).unwrap();
            assert_eq!(on_disk, expected);
            let clock_on_disk: HashMap<String, u64> =
                serde_bare::from_slice(&std::fs::read("vector_clock").unwrap()).unwrap();
            assert_eq!(clock_on_disk, vc(&[("a", 2)]));
        })
    });
}

// ---------------------------------------------------------------------------
// Namespace operations
// ---------------------------------------------------------------------------

#[test]
fn place_file_locally_creates_entry_backing_file_and_log() {
    in_tmp_cwd(|dir| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            let entry = place_file("x", &"a".to_string(), &state).await.unwrap();
            assert_eq!(entry.owner, "a");
            assert_eq!(entry.name, "x");
            assert!(dir.join(&entry.uri).exists());
            assert_eq!(find("x", &state), Ok(entry.clone()));
            assert_eq!(
                *state.log.lock().unwrap(),
                vec![log_entry("a", &[("a", 1)], LogOp::Create(entry.clone()))]
            );
            // persisted namespace
            assert!(dir.join("file_system").exists());

            assert_eq!(
                place_file("x", &"a".to_string(), &state).await,
                Err(VPFSError::AlreadyExists(entry))
            );
            assert_eq!(state.log.lock().unwrap().len(), 1, "failed place does not log");
        })
    });
}

#[test]
fn place_file_on_unconnected_node_is_not_accessible() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            assert_eq!(
                place_file("x", &"ghost".to_string(), &state).await,
                Err(VPFSError::NotAccessible)
            );
            assert_eq!(find("x", &state), Err(VPFSError::DoesNotExist));
            assert!(state.log.lock().unwrap().is_empty());
        })
    });
}

#[test]
fn list_files_returns_everything_regardless_of_dir() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            place_file_in_memory(&state.file_system, "d1/x", fe("a", "u1", "d1/x"));
            place_file_in_memory(&state.file_system, "d2/y", fe("b", "u2", "d2/y"));
            for dir in ["", "d1", "does-not-exist"] {
                let mut names: Vec<String> =
                    list_files(dir, &state).unwrap().into_iter().map(|e| e.name).collect();
                names.sort();
                assert_eq!(names, vec!["d1/x", "d2/y"]);
            }
        })
    });
}

#[test]
fn place_file_in_memory_overwrites_and_persists() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            place_file_in_memory(&state.file_system, "x", fe("a", "u1", "x"));
            place_file_in_memory(&state.file_system, "x", fe("b", "u2", "x"));
            assert_eq!(find("x", &state), Ok(fe("b", "u2", "x")));

            let fresh = make_state("a", 1024).await;
            restore_file_system(&fresh);
            assert_eq!(find("x", &fresh), Ok(fe("b", "u2", "x")));
        })
    });
}

// ---------------------------------------------------------------------------
// Local file I/O
// ---------------------------------------------------------------------------

/// Current behaviour (security): `read_local` ignores the namespace and reads
/// any path the daemon process can access.
#[test]
fn read_local_reads_any_path() {
    in_tmp_cwd(|dir| {
        let outside = dir.join("outside.txt");
        std::fs::write(&outside, b"secret").unwrap();
        let fs_map = RwLock::new(HashMap::new());
        assert_eq!(read_local(outside.to_str().unwrap(), &fs_map).unwrap(), b"secret");
        assert_eq!(
            read_local("missing", &fs_map).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    });
}

#[test]
fn write_local_requires_uri_in_namespace_and_existing_file() {
    in_tmp_cwd(|_| {
        let fs_map = RwLock::new(HashMap::new());
        std::fs::write("u1", b"old").unwrap();
        // not referenced by namespace
        assert!(write_local("u1", &b"new".to_vec(), &fs_map).is_err());
        assert_eq!(std::fs::read("u1").unwrap(), b"old");

        fs_map.write().unwrap().insert("x".into(), fe("a", "u1", "x"));
        write_local("u1", &b"new".to_vec(), &fs_map).unwrap();
        assert_eq!(std::fs::read("u1").unwrap(), b"new");

        // referenced but no backing file: not created
        fs_map.write().unwrap().insert("y".into(), fe("a", "u2", "y"));
        assert!(write_local("u2", &b"z".to_vec(), &fs_map).is_err());
        assert!(!std::path::Path::new("u2").exists());
    });
}

#[test]
fn fd_read_readline_close_locally() {
    in_tmp_cwd(|_| {
        let open_files = Mutex::new(HashMap::new());
        std::fs::write("u1", b"line1\nline2\nlast").unwrap();
        let fd = open_file_local("u1", &open_files).unwrap();
        assert!(fd > 2);

        assert_eq!(read_fd_local(fd, 3, &open_files).unwrap(), b"lin");
        assert_eq!(read_line_fd_local(fd, &open_files).unwrap(), b"e1\n");
        assert_eq!(read_line_fd_local(fd, &open_files).unwrap(), b"line2\n");
        assert_eq!(read_line_fd_local(fd, &open_files).unwrap(), b"last");
        assert_eq!(read_line_fd_local(fd, &open_files).unwrap(), b"");
        assert_eq!(read_fd_local(fd, 10, &open_files).unwrap(), b"");

        close_file_local(fd, &open_files).unwrap();
        assert!(close_file_local(fd, &open_files).is_err());
        assert!(read_fd_local(fd, 1, &open_files).is_err());
        assert!(read_line_fd_local(fd, &open_files).is_err());
        assert!(open_file_local("missing", &open_files).is_err());
    });
}

#[test]
fn state_level_fd_ops_for_local_and_unreachable_owner() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("a", 1024).await;
            std::fs::write("u1", b"hello\nworld\n").unwrap();
            let local = fe("a", "u1", "x");
            let fd = open_file(local.clone(), &state).await.unwrap();
            assert_eq!(read_line_fd(&local, fd, &state).await.unwrap(), b"hello\n");
            assert_eq!(read_fd(&local, fd, 100, &state).await.unwrap(), b"world\n");
            close_file(&"a".to_string(), fd, &state).await.unwrap();
            assert_eq!(close_file(&"a".to_string(), fd, &state).await, Err(VPFSError::FileNotOpen));
            assert_eq!(read_fd(&local, fd, 1, &state).await, Err(VPFSError::FileNotOpen));
            assert_eq!(read_line_fd(&local, fd, &state).await, Err(VPFSError::FileNotOpen));
            assert_eq!(
                open_file(fe("a", "missing", "m"), &state).await,
                Err(VPFSError::DoesNotExist)
            );

            let remote = fe("b", "u9", "r");
            assert_eq!(open_file(remote.clone(), &state).await, Err(VPFSError::NotAccessible));
            assert_eq!(read_fd(&remote, 3, 1, &state).await, Err(VPFSError::NotAccessible));
            assert_eq!(read_line_fd(&remote, 3, &state).await, Err(VPFSError::NotAccessible));
            assert_eq!(close_file(&"b".to_string(), 3, &state).await, Err(VPFSError::NotAccessible));
        })
    });
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

#[test]
fn add_cache_entry_writes_file_tracks_size_and_persists() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("b", 100).await;
            {
                let mut cache = state.cache.lock().unwrap();
                add_cache_entry(&fe("a", "u1", "x"), b"12345", &mut cache, &state);
            }
            assert_eq!(std::fs::read("u1").unwrap(), b"12345", "cache file named after owner's uri");
            assert_eq!(*state.used_cache_bytes.read().unwrap(), 5);

            // replacing same name adjusts the size rather than double counting
            {
                let mut cache = state.cache.lock().unwrap();
                add_cache_entry(&fe("a", "u1", "x"), b"12", &mut cache, &state);
            }
            assert_eq!(*state.used_cache_bytes.read().unwrap(), 2);

            let fresh = make_state("b", 100).await;
            restore_cache(&fresh);
            assert_eq!(*fresh.used_cache_bytes.read().unwrap(), 2);
            assert_eq!(
                fresh.cache.lock().unwrap().peek("x"),
                Some(&CacheEntry { uri: "u1".into() })
            );
        })
    });
}

#[test]
fn add_cache_entry_evicts_least_recently_used_over_limit() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("b", 10).await;
            let mut cache = state.cache.lock().unwrap();
            add_cache_entry(&fe("a", "u1", "x"), b"aaaa", &mut cache, &state);
            add_cache_entry(&fe("a", "u2", "y"), b"bbbb", &mut cache, &state);
            cache.get("x"); // x becomes most recently used
            add_cache_entry(&fe("a", "u3", "z"), b"cccc", &mut cache, &state);

            assert!(cache.peek("y").is_none());
            assert!(!std::path::Path::new("u2").exists());
            assert!(cache.peek("x").is_some() && cache.peek("z").is_some());
            assert_eq!(*state.used_cache_bytes.read().unwrap(), 8);

            // a single entry bigger than the whole cache evicts everything including itself
            add_cache_entry(&fe("a", "u4", "big"), b"0123456789ABC", &mut cache, &state);
            assert_eq!(cache.len(), 0);
            assert_eq!(*state.used_cache_bytes.read().unwrap(), 0);
            assert!(!std::path::Path::new("u4").exists());
        })
    });
}

/// Current behaviour: an empty `cache` state file makes restore panic.
#[test]
fn restore_cache_panics_on_empty_cache_file() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            std::fs::write("cache", b"").unwrap();
            let state = make_state("b", 10).await;
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| restore_cache(&state)));
            assert!(result.is_err());
        })
    });
}

#[test]
fn read_remote_without_connection_uses_cache_status() {
    in_tmp_cwd(|_| {
        rt().block_on(async {
            let state = make_state("b", 100).await;
            let remote = fe("a", "u1", "x");
            assert_eq!(read_remote(&remote, &state).await, Err(VPFSError::NotAccessible));

            {
                let mut cache = state.cache.lock().unwrap();
                add_cache_entry(&remote, b"cached", &mut cache, &state);
            }
            // Cached copy is offered back as a local entry pointing at the cache file.
            assert_eq!(
                read_remote(&remote, &state).await,
                Err(VPFSError::OnlyInCache(fe("b", "u1", "x")))
            );
        })
    });
}
