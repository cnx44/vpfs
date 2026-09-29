//! End-to-end tests with two real daemons talking over iroh.
//!
//! Joining uses iroh discovery/relays, so these tests need network access.
//! Offline scenarios restart nodes *standalone* (without `--remote-id`)
//! because a surviving node blocks on its stale connection to a dead peer
//! (see `mutations_hang_after_peer_dies`).

mod common;

use std::io::Write;
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use vpfs::messages::{
    ConflictResolutionRequest, ConflictResolutionResponse, FileEntry, LogOp, VPFSError,
};

const SYNC: Duration = Duration::from_secs(15);

fn root(dir: &TempDir) -> Daemon {
    Daemon::start(dir.path(), "a", DaemonOpts::default())
}

fn join(dir: &TempDir, name: &str, to: &Daemon, resolver: &FakeResolver) -> Daemon {
    join_with(dir, name, to, resolver, DaemonOpts::default())
}

fn join_with(dir: &TempDir, name: &str, to: &Daemon, resolver: &FakeResolver, opts: DaemonOpts) -> Daemon {
    Daemon::start(
        dir.path(),
        name,
        DaemonOpts {
            remote_id: Some(to.endpoint_id.clone()),
            conflict_port: Some(resolver.port),
            ..opts
        },
    )
}

fn standalone(dir: &TempDir, name: &str) -> Daemon {
    Daemon::start(dir.path(), name, DaemonOpts::default())
}

// ---------------------------------------------------------------------------
// Joining and online operations
// ---------------------------------------------------------------------------

#[test]
fn joining_node_receives_namespace_and_log_from_root() {
    let (da, db) = (TempDir::new("join-a"), TempDir::new("join-b"));
    let a = root(&da);
    a.client().store("pre", &b"from a".to_vec()).unwrap();
    let pre = a.client().find("pre").unwrap();

    let resolver = FakeResolver::start(Pick::Local);
    let b = join(&db, "b", &a, &resolver);
    let cb = b.client();
    assert_eq!(cb.local, "b");
    assert_eq!(cb.find("pre"), Ok(pre.clone()));
    assert_eq!(cb.fetch("pre").unwrap(), b"from a");
    assert!(resolver.requests().is_empty());

    let log_b = read_log(&b.files_dir());
    assert_eq!(log_b.len(), 2, "b merged a's Create + Modify");
    assert!(log_b.iter().all(|e| e.node == "a"));
    assert_eq!(read_vector_clock(&b.files_dir()), clock(&[("a", 2), ("b", 0)]));
    assert!(a.output().contains("Received InitHello"));
    assert!(a.output().contains("Received DaemonHello from node: b"));
}

#[test]
fn place_and_write_through_peer() {
    let (da, db) = (TempDir::new("peer-a"), TempDir::new("peer-b"));
    let a = root(&da);
    let resolver = FakeResolver::start(Pick::Local);
    let b = join(&db, "b", &a, &resolver);
    let (ca, cb) = (a.client(), b.client());

    // b places a file whose owner is a
    let r = cb.place("r", "a".into()).unwrap();
    assert_eq!(r.owner, "a");
    assert!(a.files_dir().join(&r.uri).exists(), "backing file lives on the owner");
    assert!(!b.files_dir().join(&r.uri).exists());
    assert_eq!(ca.find("r"), Ok(r.clone()), "AddEntry propagated to owner");

    cb.write(r.clone(), &b"via b".to_vec()).unwrap();
    assert_eq!(ca.fetch("r").unwrap(), b"via b");
    assert_eq!(cb.fetch("r").unwrap(), b"via b");

    // The owner logs the remote write and fans it out back to b.
    let modify_by_a = |dir: &std::path::Path| {
        read_log(dir).iter().any(|e| e.node == "a" && e.op == LogOp::Modify(r.clone()))
    };
    assert!(modify_by_a(&a.files_dir()));
    assert!(wait_until(SYNC, || modify_by_a(&b.files_dir())));

    // Symmetric: b-owned file readable from a.
    cb.store("mine", &b"b data".to_vec()).unwrap();
    let mine = ca.find("mine").unwrap();
    assert_eq!(mine.owner, "b");
    assert_eq!(ca.fetch("mine").unwrap(), b"b data");

    // Same path from both sides -> AlreadyExists
    assert!(matches!(ca.place("r", "a".into()), Err(VPFSError::AlreadyExists(_))));
    assert!(matches!(ca.place("mine", "b".into()), Err(VPFSError::AlreadyExists(_))));
}

#[test]
fn remote_reads_populate_and_refresh_reader_cache() {
    let (da, db) = (TempDir::new("cache-a"), TempDir::new("cache-b"));
    let a = root(&da);
    let resolver = FakeResolver::start(Pick::Local);
    let b = join(&db, "b", &a, &resolver);
    let (ca, cb) = (a.client(), b.client());

    ca.store("c", &b"cached-data".to_vec()).unwrap();
    let c = ca.find("c").unwrap();
    assert_eq!(cb.fetch("c").unwrap(), b"cached-data");
    // Cache copy is stored under the owner's uri in the reader's files dir.
    assert_eq!(std::fs::read(b.files_dir().join(&c.uri)).unwrap(), b"cached-data");
    assert_eq!(
        read_cache_state(&b.files_dir()),
        (11, vec![("c".to_string(), vpfs::messages::CacheEntry { uri: c.uri.clone() })])
    );

    ca.write(c.clone(), &b"new".to_vec()).unwrap();
    assert_eq!(cb.fetch("c").unwrap(), b"new");
    assert_eq!(std::fs::read(b.files_dir().join(&c.uri)).unwrap(), b"new");
    assert_eq!(read_cache_state(&b.files_dir()).0, 3);

    // The owner never caches its own files.
    assert!(!a.files_dir().join("cache").exists());
}

#[test]
fn cache_size_limit_evicts_least_recently_used() {
    let (da, db) = (TempDir::new("evict-a"), TempDir::new("evict-b"));
    let a = root(&da);
    let resolver = FakeResolver::start(Pick::Local);
    let b = join_with(&db, "b", &a, &resolver, DaemonOpts { cache_size: Some(10), ..Default::default() });
    let (ca, cb) = (a.client(), b.client());

    ca.store("x", &b"12345678".to_vec()).unwrap();
    ca.store("y", &b"abcdefgh".to_vec()).unwrap();
    let (x, y) = (ca.find("x").unwrap(), ca.find("y").unwrap());

    cb.fetch("x").unwrap();
    assert!(b.files_dir().join(&x.uri).exists());
    cb.fetch("y").unwrap();
    assert!(!b.files_dir().join(&x.uri).exists(), "x evicted");
    assert!(b.files_dir().join(&y.uri).exists());
    let (used, entries) = read_cache_state(&b.files_dir());
    assert_eq!(used, 8);
    assert_eq!(entries.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), vec!["y"]);
    assert_eq!(cb.fetch("x").unwrap(), b"12345678", "still readable while owner online");
}

#[test]
fn fd_operations_on_remote_file() {
    let (da, db) = (TempDir::new("fd-a"), TempDir::new("fd-b"));
    let a = root(&da);
    let resolver = FakeResolver::start(Pick::Local);
    let b = join(&db, "b", &a, &resolver);
    a.client().store("f", &b"l1\nl2\nrest".to_vec()).unwrap();

    let cb = b.client();
    let fd = cb.open("f").unwrap();
    assert_eq!(fd, 3);
    assert_eq!(cb.read_line_fd(fd).unwrap(), b"l1\n");
    assert_eq!(cb.read_fd(fd, 2).unwrap(), b"l2");
    assert_eq!(cb.read_fd(fd, 100).unwrap(), b"\nrest");
    assert_eq!(cb.read_fd(fd, 100).unwrap(), b"");
    cb.close(fd).unwrap();
    assert_eq!(cb.close(fd), Err(VPFSError::FileNotOpen));
    // fd reads do not populate the cache
    assert!(!b.files_dir().join("cache").exists());
}

// ---------------------------------------------------------------------------
// Failure behaviour
// ---------------------------------------------------------------------------

/// Legacy (bug): idle timeout is disabled, so when a peer dies the survivor
/// keeps a "live" connection to it. Any mutation fans out to that peer and
/// blocks forever waiting for a reply; purely local reads still work.
/// Refactor: broadcast is bounded by a timeout, so the mutation completes.
#[test]
fn mutations_hang_after_peer_dies() {
    let (da, db) = (TempDir::new("hang-a"), TempDir::new("hang-b"));
    let a = root(&da);
    let resolver = FakeResolver::start(Pick::Local);
    let mut b = join(&db, "b", &a, &resolver);
    a.client().store("local", &b"ok".to_vec()).unwrap();
    b.kill();

    let port = a.listen_port;
    let read = with_timeout(Duration::from_secs(10), move || {
        vpfs::VPFS::connect(port).unwrap().fetch("local")
    });
    assert_eq!(read, Some(Ok(b"ok".to_vec())));

    let place = with_timeout(Duration::from_secs(10), move || {
        vpfs::VPFS::connect(port).unwrap().place("after", "a".into())
    });
    if LEGACY {
        assert!(place.is_none(), "place returned {place:?} instead of hanging");
    } else {
        assert!(matches!(place, Some(Ok(_))), "place returned {place:?}");
    }
}

/// Legacy: the joining daemon connects to the resolver eagerly and panics.
/// Refactor: the resolver is contacted only when a conflict needs a human.
#[test]
fn joining_without_conflict_resolver_crashes() {
    let (da, db) = (TempDir::new("nores-a"), TempDir::new("nores-b"));
    let a = root(&da);
    if !LEGACY {
        let resolver_port = free_port();
        let b = Daemon::start(
            db.path(),
            "b",
            DaemonOpts { remote_id: Some(a.endpoint_id.clone()), conflict_port: Some(resolver_port), ..Default::default() },
        );
        b.client().store("ok", &b"x".to_vec()).unwrap();
        return;
    }
    let result = Daemon::try_start(
        db.path(),
        "b",
        DaemonOpts {
            remote_id: Some(a.endpoint_id.clone()),
            conflict_port: Some(free_port()),
            ..Default::default()
        },
    );
    let out = result.err().expect("daemon should not start");
    assert!(out.contains("Connected to network"), "{out}");
    assert!(out.contains("panicked") && out.contains("Connection refused"), "{out}");
}

#[test]
fn standalone_restart_serves_cache_and_takes_ownership_on_offline_write() {
    let (da, db) = (TempDir::new("off-a"), TempDir::new("off-b"));
    let (x, cached_x) = {
        let mut a = root(&da);
        let resolver = FakeResolver::start(Pick::Local);
        let mut b = join(&db, "b", &a, &resolver);
        a.client().store("x", &b"v1".to_vec()).unwrap();
        a.client().store("y", &b"never read by b".to_vec()).unwrap();
        b.client().fetch("x").unwrap();
        let x = a.client().find("x").unwrap();
        b.kill();
        a.kill();
        let cached = entry("b", &x.uri, "x");
        (x, cached)
    };

    let b = standalone(&db, "b");
    let cb = b.client();
    assert_eq!(cb.find("x"), Ok(x.clone()));
    assert_eq!(cb.read(x.clone()), Err(VPFSError::OnlyInCache(cached_x.clone())));
    assert_eq!(cb.fetch("y"), Err(VPFSError::NotAccessible));
    assert_eq!(cb.read(cached_x.clone()).unwrap(), b"v1");

    assert_eq!(cb.write(x.clone(), &b"v2".to_vec()), Err(VPFSError::OnlyInCache(cached_x.clone())));
    assert_eq!(cb.write(cached_x.clone(), &b"v2".to_vec()), Ok(()));
    let now = cb.find("x").unwrap();
    assert_eq!(now.owner, "b", "offline write moves ownership");
    assert_ne!(now.uri, x.uri);
    assert_eq!(cb.fetch("x").unwrap(), b"v2");
    assert!(!b.files_dir().join(&x.uri).exists(), "cache copy removed");
    assert_eq!(
        read_log(&b.files_dir()).last().map(|e| (e.node.clone(), e.op.clone())),
        Some(("b".to_string(), LogOp::Modify(now.clone())))
    );
    // Legacy (bug): the persisted cache index is not rewritten when the entry
    // is dropped, so it still references the deleted copy.
    let (_, persisted) = read_cache_state(&b.files_dir());
    assert_eq!(persisted.iter().any(|(k, _)| k == "x"), LEGACY);
}

// ---------------------------------------------------------------------------
// Rejoin / reconciliation
// ---------------------------------------------------------------------------

#[test]
fn rejoin_exchanges_changes_made_while_apart() {
    let (da, db) = (TempDir::new("rejoin-a"), TempDir::new("rejoin-b"));
    {
        let mut a = root(&da);
        let resolver = FakeResolver::start(Pick::Local);
        let mut b = join(&db, "b", &a, &resolver);
        a.client().store("shared", &b"v1".to_vec()).unwrap();
        b.client().fetch("shared").unwrap();
        b.kill();
        a.kill();
    }
    {
        let b = standalone(&db, "b");
        b.client().store("new-on-b", &b"B".to_vec()).unwrap();
    }
    let a = standalone(&da, "a");
    let ca = a.client();
    ca.store("new-on-a", &b"A".to_vec()).unwrap();
    ca.write(ca.find("shared").unwrap(), &b"v2".to_vec()).unwrap();

    let resolver = FakeResolver::start(Pick::Local);
    let b = join(&db, "b", &a, &resolver);
    let cb = b.client();
    assert!(resolver.requests().is_empty(), "no conflicts expected");

    assert_eq!(cb.find("new-on-a").map(|e| e.owner), Ok("a".into()));
    assert_eq!(cb.fetch("new-on-a").unwrap(), b"A");
    assert_eq!(cb.fetch("shared").unwrap(), b"v2");

    assert!(wait_until(SYNC, || ca.find("new-on-b").is_ok()), "UpdatedFiles reached a");
    assert_eq!(ca.find("new-on-b").unwrap().owner, "b");
    assert_eq!(ca.fetch("new-on-b").unwrap(), b"B");

    // Both logs converge on the union of entries.
    assert!(wait_until(SYNC, || {
        let (la, lb) = (read_log(&a.files_dir()), read_log(&b.files_dir()));
        la.len() == lb.len() && la.iter().all(|e| lb.contains(e))
    }));
}

/// Build a conflict on "doc": b takes ownership offline and writes, a writes
/// its own copy offline, then b rejoins a. Returns (a, b, a's entry, b's entry).
fn conflicting_rejoin(
    da: &TempDir,
    db: &TempDir,
    resolver: &FakeResolver,
) -> (Daemon, Daemon, FileEntry, FileEntry) {
    {
        let mut a = root(da);
        let res = FakeResolver::start(Pick::Local);
        let mut b = join(db, "b", &a, &res);
        a.client().store("doc", &b"base".to_vec()).unwrap();
        b.client().fetch("doc").unwrap();
        b.kill();
        a.kill();
    }
    let b_entry = {
        let b = standalone(db, "b");
        let cb = b.client();
        let Err(VPFSError::OnlyInCache(ce)) = cb.write(cb.find("doc").unwrap(), &b"b-version".to_vec())
        else {
            panic!("expected OnlyInCache")
        };
        cb.write(ce, &b"b-version".to_vec()).unwrap();
        cb.find("doc").unwrap()
    };
    let a = standalone(da, "a");
    let a_entry = a.client().find("doc").unwrap();
    a.client().write(a_entry.clone(), &b"a-version".to_vec()).unwrap();

    let b = join(db, "b", &a, resolver);
    (a, b, a_entry, b_entry)
}

#[test]
fn concurrent_offline_edits_resolved_in_favour_of_remote() {
    let (da, db) = (TempDir::new("conf-r-a"), TempDir::new("conf-r-b"));
    let resolver = FakeResolver::start(Pick::Remote);
    let (a, b, a_entry, b_entry) = conflicting_rejoin(&da, &db, &resolver);

    // Resolution may complete after startup (refactor resolves asynchronously).
    assert!(wait_until(SYNC, || !resolver.requests().is_empty()));
    assert_eq!(resolver.requests(), vec![vec![b_entry, a_entry.clone()]], "[local, remote]");
    let cb = b.client();
    assert!(wait_until(SYNC, || cb.find("doc") == Ok(a_entry.clone())));
    assert_eq!(cb.fetch("doc").unwrap(), b"a-version");
    assert_eq!(a.client().find("doc"), Ok(a_entry.clone()));

    // A resolution entry (Modify by b, superseding both clocks) is appended on both sides.
    let resolution = read_log(&b.files_dir()).into_iter().rev()
        .find(|e| e.op == LogOp::Modify(a_entry.clone()) && e.node == "b")
        .expect("resolution entry in b's log");
    assert!(wait_until(SYNC, || read_log(&a.files_dir()).contains(&resolution)));
}

#[test]
fn concurrent_offline_edits_resolved_in_favour_of_local() {
    let (da, db) = (TempDir::new("conf-l-a"), TempDir::new("conf-l-b"));
    let resolver = FakeResolver::start(Pick::Local);
    let (a, b, a_entry, b_entry) = conflicting_rejoin(&da, &db, &resolver);

    assert!(wait_until(SYNC, || !resolver.requests().is_empty()));
    assert_eq!(resolver.requests(), vec![vec![b_entry.clone(), a_entry]]);
    let cb = b.client();
    assert!(wait_until(SYNC, || cb.find("doc") == Ok(b_entry.clone())));
    let ca = a.client();
    assert!(wait_until(SYNC, || ca.find("doc") == Ok(b_entry.clone())), "UpdatedFiles reached a");
    assert_eq!(ca.fetch("doc").unwrap(), b"b-version");
}

// ---------------------------------------------------------------------------
// Bundled conflict_resolver binary
// ---------------------------------------------------------------------------

#[test]
fn conflict_resolver_binary_picks_by_stdin_defaulting_to_local() {
    let port = free_port();
    let mut child = Command::new(CONFLICT_RESOLVER)
        .args(["-p", &port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"2\nwhatever\n1\n").unwrap();

    let mut s = None;
    assert!(wait_until(Duration::from_secs(10), || {
        s = TcpStream::connect(("127.0.0.1", port)).ok();
        s.is_some()
    }));
    let mut s = s.unwrap();
    let (l, r) = (entry("b", "l", "f"), entry("a", "r", "f"));
    for expected in [&r, &l, &l] {
        send_frame(&mut s, &ConflictResolutionRequest::Versions(vec![l.clone(), r.clone()])).unwrap();
        let ConflictResolutionResponse::FinalVersion(got) = recv_frame(&mut s).unwrap();
        assert_eq!(&got, expected);
    }
    // Stdin exhausted: the next prompt reads an empty line and defaults to local.
    send_frame(&mut s, &ConflictResolutionRequest::Versions(vec![l.clone(), r.clone()])).unwrap();
    let ConflictResolutionResponse::FinalVersion(got) = recv_frame(&mut s).unwrap();
    assert_eq!(got, l);

    // Fewer than two versions crashes the resolver.
    send_frame(&mut s, &ConflictResolutionRequest::Versions(vec![l.clone()])).unwrap();
    let status = with_timeout(Duration::from_secs(10), move || child.wait().unwrap())
        .expect("resolver should exit");
    assert!(!status.success());
}
