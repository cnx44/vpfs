//! End-to-end tests against a single real `daemon` process, driven through the
//! `vpfs` client library and the shipped client binaries.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use vpfs::messages::{FileEntry, Hello, LogOp, VPFSError};
use vpfs::VPFS;

fn start(tag: &str) -> (TempDir, Daemon) {
    let dir = TempDir::new(tag);
    let daemon = Daemon::start(dir.path(), "alpha", DaemonOpts::default());
    (dir, daemon)
}

fn sorted_names(entries: Vec<FileEntry>) -> Vec<String> {
    let mut names: Vec<String> = entries.into_iter().map(|e| e.name).collect();
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// Startup / connection
// ---------------------------------------------------------------------------

#[test]
fn daemon_help_documents_defaults() {
    let out = Command::new(DAEMON).arg("--help").output().unwrap();
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    for expected in ["[default: 8081]", "[default: 8082]", "[default: 8083]", "[default: 65536]", "--name"] {
        assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
    }
}

#[test]
fn daemon_requires_name() {
    let out = Command::new(DAEMON).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--name"));
}

#[test]
fn fresh_daemon_creates_empty_files_dir_and_greets_with_its_name() {
    let (_dir, daemon) = start("fresh");
    assert!(daemon.files_dir().is_dir());
    assert_eq!(std::fs::read_dir(daemon.files_dir()).unwrap().count(), 0, "no state written yet");
    let client = daemon.client();
    assert_eq!(client.local, "alpha");
    assert_eq!(client.ls("").unwrap(), vec![]);
}

#[test]
fn connect_to_closed_port_is_an_error() {
    assert!(VPFS::connect(free_port()).is_err());
}

// ---------------------------------------------------------------------------
// Namespace: place / find / ls
// ---------------------------------------------------------------------------

#[test]
fn find_missing_file_is_does_not_exist() {
    let (_dir, daemon) = start("find-missing");
    assert_eq!(daemon.client().find("nope"), Err(VPFSError::DoesNotExist));
}

#[test]
fn place_creates_owned_entry_and_empty_backing_file() {
    let (_dir, daemon) = start("place");
    let client = daemon.client();
    let e = client.place("notes.txt", "alpha".into()).unwrap();
    assert_eq!(e.owner, "alpha");
    assert_eq!(e.name, "notes.txt");
    assert!(!e.uri.is_empty() && e.uri.len() <= 16 && e.uri.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(std::fs::metadata(daemon.files_dir().join(&e.uri)).unwrap().len(), 0);
    assert_eq!(client.find("notes.txt"), Ok(e.clone()));
    assert_eq!(client.read(e), Ok(vec![]));
}

#[test]
fn place_existing_path_returns_already_exists_with_current_entry() {
    let (_dir, daemon) = start("place-dup");
    let client = daemon.client();
    let e = client.place("x", "alpha".into()).unwrap();
    assert_eq!(client.place("x", "alpha".into()), Err(VPFSError::AlreadyExists(e.clone())));
    assert_eq!(client.place("x", "other-node".into()), Err(VPFSError::AlreadyExists(e)));
}

#[test]
fn place_on_unknown_node_is_not_accessible_and_creates_nothing() {
    let (_dir, daemon) = start("place-unknown");
    let client = daemon.client();
    assert_eq!(client.place("x", "ghost".into()), Err(VPFSError::NotAccessible));
    assert_eq!(client.find("x"), Err(VPFSError::DoesNotExist));
}

/// Paths are opaque strings: no normalisation, empty path allowed.
#[test]
fn place_accepts_arbitrary_path_strings_verbatim() {
    let (_dir, daemon) = start("place-paths");
    let client = daemon.client();
    for p in ["", "a/b/../c", "/abs", "with space"] {
        let e = client.place(p, "alpha".into()).unwrap();
        assert_eq!(client.find(p), Ok(e));
    }
    assert_eq!(client.find("a/c"), Err(VPFSError::DoesNotExist));
    assert_eq!(client.find("abs"), Err(VPFSError::DoesNotExist));
}

/// Directories are not implemented: `ls` returns every entry for any argument.
#[test]
fn ls_ignores_directory_argument() {
    let (_dir, daemon) = start("ls");
    let client = daemon.client();
    client.place("d1/x", "alpha".into()).unwrap();
    client.place("d2/y", "alpha".into()).unwrap();
    for dir in ["", "d1", "missing"] {
        assert_eq!(sorted_names(client.ls(dir).unwrap()), vec!["d1/x", "d2/y"]);
    }
}

// ---------------------------------------------------------------------------
// Whole-file read / write
// ---------------------------------------------------------------------------

#[test]
fn write_read_roundtrip_and_overwrite_truncates() {
    let (_dir, daemon) = start("rw");
    let client = daemon.client();
    let e = client.place("f", "alpha".into()).unwrap();

    client.write(e.clone(), &b"hello world".to_vec()).unwrap();
    assert_eq!(client.read(e.clone()).unwrap(), b"hello world");
    assert_eq!(std::fs::read(daemon.files_dir().join(&e.uri)).unwrap(), b"hello world");

    client.write(e.clone(), &b"bye".to_vec()).unwrap();
    assert_eq!(client.read(e.clone()).unwrap(), b"bye");

    client.write(e.clone(), &vec![]).unwrap();
    assert_eq!(client.read(e.clone()).unwrap(), b"");
    assert_eq!(client.find("f"), Ok(e), "local writes keep the uri");
}

#[test]
fn large_binary_payload_roundtrip() {
    let (_dir, daemon) = start("large");
    let client = daemon.client();
    let data: Vec<u8> = (0..(3 << 20)).map(|i| (i * 7 % 256) as u8).collect();
    client.store("big.bin", &data).unwrap();
    assert_eq!(client.fetch("big.bin").unwrap(), data);
}

#[test]
fn store_creates_or_overwrites_and_fetch_reads_back() {
    let (_dir, daemon) = start("store");
    let client = daemon.client();
    client.store("s", &b"one".to_vec()).unwrap();
    let e = client.find("s").unwrap();
    assert_eq!(e.owner, "alpha");
    assert_eq!(client.fetch("s").unwrap(), b"one");

    client.store("s", &b"two".to_vec()).unwrap();
    assert_eq!(client.fetch("s").unwrap(), b"two");
    assert_eq!(client.find("s"), Ok(e), "store on existing path reuses the entry");

    assert_eq!(client.fetch("missing"), Err(VPFSError::DoesNotExist));
}

#[test]
fn read_with_missing_backing_file_is_does_not_exist() {
    let (_dir, daemon) = start("read-missing");
    let client = daemon.client();
    let e = client.place("f", "alpha".into()).unwrap();
    std::fs::remove_file(daemon.files_dir().join(&e.uri)).unwrap();
    assert_eq!(client.read(e.clone()), Err(VPFSError::DoesNotExist));
    assert_eq!(client.write(e, &b"x".to_vec()), Err(VPFSError::DoesNotExist));
}

#[test]
fn read_and_write_of_remote_owned_entry_without_peer_is_not_accessible() {
    let (_dir, daemon) = start("remote-owned");
    let client = daemon.client();
    let remote = entry("ghost", "abc", "r");
    assert_eq!(client.read(remote.clone()), Err(VPFSError::NotAccessible));
    assert_eq!(client.write(remote, &b"data".to_vec()), Err(VPFSError::NotAccessible));
    // The connection is still usable after the payload was drained.
    assert_eq!(client.ls("").unwrap(), vec![]);
}

/// Legacy (bug): a write with a stale/unknown uri fails, but the daemon has
/// already re-bound the path to that uri, so the name now resolves to a
/// dangling entry. Refactor: the write is validated before anything changes.
#[test]
fn failed_write_with_unknown_uri_rebinds_path() {
    let (_dir, daemon) = start("rebind");
    let client = daemon.client();
    let good = client.store("f", &b"content".to_vec()).and_then(|_| client.find("f")).unwrap();

    let bogus = entry("alpha", "deadbeef", "f");
    assert_eq!(client.write(bogus.clone(), &b"x".to_vec()), Err(VPFSError::DoesNotExist));
    if LEGACY {
        assert_eq!(client.find("f"), Ok(bogus));
        assert_eq!(client.fetch("f"), Err(VPFSError::DoesNotExist));
    } else {
        assert_eq!(client.find("f"), Ok(good.clone()));
        assert_eq!(client.fetch("f").unwrap(), b"content");
    }
    // Old data still on disk but unreachable by name.
    assert_eq!(std::fs::read(daemon.files_dir().join(&good.uri)).unwrap(), b"content");
}

/// Legacy (security): the uri in a client `Read` is used as a raw filesystem
/// path, so any file readable by the daemon can be read. Refactor: uris that
/// are not plain blob names are rejected.
#[test]
fn read_uri_is_an_unchecked_host_path() {
    let (dir, daemon) = start("read-anywhere");
    let secret = dir.join("secret.txt");
    std::fs::write(&secret, b"top secret").unwrap();
    let client = daemon.client();
    let e = entry("alpha", secret.to_str().unwrap(), "irrelevant");
    let rel = entry("alpha", "../secret.txt", "irrelevant");
    if LEGACY {
        assert_eq!(client.read(e.clone()).unwrap(), b"top secret");
        assert_eq!(client.read(rel).unwrap(), b"top secret");
    } else {
        assert_eq!(client.read(e.clone()), Err(VPFSError::DoesNotExist));
        assert_eq!(client.read(rel), Err(VPFSError::DoesNotExist));
    }
    assert_eq!(client.find("irrelevant"), Err(VPFSError::DoesNotExist), "read does not bind");
}

/// Legacy (security): a client `Write` binds the name to the given uri before
/// validating it, so any existing file writable by the daemon can be
/// overwritten. Refactor: rejected, nothing is bound or written.
#[test]
fn write_uri_can_overwrite_existing_host_file() {
    let (dir, daemon) = start("write-anywhere");
    let victim = dir.join("victim.txt");
    std::fs::write(&victim, b"original").unwrap();
    let client = daemon.client();
    let e = entry("alpha", "../victim.txt", "evil");
    if LEGACY {
        assert_eq!(client.write(e.clone(), &b"pwned".to_vec()), Ok(()));
        assert_eq!(std::fs::read(&victim).unwrap(), b"pwned");
        assert_eq!(client.find("evil"), Ok(e));
    } else {
        assert_eq!(client.write(e.clone(), &b"pwned".to_vec()), Err(VPFSError::DoesNotExist));
        assert_eq!(std::fs::read(&victim).unwrap(), b"original");
        assert_eq!(client.find("evil"), Err(VPFSError::DoesNotExist));
    }
}

// ---------------------------------------------------------------------------
// fd API
// ---------------------------------------------------------------------------

#[test]
fn read_fd_returns_chunks_until_empty() {
    let (_dir, daemon) = start("readfd");
    let client = daemon.client();
    let data: Vec<u8> = (0..2500).map(|i| (i % 251) as u8).collect();
    client.store("f", &data).unwrap();
    let fd = client.open("f").unwrap();
    let mut got = vec![];
    let mut sizes = vec![];
    loop {
        let chunk = client.read_fd(fd, 1024).unwrap();
        if chunk.is_empty() {
            break;
        }
        sizes.push(chunk.len());
        got.extend(chunk);
    }
    assert_eq!(sizes, vec![1024, 1024, 452]);
    assert_eq!(got, data);
    assert_eq!(client.read_fd(fd, 1024).unwrap(), b"", "stays at EOF");
    client.close(fd).unwrap();
}

#[test]
fn read_line_fd_returns_lines_with_newline_then_tail_then_empty() {
    let (_dir, daemon) = start("readline");
    let client = daemon.client();
    client.store("f", &b"l1\n\nl3\nlast".to_vec()).unwrap();
    let fd = client.open("f").unwrap();
    for expected in [&b"l1\n"[..], b"\n", b"l3\n", b"last", b""] {
        assert_eq!(client.read_line_fd(fd).unwrap(), expected);
    }
    client.close(fd).unwrap();
}

#[test]
fn mixed_read_fd_and_read_line_fd_share_offset() {
    let (_dir, daemon) = start("mixed");
    let client = daemon.client();
    client.store("f", &b"abcdef\nxyz\n".to_vec()).unwrap();
    let fd = client.open("f").unwrap();
    assert_eq!(client.read_fd(fd, 2).unwrap(), b"ab");
    assert_eq!(client.read_line_fd(fd).unwrap(), b"cdef\n");
    assert_eq!(client.read_fd(fd, 100).unwrap(), b"xyz\n");
}

#[test]
fn client_fds_start_at_3_and_reuse_lowest_free() {
    let (_dir, daemon) = start("fds");
    let client = daemon.client();
    client.store("a", &b"A".to_vec()).unwrap();
    client.store("b", &b"B".to_vec()).unwrap();
    let fa = client.open("a").unwrap();
    let fb = client.open("b").unwrap();
    let fc = client.open("a").unwrap();
    assert_eq!((fa, fb, fc), (3, 4, 5));
    client.close(fa).unwrap();
    assert_eq!(client.open("b").unwrap(), 3);
    assert_eq!(client.open("b").unwrap(), 6);
}

#[test]
fn independent_opens_have_independent_offsets() {
    let (_dir, daemon) = start("offsets");
    let client = daemon.client();
    client.store("f", &b"0123456789".to_vec()).unwrap();
    let f1 = client.open("f").unwrap();
    let f2 = client.open("f").unwrap();
    assert_eq!(client.read_fd(f1, 4).unwrap(), b"0123");
    assert_eq!(client.read_fd(f2, 2).unwrap(), b"01");
    assert_eq!(client.read_fd(f1, 4).unwrap(), b"4567");
}

#[test]
fn fd_errors() {
    let (_dir, daemon) = start("fd-errors");
    let client = daemon.client();
    assert_eq!(client.open("missing"), Err(VPFSError::DoesNotExist));
    assert_eq!(client.read_fd(3, 10), Err(VPFSError::FileNotOpen));
    assert_eq!(client.read_line_fd(3), Err(VPFSError::FileNotOpen));
    assert_eq!(client.close(3), Err(VPFSError::FileNotOpen));

    client.store("f", &b"x".to_vec()).unwrap();
    let fd = client.open("f").unwrap();
    client.close(fd).unwrap();
    assert_eq!(client.close(fd), Err(VPFSError::FileNotOpen));
    assert_eq!(client.read_fd(fd, 1), Err(VPFSError::FileNotOpen));
}

/// The library maps every daemon-side open failure to `FileNotOpen`.
#[test]
fn open_with_missing_backing_file_reports_file_not_open() {
    let (_dir, daemon) = start("open-missing");
    let client = daemon.client();
    let e = client.place("f", "alpha".into()).unwrap();
    std::fs::remove_file(daemon.files_dir().join(&e.uri)).unwrap();
    assert_eq!(client.open("f"), Err(VPFSError::FileNotOpen));
}

/// Current behaviour: FIONREAD is accepted but never implemented; it returns
/// Ok(0) and leaves the argument untouched. Other requests panic.
#[test]
fn ioctl_fionread_is_a_stub() {
    let (_dir, daemon) = start("ioctl");
    let client = daemon.client();
    client.store("f", &b"12345".to_vec()).unwrap();
    let fd = client.open("f").unwrap();
    let mut arg: u64 = 777;
    assert_eq!(client.ioctl(fd, libc::FIONREAD as u64, &mut arg), Ok(0));
    assert_eq!(arg, 777);
    assert_eq!(client.ioctl(99, libc::FIONREAD as u64, &mut arg), Err(VPFSError::FileNotOpen));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut a: u64 = 0;
        client.ioctl(fd, 0x1234, &mut a)
    }));
    assert!(r.is_err(), "unsupported ioctl request panics");
}

/// C ABI entry points validate null pointers before touching the global client.
#[test]
fn c_abi_rejects_null_pointers_without_connecting() {
    unsafe {
        assert_eq!(vpfs::vpfs_open(std::ptr::null()), -1);
        assert_eq!(vpfs::vpfs_read_fd(3, std::ptr::null_mut(), 10), -1);
    }
    assert_eq!(vpfs::vpfs_ioctl(3, libc::FIONREAD as _, std::ptr::null_mut()), -1);
}

// ---------------------------------------------------------------------------
// Multiple clients / robustness
// ---------------------------------------------------------------------------

#[test]
fn concurrent_clients_share_the_namespace() {
    let (_dir, daemon) = start("multi");
    let port = daemon.listen_port;
    let handles: Vec<_> = (0..4)
        .map(|i| {
            std::thread::spawn(move || {
                let c = VPFS::connect(port).unwrap();
                c.store(&format!("f{i}"), &format!("data{i}").into_bytes()).unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let other = daemon.client();
    assert_eq!(sorted_names(other.ls("").unwrap()), vec!["f0", "f1", "f2", "f3"]);
    for i in 0..4 {
        assert_eq!(other.fetch(&format!("f{i}")).unwrap(), format!("data{i}").into_bytes());
    }
}

#[test]
fn daemon_survives_abrupt_and_malformed_clients() {
    let (_dir, mut daemon) = start("robust");
    let addr = format!("127.0.0.1:{}", daemon.listen_port);

    drop(TcpStream::connect(&addr).unwrap()); // connect + close without hello

    let mut s = TcpStream::connect(&addr).unwrap(); // hello then disconnect
    send_frame(&mut s, &Hello::ClientHello).unwrap();
    let _: vpfs::messages::HelloResponse = recv_frame(&mut s).unwrap();
    drop(s);

    let mut s = TcpStream::connect(&addr).unwrap(); // wrong hello
    send_frame(&mut s, &Hello::InitHello(Default::default())).unwrap();
    let mut buf = [0u8; 1];
    assert_eq!(s.read(&mut buf).unwrap(), 0, "daemon closes connection on unexpected hello");

    let mut s = TcpStream::connect(&addr).unwrap(); // garbage payload
    s.write_all(&4u64.to_be_bytes()).unwrap();
    s.write_all(&[0xff; 4]).unwrap();
    assert_eq!(s.read(&mut buf).unwrap(), 0);

    std::thread::sleep(Duration::from_millis(300));
    assert!(daemon.is_running());
    let c = daemon.client();
    c.store("still", &b"alive".to_vec()).unwrap();
    assert_eq!(c.fetch("still").unwrap(), b"alive");
}

/// Current behaviour: the library does not surface daemon loss as an error;
/// the next request panics.
#[test]
fn client_panics_when_daemon_goes_away() {
    let (_dir, mut daemon) = start("gone");
    let client = daemon.client();
    daemon.kill();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client.find("x")));
    assert!(r.is_err());
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

#[test]
fn log_and_vector_clock_record_creates_and_modifies() {
    let (_dir, daemon) = start("log");
    let client = daemon.client();
    let e = client.place("f", "alpha".into()).unwrap();
    client.write(e.clone(), &b"1".to_vec()).unwrap();
    client.write(e.clone(), &b"2".to_vec()).unwrap();
    // Reads, failed places and failed writes do not log.
    client.read(e.clone()).unwrap();
    let _ = client.place("f", "alpha".into());
    let _ = client.write(entry("alpha", "nope", "g"), &b"x".to_vec());

    let log = read_log(&daemon.files_dir());
    let summary: Vec<_> = log
        .iter()
        .map(|l| {
            let kind = match &l.op {
                LogOp::Create(_) => "create",
                LogOp::Modify(_) => "modify",
                LogOp::Remove(_) => "remove",
            };
            (kind, l.node.clone(), l.clock.clone())
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            ("create", "alpha".into(), clock(&[("alpha", 1)])),
            ("modify", "alpha".into(), clock(&[("alpha", 2)])),
            ("modify", "alpha".into(), clock(&[("alpha", 3)])),
        ]
    );
    assert!(log.iter().all(|l| matches!(&l.op, LogOp::Create(f) | LogOp::Modify(f) if *f == e)));
    assert_eq!(read_vector_clock(&daemon.files_dir()), clock(&[("alpha", 3)]));
    let fs = read_file_system(&daemon.files_dir());
    assert_eq!(fs.get("f"), Some(&e));
    // Legacy: the bogus write above re-bound "g" even though it failed (see failed_write_with_unknown_uri_rebinds_path).
    let g = if LEGACY { Some(entry("alpha", "nope", "g")) } else { None };
    assert_eq!(fs.get("g"), g.as_ref());
}

#[test]
fn restart_restores_namespace_contents_and_clock() {
    let dir = TempDir::new("restart");
    let e = {
        let d = Daemon::start(dir.path(), "alpha", DaemonOpts::default());
        let c = d.client();
        c.store("keep", &b"persisted".to_vec()).unwrap();
        c.find("keep").unwrap()
    };
    let d = Daemon::start(dir.path(), "alpha", DaemonOpts::default());
    assert!(d.output().contains("Running as first node"));
    let c = d.client();
    assert_eq!(c.find("keep"), Ok(e.clone()));
    assert_eq!(c.fetch("keep").unwrap(), b"persisted");

    c.write(e, &b"again".to_vec()).unwrap();
    let log = read_log(&d.files_dir());
    assert_eq!(log.len(), 3);
    assert_eq!(log[2].clock, clock(&[("alpha", 3)]), "clock continues after restart");
}

/// The daemon name is not checked against persisted state: restarting the
/// same directory under another name keeps entries owned by the old name,
/// which are then treated as remote and unreachable.
#[test]
fn restart_under_different_name_orphans_owned_files() {
    let dir = TempDir::new("rename");
    {
        let d = Daemon::start(dir.path(), "alpha", DaemonOpts::default());
        d.client().store("f", &b"x".to_vec()).unwrap();
    }
    let d = Daemon::start(dir.path(), "beta", DaemonOpts::default());
    let c = d.client();
    let e = c.find("f").unwrap();
    assert_eq!(e.owner, "alpha");
    assert_eq!(c.read(e), Err(VPFSError::NotAccessible));
}

// ---------------------------------------------------------------------------
// Client binaries
// ---------------------------------------------------------------------------

fn run(bin: &str, args: &[&str]) -> std::process::Output {
    Command::new(bin).args(args).output().unwrap()
}

#[test]
fn cat_binary_concatenates_and_numbers_lines() {
    let (_dir, daemon) = start("cat");
    let c = daemon.client();
    c.store("a", &b"one\ntwo\n".to_vec()).unwrap();
    c.store("b", &b"three".to_vec()).unwrap();
    let port = daemon.listen_port.to_string();

    let out = run(CAT, &["-p", &port, "a", "b"]);
    assert!(out.status.success());
    assert_eq!(out.stdout, b"one\ntwo\nthree");

    let out = run(CAT, &["-p", &port, "-l", "a", "b"]);
    assert_eq!(String::from_utf8(out.stdout).unwrap(), "     1\tone\n     2\ttwo\n     3\tthree");
}

#[test]
fn cat_binary_fails_on_missing_file_after_printing_earlier_ones() {
    let (_dir, daemon) = start("cat-missing");
    daemon.client().store("a", &b"A".to_vec()).unwrap();
    let out = run(CAT, &["-p", &daemon.listen_port.to_string(), "a", "missing", "a"]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(out.stdout, b"A");
    assert!(String::from_utf8_lossy(&out.stderr).contains("DoesNotExist"));
}

#[test]
fn cat2_binary_flags() {
    let (_dir, daemon) = start("cat2");
    daemon.client().store("f", &b"a\tb\n\n\n\nc\x01\n".to_vec()).unwrap();
    daemon.client().store("g", &b"x\n".to_vec()).unwrap();
    let port = daemon.listen_port.to_string();
    let cat2 = |flags: &[&str]| {
        let mut args = vec!["-p", port.as_str()];
        args.extend_from_slice(flags);
        args.push("f");
        let out = run(CAT2, &args);
        assert!(out.status.success(), "{:?}", out);
        String::from_utf8(out.stdout).unwrap()
    };

    assert_eq!(cat2(&[]), "a\tb\n\n\n\nc\x01\n");
    assert_eq!(cat2(&["-n"]), "     1\ta\tb\n     2\t\n     3\t\n     4\t\n     5\tc\x01\n");
    assert_eq!(cat2(&["-b"]), "     1\ta\tb\n\n\n\n     2\tc\x01\n");
    assert_eq!(cat2(&["-s"]), "a\tb\n\nc\x01\n");
    assert_eq!(cat2(&["-E"]), "a\tb$\n$\n$\n$\nc\x01$\n");
    assert_eq!(cat2(&["-T"]), "a^Ib\n\n\n\nc\x01\n");
    assert_eq!(cat2(&["-v"]), "a\tb\n\n\n\nc^A\n");
    assert_eq!(cat2(&["-A"]), "a^Ib$\n$\n$\n$\nc^A$\n");
    assert_eq!(cat2(&["-e"]), "a\tb$\n$\n$\n$\nc^A$\n");
    assert_eq!(cat2(&["-t"]), "a^Ib\n\n\n\nc^A\n");
    assert_eq!(cat2(&["-s", "-n"]), "     1\ta\tb\n     2\t\n     3\tc\x01\n");

    // numbering continues across files
    let out = run(CAT2, &["-p", &port, "-n", "g", "g"]);
    assert_eq!(String::from_utf8(out.stdout).unwrap(), "     1\tx\n     2\tx\n");
}

// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

/// Run the shell with the given script on stdin (script must end with `exit`).
fn sh(daemon: &Daemon, script: &str) -> String {
    let mut child = Command::new(SH)
        .args(["-p", &daemon.listen_port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(script.as_bytes()).unwrap();
    let out = with_timeout(Duration::from_secs(30), move || child.wait_with_output().unwrap())
        .expect("shell did not exit");
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn shell_redirects_pipes_and_builtins() {
    let (_dir, daemon) = start("sh");
    let out = sh(
        &daemon,
        "echo hello world > greeting\n\
         cat < greeting\n\
         echo abc | tr a-z A-Z > upper\n\
         cat < upper | tr A-Z a-z\n\
         pwd\n\
         ls\n\
         exit\n",
    );
    assert!(out.contains("alpha:/$ "), "prompt shows node name: {out}");
    assert!(out.contains("hello world\n"));
    assert!(out.contains("abc\n"));
    assert!(out.contains("/\n"), "pwd prints root");
    assert!(out.contains("greeting alpha\n") && out.contains("upper alpha\n"), "{out}");

    let c = daemon.client();
    assert_eq!(c.fetch("greeting").unwrap(), b"hello world\n");
    assert_eq!(c.fetch("upper").unwrap(), b"ABC\n");
}

#[test]
fn shell_normalizes_redirect_paths_and_overwrites() {
    let (_dir, daemon) = start("sh-paths");
    sh(&daemon, "echo first > ./d/../f\necho second > /f\nexit\n");
    let c = daemon.client();
    assert_eq!(sorted_names(c.ls("").unwrap()), vec!["f"]);
    assert_eq!(c.fetch("f").unwrap(), b"second\n");
}

#[test]
fn shell_reports_missing_input_and_unknown_programs() {
    let (_dir, daemon) = start("sh-errors");
    let out = sh(&daemon, "cat < nothere\nno-such-program-xyz\n| x\nexit\n");
    assert!(out.contains("Could not locate \"nothere\""), "{out}");
    assert!(out.contains("Failed to run \"cat\""), "{out}");
    assert!(out.contains("Failed to run \"no-such-program-xyz\""), "{out}");
    assert!(out.contains("Syntax error, left side of pipe invalid"), "{out}");
}

/// Current behaviour: `>` on the left side of a pipe is silently replaced by the pipe.
#[test]
fn shell_output_redirect_before_pipe_is_dropped() {
    let (_dir, daemon) = start("sh-pipe-redirect");
    let out = sh(&daemon, "echo hi > lost | cat\nexit\n");
    assert!(out.contains("hi\n"));
    assert_eq!(daemon.client().find("lost"), Err(VPFSError::DoesNotExist));
}

/// Current behaviour (bug): at EOF without `exit`, `read_line` keeps returning
/// Ok(0) and the shell loops forever printing prompts.
#[test]
fn shell_spins_forever_on_eof() {
    let (_dir, daemon) = start("sh-eof");
    let mut child = Command::new(SH)
        .args(["-p", &daemon.listen_port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"pwd\n").unwrap(); // stdin now closed
    let mut stdout = child.stdout.take().unwrap();
    let mut buf = vec![0u8; 64 * 1024];
    let mut total = 0;
    while total < buf.len() {
        match stdout.read(&mut buf[total..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => total += n,
        }
    }
    let prompts = String::from_utf8_lossy(&buf[..total]).matches("alpha:/$ ").count();
    let still_running = child.try_wait().unwrap().is_none();
    let _ = child.kill();
    let _ = child.wait();
    assert!(still_running);
    assert!(prompts > 1000, "only {prompts} prompts");
}

/// Current behaviour (bug): a final line without a trailing newline makes the
/// tokenizer loop forever while pushing empty args, exhausting memory. The
/// shell is run under an address-space limit so it dies instead of the host.
#[test]
fn shell_last_line_without_newline_exhausts_memory() {
    let (_dir, daemon) = start("sh-no-newline");
    let run_limited = |input: &'static [u8]| {
        let mut cmd = Command::new(SH);
        cmd.args(["-p", &daemon.listen_port.to_string()])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            cmd.pre_exec(|| {
                let limit = libc::rlimit { rlim_cur: 512 << 20, rlim_max: 512 << 20 };
                if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn().unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        with_timeout(Duration::from_secs(60), move || child.wait().unwrap())
            .expect("shell neither finished nor crashed")
    };
    // Control: the same limit is fine for well-formed input.
    assert!(run_limited(b"pwd\nexit\n").success());
    let status = run_limited(b"pwd");
    assert!(!status.success(), "expected abnormal exit, got {status:?}");
}

// ---------------------------------------------------------------------------
// File kinds (refactor only)
// ---------------------------------------------------------------------------

#[cfg(not(feature = "legacy"))]
#[test]
fn text_files_accept_positional_edits_and_blobs_do_not() {
    use vpfs::messages::{FileKind, Mutation};
    let (_dir, daemon) = start("kinds");
    let client = daemon.client();

    let t = client.place_kind("notes.txt", "alpha".into(), FileKind::Text).unwrap();
    assert_eq!(t.kind, FileKind::Text);
    client.write(t.clone(), &b"hello".to_vec()).unwrap();
    let edits = vec![
        Mutation::InsertAt { pos: 5, data: " wörld".as_bytes().to_vec() },
        Mutation::DeleteAt { pos: 0, len: 1 },
    ];
    assert_eq!(client.mutate(t.clone(), edits), Ok("ello wörld".len()));
    assert_eq!(client.fetch("notes.txt").unwrap(), "ello wörld".as_bytes());
    // All-or-nothing: the second edit is out of range, so the first is not applied either.
    let bad = vec![Mutation::InsertAt { pos: 0, data: b"X".to_vec() }, Mutation::DeleteAt { pos: 99, len: 1 }];
    assert!(client.mutate(t.clone(), bad).is_err());
    assert_eq!(client.fetch("notes.txt").unwrap(), "ello wörld".as_bytes());

    let b = client.place("photo.bin", "alpha".into()).unwrap();
    assert_eq!(b.kind, FileKind::Blob);
    let insert = vec![Mutation::InsertAt { pos: 0, data: b"x".to_vec() }];
    assert_eq!(client.mutate(b, insert), Err(VPFSError::Unsupported(FileKind::Blob)));
}
