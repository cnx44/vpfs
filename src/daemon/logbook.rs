//! Operation log and vector clock. They always change together, so they live
//! in one struct (the old code locked them separately, in opposite orders).
//!
//! Besides the log, it tracks the *head* of every path: the entry whose
//! operation the namespace currently reflects. Incoming entries are compared
//! against the head to decide whether they are old news, newer, or concurrent.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use vpfs::messages::{Clock, LogEntry, LogOp};

/// `a` happened before `b`: every component of `a` is <= `b`, and `b` is not equal to `a`.
pub fn happens_before(a: &Clock, b: &Clock) -> bool {
    let all_le = a.iter().all(|(k, v)| *v <= *b.get(k).unwrap_or(&0));
    let b_greater = b.iter().any(|(k, v)| *v > *a.get(k).unwrap_or(&0));
    all_le && b_greater
}

/// Component-wise maximum, in place.
pub fn merge_clock(into: &mut Clock, from: &Clock) {
    for (k, v) in from {
        let cur = into.entry(k.clone()).or_insert(0);
        *cur = (*cur).max(*v);
    }
}

/// How an incoming entry relates to what this node already has.
#[derive(Debug, PartialEq)]
pub enum Arrival {
    /// Already in the log.
    Known,
    /// Older than the current head: goes in the log, does not change the namespace.
    Stale,
    /// Supersedes the current head (or the path has none).
    Newer,
    /// Neither is older: a conflict with the given head.
    Concurrent(LogEntry),
}

pub struct Logbook {
    me: String,
    /// Every entry this node knows, local and remote, in arrival order. Never trimmed.
    entries: Vec<LogEntry>,
    /// Component-wise max of every entry seen; our own component counts our local operations.
    clock: Clock,
    /// Path -> the entry the namespace currently reflects. Not persisted: rebuilt from the log.
    heads: HashMap<String, LogEntry>,
    /// `./files/log`.
    log_file: PathBuf,
    /// `./files/vector_clock`.
    clock_file: PathBuf,
}

impl Logbook {
    /// Load `log` and `vector_clock` from `dir` if present. Called once by `State::open`.
    /// Heads are rebuilt by replaying the log.
    pub fn open(dir: &Path, me: &str) -> Logbook {
        let mut book = Logbook {
            me: me.to_string(),
            entries: Vec::new(),
            clock: Clock::from([(me.to_string(), 0)]),
            heads: HashMap::new(),
            log_file: dir.join("log"),
            clock_file: dir.join("vector_clock"),
        };
        if let Some(saved) = fs::read(&book.clock_file).ok().and_then(|b| serde_bare::from_slice::<Clock>(&b).ok()) {
            merge_clock(&mut book.clock, &saved);
        }
        let saved: Vec<LogEntry> = fs::read(&book.log_file).ok()
            .and_then(|b| serde_bare::from_slice(&b).ok())
            .unwrap_or_default();
        for entry in saved {
            merge_clock(&mut book.clock, &entry.clock);
            // Replay: the last entry that is not older than the head becomes the head.
            let older = book.heads.get(entry.op.path()).is_some_and(|h| happens_before(&entry.clock, &h.clock));
            if !older {
                book.heads.insert(entry.op.path().to_string(), entry.clone());
            }
            book.entries.push(entry);
        }
        book
    }

    /// Current vector clock. Sent to a peer in `LogSince` so it returns what we miss.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Record a local operation: tick our clock component and append.
    /// The result supersedes everything this node has seen, so it becomes the head.
    pub fn record(&mut self, op: LogOp) -> LogEntry {
        *self.clock.entry(self.me.clone()).or_insert(0) += 1;
        let entry = LogEntry { clock: self.clock.clone(), node: self.me.clone(), op };
        self.heads.insert(entry.op.path().to_string(), entry.clone());
        self.entries.push(entry.clone());
        self.save();
        entry
    }

    /// How `entry` relates to what we have (see `Arrival`). Changes nothing.
    /// `Known` is a linear scan of the log. A different entry with the same clock is `Stale`.
    pub fn classify(&self, entry: &LogEntry) -> Arrival {
        if self.entries.contains(entry) {
            return Arrival::Known;
        }
        match self.heads.get(entry.op.path()) {
            None => Arrival::Newer,
            Some(head) if happens_before(&head.clock, &entry.clock) => Arrival::Newer,
            Some(head) if happens_before(&entry.clock, &head.clock) || head.clock == entry.clock => Arrival::Stale,
            Some(head) => Arrival::Concurrent(head.clone()),
        }
    }

    /// Append an entry received from another node.
    /// `as_head` is true only for `Newer` entries, the ones applied to the namespace.
    /// Stale and concurrent entries are logged but do not become the head.
    pub fn merge(&mut self, entry: LogEntry, as_head: bool) {
        merge_clock(&mut self.clock, &entry.clock);
        if as_head {
            self.heads.insert(entry.op.path().to_string(), entry.clone());
        }
        self.entries.push(entry);
        self.save();
    }

    /// Entries not yet seen by `clock`: those whose author's own component exceeds what `clock` records.
    /// Used to answer `LogSince`, and to push our entries to a peer after `sync_with`.
    pub fn since(&self, clock: &Clock) -> Vec<LogEntry> {
        self.entries.iter()
            .filter(|e| e.clock.get(&e.node).copied().unwrap_or(0) > clock.get(&e.node).copied().unwrap_or(0))
            .cloned()
            .collect()
    }

    /// Rewrite both files on every change (cost grows with the log). Not atomic.
    fn save(&self) {
        let log = serde_bare::to_vec(&self.entries).expect("Failed to encode log");
        fs::write(&self.log_file, log).expect("Failed to write log");
        let clock = serde_bare::to_vec(&self.clock).expect("Failed to encode vector_clock");
        fs::write(&self.clock_file, clock).expect("Failed to write vector_clock");
    }
}
