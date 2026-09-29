//! What happens when two concurrent operations touch the same path.
//!
//! Exactly one node per conflict is responsible for resolving it (see
//! `designated_resolver`), so two nodes never produce competing resolutions.
//! That node first tries the heuristics registered for the file's kind; if
//! none decides, the path is quarantined (changes are refused) and the
//! conflict is handed to a human through the conflict resolver.
//! The other nodes quarantine the path too and wait for the resolution entry.
//!
//! Adding a heuristic: implement `ConflictHeuristic`, list it in `heuristics_for`.

use vpfs::messages::{FileEntry, FileKind, LogEntry, LogOp};

#[derive(Clone, Debug)]
pub struct Conflict {
    pub path: String,
    /// What this node currently has.
    pub local: LogEntry,
    /// The concurrent entry that arrived.
    pub remote: LogEntry,
}

pub trait ConflictHeuristic: Sync {
    /// The version to keep, or `None` if this heuristic cannot decide.
    fn resolve(&self, conflict: &Conflict) -> Option<FileEntry>;
}

pub fn heuristics_for(kind: FileKind) -> &'static [&'static dyn ConflictHeuristic] {
    match kind {
        FileKind::Blob | FileKind::Text => &[&SameVersion],
    }
}

/// The node that must resolve `conflict`: the greatest author name. Any rule
/// works as long as every node computes the same answer.
pub fn designated_resolver(conflict: &Conflict) -> &str {
    conflict.local.node.as_str().max(conflict.remote.node.as_str())
}

/// Both sides did the same create/modify: nothing to choose.
struct SameVersion;

impl ConflictHeuristic for SameVersion {
    fn resolve(&self, c: &Conflict) -> Option<FileEntry> {
        let same = c.local.op == c.remote.op && !matches!(c.local.op, LogOp::Remove(_));
        same.then(|| c.local.op.file().clone())
    }
}
