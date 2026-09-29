//! Write policies: how a list of `Mutation`s changes a file's content,
//! chosen by the file's `FileKind`.
//!
//! A policy is a pure function from old content to new content. The caller
//! stores the result atomically, so a list of mutations is all-or-nothing and
//! a policy can reject anything it does not accept (unsupported mutation,
//! invalid result) without side effects.
//!
//! Adding a kind: add a `FileKind` variant, implement `ContentPolicy`, map it in `policy_for`.

use vpfs::messages::{FileKind, Mutation, VPFSError};

pub trait ContentPolicy: Sync {
    fn apply(&self, content: Vec<u8>, mutations: &[Mutation]) -> Result<Vec<u8>, VPFSError>;
}

pub fn policy_for(kind: FileKind) -> &'static dyn ContentPolicy {
    match kind {
        FileKind::Blob => &BlobPolicy,
        FileKind::Text => &TextPolicy,
    }
}

/// Opaque bytes (images, archives, ...): only whole replacement.
struct BlobPolicy;

impl ContentPolicy for BlobPolicy {
    fn apply(&self, mut content: Vec<u8>, mutations: &[Mutation]) -> Result<Vec<u8>, VPFSError> {
        for m in mutations {
            match m {
                Mutation::Replace(data) => content = data.clone(),
                _ => return Err(VPFSError::Unsupported(FileKind::Blob)),
            }
        }
        Ok(content)
    }
}

/// UTF-8 text; positions count characters, and the result must stay valid UTF-8.
struct TextPolicy;

impl ContentPolicy for TextPolicy {
    fn apply(&self, content: Vec<u8>, mutations: &[Mutation]) -> Result<Vec<u8>, VPFSError> {
        let utf8 = |bytes: Vec<u8>| String::from_utf8(bytes).map_err(|_| VPFSError::Other("text is not valid UTF-8".into()));
        let mut text = utf8(content)?;
        for m in mutations {
            // Byte offset of the `pos`-th character; the end of the text is a valid position.
            let offset = |text: &str, pos: u64| {
                text.char_indices().map(|(i, _)| i).chain([text.len()]).nth(pos as usize)
                    .ok_or_else(|| VPFSError::Other(format!("position {pos} is past the end of the text")))
            };
            match m {
                Mutation::Replace(data) => text = utf8(data.clone())?,
                Mutation::InsertAt { pos, data } => {
                    let at = offset(&text, *pos)?;
                    text.insert_str(at, &utf8(data.clone())?);
                }
                Mutation::DeleteAt { pos, len } => {
                    let start = offset(&text, *pos)?;
                    let end = offset(&text, pos + len)?;
                    text.replace_range(start..end, "");
                }
            }
        }
        Ok(text.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(content: &str, mutations: &[Mutation]) -> Result<String, VPFSError> {
        policy_for(FileKind::Text).apply(content.as_bytes().to_vec(), mutations).map(|b| String::from_utf8(b).unwrap())
    }

    #[test]
    fn blob_accepts_only_replace() {
        let blob = policy_for(FileKind::Blob);
        assert_eq!(blob.apply(b"old".to_vec(), &[Mutation::Replace(vec![0xff, 0])]), Ok(vec![0xff, 0]));
        let insert = Mutation::InsertAt { pos: 0, data: b"x".to_vec() };
        assert_eq!(blob.apply(b"old".to_vec(), &[insert]), Err(VPFSError::Unsupported(FileKind::Blob)));
    }

    #[test]
    fn text_insert_and_delete_count_characters() {
        let ins = |pos, s: &str| Mutation::InsertAt { pos, data: s.as_bytes().to_vec() };
        assert_eq!(text("héllo", &[ins(2, "XY")]), Ok("héXYllo".into()));
        assert_eq!(text("héllo", &[ins(5, "!")]), Ok("héllo!".into()));
        assert_eq!(text("héllo", &[Mutation::DeleteAt { pos: 1, len: 3 }]), Ok("ho".into()));
        assert_eq!(text("ab", &[ins(0, "x"), Mutation::DeleteAt { pos: 2, len: 1 }]), Ok("xa".into()));
    }

    #[test]
    fn text_rejects_out_of_range_and_invalid_utf8() {
        assert!(text("ab", &[Mutation::InsertAt { pos: 3, data: b"x".to_vec() }]).is_err());
        assert!(text("ab", &[Mutation::DeleteAt { pos: 1, len: 5 }]).is_err());
        assert!(text("ab", &[Mutation::InsertAt { pos: 0, data: vec![0xff] }]).is_err());
        assert!(policy_for(FileKind::Text).apply(vec![0xff], &[]).is_err());
    }
}
