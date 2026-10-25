//! Where storage v4 keeps everything: the keys a store holds, and the
//! fixed hashes every store shares.
//!
//! ```text
//! __kvgit_version__               the storage version stamp
//! __branch_head__<branch>         a branch's tip commit (JSON string)
//! __branch_head_prev__<branch>    the tip before it, for recovery
//! __commit_root__<commit>         a commit's keyset root
//! __parent_commit__<commit>       its parents (JSON list)
//! __parent_gens__<commit>         its parents' generations (JSON list)
//! __commit_time__<commit>         its time (a float)
//! __info__<commit>                its info, when it has some
//! __tag_info__<tag>               a tag's record
//! __inflight__<commit>            an unpublished commit's protection
//! __gc_lease__                    the lease every sweep runs under
//! kvgit:blob:<sha256>             a value's bytes, keyed by content
//! kvgit:keyset:<sha256>           a keyset node, keyed by content
//! ```
//!
//! A tag's commit pointer is a branch head under a reserved name,
//! `__branch_head__refs/tags/<tag>`, so a tagged commit is reachable
//! to any reader that walks branch heads.

use crate::hash::sha256_hex;
use std::sync::LazyLock;

pub const STORAGE_VERSION: u32 = 4;
pub const STORAGE_VERSION_KEY: &str = "__kvgit_version__";

pub const BRANCH_HEAD_PREFIX: &str = "__branch_head__";
pub const BRANCH_HEAD_PREV_PREFIX: &str = "__branch_head_prev__";
pub const COMMIT_ROOT_PREFIX: &str = "__commit_root__";
pub const PARENT_COMMIT_PREFIX: &str = "__parent_commit__";
pub const PARENT_GENS_PREFIX: &str = "__parent_gens__";
pub const COMMIT_TIME_PREFIX: &str = "__commit_time__";
pub const INFO_PREFIX: &str = "__info__";
pub const TAG_INFO_PREFIX: &str = "__tag_info__";
pub const IN_FLIGHT_PREFIX: &str = "__inflight__";
pub const GC_LEASE_KEY: &str = "__gc_lease__";
pub const BLOB_PREFIX: &str = "kvgit:blob:";
pub const KEYSET_PREFIX: &str = "kvgit:keyset:";

/// The branch-name prefix a tag's commit pointer lives under.
pub const TAG_BRANCH_PREFIX: &str = "refs/tags/";

/// Hex digits in a commit hash: SHA-256, truncated.
pub const COMMIT_HASH_LEN: usize = 40;

/// The commit every branch starts at. Fixed rather than derived, so any
/// two branches share it as an ancestor whichever kvgit made them.
pub const ROOT_COMMIT: &str = "821bf06b4dcb406ea508a4a992eadc22f29850cd";

/// The bytes of the empty keyset's root node. The node itself is never
/// written: its hash alone stands for the empty keyset.
pub const EMPTY_LEAF: &str = r#"{"items":{},"kind":"leaf"}"#;

/// The root of the empty keyset.
pub static EMPTY_HASH: LazyLock<String> = LazyLock::new(|| sha256_hex(EMPTY_LEAF.as_bytes()));

pub fn branch_head(branch: &str) -> String {
    format!("{BRANCH_HEAD_PREFIX}{branch}")
}

pub fn branch_head_prev(branch: &str) -> String {
    format!("{BRANCH_HEAD_PREV_PREFIX}{branch}")
}

pub fn commit_root(commit: &str) -> String {
    format!("{COMMIT_ROOT_PREFIX}{commit}")
}

pub fn parent_commit(commit: &str) -> String {
    format!("{PARENT_COMMIT_PREFIX}{commit}")
}

pub fn parent_gens(commit: &str) -> String {
    format!("{PARENT_GENS_PREFIX}{commit}")
}

pub fn commit_time(commit: &str) -> String {
    format!("{COMMIT_TIME_PREFIX}{commit}")
}

pub fn info_key(commit: &str) -> String {
    format!("{INFO_PREFIX}{commit}")
}

pub fn tag_info_key(tag: &str) -> String {
    format!("{TAG_INFO_PREFIX}{tag}")
}

pub fn in_flight_key(commit: &str) -> String {
    format!("{IN_FLIGHT_PREFIX}{commit}")
}

/// The key a value's bytes are stored under.
pub fn blob_key(value: &[u8]) -> String {
    format!("{BLOB_PREFIX}{}", sha256_hex(value))
}

/// The key a keyset node is stored under.
pub fn keyset_node_key(node_hash: &str) -> String {
    format!("{KEYSET_PREFIX}{node_hash}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_empty_keyset_root() {
        assert_eq!(
            *EMPTY_HASH,
            "a01262f23a252767acf71bcd9d1ca8deca5fda1519228657ef7d44990698fd30"
        );
    }

    #[test]
    fn keys() {
        assert_eq!(branch_head("main"), "__branch_head__main");
        assert_eq!(
            branch_head(&format!("{TAG_BRANCH_PREFIX}v1")),
            "__branch_head__refs/tags/v1"
        );
        assert_eq!(
            commit_root(ROOT_COMMIT),
            "__commit_root__821bf06b4dcb406ea508a4a992eadc22f29850cd"
        );
        assert_eq!(
            blob_key(b"after the lease"),
            "kvgit:blob:6f11d1e7ab7e03e0d96fc6b9bc80fd38513787cac3de28f03c894dd42ac40b1e"
        );
    }
}
