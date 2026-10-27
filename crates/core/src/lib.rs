//! The kvgit core: everything that is bytes or protocol, shared by the
//! Python and TypeScript packages.

pub mod ancestry;
pub mod base64;
pub mod cache;
pub mod commit;
pub mod error;
pub mod gc;
pub mod hash;
pub mod head;
pub mod history;
pub mod json;
pub mod keyset;
pub mod layout;
pub mod lease;
pub mod machine;
pub mod merge;
pub mod ops;
pub mod refs;
pub mod store;
pub mod synced;
pub mod text;
pub mod tree;
pub mod version;
pub mod worktree;
pub mod write;

/// The version of the core, which is the version of both packages.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_the_workspace_version() {
        assert_eq!(super::version(), env!("CARGO_PKG_VERSION"));
    }
}
