//! The kvgit core: everything that is bytes or protocol, shared by the
//! Python and TypeScript packages.

pub mod base64;
pub mod commit;
pub mod hash;
pub mod json;
pub mod keyset;
pub mod layout;
pub mod machine;
pub mod tree;

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
