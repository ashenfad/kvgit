//! What an operation reports when it fails. Each host raises its own
//! exception class for each kind, named the same in both languages.

use crate::machine::HostFailed;
use crate::tree::TreeError;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The host failed a request; the host holds the reason and raises
    /// it.
    Host,
    /// A commit made to refuse merging found its branch moved.
    BranchMoved {
        branch: String,
        base: String,
        head: String,
    },
    /// No branch has this name.
    UnknownBranch { branch: String },
    /// The branch a commit was made on has no head any more: it was
    /// deleted while the commit was being made. The same kind as
    /// [`Error::UnknownBranch`].
    HeadGone { branch: String },
    /// The branch's head and its backup are unusable, and no recoverer
    /// named a commit.
    CorruptHead { branch: String },
    /// No tag has this name.
    UnknownTag { tag: String },
    /// No commit has this hash in the store.
    UnknownCommit { commit: String },
    /// A branch of this name exists.
    BranchExists { branch: String },
    /// A tag of this name exists.
    TagExists { tag: String },
    /// Another sweep holds the GC lease, and the caller would rather
    /// not wait.
    GcBusy(String),
    /// The caller asked for something that can't be: a name that can't
    /// be a branch or a tag, arguments of the wrong shape.
    Invalid(String),
    /// Another writer won a race this call can't merge past.
    Concurrency(String),
    /// Keys no rule resolved, sorted. A rule that failed on a key gives
    /// the reason with it; the host maps a merge function's failure
    /// back to what the function raised.
    MergeConflict {
        keys: Vec<String>,
        failures: Vec<(String, String)>,
    },
    /// Something read from the store that should be a record isn't.
    Malformed(String),
}

impl Error {
    pub fn head_gone(branch: &str) -> Error {
        Error::HeadGone {
            branch: branch.to_string(),
        }
    }
}

impl Error {
    /// The kind's name, as hosts match on it.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Host => "host",
            Error::BranchMoved { .. } => "branch_moved",
            Error::UnknownBranch { .. } | Error::HeadGone { .. } => "unknown_branch",
            Error::CorruptHead { .. } => "corrupt_head",
            Error::UnknownTag { .. } => "unknown_tag",
            Error::UnknownCommit { .. } => "unknown_commit",
            Error::BranchExists { .. } => "branch_exists",
            Error::TagExists { .. } => "tag_exists",
            Error::Concurrency(_) => "concurrency",
            Error::MergeConflict { .. } => "merge_conflict",
            Error::Malformed(_) => "malformed",
            Error::Invalid(_) => "invalid",
            Error::GcBusy(_) => "gc_busy",
        }
    }

    /// The kind's fields besides its message, by name.
    pub fn fields(&self) -> Vec<(&'static str, &str)> {
        match self {
            Error::BranchMoved { branch, base, head } => {
                vec![("branch", branch), ("base", base), ("head", head)]
            }
            Error::UnknownBranch { branch }
            | Error::HeadGone { branch }
            | Error::CorruptHead { branch }
            | Error::BranchExists { branch } => vec![("branch", branch)],
            Error::UnknownTag { tag } | Error::TagExists { tag } => vec![("tag", tag)],
            Error::UnknownCommit { commit } => vec![("commit", commit)],
            _ => vec![],
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Host => f.write_str("the host failed a request"),
            Error::BranchMoved { branch, base, head } => write!(
                f,
                "Branch '{branch}' moved from {base} to {head}; nothing was committed"
            ),
            Error::UnknownBranch { branch } => write!(f, "Branch '{branch}' does not exist"),
            Error::HeadGone { branch } => write!(f, "Branch '{branch}' has no HEAD"),
            Error::CorruptHead { branch } => {
                write!(f, "Branch '{branch}' HEAD is corrupt and unrecoverable")
            }
            Error::UnknownTag { tag } => write!(f, "Tag '{tag}' does not exist"),
            Error::UnknownCommit { commit } => write!(f, "Commit '{commit}' does not exist"),
            Error::BranchExists { branch } => write!(f, "Branch '{branch}' already exists"),
            Error::TagExists { tag } => write!(f, "Tag '{tag}' already exists"),
            Error::Concurrency(message)
            | Error::Malformed(message)
            | Error::Invalid(message)
            | Error::GcBusy(message) => f.write_str(message),
            Error::MergeConflict { keys, .. } => {
                write!(f, "Merge conflict on keys: {}", keys.join(", "))
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<HostFailed> for Error {
    fn from(_: HostFailed) -> Error {
        Error::Host
    }
}

impl From<TreeError> for Error {
    fn from(e: TreeError) -> Error {
        match e {
            TreeError::Host => Error::Host,
            other => Error::Malformed(other.to_string()),
        }
    }
}
