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
    /// The branch has no head: it was deleted, or never made.
    UnknownBranch { branch: String },
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
    pub fn unknown_branch(branch: &str) -> Error {
        Error::UnknownBranch {
            branch: branch.to_string(),
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
            Error::UnknownBranch { branch } => write!(f, "Branch '{branch}' has no HEAD"),
            Error::Concurrency(message) | Error::Malformed(message) => f.write_str(message),
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
