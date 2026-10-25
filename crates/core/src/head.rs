//! Reading a branch's head, and healing one that is damaged.
//!
//! Reads never write. A head that is present but unusable resolves to
//! its backup, the head it held before, or failing that to the host's
//! recoverer's guess, if it has one, and the answer is forgotten; only
//! a write that has to move the head anyway heals it, as part of the
//! same compare-and-swap.

use crate::error::Error;
use crate::layout::{GC_LEASE_KEY, branch_head, branch_head_prev, commit_root};
use crate::lease::{try_land, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::store::loads_str;

/// Something the host should tell its user about, as it logs warnings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Note {
    /// The head was unusable; its backup stood in.
    FromBackup { branch: String },
    /// The head and its backup were unusable; the recoverer's guess
    /// stood in.
    FromRecoverer { branch: String },
    /// The recoverer named something that isn't a commit in the store.
    RecovererMissed { branch: String, answer: String },
    /// A damaged head was replaced.
    Healed { branch: String },
}

/// Notes made while an operation ran.
pub type Notes = Vec<Note>;

/// The commit `raw` names, if it is a commit whose root is stored.
async fn usable(io: &Io, raw: Option<&[u8]>) -> Result<Option<String>, Error> {
    let Some(commit) = raw.and_then(loads_str) else {
        return Ok(None);
    };
    Ok(io.get_one(&commit_root(&commit)).await?.map(|_| commit))
}

/// A branch's head: the commit it names, or its backup's, or the
/// recoverer's, whichever is first a commit in the store; `None` if
/// none is, or the branch has no head. `head` is the head's bytes when
/// the caller has read them already.
pub async fn resolve_head(
    io: &Io,
    branch: &str,
    recover: bool,
    head: Option<Option<Vec<u8>>>,
    notes: &mut Notes,
) -> Result<Option<String>, Error> {
    let head = match head {
        Some(head) => head,
        None => io.get_one(&branch_head(branch)).await?,
    };
    // An absent head is a deleted branch, not damage: a backup that
    // outlived its branch must not bring it back.
    let Some(head) = head else { return Ok(None) };
    if let Some(commit) = usable(io, Some(&head)).await? {
        return Ok(Some(commit));
    }
    let prev = io.get_one(&branch_head_prev(branch)).await?;
    if let Some(commit) = usable(io, prev.as_deref()).await? {
        notes.push(Note::FromBackup {
            branch: branch.to_string(),
        });
        return Ok(Some(commit));
    }
    if !recover {
        return Ok(None);
    }
    // The store no longer holds the answer; the host's recoverer guesses,
    // and its guess is checked like anything else read from the store.
    let Some(guess) = io.recover(branch).await? else {
        return Ok(None);
    };
    if io.get_one(&commit_root(&guess)).await?.is_some() {
        notes.push(Note::FromRecoverer {
            branch: branch.to_string(),
        });
        return Ok(Some(guess));
    }
    notes.push(Note::RecovererMissed {
        branch: branch.to_string(),
        answer: guess,
    });
    Ok(None)
}

/// A branch's head as a writer based on `base` reads it, and the lease
/// record read with it. One read in the common case, where the head is
/// still `base`: the read that fetches the head fetches `base`'s root
/// too, and the lease, which the writer's batch expects. Any other
/// answer is resolved the full way. `extra` keys ride along in the same
/// read, their values returned.
pub async fn latest_head(
    io: &Io,
    branch: &str,
    base: &str,
    recover: bool,
    extra: Vec<String>,
    notes: &mut Notes,
) -> Result<Latest, Error> {
    let head_key = branch_head(branch);
    let root_key = commit_root(base);
    let mut keys = vec![head_key.clone(), GC_LEASE_KEY.to_string(), root_key.clone()];
    keys.extend(extra);
    let mut found = io.get(keys).await?;
    let lease = found.remove(GC_LEASE_KEY);
    let head_bytes = found.remove(&head_key);
    let head = if head_bytes.as_deref().and_then(loads_str).as_deref() == Some(base)
        && found.contains_key(&root_key)
    {
        Some(base.to_string())
    } else {
        resolve_head(io, branch, recover, Some(head_bytes), notes).await?
    };
    Ok(Latest { head, lease, found })
}

pub struct Latest {
    pub head: Option<String>,
    /// The lease record read with the head.
    pub lease: Option<Vec<u8>>,
    /// The values of the extra keys asked for.
    pub found: std::collections::HashMap<String, Vec<u8>>,
}

/// Replace a branch's unusable head with `recovered`, atomically.
/// `true` only when the head was damaged and this call replaced it. A
/// head that resolves moved, rather than broke; one already holding
/// `recovered` needs nothing; an absent one is a deleted branch, which
/// healing must not bring back. The checks and the write are decided
/// against one lease record, since installing a head makes a commit
/// reachable under any sweep that already decided what is.
pub async fn heal_head(
    io: &Io,
    branch: &str,
    recovered: &[u8],
    notes: &mut Notes,
) -> Result<bool, Error> {
    let key = branch_head(branch);
    loop {
        let lease = wait_for_gc(io, None).await?;
        let Some(raw) = io.get_one(&key).await? else {
            return Ok(false);
        };
        if raw == recovered || usable(io, Some(&raw)).await?.is_some() {
            return Ok(false);
        }
        let cas = Cas {
            expect: vec![(key.clone(), Some(raw))],
            writes: vec![(key.clone(), Write::Bytes(recovered.to_vec()))],
            removes: vec![],
        };
        match try_land(io, lease, cas).await? {
            None => continue,
            Some(false) => return Ok(false),
            Some(true) => {
                notes.push(Note::Healed {
                    branch: branch.to_string(),
                });
                return Ok(true);
            }
        }
    }
}
