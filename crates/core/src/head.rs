//! Reading a branch's head, and healing one that is damaged.
//!
//! Reads never write. A head that is present but unusable resolves to
//! its backup, the head it held before, or failing that to the host's
//! recoverer's guess, if it has one, and the answer is forgotten; only
//! a write that has to move the head anyway heals it, as part of the
//! same compare-and-swap.

use crate::error::Error;
use crate::json::Value;
use crate::layout::{
    BRANCH_HEAD_PREFIX, COMMIT_ROOT_PREFIX, GC_LEASE_KEY, branch_head, branch_head_prev,
    commit_root, commit_time, parent_commit,
};
use crate::lease::{try_land, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::refs::check_branch_name;
use crate::store::{dumps_str, loads_str, safe_loads};
use std::collections::{HashMap, HashSet};

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

/// A head resolved: its commit, and that commit's root, read on the way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub commit: String,
    /// `None` where the stored root isn't a string.
    pub root: Option<String>,
}

/// `commit`, if it is a commit whose root is stored.
async fn present(io: &Io, commit: String) -> Result<Option<Resolved>, Error> {
    Ok(io
        .get_one(&commit_root(&commit))
        .await?
        .map(|raw| Resolved {
            commit,
            root: loads_str(&raw),
        }))
}

/// The commit `raw` names, if it is a commit whose root is stored.
async fn usable(io: &Io, raw: Option<&[u8]>) -> Result<Option<Resolved>, Error> {
    match raw.and_then(loads_str) {
        Some(commit) => present(io, commit).await,
        None => Ok(None),
    }
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
    Ok(resolve(io, branch, recover, head, notes)
        .await?
        .map(|r| r.commit))
}

/// [`resolve_head`], with the root of the commit it resolves to.
pub async fn resolve(
    io: &Io,
    branch: &str,
    recover: bool,
    head: Option<Option<Vec<u8>>>,
    notes: &mut Notes,
) -> Result<Option<Resolved>, Error> {
    let head = match head {
        Some(head) => head,
        None => io.get_one(&branch_head(branch)).await?,
    };
    // An absent head is a deleted branch, not damage: a backup that
    // outlived its branch must not bring it back.
    let Some(head) = head else { return Ok(None) };
    if let Some(found) = usable(io, Some(&head)).await? {
        return Ok(Some(found));
    }
    let prev = io.get_one(&branch_head_prev(branch)).await?;
    if let Some(found) = usable(io, prev.as_deref()).await? {
        notes.push(Note::FromBackup {
            branch: branch.to_string(),
        });
        return Ok(Some(found));
    }
    if !recover {
        return Ok(None);
    }
    // The store no longer holds the answer; the host's recoverer guesses,
    // and its guess is checked like anything else read from the store.
    let Some(guess) = io.recover(branch).await? else {
        return Ok(None);
    };
    if let Some(found) = present(io, guess.clone()).await? {
        notes.push(Note::FromRecoverer {
            branch: branch.to_string(),
        });
        return Ok(Some(found));
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

/// The head of branch `name`, which must exist and resolve: a missing
/// branch is unknown, and one whose head is damaged past recovery is
/// corrupt, since damage is not absence.
pub async fn head_of(
    io: &Io,
    name: &str,
    recover: bool,
    notes: &mut Notes,
) -> Result<Resolved, Error> {
    check_branch_name(name)?;
    let head = io.get_one(&branch_head(name)).await?;
    let present = head.is_some();
    match resolve(io, name, recover, Some(head), notes).await? {
        Some(found) => Ok(found),
        None if present => Err(Error::CorruptHead {
            branch: name.to_string(),
        }),
        None => Err(Error::UnknownBranch {
            branch: name.to_string(),
        }),
    }
}

/// Make a damaged branch's recovered head durable: resolve it as a read
/// would and write the answer back, against the damaged bytes, so a
/// head another writer fixed or advanced meanwhile is never overwritten.
/// A no-op on a healthy branch. When the write doesn't win, the branch
/// is resolved again, so the answer describes the store rather than the
/// attempt. `None` when the branch doesn't exist or nothing recovers.
pub async fn repair_head(
    io: &Io,
    branch: &str,
    recover: bool,
    notes: &mut Notes,
) -> Result<Option<String>, Error> {
    let Some(commit) = resolve_head(io, branch, recover, None, notes).await? else {
        return Ok(None);
    };
    if heal_head(io, branch, &dumps_str(&commit), notes).await? {
        return Ok(Some(commit));
    }
    resolve_head(io, branch, recover, None, notes).await
}

/// A commit's time, or 0 where it has none a number can be read from:
/// the scan runs over a store already known to be damaged.
fn time_of(raw: Option<&Vec<u8>>) -> f64 {
    match raw.and_then(|raw| safe_loads(raw)) {
        Some(Value::Int(n)) => n as f64,
        Some(Value::BigInt(digits)) => digits.parse().unwrap_or(0.0),
        Some(Value::Float(x)) => x,
        _ => 0.0,
    }
}

/// The parents a damaged store's record still names: whatever strings
/// it holds, alone or in a list.
fn parents_of(raw: Option<&Vec<u8>>) -> Vec<String> {
    match raw.and_then(|raw| safe_loads(raw)) {
        Some(Value::Str(parent)) => vec![parent],
        Some(Value::Array(items)) => items
            .into_iter()
            .filter_map(|v| match v {
                Value::Str(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// Guess a branch's head from a scan of every commit: of the commits no
/// other branch or tag reaches, the newest tip, by commit time. Ties go
/// to the smallest hash. A heuristic over a store that has lost the
/// answer, offered as a recoverer the caller opts into: a deleted
/// branch's commits are unclaimed until a sweep takes them, so it can
/// hand a damaged branch another's history, and it reads the whole
/// store.
pub async fn recover_by_commit_scan(io: &Io, branch: &str) -> Result<Option<String>, Error> {
    let commits: Vec<String> = io
        .scan(COMMIT_ROOT_PREFIX)
        .await?
        .into_iter()
        .map(|k| k[COMMIT_ROOT_PREFIX.len()..].to_string())
        .filter(|c| !c.is_empty())
        .collect();
    if commits.is_empty() {
        return Ok(None);
    }
    let times = io
        .get(commits.iter().map(|c| commit_time(c)).collect())
        .await?;
    let time: HashMap<&str, f64> = commits
        .iter()
        .map(|c| (c.as_str(), time_of(times.get(&commit_time(c)))))
        .collect();

    // Every commit a healthy head of another branch or tag reaches.
    let heads: Vec<String> = io
        .scan(BRANCH_HEAD_PREFIX)
        .await?
        .into_iter()
        .filter(|k| k.len() > BRANCH_HEAD_PREFIX.len() && k[BRANCH_HEAD_PREFIX.len()..] != *branch)
        .collect();
    let found = io.get(heads.clone()).await?;
    let tips: Vec<String> = heads
        .iter()
        .filter_map(|k| found.get(k).and_then(|raw| loads_str(raw)))
        .collect();
    let roots = io
        .get(tips.iter().map(|c| commit_root(c)).collect())
        .await?;
    let mut claimed: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = tips
        .into_iter()
        .filter(|c| roots.contains_key(&commit_root(c)))
        .collect();
    while !frontier.is_empty() {
        frontier.retain(|c| claimed.insert(c.clone()));
        let parents = io
            .get(frontier.iter().map(|c| parent_commit(c)).collect())
            .await?;
        frontier = frontier
            .iter()
            .flat_map(|c| parents_of(parents.get(&parent_commit(c))))
            .filter(|p| !claimed.contains(p))
            .collect();
    }

    let mut candidates: Vec<&String> = commits.iter().filter(|c| !claimed.contains(*c)).collect();
    if candidates.is_empty() {
        candidates = commits.iter().collect();
    }
    let parents = io
        .get(candidates.iter().map(|c| parent_commit(c)).collect())
        .await?;
    let below: HashSet<String> = candidates
        .iter()
        .flat_map(|c| parents_of(parents.get(&parent_commit(c))))
        .collect();
    let mut tips: Vec<&String> = candidates
        .iter()
        .copied()
        .filter(|c| !below.contains(*c))
        .collect();
    if tips.is_empty() {
        tips = candidates;
    }
    Ok(tips
        .into_iter()
        .max_by(|a, b| {
            time[a.as_str()]
                .total_cmp(&time[b.as_str()])
                .then_with(|| b.cmp(a))
        })
        .cloned())
}
