//! Branches and tags: their names, creating them, deleting them only as
//! they were read, and the record a tag keeps.
//!
//! A tag is a branch head under the reserved `refs/tags/` namespace,
//! plus a `__tag_info__<name>` record of when it was made and what the
//! caller said about it. The branch calls refuse names in that
//! namespace, so a branch can never pass for a tag, or move one.

use crate::error::Error;
use crate::head::{Notes, Resolved, head_of};
use crate::json::Value;
use crate::layout::{
    BRANCH_HEAD_PREFIX, EMPTY_HASH, ROOT_COMMIT, TAG_BRANCH_PREFIX, branch_head, branch_head_prev,
    commit_root, commit_time, parent_commit, tag_info_key,
};
use crate::lease::{try_land, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::store::{dumps, dumps_str, loads_str, safe_loads};

/// Refuse a name that can't be a branch: the empty name, whose head no
/// listing would show, and names in the tag namespace, which would list
/// as tags and move them when moved.
pub fn check_branch_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::Invalid(
            "Branch name must be a non-empty string".into(),
        ));
    }
    if name.starts_with(TAG_BRANCH_PREFIX) {
        return Err(Error::Invalid(format!(
            "Branch name '{name}' is reserved for tags (the '{TAG_BRANCH_PREFIX}' namespace); \
             use the tag API instead"
        )));
    }
    Ok(())
}

/// Refuse a name that can't be a tag. Any non-empty name is allowed,
/// `/` included, but `%`: Python kvgit builds keys by `%`-formatting,
/// so it can't store a name that carries one.
pub fn check_tag_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::Invalid("Tag name must be a non-empty string".into()));
    }
    if name.contains('%') {
        return Err(Error::Invalid(format!(
            "Tag name must not contain '%': '{name}'"
        )));
    }
    Ok(())
}

fn tag_head(name: &str) -> String {
    branch_head(&format!("{TAG_BRANCH_PREFIX}{name}"))
}

fn tag_head_prev(name: &str) -> String {
    branch_head_prev(&format!("{TAG_BRANCH_PREFIX}{name}"))
}

/// Every branch name, sorted. Tags are not branches.
pub async fn branches(io: &Io) -> Result<Vec<String>, Error> {
    // A backup's key doesn't start with the head prefix, so the scan
    // leaves backups out.
    Ok(io
        .scan(BRANCH_HEAD_PREFIX)
        .await?
        .into_iter()
        .map(|k| k[BRANCH_HEAD_PREFIX.len()..].to_string())
        .filter(|name| !name.is_empty() && !name.starts_with(TAG_BRANCH_PREFIX))
        .collect())
}

/// Whether a branch exists: whether it has a head, damaged or not. The
/// names [`branches`] never lists never exist.
pub async fn branch_exists(io: &Io, name: &str) -> Result<bool, Error> {
    if check_branch_name(name).is_err() {
        return Ok(false);
    }
    Ok(io.get_one(&branch_head(name)).await?.is_some())
}

/// The records of the empty root commit, made the first time a store
/// gains a branch.
async fn root_commit_writes(io: &Io) -> Result<Vec<(String, Write)>, Error> {
    let created = io.now().await?;
    Ok(vec![
        (
            commit_root(ROOT_COMMIT),
            Write::Bytes(dumps_str(EMPTY_HASH.as_str())),
        ),
        (
            parent_commit(ROOT_COMMIT),
            Write::Bytes(dumps(&Value::Array(vec![]))),
        ),
        (
            commit_time(ROOT_COMMIT),
            Write::Bytes(dumps(&Value::Float(created))),
        ),
    ])
}

/// Create branch `name` at commit `target`, which may be the root
/// commit: a store without it gains it in the same write as the head.
///
/// "The commit is here" and "the head names it" are decided against one
/// lease record, so a sweep starting in between fails the write and the
/// check runs again: the outcome is a branch on a commit that loads, or
/// a refusal, never a head naming nothing. The write also drops any
/// backup under the name, which a branch just made can't have had.
pub async fn create_branch(io: &Io, name: &str, target: &str) -> Result<(), Error> {
    check_branch_name(name)?;
    let head = branch_head(name);
    loop {
        let lease = wait_for_gc(io, None).await?;
        let mut expect = vec![(head.clone(), None)];
        let mut writes = vec![(head.clone(), Write::Bytes(dumps_str(target)))];
        if io.get_one(&commit_root(target)).await?.is_none() {
            if target != ROOT_COMMIT {
                return Err(Error::UnknownCommit {
                    commit: target.to_string(),
                });
            }
            // Minted with the head, and only if nobody minted it first.
            expect.push((commit_root(ROOT_COMMIT), None));
            writes.extend(root_commit_writes(io).await?);
        }
        let cas = Cas {
            expect,
            writes,
            removes: vec![branch_head_prev(name)],
        };
        match try_land(io, lease, cas).await? {
            Some(true) => return Ok(()),
            Some(false) if io.get_one(&head).await?.is_some() => {
                return Err(Error::BranchExists {
                    branch: name.to_string(),
                });
            }
            // A sweep moved the lease, or another writer minted the
            // root commit first: decide again.
            _ => {}
        }
    }
}

/// Delete a branch: its head and its backup, in one removal expecting
/// the head as read. A branch that moved, or was deleted and made again
/// elsewhere, in between is left alone, and the call fails rather than
/// retry, since the decision was made about the head it read. One made
/// again at the same commit is indistinguishable, and goes, as with
/// git's `update-ref -d <ref> <old-oid>`.
///
/// A delete doesn't wait out a sweep: removing a root from under one
/// only means the sweep keeps what it reached until the next.
pub async fn delete_branch(io: &Io, name: &str) -> Result<(), Error> {
    check_branch_name(name)?;
    let head = branch_head(name);
    let Some(current) = io.get_one(&head).await? else {
        return Err(Error::UnknownBranch {
            branch: name.to_string(),
        });
    };
    let cas = Cas {
        expect: vec![(head.clone(), Some(current))],
        writes: vec![],
        removes: vec![head, branch_head_prev(name)],
    };
    if io.cas(cas).await? {
        Ok(())
    } else {
        Err(Error::Concurrency(format!(
            "Branch '{name}' changed while being deleted; nothing was removed"
        )))
    }
}

/// The commit a tag names, if it has one. A tag never moves, so it has
/// no backup to fall back on: one left by something that moved the tag
/// as a branch describes a move that isn't the tag's.
pub async fn resolve_tag(io: &Io, name: &str) -> Result<Option<String>, Error> {
    Ok(io
        .get_one(&tag_head(name))
        .await?
        .and_then(|raw| loads_str(&raw)))
}

/// Every tag and the commit it names, sorted by name. A tag whose
/// commit is gone is listed: leaving it out would make damage look like
/// deletion.
pub async fn tags(io: &Io) -> Result<Vec<(String, String)>, Error> {
    let prefix = tag_head("");
    let keys: Vec<String> = io
        .scan(&prefix)
        .await?
        .into_iter()
        .filter(|k| k.len() > prefix.len())
        .collect();
    let found = io.get(keys.clone()).await?;
    Ok(keys
        .into_iter()
        .filter_map(|k| {
            let commit = loads_str(found.get(&k)?)?;
            Some((k[prefix.len()..].to_string(), commit))
        })
        .collect())
}

/// What a store records about one tag.
#[derive(Clone, Debug, PartialEq)]
pub struct TagInfo {
    pub name: String,
    pub commit: String,
    /// When the tag was made, in seconds; `None` without a record.
    pub time: Option<f64>,
    /// What the caller said about it: an object, or `None`.
    pub info: Option<Value>,
    /// Its commit isn't in the store, so it keeps nothing alive.
    pub dangling: bool,
}

/// Describe one tag, or `None` if there is no such tag.
pub async fn tag_info(io: &Io, name: &str) -> Result<Option<TagInfo>, Error> {
    let Some(commit) = resolve_tag(io, name).await? else {
        return Ok(None);
    };
    let record_key = tag_info_key(name);
    let root_key = commit_root(&commit);
    let found = io.get(vec![record_key.clone(), root_key.clone()]).await?;
    let record = found.get(&record_key).and_then(|raw| safe_loads(raw));
    let field = |name: &str| record.as_ref().and_then(|r| r.get(name));
    let time = match field("time") {
        Some(Value::Int(n)) => Some(*n as f64),
        Some(Value::BigInt(digits)) => digits.parse().ok(),
        Some(Value::Float(x)) => Some(*x),
        _ => None,
    };
    let info = match field("info") {
        Some(info @ Value::Object(_)) => Some(info.clone()),
        _ => None,
    };
    Ok(Some(TagInfo {
        name: name.to_string(),
        commit,
        time,
        info,
        dangling: !found.contains_key(&root_key),
    }))
}

/// Name `target` permanently, keeping it and its ancestry alive. A tag
/// never moves: one over a taken name fails.
///
/// A tag is a GC root, so it mustn't be planted under a sweep that has
/// decided what is reachable, and the commit must still be there when
/// it lands: both are decided against one lease record. The head is
/// claimed against absence, so two writers racing one name can't both
/// win; the record lands with it, and any backup an earlier holder of
/// the name left goes in the same write.
pub async fn create_tag(
    io: &Io,
    name: &str,
    target: &str,
    info: Option<Value>,
) -> Result<(), Error> {
    check_tag_name(name)?;
    let head = tag_head(name);
    loop {
        let lease = wait_for_gc(io, None).await?;
        if io.get_one(&commit_root(target)).await?.is_none() {
            return Err(Error::UnknownCommit {
                commit: target.to_string(),
            });
        }
        // Made after the wait, so its time is when the tag lands.
        let record = Value::Object(vec![
            ("time".into(), Value::Float(io.now().await?)),
            ("info".into(), info.clone().unwrap_or(Value::Null)),
        ]);
        let cas = Cas {
            expect: vec![(head.clone(), None)],
            writes: vec![
                (head.clone(), Write::Bytes(dumps_str(target))),
                (tag_info_key(name), Write::Bytes(dumps(&record))),
            ],
            removes: vec![tag_head_prev(name)],
        };
        match try_land(io, lease, cas).await? {
            None => continue,
            Some(true) => return Ok(()),
            Some(false) => {
                return Err(Error::TagExists {
                    tag: name.to_string(),
                });
            }
        }
    }
}

/// Delete a tag: its head, any backup of it and its record, in one
/// removal expecting the head and the record as read. A tag deleted and
/// made again in between keeps its new head and record, even at the
/// same commit, since the record carries the time it landed; a tag
/// without a record is compared by its head alone. The call fails
/// rather than retry, and doesn't wait out a sweep.
pub async fn delete_tag(io: &Io, name: &str) -> Result<(), Error> {
    check_tag_name(name)?;
    let head = tag_head(name);
    let record = tag_info_key(name);
    let mut found = io.get(vec![head.clone(), record.clone()]).await?;
    let Some(current) = found.remove(&head) else {
        return Err(Error::UnknownTag {
            tag: name.to_string(),
        });
    };
    let cas = Cas {
        expect: vec![
            (head.clone(), Some(current)),
            (record.clone(), found.remove(&record)),
        ],
        writes: vec![],
        removes: vec![head, tag_head_prev(name), record],
    };
    if io.cas(cas).await? {
        Ok(())
    } else {
        Err(Error::Concurrency(format!(
            "Tag '{name}' changed while being deleted; nothing was removed"
        )))
    }
}

/// What a caller names a commit by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ref {
    Commit(String),
    Branch(String),
    Tag(String),
}

/// The commit `r` names, which must be in the store, and its root,
/// which must be a string: a reader about to open the tree must not
/// take damage for an empty one.
pub async fn resolve_ref(
    io: &Io,
    r: &Ref,
    recover: bool,
    notes: &mut Notes,
) -> Result<(String, String), Error> {
    let found = match r {
        Ref::Branch(name) => head_of(io, name, recover, notes).await?,
        Ref::Tag(name) => {
            let commit = resolve_tag(io, name)
                .await?
                .ok_or_else(|| Error::UnknownTag { tag: name.clone() })?;
            present(io, commit).await?
        }
        Ref::Commit(commit) => present(io, commit.clone()).await?,
    };
    match found.root {
        Some(root) => Ok((found.commit, root)),
        None => Err(Error::Malformed(format!(
            "commit {}'s root isn't a string",
            found.commit
        ))),
    }
}

/// `commit` and its root as stored, if it is in the store.
async fn present(io: &Io, commit: String) -> Result<Resolved, Error> {
    match io.get_one(&commit_root(&commit)).await? {
        Some(raw) => Ok(Resolved {
            root: loads_str(&raw),
            commit,
        }),
        None => Err(Error::UnknownCommit { commit }),
    }
}
