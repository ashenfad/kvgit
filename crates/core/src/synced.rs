//! What a branch last synced to on each remote, and publishing a head
//! a transfer brought in.
//!
//! A branch's sync head is the last commit it is known to share with a
//! remote. A remote with an id keeps its own record per branch, so one
//! branch synced with two remotes never takes one's head for the
//! other's; remotes without one share a single record per branch. A
//! store synced before per-remote records holds its head in the shared
//! record, which the first remote to read it takes up as its own.
//!
//! A branch's sync mark says it syncs somewhere: a push sets it before
//! reading the branch, and it goes with the branch's last sync head.

use crate::error::Error;
use crate::json::Value;
use crate::layout::{
    branch_head, branch_head_prev, commit_root, in_flight_key, sync_head, sync_head_at,
    sync_heads_of, sync_mark,
};
use crate::lease::{try_land, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::refs::check_branch_name;
use crate::store::{dumps, dumps_str, loads_str};

/// Whose sync head a call is about.
#[derive(Clone, Copy, Debug)]
pub enum Owner<'a> {
    /// No remote named: the branch's one sync head, whichever remote
    /// it is with.
    Any,
    /// A remote without an id: the shared record.
    Shared,
    /// The remote with this id.
    Remote(&'a str),
}

fn marked() -> Vec<u8> {
    dumps(&Value::Bool(true))
}

async fn read_head(io: &Io, key: &str) -> Result<Option<String>, Error> {
    Ok(io.get_one(key).await?.and_then(|raw| loads_str(&raw)))
}

/// The last commit `branch` is known to share with `owner`, or `None`.
///
/// A remote with no record of its own reads the shared one. With no
/// remote named, the answer is the shared record, or else the
/// per-remote record when exactly one remote has one.
pub async fn sync_head_of(
    io: &Io,
    branch: &str,
    owner: Owner<'_>,
) -> Result<Option<String>, Error> {
    let shared = sync_head(branch);
    match owner {
        Owner::Remote(id) => match read_head(io, &sync_head_at(branch, id)).await? {
            Some(own) => Ok(Some(own)),
            None => read_head(io, &shared).await,
        },
        Owner::Shared => read_head(io, &shared).await,
        Owner::Any => {
            if let Some(head) = read_head(io, &shared).await? {
                return Ok(Some(head));
            }
            match io.scan(&sync_heads_of(branch)).await?.as_slice() {
                [only] => read_head(io, only).await,
                _ => Ok(None),
            }
        }
    }
}

/// Whether `branch` has a sync head with any remote, or a push of it
/// has begun: it syncs somewhere, and wasn't forgotten since.
pub async fn has_sync_head(io: &Io, branch: &str) -> Result<bool, Error> {
    let found = io.get(vec![sync_mark(branch), sync_head(branch)]).await?;
    if !found.is_empty() {
        return Ok(true);
    }
    Ok(!io.scan(&sync_heads_of(branch)).await?.is_empty())
}

/// Mark `branch` as syncing, before a push reads it.
pub async fn mark_syncing(io: &Io, branch: &str) -> Result<(), Error> {
    let key = sync_mark(branch);
    if io.get_one(&key).await?.is_none() {
        io.cas(Cas {
            expect: vec![],
            writes: vec![(key, Write::Bytes(marked()))],
            removes: vec![],
        })
        .await?;
    }
    Ok(())
}

/// Record `commit` as the head `branch` last synced to with `owner`. A
/// remote's own record retires the shared one, so another remote can't
/// take it up.
pub async fn set_sync_head(
    io: &Io,
    branch: &str,
    commit: &str,
    owner: Owner<'_>,
) -> Result<(), Error> {
    let (key, removes) = match owner {
        Owner::Remote(id) => (sync_head_at(branch, id), vec![sync_head(branch)]),
        Owner::Any | Owner::Shared => (sync_head(branch), vec![]),
    };
    io.cas(Cas {
        expect: vec![],
        writes: vec![
            (key, Write::Bytes(dumps_str(commit))),
            (sync_mark(branch), Write::Bytes(marked())),
        ],
        removes,
    })
    .await?;
    Ok(())
}

/// The head `branch` last synced to with `owner`, as a sync reads it:
/// a shared record is taken up as the remote's own the first time a
/// remote with an id reads it, in one write, so of two remotes reading
/// it at once, one has it.
pub async fn claim_sync_head(
    io: &Io,
    branch: &str,
    owner: Owner<'_>,
) -> Result<Option<String>, Error> {
    let shared_key = sync_head(branch);
    let Owner::Remote(id) = owner else {
        return read_head(io, &shared_key).await;
    };
    let own_key = sync_head_at(branch, id);
    loop {
        if let Some(own) = read_head(io, &own_key).await? {
            return Ok(Some(own));
        }
        let Some(raw) = io.get_one(&shared_key).await? else {
            return Ok(None);
        };
        let Some(shared) = loads_str(&raw) else {
            return Ok(None);
        };
        let cas = Cas {
            expect: vec![
                (shared_key.clone(), Some(raw.clone())),
                (own_key.clone(), None),
            ],
            writes: vec![(own_key.clone(), Write::Bytes(raw))],
            removes: vec![shared_key.clone()],
        };
        if io.cas(cas).await? {
            return Ok(Some(shared));
        }
    }
}

/// Forget what `branch` last synced to with `owner`, the shared record
/// with it; with no remote named, what it last synced to everywhere.
/// The mark goes with the last head.
pub async fn clear_sync_head(io: &Io, branch: &str, owner: Owner<'_>) -> Result<(), Error> {
    let mut removes = vec![sync_head(branch)];
    match owner {
        Owner::Remote(id) => removes.push(sync_head_at(branch, id)),
        Owner::Any => removes.extend(io.scan(&sync_heads_of(branch)).await?),
        Owner::Shared => {}
    }
    io.cas(Cas {
        expect: vec![],
        writes: vec![],
        removes,
    })
    .await?;
    if io.scan(&sync_heads_of(branch)).await?.is_empty() {
        io.cas(Cas {
            expect: vec![],
            writes: vec![],
            removes: vec![sync_mark(branch)],
        })
        .await?;
    }
    Ok(())
}

/// Delete `branch` unless it syncs with some remote; whether it went.
/// The check and the delete are one write against a sync taking the
/// branch up: a push marks the branch before reading it, so either the
/// delete lands first and the push finds no branch, or the delete sees
/// the mark and keeps the branch.
pub async fn delete_branch_unless_synced(io: &Io, branch: &str) -> Result<bool, Error> {
    check_branch_name(branch)?;
    let head = branch_head(branch);
    loop {
        let Some(current) = io.get_one(&head).await? else {
            return Err(Error::UnknownBranch {
                branch: branch.to_string(),
            });
        };
        if has_sync_head(io, branch).await? {
            return Ok(false);
        }
        let cas = Cas {
            expect: vec![
                (head.clone(), Some(current)),
                (sync_mark(branch), None),
                (sync_head(branch), None),
            ],
            writes: vec![],
            removes: vec![head.clone(), branch_head_prev(branch)],
        };
        if io.cas(cas).await? {
            return Ok(true);
        }
        // it moved, or began syncing: look again
    }
}

/// Move `branch` from `expected` (`None`: create it) to `head`, with
/// its backup, while no sweep runs, dropping the in-flight markers of
/// the commits this publishes in the same write. False when the branch
/// isn't at `expected`, or exists, for a create.
pub async fn publish_head(
    io: &Io,
    branch: &str,
    expected: Option<&str>,
    head: &str,
    in_flight: &[String],
) -> Result<bool, Error> {
    check_branch_name(branch)?;
    let key = branch_head(branch);
    let expected_bytes = expected.map(dumps_str);
    let mut writes = vec![(key.clone(), Write::Bytes(dumps_str(head)))];
    let mut removes: Vec<String> = in_flight.iter().map(|c| in_flight_key(c)).collect();
    match &expected_bytes {
        None => removes.push(branch_head_prev(branch)),
        Some(bytes) => writes.push((branch_head_prev(branch), Write::Bytes(bytes.clone()))),
    }
    loop {
        let lease = wait_for_gc(io, None).await?;
        // The head must name a commit that is here when it lands.
        if io.get_one(&commit_root(head)).await?.is_none() {
            return Err(Error::UnknownCommit {
                commit: head.to_string(),
            });
        }
        let cas = Cas {
            expect: vec![(key.clone(), expected_bytes.clone())],
            writes: writes.clone(),
            removes: removes.clone(),
        };
        if let Some(landed) = try_land(io, lease, cas).await? {
            return Ok(landed);
        }
    }
}
