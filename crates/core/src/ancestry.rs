//! What one commit's history holds that others' lack, and whether one
//! commit is in another's history: the questions a transfer asks.
//!
//! A search visits commits highest generation first, every commit of
//! one generation in one read. A commit's generation is above all of
//! its ancestors', so when the search visits a commit it has visited
//! every descendant it can reach, and which starting points reach the
//! commit is settled. Each question then costs about the stretch of
//! history it is about, not the history's length. History written
//! before generations were stored is walked whole.

use crate::error::Error;
use crate::history::{Record, decode_record};
use crate::layout::{parent_commit, parent_gens};
use crate::machine::Io;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// The records of several commits, in one read; `None` for a commit
/// the store lacks.
async fn load(io: &Io, commits: &[String]) -> Result<HashMap<String, Option<Record>>, Error> {
    let unique: BTreeSet<&String> = commits.iter().collect();
    let keys = unique
        .iter()
        .flat_map(|c| [parent_commit(c), parent_gens(c)])
        .collect();
    let found = io.get(keys).await?;
    unique
        .into_iter()
        .map(|c| {
            let record = match found.get(&parent_commit(c)) {
                None => None,
                Some(raw) => Some(decode_record(
                    Some(raw),
                    found.get(&parent_gens(c)).map(Vec::as_slice),
                )?),
            };
            Ok((c.clone(), record))
        })
        .collect()
}

fn missing(commit: &str) -> Error {
    Error::UnknownCommit {
        commit: commit.to_string(),
    }
}

const WANT: u8 = 1;
const HAVE: u8 = 2;

#[derive(Default)]
struct Painted {
    /// Commits reachable from `want` and not from `have`, with their
    /// parents, in the order visited.
    delta: Vec<(String, Vec<String>)>,
    /// Commits reachable from `have` that the search visited.
    had: HashSet<String>,
}

/// Search down from `want` and from `have` at once. The search stops
/// once nothing queued is reachable from `want` alone, or, with
/// `floor`, once it has also visited every commit reachable from `have`
/// down to that generation. With `first`, it stops at the first commit
/// of the delta.
///
/// A commit the store lacks is skipped when only `have` reaches it (a
/// receiver's claim this store can't check), and is an error when
/// `want` does. `None` when the search reaches history without stored
/// generations, or records that disagree about them.
async fn paint(
    io: &Io,
    want: Option<&str>,
    have: &[String],
    first: bool,
    floor: Option<u64>,
) -> Result<Option<Painted>, Error> {
    let starts: Vec<String> = have
        .iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .cloned()
        .collect();
    let mut first_read = starts.clone();
    first_read.extend(want.map(str::to_string));
    let mut records = load(io, &first_read).await?;
    let mut flags: HashMap<String, u8> = HashMap::new();
    let mut levels: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    // the queued commits reachable from `want` alone
    let mut pending: usize = 0;
    if let Some(w) = want {
        let Some(record) = &records[w] else {
            return Err(missing(w));
        };
        let Some(g) = record.generation() else {
            return Ok(None);
        };
        flags.insert(w.to_string(), WANT);
        levels.entry(g).or_default().insert(w.to_string());
        pending += 1;
    }
    for h in &starts {
        let Some(record) = &records[h] else { continue };
        let Some(g) = record.generation() else {
            return Ok(None);
        };
        let before = flags.get(h).copied().unwrap_or(0);
        if before == WANT {
            pending -= 1;
        }
        flags.insert(h.clone(), before | HAVE);
        levels.entry(g).or_default().insert(h.clone());
    }

    let mut painted = Painted::default();
    while let Some((&level_gen, _)) = levels.last_key_value() {
        if pending == 0 && floor.is_none_or(|f| level_gen < f) {
            break;
        }
        let (level_gen, level) = levels.pop_last().expect("a level is queued");
        let unread: Vec<String> = level
            .iter()
            .filter(|c| !records.contains_key(*c))
            .cloned()
            .collect();
        if !unread.is_empty() {
            records.extend(load(io, &unread).await?);
        }
        for c in &level {
            let f = flags[c];
            let record = if f == WANT {
                pending -= 1;
                let Some(record) = &records[c] else {
                    return Err(missing(c));
                };
                painted.delta.push((c.clone(), record.parents.clone()));
                if first {
                    return Ok(Some(painted));
                }
                record
            } else {
                let Some(record) = &records[c] else { continue };
                painted.had.insert(c.clone());
                record
            };
            if record.parents.is_empty() {
                continue;
            }
            let Some(gens) = &record.gens else {
                return Ok(None);
            };
            for (parent, &parent_gen) in record.parents.iter().zip(gens) {
                if parent_gen >= level_gen {
                    return Ok(None);
                }
                let before = flags.get(parent).copied();
                let after = before.unwrap_or(0) | f;
                if Some(after) == before {
                    continue;
                }
                flags.insert(parent.clone(), after);
                match before {
                    None => {
                        levels.entry(parent_gen).or_default().insert(parent.clone());
                        if after == WANT {
                            pending += 1;
                        }
                    }
                    Some(WANT) => pending -= 1,
                    Some(_) => {}
                }
            }
        }
    }
    Ok(Some(painted))
}

/// Every commit reachable from `starts`, themselves included, the whole
/// way down, one read per level; commits the store lacks are skipped.
async fn reachable(io: &Io, starts: &[String]) -> Result<HashSet<String>, Error> {
    let mut found = HashSet::new();
    let mut frontier: Vec<String> = starts.to_vec();
    while !frontier.is_empty() {
        let records = load(io, &frontier).await?;
        let mut next = Vec::new();
        for (c, record) in records {
            let Some(record) = record else { continue };
            if !found.insert(c) {
                continue;
            }
            next.extend(record.parents.into_iter().filter(|p| !found.contains(p)));
        }
        frontier = next;
    }
    Ok(found)
}

/// The commits reachable from `want` and not from any of `have`, each
/// with its parents. A commit of `have` the store lacks contributes
/// nothing; a commit on `want`'s side the store lacks is an error,
/// since a partial history can't be sent.
pub async fn delta(
    io: &Io,
    want: &str,
    have: &[String],
) -> Result<Vec<(String, Vec<String>)>, Error> {
    if let Some(painted) = paint(io, Some(want), have, false, None).await? {
        return Ok(painted.delta);
    }
    let had = reachable(io, have).await?;
    let mut delta = Vec::new();
    if had.contains(want) {
        return Ok(delta);
    }
    let mut seen: HashSet<String> = HashSet::from([want.to_string()]);
    let mut frontier = vec![want.to_string()];
    while !frontier.is_empty() {
        let records = load(io, &frontier).await?;
        let mut next = Vec::new();
        for c in frontier {
            let Some(record) = &records[&c] else {
                return Err(missing(&c));
            };
            for p in &record.parents {
                if !had.contains(p) && seen.insert(p.clone()) {
                    next.push(p.clone());
                }
            }
            delta.push((c, record.parents.clone()));
        }
        frontier = next;
    }
    Ok(delta)
}

/// Whether `ancestor` is `from` or reachable from it. A commit the
/// store lacks is reachable from nothing, and nothing is reachable from
/// it.
pub async fn is_ancestor(io: &Io, ancestor: &str, from: &str) -> Result<bool, Error> {
    let records = load(io, &[ancestor.to_string()]).await?;
    if records[ancestor].is_none() {
        return Ok(false);
    }
    if ancestor == from {
        return Ok(true);
    }
    if let Some(painted) = paint(io, Some(ancestor), &[from.to_string()], true, None).await? {
        return Ok(painted.delta.is_empty());
    }
    Ok(reachable(io, &[from.to_string()]).await?.contains(ancestor))
}

/// The commits reachable from `have`, each of them included, of
/// generation `floor` and above: everything a receiver at `have` holds
/// that a transfer of commits that high could repeat. On history
/// without generations, every commit reachable from `have`.
pub async fn above(io: &Io, have: &[String], floor: u64) -> Result<HashSet<String>, Error> {
    if let Some(painted) = paint(io, None, have, false, Some(floor)).await? {
        return Ok(painted.had);
    }
    reachable(io, have).await
}
