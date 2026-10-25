//! Commit history: each commit's parents, its generation, and the
//! lowest common ancestor of two commits.
//!
//! A commit's generation is one more than its highest parent's, and 0
//! for a commit with no parents, so a commit's generation is above all
//! of its ancestors'. A commit stores its parents' generations beside
//! its parents, so a search that reads one record learns where each
//! parent falls. Commits written before generations were stored have no
//! such record; a search that reaches one walks whole histories instead.

use crate::error::Error;
use crate::json::{Value, loads};
use crate::layout::{parent_commit, parent_gens};
use crate::machine::Io;
use crate::store::safe_loads;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

/// A commit's parents, and their generations if the commit stores them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub parents: Vec<String>,
    pub gens: Option<Vec<u64>>,
}

impl Record {
    /// The commit's generation; `None` if it stores none.
    pub fn generation(&self) -> Option<u64> {
        if self.parents.is_empty() {
            return Some(0);
        }
        self.gens
            .as_ref()
            .map(|g| 1 + g.iter().max().copied().unwrap_or(0))
    }
}

/// A commit's parents, in order; none for a root or unknown commit.
pub fn decode_parents(raw: Option<&[u8]>) -> Result<Vec<String>, Error> {
    let Some(raw) = raw else { return Ok(vec![]) };
    let malformed = || Error::Malformed(format!("parents {:?}", String::from_utf8_lossy(raw)));
    match loads(raw).map_err(|_| malformed())? {
        Value::Null => Ok(vec![]),
        Value::Str(parent) => Ok(vec![parent]),
        Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Str(s) => Ok(s),
                _ => Err(malformed()),
            })
            .collect(),
        _ => Err(malformed()),
    }
}

/// A record from its two stored values. Generations that don't match
/// the parents one for one, as non-negative integers, count as none.
pub fn decode_record(parents_raw: Option<&[u8]>, gens_raw: Option<&[u8]>) -> Result<Record, Error> {
    let parents = decode_parents(parents_raw)?;
    let gens = match gens_raw.and_then(safe_loads) {
        Some(Value::Array(items)) if items.len() == parents.len() => items
            .iter()
            .map(|g| match g {
                Value::Int(n) => u64::try_from(*n).ok(),
                _ => None,
            })
            .collect(),
        _ => None,
    };
    Ok(Record { parents, gens })
}

/// The records of several commits, in one read.
pub async fn load_records(io: &Io, commits: &[String]) -> Result<HashMap<String, Record>, Error> {
    let mut unique: Vec<&String> = Vec::new();
    let mut seen = HashSet::new();
    for c in commits {
        if seen.insert(c) {
            unique.push(c);
        }
    }
    let keys = unique
        .iter()
        .flat_map(|c| [parent_commit(c), parent_gens(c)])
        .collect();
    let found = io.get(keys).await?;
    unique
        .into_iter()
        .map(|c| {
            let record = decode_record(
                found.get(&parent_commit(c)).map(Vec::as_slice),
                found.get(&parent_gens(c)).map(Vec::as_slice),
            )?;
            Ok((c.clone(), record))
        })
        .collect()
}

/// A commit's parents.
pub async fn load_parents(io: &Io, commit: &str) -> Result<Vec<String>, Error> {
    let raw = io.get_one(&parent_commit(commit)).await?;
    decode_parents(raw.as_deref())
}

/// The generation of each commit, reading as little as it can, and
/// recording each one worked out in `known`. A commit that stores its
/// parents' generations costs its own record; one written before
/// generations were stored costs a walk down its history to commits
/// that store them, or to roots, one read per level.
pub async fn generations(
    io: &Io,
    commits: &[String],
    known: &mut HashMap<String, u64>,
) -> Result<Vec<u64>, Error> {
    let mut records: HashMap<String, Record> = HashMap::new();
    let mut frontier: Vec<String> = commits
        .iter()
        .filter(|c| !known.contains_key(*c))
        .cloned()
        .collect();
    while !frontier.is_empty() {
        records.extend(load_records(io, &frontier).await?);
        let mut next: Vec<String> = Vec::new();
        for c in &frontier {
            if let Some(g) = records[c].generation() {
                known.insert(c.clone(), g);
                continue;
            }
            for parent in &records[c].parents {
                if !known.contains_key(parent)
                    && !records.contains_key(parent)
                    && !next.contains(parent)
                {
                    next.push(parent.clone());
                }
            }
        }
        frontier = next;
    }
    // What is left is history without stored generations: fill it in
    // from the parents up, deepest first.
    for c in commits {
        let mut stack = vec![c.clone()];
        while let Some(node) = stack.last().cloned() {
            if known.contains_key(&node) {
                stack.pop();
                continue;
            }
            let parents = &records[&node].parents;
            let missing: Vec<String> = parents
                .iter()
                .filter(|p| !known.contains_key(*p))
                .cloned()
                .collect();
            if !missing.is_empty() {
                stack.extend(missing);
                continue;
            }
            let g = parents.iter().map(|p| known[p] + 1).max().unwrap_or(0);
            known.insert(node, g);
            stack.pop();
        }
    }
    Ok(commits.iter().map(|c| known[c]).collect())
}

/// The lowest common ancestor of two commits, or `None` if they share
/// no history. When several tie for lowest (criss-cross histories), the
/// smallest hash wins: deterministic, if arbitrary. The generations the
/// search works out are recorded in `known`.
pub async fn merge_base(
    io: &Io,
    a: &str,
    b: &str,
    known: &mut HashMap<String, u64>,
) -> Result<Option<String>, Error> {
    if a == b {
        return Ok(Some(a.to_string()));
    }
    match by_generation(io, a, b, known).await? {
        Search::Found(base) => Ok(base),
        Search::NoGenerations => by_walk(io, a, b).await,
    }
}

enum Search {
    Found(Option<String>),
    /// The search reached history written before generations were
    /// stored, or records that disagree about them.
    NoGenerations,
}

const FROM_A: u8 = 1;
const FROM_B: u8 = 2;
const STALE: u8 = 4;
const FROM_BOTH: u8 = FROM_A | FROM_B;

/// Search down from both commits at once, highest generation first,
/// every commit of one generation in one read. A commit is visited only
/// after every descendant the search reached, so the sides it was
/// reached from are final when it is visited. Reached from both, it is
/// a common ancestor, and a lowest one unless it was reached through a
/// common ancestor already found: finding one marks everything below it
/// stale. The search stops once every commit still queued is stale; for
/// two tips that forked recently, that is a few reads whatever the
/// length of the history below.
async fn by_generation(
    io: &Io,
    a: &str,
    b: &str,
    known: &mut HashMap<String, u64>,
) -> Result<Search, Error> {
    let mut records = load_records(io, &[a.to_string(), b.to_string()]).await?;
    let mut gens: HashMap<String, u64> = HashMap::new();
    for c in [a, b] {
        let Some(g) = records[c].generation() else {
            return Ok(Search::NoGenerations);
        };
        gens.insert(c.to_string(), g);
    }
    known.extend(gens.iter().map(|(c, g)| (c.clone(), *g)));
    let mut flags: HashMap<String, u8> =
        HashMap::from([(a.to_string(), FROM_A), (b.to_string(), FROM_B)]);
    let mut levels: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for c in [a, b] {
        levels.entry(gens[c]).or_default().insert(c.to_string());
    }
    let mut active: HashSet<String> = HashSet::from([a.to_string(), b.to_string()]);
    let mut found: Vec<String> = Vec::new();
    while !active.is_empty() {
        let Some((level_gen, level)) = levels.pop_last() else {
            break;
        };
        let unread: Vec<String> = level
            .iter()
            .filter(|c| !records.contains_key(*c))
            .cloned()
            .collect();
        if !unread.is_empty() {
            records.extend(load_records(io, &unread).await?);
        }
        for c in &level {
            active.remove(c);
            let mut f = flags[c];
            if f & FROM_BOTH == FROM_BOTH && f & STALE == 0 {
                found.push(c.clone());
                f |= STALE;
                flags.insert(c.clone(), f);
            }
            let record = &records[c];
            if record.parents.is_empty() {
                continue;
            }
            let Some(parent_gens) = &record.gens else {
                return Ok(Search::NoGenerations);
            };
            for (parent, &parent_gen) in record.parents.iter().zip(parent_gens) {
                if parent_gen >= level_gen {
                    // Records that disagree: search the slow way.
                    return Ok(Search::NoGenerations);
                }
                let before = flags.get(parent).copied().unwrap_or(0);
                let after = before | f;
                if after == before {
                    continue;
                }
                flags.insert(parent.clone(), after);
                if !gens.contains_key(parent) {
                    gens.insert(parent.clone(), parent_gen);
                    levels.entry(parent_gen).or_default().insert(parent.clone());
                }
                if after & STALE != 0 {
                    active.remove(parent);
                } else {
                    active.insert(parent.clone());
                }
            }
        }
    }
    known.extend(gens);
    Ok(Search::Found(found.into_iter().min()))
}

/// All ancestors of `start`, itself included, recording each visited
/// commit's parents in `parents`.
async fn ancestors(
    io: &Io,
    start: &str,
    parents: &mut HashMap<String, Vec<String>>,
) -> Result<HashSet<String>, Error> {
    let mut found = HashSet::from([start.to_string()]);
    let mut stack = vec![start.to_string()];
    while let Some(current) = stack.pop() {
        if parents.contains_key(&current) {
            continue;
        }
        let ps = load_parents(io, &current).await?;
        for p in &ps {
            if found.insert(p.clone()) {
                stack.push(p.clone());
            }
        }
        parents.insert(current, ps);
    }
    Ok(found)
}

/// The lowest common ancestor from the two commits' whole histories,
/// for history written before generations were stored: the common
/// ancestors, less any that has another below it.
async fn by_walk(io: &Io, a: &str, b: &str) -> Result<Option<String>, Error> {
    let mut parents: HashMap<String, Vec<String>> = HashMap::new();
    let of_a = ancestors(io, a, &mut parents).await?;
    // One inside the other's history is the lowest outright.
    if of_a.contains(b) {
        return Ok(Some(b.to_string()));
    }
    let of_b = ancestors(io, b, &mut parents).await?;
    if of_b.contains(a) {
        return Ok(Some(a.to_string()));
    }
    let common: HashSet<&String> = of_a.intersection(&of_b).collect();
    if common.is_empty() {
        return Ok(None);
    }
    // One pass from the tips down: a commit has a candidate at or below
    // it when one of its children is, or has, one.
    let mut children: HashMap<&String, Vec<&String>> =
        parents.keys().map(|n| (n, vec![])).collect();
    for (node, ps) in &parents {
        for p in ps {
            if let Some(kids) = children.get_mut(p) {
                kids.push(node);
            }
        }
    }
    let mut below: HashMap<&String, bool> = parents.keys().map(|n| (n, false)).collect();
    let mut remaining: HashMap<&String, usize> =
        children.iter().map(|(n, k)| (*n, k.len())).collect();
    let mut queue: VecDeque<&String> = children
        .iter()
        .filter(|(_, kids)| kids.is_empty())
        .map(|(n, _)| *n)
        .collect();
    while let Some(node) = queue.pop_front() {
        let marks = common.contains(node) || below[node];
        for p in &parents[node] {
            if marks {
                below.insert(p, true);
            }
            let left = remaining.get_mut(p).expect("every parent was walked");
            *left -= 1;
            if *left == 0 {
                queue.push_back(p);
            }
        }
    }
    Ok(common.into_iter().filter(|c| !below[*c]).min().cloned())
}
