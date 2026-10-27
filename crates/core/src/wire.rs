//! Commits as they travel between stores: the walk that sends the
//! commits one store has and another lacks, and the replay that writes
//! them, checking every hash before anything lands.
//!
//! A commit travels as its parents, its records, and its keyset's
//! changes from its first parent's: each key whose entry differs, with
//! the value's blob, and each key removed. Tree nodes never travel; the
//! replay rebuilds the keyset over the first parent's, so the commit
//! hash, recomputed from the rebuilt root and the recorded time and
//! info, checks the whole commit.

use crate::ancestry;
use crate::error::Error;
use crate::history::generations;
use crate::json::Value;
use crate::keyset::{self, Entry};
use crate::layout::{
    EMPTY_HASH, ROOT_COMMIT, commit_root, commit_time, in_flight_key, info_key, parent_commit,
    parent_gens,
};
use crate::lease::{IN_FLIGHT_TTL, try_land, under_lease, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::store::{dumps, dumps_str, loads_str, safe_loads};
use crate::tree;
use std::collections::{BTreeMap, HashMap};

/// Wire commits per emitted part.
pub const WIRE_BATCH: usize = 64;

/// A commit's time as the walk orders by it: seconds, or 0 for a time
/// that isn't a number.
fn seconds(raw: Option<&Vec<u8>>) -> f64 {
    match raw.and_then(|r| safe_loads(r)) {
        Some(Value::Float(t)) => t,
        Some(Value::Int(t)) => t as f64,
        _ => 0.0,
    }
}

fn text(raw: &[u8]) -> Value {
    Value::Str(String::from_utf8_lossy(raw).into_owned())
}

/// Emit the commits reachable from `want` and from none of `have`,
/// parents before children, in parts of [`WIRE_BATCH`]. Of the commits
/// ready to send, the oldest goes first (by time, then hash), so a
/// history always walks in the same order.
///
/// Each is `{hash, parents, parent_gens, time, info, updates,
/// removals}`: `time` and `info` are the records' text, `info` null
/// when the commit has none, and `updates` is `[key, blob]` for each
/// key whose entry differs from the first parent's.
pub async fn walk(io: &Io, want: &str, have: &[String]) -> Result<(), Error> {
    let delta = ancestry::delta(io, want, have).await?;
    let times = io
        .get(delta.iter().map(|(c, _)| commit_time(c)).collect())
        .await?;
    let parents: HashMap<&str, &[String]> = delta
        .iter()
        .map(|(c, ps)| (c.as_str(), ps.as_slice()))
        .collect();
    let age = |c: &str| (seconds(times.get(&commit_time(c))), c.to_string());

    // Kahn's algorithm, counting only parents inside the delta: the
    // rest the receiver has.
    let mut waiting: HashMap<&str, usize> = HashMap::new();
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for (c, ps) in &parents {
        let inside: Vec<&String> = ps
            .iter()
            .filter(|p| parents.contains_key(p.as_str()))
            .collect();
        for p in &inside {
            children.entry(p.as_str()).or_default().push(c);
        }
        waiting.insert(c, inside.len());
    }
    let mut ready: BTreeMap<(OrderedTime, String), &str> = waiting
        .iter()
        .filter(|(_, n)| **n == 0)
        .map(|(c, _)| (key(age(c)), *c))
        .collect();
    let mut order: Vec<&str> = Vec::with_capacity(parents.len());
    while let Some((_, c)) = ready.pop_first() {
        order.push(c);
        for child in children.get(c).map(Vec::as_slice).unwrap_or_default() {
            let n = waiting.get_mut(child).expect("a child in the delta");
            *n -= 1;
            if *n == 0 {
                ready.insert(key(age(child)), child);
            }
        }
    }
    if order.len() != parents.len() {
        return Err(Error::Malformed(format!(
            "the commit graph has a cycle ({} of {} commits ordered)",
            order.len(),
            parents.len()
        )));
    }

    for batch in order.chunks(WIRE_BATCH) {
        let mut part = Vec::with_capacity(batch.len());
        let mut keys = Vec::new();
        for c in batch {
            keys.extend([commit_root(c), info_key(c), parent_gens(c)]);
            if let Some(first) = parents[c].first() {
                keys.push(commit_root(first));
            }
        }
        let records = io.get(keys).await?;
        let root = |c: &str| -> Result<String, Error> {
            match records.get(&commit_root(c)) {
                Some(raw) => Ok(loads_str(raw).unwrap_or_else(|| EMPTY_HASH.clone())),
                None => Err(Error::UnknownCommit {
                    commit: c.to_string(),
                }),
            }
        };
        for c in batch {
            let ps = parents[c];
            let base = match ps.first() {
                Some(first) => root(first)?,
                None => EMPTY_HASH.clone(),
            };
            let diff = tree::diff_whole(io.clone(), keyset::shape(), base, root(c)?)
                .await?
                .ok_or_else(|| Error::Malformed(format!("a tree node of commit {c} is missing")))?;
            let changed = diff
                .added
                .into_iter()
                .chain(diff.modified.into_iter().map(|(k, _, new)| (k, new)));
            let mut updates = Vec::new();
            for (key, raw) in changed {
                let entry = Entry::decode(&raw).map_err(Error::Malformed)?;
                if !entry.chunks.is_empty() {
                    return Err(Error::Invalid(format!(
                        "'{key}' at {c} is chunked; chunked values don't transfer"
                    )));
                }
                updates.push(Value::Array(vec![Value::Str(key), Value::Str(entry.blob)]));
            }
            let removals = diff
                .removed
                .into_iter()
                .map(|(k, _)| Value::Str(k))
                .collect();
            let gens = match records.get(&parent_gens(c)).and_then(|r| safe_loads(r)) {
                Some(gens @ Value::Array(_)) => gens,
                _ => Value::Null,
            };
            part.push(Value::Object(vec![
                ("hash".into(), Value::Str(c.to_string())),
                (
                    "parents".into(),
                    Value::Array(ps.iter().map(|p| Value::Str(p.clone())).collect()),
                ),
                ("parent_gens".into(), gens),
                (
                    "time".into(),
                    times
                        .get(&commit_time(c))
                        .map_or(Value::Null, |raw| text(raw)),
                ),
                (
                    "info".into(),
                    records
                        .get(&info_key(c))
                        .map_or(Value::Null, |raw| text(raw)),
                ),
                ("updates".into(), Value::Array(updates)),
                ("removals".into(), Value::Array(removals)),
            ]));
        }
        io.emit(Value::Array(part)).await?;
    }
    Ok(())
}

/// A time that sorts: seconds, ordered as `f64::total_cmp` orders them.
#[derive(Clone, Copy)]
struct OrderedTime(f64);

impl PartialEq for OrderedTime {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for OrderedTime {}

impl PartialOrd for OrderedTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

fn key((time, hash): (f64, String)) -> (OrderedTime, String) {
    (OrderedTime(time), hash)
}

/// A commit as it arrives: its parents, its records as the sender has
/// them, and its keyset's changes from its first parent's, each update
/// a key and the blob the receiver holds its value's bytes under.
pub struct WireCommit {
    pub hash: String,
    pub parents: Vec<String>,
    pub parent_gens: Option<Vec<u64>>,
    pub time: f64,
    pub info: Option<Value>,
    pub updates: Vec<(String, String, u64)>,
    pub removals: Vec<String>,
}

/// What a replay did with a commit.
#[derive(Debug, PartialEq, Eq)]
pub enum Replayed {
    /// Written, marked in flight.
    Applied,
    /// Already in the store; given a fresh in-flight marker.
    Present,
}

fn refused(why: String) -> Error {
    Error::Integrity(why)
}

/// Write one commit of a stream that sends parents first, marked in
/// flight, so a sweep keeps it until the head that names it lands.
///
/// A commit already here is the same commit, being content-addressed,
/// but may not be reachable (an orphan of an earlier transfer), so it
/// gets a fresh marker instead. Otherwise the keyset is rebuilt over
/// the first parent's, the commit hash recomputed from it, and the
/// parents' generations checked against the parents' own records,
/// before anything is written: a commit that fails either is refused.
pub async fn replay(io: &Io, wc: WireCommit) -> Result<Replayed, Error> {
    if remark(io, &wc.hash).await? {
        return Ok(Replayed::Present);
    }
    let unsent = |p: &str| {
        refused(format!(
            "parent {p} of {} is neither in the stream nor the store (a stream sends parents first)",
            wc.hash
        ))
    };
    let base = match wc.parents.first() {
        Some(first) => match io.get_one(&commit_root(first)).await? {
            Some(raw) => loads_str(&raw).unwrap_or_else(|| EMPTY_HASH.clone()),
            None => return Err(unsent(first)),
        },
        None => EMPTY_HASH.clone(),
    };
    let mut writes: BTreeMap<String, Write> = BTreeMap::new();
    let mut updates = Vec::with_capacity(wc.updates.len());
    for (key, blob, size) in &wc.updates {
        writes.insert(blob.clone(), Write::Held);
        updates.push((
            key.clone(),
            Entry::new(blob.clone(), *size, vec![]).encode(),
        ));
    }
    let updated = tree::update(
        io.clone(),
        keyset::shape(),
        base,
        updates,
        wc.removals.clone(),
        HashMap::new(),
    )
    .await?;

    // The root commit's hash is fixed rather than derived, so it is
    // checked by its shape: no parents, no info, an empty keyset.
    let recomputed = if wc.hash == ROOT_COMMIT {
        let empty = wc.parents.is_empty() && updated.root == *EMPTY_HASH && wc.info.is_none();
        if empty {
            ROOT_COMMIT.to_string()
        } else {
            "a root commit with content".to_string()
        }
    } else {
        let refs: Vec<&str> = wc.parents.iter().map(String::as_str).collect();
        crate::commit::commit_hash(&refs, &updated.root, wc.time, wc.info.as_ref())
    };
    if recomputed != wc.hash {
        return Err(refused(format!(
            "integrity check failed for {} (recomputed {recomputed}); refusing the commit",
            wc.hash
        )));
    }

    // The hash doesn't cover parent generations, so they are checked
    // against the parents' own records: commits made on this one count
    // from them.
    let expected = if wc.parents.is_empty() {
        None
    } else {
        let present = io
            .get(wc.parents.iter().map(|p| parent_commit(p)).collect())
            .await?;
        if let Some(p) = wc
            .parents
            .iter()
            .find(|p| !present.contains_key(&parent_commit(p)))
        {
            return Err(unsent(p));
        }
        Some(generations(io, &wc.parents, &mut HashMap::new()).await?)
    };
    if expected != wc.parent_gens {
        let show = |g: &Option<Vec<u64>>| match g {
            None => "null".to_string(),
            Some(g) => format!("{g:?}"),
        };
        return Err(refused(format!(
            "parent generations {} for {} do not match its parents' {}; refusing the commit",
            show(&wc.parent_gens),
            wc.hash,
            show(&expected)
        )));
    }

    // One batch, as a commit is written: blobs, nodes, records.
    let prefix = keyset::shape().prefix;
    for (hash, bytes) in updated.nodes {
        writes.insert(format!("{prefix}{hash}"), Write::Bytes(bytes));
    }
    let strings =
        |items: &[String]| Value::Array(items.iter().map(|s| Value::Str(s.clone())).collect());
    writes.insert(
        commit_root(&wc.hash),
        Write::Bytes(dumps_str(&updated.root)),
    );
    writes.insert(
        parent_commit(&wc.hash),
        Write::Bytes(dumps(&strings(&wc.parents))),
    );
    if let Some(gens) = &wc.parent_gens {
        let gens = Value::Array(gens.iter().map(|g| Value::Int(*g as i64)).collect());
        writes.insert(parent_gens(&wc.hash), Write::Bytes(dumps(&gens)));
    }
    writes.insert(
        commit_time(&wc.hash),
        Write::Bytes(dumps(&Value::Float(wc.time))),
    );
    if let Some(info) = &wc.info {
        writes.insert(info_key(&wc.hash), Write::Bytes(dumps(info)));
    }
    // Landed while no sweep runs, marked in flight: a commit no head
    // names yet must not be swept before one does.
    loop {
        let lease = wait_for_gc(io, None).await?;
        let mut batch = writes.clone();
        batch.insert(
            in_flight_key(&wc.hash),
            Write::Bytes(dumps(&Value::Float(io.wall() + IN_FLIGHT_TTL))),
        );
        let cas = Cas {
            expect: vec![],
            writes: batch.into_iter().collect(),
            removes: vec![],
        };
        if io.cas(under_lease(lease, cas)).await? {
            return Ok(Replayed::Applied);
        }
    }
}

/// Give a commit already in the store a fresh in-flight marker, while
/// no sweep runs and only while its root is still there. False when
/// the store lacks it, or a sweep took it meanwhile, so the caller
/// writes it in full.
async fn remark(io: &Io, commit: &str) -> Result<bool, Error> {
    loop {
        let lease = wait_for_gc(io, None).await?;
        let Some(root) = io.get_one(&commit_root(commit)).await? else {
            return Ok(false);
        };
        let marker = dumps(&Value::Float(io.wall() + IN_FLIGHT_TTL));
        let cas = Cas {
            expect: vec![(commit_root(commit), Some(root))],
            writes: vec![(in_flight_key(commit), Write::Bytes(marker))],
            removes: vec![],
        };
        if let Some(landed) = try_land(io, lease, cas).await? {
            return Ok(landed);
        }
    }
}
