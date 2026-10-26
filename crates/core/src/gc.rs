//! Garbage collection: sweep what no live commit reaches.
//!
//! A commit is live if a branch head or a tag reaches it, if it is in
//! flight (written, not yet published), or if it is younger than the
//! caller's `min_age`, which is policy alone: how long abandoned work
//! lingers. Everything an orphan alone held goes with it: its records,
//! blobs, tree nodes and chunks. A deep sweep also scans the content
//! namespaces for what no commit refers to at all, the leftovers of a
//! crash or an interrupted write.
//!
//! A sweep runs under the store's GC lease. No commit's batch lands
//! while it is held, and every commit written but not published carries
//! an in-flight marker the sweep marks from, so a sweep is safe beside
//! writers at any `min_age`, 0 included.

use crate::error::Error;
use crate::head::{Note, Notes, resolve_head};
use crate::history::decode_parents;
use crate::json::Value;
use crate::keyset::Entry;
use crate::layout::{
    BLOB_PREFIX, BRANCH_HEAD_PREFIX, COMMIT_ROOT_PREFIX, EMPTY_HASH, GC_LEASE_KEY,
    IN_FLIGHT_PREFIX, commit_root, commit_time, info_key, parent_commit, parent_gens,
};
use crate::lease::{lease_expiry, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::store::{dumps, loads_str, safe_loads};
use crate::tree::{Shape, walk_many};
use std::collections::{BTreeSet, HashSet};

/// Where chunks live: content a value refers to beside its blob.
pub const CHUNK_PREFIX: &str = "kvgit:chunk:";

/// A sweep to run.
#[derive(Clone, Debug)]
pub struct Gc {
    /// Seconds an orphan is kept after it was made.
    pub min_age: f64,
    /// Whether to scan the content namespaces too.
    pub deep: bool,
    /// Whether to wait out a sweep holding the lease, rather than fail.
    pub wait: bool,
    /// Seconds the lease stays live: what a sweep that dies holding it
    /// costs writers, at most.
    pub lease_ttl: f64,
}

/// Run a sweep under the lease, and return how many commits it removed.
/// A sweep that outlives its lease finishes, and notes it.
pub async fn gc(io: &Io, shape: Shape, args: &Gc, notes: &mut Notes) -> Result<u64, Error> {
    let (ours, expires) = loop {
        if args.wait {
            wait_for_gc(io, None).await?;
        }
        match acquire(io, args.lease_ttl).await {
            Err(Error::GcBusy(_)) if args.wait => continue,
            taken => break taken?,
        }
    };
    // A sweep reads every live node once; leaving them in the content
    // cache would only push out what commits and reads use.
    io.pass_cache(true);
    let swept = sweep(io, shape, args.min_age, args.deep, notes).await;
    io.pass_cache(false);
    let released = release(io, &ours, expires, args.lease_ttl, notes).await;
    let removed = swept?;
    released?;
    Ok(removed)
}

/// Take the lease by compare-and-swap, or fail: the write expects
/// exactly the bytes the decision was made from, so two sweeps racing
/// one expired lease can't both win, and a live one is never taken.
/// Bytes that don't hold a numeric expiry read as expired, so damage
/// under the key can't wedge the store.
async fn acquire(io: &Io, ttl: f64) -> Result<(Vec<u8>, f64), Error> {
    let raw = io.get_one(GC_LEASE_KEY).await?;
    let now = io.wall();
    let expiry = lease_expiry(raw.as_deref());
    if expiry > now {
        let owner = raw
            .as_deref()
            .and_then(safe_loads)
            .and_then(|r| r.get("owner").cloned())
            .unwrap_or(Value::Null);
        return Err(Error::GcBusy(format!(
            "A sweep holds the GC lease (owner {}, expires in {:.1}s)",
            crate::json::dumps(&owner, false),
            expiry - now
        )));
    }
    let expires = now + ttl;
    let record = Value::Object(vec![
        ("owner".into(), Value::Str(io.owner().await?)),
        ("expires".into(), Value::Float(expires)),
    ]);
    let ours = dumps(&record);
    let cas = Cas {
        expect: vec![(GC_LEASE_KEY.to_string(), raw)],
        writes: vec![(GC_LEASE_KEY.to_string(), Write::Bytes(ours.clone()))],
        removes: vec![],
    };
    if !io.cas(cas).await? {
        return Err(Error::GcBusy(
            "Another sweep took the GC lease first".into(),
        ));
    }
    Ok((ours, expires))
}

/// Give up a lease this sweep took, if the store still holds it: our
/// record overwritten with itself expired, never deleted, so a writer
/// that read the lease before this sweep began can never match it
/// again. A lease that is no longer ours ran out and another sweep took
/// it, and must not be cleared.
async fn release(
    io: &Io,
    ours: &[u8],
    expires: f64,
    ttl: f64,
    notes: &mut Notes,
) -> Result<(), Error> {
    let overrun = io.wall() - expires;
    if overrun > 0.0 {
        notes.push(Note::LeaseOverrun { overrun, ttl });
    }
    let owner = safe_loads(ours)
        .and_then(|r| r.get("owner").cloned())
        .unwrap_or(Value::Null);
    let record = Value::Object(vec![
        ("owner".into(), owner),
        ("expires".into(), Value::Int(0)),
    ]);
    let cas = Cas {
        expect: vec![(GC_LEASE_KEY.to_string(), Some(ours.to_vec()))],
        writes: vec![(GC_LEASE_KEY.to_string(), Write::Bytes(dumps(&record)))],
        removes: vec![],
    };
    io.cas(cas).await?;
    Ok(())
}

/// What the mark phase found live.
#[derive(Default)]
struct Marks {
    commits: HashSet<String>,
    blobs: HashSet<String>,
    nodes: HashSet<String>,
    chunks: HashSet<String>,
}

impl Marks {
    /// Mark the content of the trees at `roots`, skipping subtrees
    /// already marked.
    async fn trees(&mut self, io: &Io, shape: &Shape, roots: Vec<String>) -> Result<(), Error> {
        let nodes = &self.nodes;
        let (items, visited) =
            walk_many(io.clone(), shape.clone(), roots, |h| nodes.contains(h)).await?;
        for (_, raw) in items {
            let entry = Entry::decode(&raw).map_err(Error::Malformed)?;
            self.chunks.extend(entry.chunks.iter().cloned());
            self.blobs.insert(entry.blob);
        }
        self.nodes.extend(visited);
        Ok(())
    }

    /// Mark every commit `tips` reach, then everything their trees hold.
    /// History goes first, a level of it per read, and then every tree
    /// at once, so the trees' shared structure is read once.
    async fn reached(&mut self, io: &Io, shape: &Shape, tips: Vec<String>) -> Result<(), Error> {
        let mut frontier: Vec<String> = tips
            .into_iter()
            .filter(|c| !self.commits.contains(c))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut roots = Vec::new();
        while !frontier.is_empty() {
            self.commits.extend(frontier.iter().cloned());
            let keys = frontier
                .iter()
                .flat_map(|c| [parent_commit(c), commit_root(c)])
                .collect();
            let found = io.get(keys).await?;
            let mut next = BTreeSet::new();
            for c in &frontier {
                if let Some(root) = found.get(&commit_root(c)).and_then(|r| loads_str(r)) {
                    roots.push(root);
                }
                for parent in decode_parents(found.get(&parent_commit(c)).map(Vec::as_slice))? {
                    if !self.commits.contains(&parent) {
                        next.insert(parent);
                    }
                }
            }
            frontier = next.into_iter().collect();
        }
        self.trees(io, shape, roots).await
    }
}

/// Mark and sweep. The caller holds the lease, so nothing this removes
/// can be written again underneath it.
async fn sweep(
    io: &Io,
    shape: Shape,
    min_age: f64,
    deep: bool,
    notes: &mut Notes,
) -> Result<u64, Error> {
    let cutoff = io.now().await? - min_age;
    let mut marks = Marks::default();

    // In-flight markers first, heads second. Publishing a commit moves
    // its head and drops its marker in one write, so a commit published
    // after its marker was read is under a head read later, and one
    // published before has no marker to miss; read the other way round,
    // a publish between the two reads would leave it under neither.
    let marker_keys = io.scan(IN_FLIGHT_PREFIX).await?;
    let markers = io.get(marker_keys.clone()).await?;
    let wall = io.wall();
    let mut stale = Vec::new();
    let mut in_flight = Vec::new();
    for key in marker_keys {
        let live = match markers.get(&key).and_then(|raw| safe_loads(raw)) {
            Some(Value::Int(n)) => n as f64 > wall,
            Some(Value::Float(x)) => x > wall,
            _ => false,
        };
        if live {
            in_flight.push(key[IN_FLIGHT_PREFIX.len()..].to_string());
        } else {
            // Never published nor withdrawn: an ordinary orphan now,
            // kept or not by `min_age`.
            stale.push(key);
        }
    }
    marks.reached(io, &shape, in_flight).await?;

    // Every root is a head, tags included: a tag is a head under the
    // reserved `refs/tags/` name. Heads resolve without the caller's
    // recoverer: a sweep must not decide what is live from a guess, so
    // a head that doesn't resolve marks nothing, and its commits stay
    // young orphans until `min_age` and a repair settle it.
    let mut tips = Vec::new();
    for key in io.scan(BRANCH_HEAD_PREFIX).await? {
        let branch = &key[BRANCH_HEAD_PREFIX.len()..];
        if let Some(head) = resolve_head(io, branch, false, None, notes).await? {
            tips.push(head);
        }
    }
    marks.reached(io, &shape, tips).await?;

    // Unreached commits, old enough to go or young enough to keep. One
    // with no time a number can be read from is left alone.
    let candidates: Vec<String> = io
        .scan(COMMIT_ROOT_PREFIX)
        .await?
        .into_iter()
        .map(|k| k[COMMIT_ROOT_PREFIX.len()..].to_string())
        .filter(|c| !c.is_empty() && !marks.commits.contains(c))
        .collect();
    let records = io
        .get(
            candidates
                .iter()
                .flat_map(|c| [commit_time(c), commit_root(c)])
                .collect(),
        )
        .await?;
    let mut orphans = Vec::new();
    let mut young = Vec::new();
    for c in candidates {
        let time = match records.get(&commit_time(&c)).and_then(|r| safe_loads(r)) {
            Some(Value::Int(n)) => n as f64,
            Some(Value::Float(x)) => x,
            _ => continue,
        };
        let root = records.get(&commit_root(&c)).and_then(|r| loads_str(r));
        if time < cutoff {
            orphans.push((c, root));
        } else if let Some(root) = root {
            young.push(root);
        }
    }
    // Young orphans are kept whole, so what they hold is live too.
    marks.trees(io, &shape, young).await?;

    // Everything an orphan alone holds. Content is keyed by what it
    // holds, so an orphan's blob or node may be one a live commit uses;
    // the mark phase saw every live commit, so unmarked is what makes a
    // key safe to take. A damaged orphan must not stall the sweep: its
    // content is left for a deep sweep, and its records still go.
    let mut removals: BTreeSet<String> = stale.into_iter().collect();
    let roots: Vec<String> = orphans
        .iter()
        .filter_map(|(_, r)| r.clone())
        .filter(|r| r != EMPTY_HASH.as_str())
        .collect();
    let content = match orphan_content(io, &shape, roots.clone(), &marks).await {
        Ok(content) => content,
        Err(Error::Host) => return Err(Error::Host),
        Err(_) => {
            let mut content = BTreeSet::new();
            for root in roots {
                match orphan_content(io, &shape, vec![root], &marks).await {
                    Ok(found) => content.extend(found),
                    Err(Error::Host) => return Err(Error::Host),
                    Err(_) => {}
                }
            }
            content
        }
    };
    removals.extend(content);
    for (c, _) in &orphans {
        removals.extend([
            commit_root(c),
            parent_commit(c),
            parent_gens(c),
            commit_time(c),
            info_key(c),
        ]);
    }

    if deep {
        // What no orphan's tree points at: a crash's leftovers, an
        // interrupted write, a damaged orphan's content.
        for key in io.scan(&shape.prefix).await? {
            let node = &key[shape.prefix.len()..];
            if !node.is_empty() && !marks.nodes.contains(node) {
                removals.insert(key);
            }
        }
        for key in io.scan(CHUNK_PREFIX).await? {
            let chunk = &key[CHUNK_PREFIX.len()..];
            if !chunk.is_empty() && !marks.chunks.contains(chunk) {
                removals.insert(key);
            }
        }
        for key in io.scan(BLOB_PREFIX).await? {
            if !marks.blobs.contains(&key) {
                removals.insert(key);
            }
        }
    }

    if !removals.is_empty() {
        let cas = Cas {
            expect: vec![],
            writes: vec![],
            removes: removals.into_iter().collect(),
        };
        io.cas(cas).await?;
    }
    Ok(orphans.len() as u64)
}

/// The keys of what the trees at `roots` hold that nothing live does.
async fn orphan_content(
    io: &Io,
    shape: &Shape,
    roots: Vec<String>,
    marks: &Marks,
) -> Result<BTreeSet<String>, Error> {
    let nodes = &marks.nodes;
    let (items, visited) =
        walk_many(io.clone(), shape.clone(), roots, |h| nodes.contains(h)).await?;
    let mut out = BTreeSet::new();
    for (_, raw) in items {
        let entry = Entry::decode(&raw).map_err(Error::Malformed)?;
        if !marks.blobs.contains(&entry.blob) {
            out.insert(entry.blob);
        }
        for chunk in entry.chunks {
            if !marks.chunks.contains(&chunk) {
                out.insert(format!("{CHUNK_PREFIX}{chunk}"));
            }
        }
    }
    out.extend(visited.into_iter().map(|n| format!("{}{n}", shape.prefix)));
    Ok(out)
}
