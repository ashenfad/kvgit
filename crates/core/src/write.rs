//! The commit, and the merges and applies a worktree makes: write
//! changes and publish them as the branch's new head.
//!
//! A commit on a branch that hasn't moved lands as a fast-forward. One
//! on a branch that has, or that loses the race to publish, merges
//! three ways onto the new head, unless it was made to refuse
//! ([`OnMoved::Raise`]).
//!
//! Every commit lands in two writes. The batch holds everything a sweep
//! could delete (blobs, chunks, tree nodes, the commit's records) and
//! the commit's in-flight marker, and lands only while no sweep runs.
//! The publish moves the head, records the head it replaced as the
//! backup and drops the markers of the commits it publishes, in one
//! compare-and-swap expecting the head the commit is based on. A commit
//! that doesn't publish withdraws its markers, leaving only orphans.

use crate::commit::commit_hash;
use crate::error::Error;
use crate::head::{Notes, heal_head, latest_head};
use crate::history::{self, decode_record, generations, merge_base};
use crate::json::Value;
use crate::keyset::Entry;
use crate::layout::{
    EMPTY_HASH, GC_LEASE_KEY, STORAGE_VERSION_KEY, branch_head, branch_head_prev, commit_root,
    commit_time, in_flight_key, info_key, parent_commit, parent_gens,
};
use crate::lease::{IN_FLIGHT_TTL, stamp_version_at_least, try_land, under_lease, wait_for_gc};
use crate::machine::{Cas, Io, Write};
use crate::merge::{Change, Merged, Rules, resolve};
use crate::store::{dumps, dumps_str, loads_str, safe_loads};
use crate::tree::{self, Shape};
use std::collections::hash_map::Entry as Slot;
use std::collections::{BTreeMap, HashMap};

/// The lowest storage version that holds chunks.
pub const CHUNK_STORAGE_VERSION: i64 = 3;
/// The lowest storage version that holds content-keyed blobs.
pub const BLOB_STORAGE_VERSION: i64 = 4;

/// What a commit does when its branch moved since its base.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnMoved {
    Merge,
    Raise,
}

/// What a commit does when it can't land: raise, or give up quietly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnConflict {
    Raise,
    Abandon,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    NoOp,
    FastForward,
    ThreeWay,
    Apply,
}

impl Strategy {
    pub fn name(self) -> &'static str {
        match self {
            Strategy::NoOp => "no_op",
            Strategy::FastForward => "fast_forward",
            Strategy::ThreeWay => "three_way",
            Strategy::Apply => "apply",
        }
    }
}

/// A commit to make.
#[derive(Clone, Debug)]
pub struct CommitArgs {
    pub branch: String,
    /// The head the worktree last saw its branch at.
    pub base: String,
    /// The commit the worktree's keyset is at.
    pub current: String,
    pub shape: Shape,
    /// Keys to set, each to an entry naming a blob the host holds the
    /// bytes of.
    pub updates: Vec<(String, Entry)>,
    pub removals: Vec<String>,
    /// Chunk keys the commit's values refer to, whose bytes the host
    /// holds.
    pub chunks: Vec<String>,
    pub info: Option<Value>,
    pub on_moved: OnMoved,
    pub on_conflict: OnConflict,
    pub rules: Rules,
    /// Whether the host has a recoverer for a head beyond repair.
    pub recover: bool,
    /// Whether this worktree has already seen the store stamped for
    /// content-keyed blobs.
    pub stamped: bool,
    /// Roots and generations the host already knows: a commit's never
    /// change, so these save reads.
    pub roots: Vec<(String, String)>,
    pub generations: Vec<(String, u64)>,
}

/// A merge to make on a worktree with nothing pending: of another
/// head into its branch, or of the change between two commits onto it.
#[derive(Clone, Debug)]
pub struct MergeArgs {
    pub branch: String,
    /// The head the worktree is at.
    pub ours: String,
    /// The head to merge, or the commit whose change to apply.
    pub theirs: String,
    /// For an apply, the commit the change is from, standing in for the
    /// common ancestor; `None` for a merge.
    pub from: Option<String>,
    /// Whether a merge of a descendant only moves the head.
    pub fast_forward: bool,
    pub shape: Shape,
    pub rules: Rules,
    pub on_conflict: OnConflict,
    pub info: Option<Value>,
    /// Whether the caller checks the values merge functions make.
    pub post_check: bool,
    pub recover: bool,
    pub stamped: bool,
    pub roots: Vec<(String, String)>,
    pub generations: Vec<(String, u64)>,
}

/// What a commit did.
#[derive(Clone, Debug, PartialEq)]
pub struct Committed {
    /// Whether it landed; `false` when it abandoned.
    pub merged: bool,
    /// The branch's head afterwards; `None` when abandoned.
    pub commit: Option<String>,
    pub strategy: Strategy,
    pub auto_merged: Vec<String>,
    pub carried: Vec<String>,
    /// The worktree's keyset afterwards, as changes to the one it
    /// committed from: an entry, or `None` for a key removed.
    pub changes: Vec<(String, Option<Entry>)>,
    /// Roots and generations of the commits it wrote.
    pub roots: Vec<(String, String)>,
    pub generations: Vec<(String, u64)>,
    /// Whether the store is now known stamped for content-keyed blobs.
    pub stamped: bool,
}

/// An operation's result, and what it noted on the way.
#[derive(Debug)]
pub struct Done<T> {
    pub result: Result<T, Error>,
    pub notes: Notes,
}

/// Commit `args`.
pub async fn commit(io: Io, args: CommitArgs) -> Done<Committed> {
    let mut w = Writer::new(&io, &args);
    let mut result = w.commit().await;
    if !matches!(result, Ok(Committed { merged: true, .. })) {
        w.roll_back().await;
    }
    if let Ok(done) = &mut result {
        done.roots = w.new_roots.clone();
        done.generations = w.new_generations.clone();
        done.stamped = w.stamped;
    }
    Done {
        result,
        notes: w.notes,
    }
}

/// Merge `args`: another head into the branch, or the change between
/// two commits onto it.
///
/// A merge of a head the branch already contains changes nothing, and
/// one of a descendant moves the head to it, unless made to write a
/// merge commit anyway; otherwise the two merge three ways from their
/// lowest common ancestor into a commit on both. An apply merges three
/// ways from the commit the change is from, into an ordinary commit on
/// ours; a change already present commits nothing. An answer of
/// nothing holds only while the branch is still at our head, so it is
/// checked against the live head. Every commit named must be in the
/// store, checked before anything else is read, since a missing one
/// would read as an empty tree.
pub async fn merge(io: Io, args: MergeArgs) -> Done<Committed> {
    let commit = CommitArgs {
        branch: args.branch.clone(),
        base: args.ours.clone(),
        current: args.ours.clone(),
        shape: args.shape.clone(),
        updates: vec![],
        removals: vec![],
        chunks: vec![],
        info: args.info.clone(),
        on_moved: OnMoved::Merge,
        on_conflict: args.on_conflict,
        rules: args.rules.clone(),
        recover: args.recover,
        stamped: args.stamped,
        roots: args.roots.clone(),
        generations: args.generations.clone(),
    };
    let mut w = Writer::new(&io, &commit);
    let mut result = w.merge(&args).await;
    if !matches!(result, Ok(Committed { merged: true, .. })) {
        w.roll_back().await;
    }
    if let Ok(done) = &mut result {
        done.roots = w.new_roots.clone();
        done.generations = w.new_generations.clone();
        done.stamped = w.stamped;
    }
    Done {
        result,
        notes: w.notes,
    }
}

/// How a three-way merge lands.
struct Onto<'p> {
    /// The common ancestor, or the commit an applied change is from.
    base: String,
    /// The merge commit's parents, in order.
    parents: Vec<&'p str>,
    /// The head the merge commit is published over.
    over: &'p str,
    strategy: Strategy,
    /// Whether a merge that changes nothing commits nothing.
    may_be_nothing: bool,
    post_check: bool,
}

/// One commit attempt and what it has written so far.
struct Writer<'a> {
    io: &'a Io,
    args: &'a CommitArgs,
    notes: Notes,
    /// The commit the worktree's keyset is at now, and its changes
    /// since the start.
    current: String,
    changes: BTreeMap<String, Option<Entry>>,
    /// Commits written and not yet published, with each one's marker.
    in_flight: Vec<(String, Vec<u8>)>,
    /// The lease record the latest batch landed against.
    lease_seen: Option<Vec<u8>>,
    /// The lease record read with the latest head, for the next batch.
    lease_hint: Option<Option<Vec<u8>>>,
    /// Extra keys read with the latest head.
    found: HashMap<String, Vec<u8>>,
    roots: HashMap<String, String>,
    gens: HashMap<String, u64>,
    new_roots: Vec<(String, String)>,
    new_generations: Vec<(String, u64)>,
    stamped: bool,
}

impl<'a> Writer<'a> {
    fn new(io: &'a Io, args: &'a CommitArgs) -> Writer<'a> {
        Writer {
            io,
            args,
            notes: Vec::new(),
            current: args.current.clone(),
            changes: BTreeMap::new(),
            in_flight: Vec::new(),
            lease_seen: None,
            lease_hint: None,
            found: HashMap::new(),
            roots: args.roots.iter().cloned().collect(),
            gens: args.generations.iter().cloned().collect(),
            new_roots: Vec::new(),
            new_generations: Vec::new(),
            stamped: args.stamped,
        }
    }

    async fn commit(&mut self) -> Result<Committed, Error> {
        let a = self.args;
        let base = a.base.as_str();
        if a.updates.is_empty() && a.removals.is_empty() && a.info.is_none() {
            // Nothing to write. A commit refusing a moved branch still
            // refuses: its base is out of date either way.
            if a.on_moved == OnMoved::Raise {
                let head = self.head(vec![]).await?;
                self.refuse_moved(head)?;
            }
            return Ok(self.landed(Strategy::NoOp, a.current.clone(), vec![], vec![]));
        }

        let head = self.first_head().await?;
        if a.on_moved == OnMoved::Raise {
            self.refuse_moved(head.clone())?;
        }
        let mut head = head;
        let mut ours_built = false;
        if head.as_deref() == Some(base) {
            let ours = self.build_ours(a.info.as_ref()).await?;
            if self.publish(base, &ours).await? {
                return Ok(self.landed(Strategy::FastForward, ours, vec![], vec![]));
            }
            if a.on_moved == OnMoved::Raise {
                self.roll_back().await;
                let head = self.head(vec![]).await?;
                self.refuse_moved(head)?;
                // The head reads as the base, yet the publish failed: it
                // moved and came back, or the commit's marker lapsed and
                // a sweep may have taken the commit.
                return Err(Error::Concurrency(
                    "Commit could not be published. Retry.".into(),
                ));
            }
            // Lost the race to another writer: merge onto the new head,
            // keeping the commit just built as our side. Rebuilding it
            // would make a second commit for the same change (the hash
            // covers the time) and leave the first behind.
            head = self.head(vec![]).await?;
            ours_built = true;
        }
        let Some(theirs) = head else {
            return Err(Error::head_gone(&a.branch));
        };
        if !ours_built {
            self.build_ours(None).await?;
        }
        self.three_way(&theirs).await
    }

    /// Refuse, for a commit made to, a head that isn't its base.
    fn refuse_moved(&self, head: Option<String>) -> Result<(), Error> {
        let a = self.args;
        match head {
            None => Err(Error::head_gone(&a.branch)),
            Some(head) if head != a.base => Err(Error::BranchMoved {
                branch: a.branch.clone(),
                base: a.base.clone(),
                head,
            }),
            Some(_) => Ok(()),
        }
    }

    fn landed(
        &self,
        strategy: Strategy,
        commit: String,
        auto_merged: Vec<String>,
        carried: Vec<String>,
    ) -> Committed {
        Committed {
            merged: true,
            commit: Some(commit),
            strategy,
            auto_merged,
            carried,
            changes: self.changes.clone().into_iter().collect(),
            roots: vec![],
            generations: vec![],
            stamped: false,
        }
    }

    fn abandoned(strategy: Strategy) -> Committed {
        Committed {
            merged: false,
            commit: None,
            strategy,
            auto_merged: vec![],
            carried: vec![],
            changes: vec![],
            roots: vec![],
            generations: vec![],
            stamped: false,
        }
    }

    /// The branch's head, and the lease read beside it.
    async fn head(&mut self, extra: Vec<String>) -> Result<Option<String>, Error> {
        let a = self.args;
        let latest = latest_head(
            self.io,
            &a.branch,
            &a.base,
            a.recover,
            extra,
            &mut self.notes,
        )
        .await?;
        self.lease_hint = Some(latest.lease);
        self.found = latest.found;
        Ok(latest.head)
    }

    /// The head, read with what the first commit needs and the host
    /// doesn't know: the current commit's root and record, and the
    /// version stamp.
    async fn first_head(&mut self) -> Result<Option<String>, Error> {
        let current = self.current.clone();
        let mut extra = Vec::new();
        if !self.roots.contains_key(&current) {
            extra.push(commit_root(&current));
        }
        if !self.gens.contains_key(&current) {
            extra.extend([parent_commit(&current), parent_gens(&current)]);
        }
        if !self.stamped {
            extra.push(STORAGE_VERSION_KEY.to_string());
        }
        let head = self.head(extra).await?;
        let found = std::mem::take(&mut self.found);
        if let Some(root) = found.get(&commit_root(&current)).and_then(|r| loads_str(r)) {
            self.roots.insert(current.clone(), root);
        }
        if let Slot::Vacant(slot) = self.gens.entry(current.clone()) {
            let record = decode_record(
                found.get(&parent_commit(&current)).map(Vec::as_slice),
                found.get(&parent_gens(&current)).map(Vec::as_slice),
            )?;
            if let Some(g) = record.generation() {
                slot.insert(g);
            }
        }
        if let Some(Value::Int(v)) = found.get(STORAGE_VERSION_KEY).and_then(|r| safe_loads(r))
            && v >= BLOB_STORAGE_VERSION
        {
            self.stamped = true;
        }
        Ok(head)
    }

    /// A commit's keyset root; one with no root reads as the empty tree.
    async fn root_of(&mut self, commit: &str) -> Result<String, Error> {
        if let Some(root) = self.roots.get(commit) {
            return Ok(root.clone());
        }
        let raw = self.io.get_one(&commit_root(commit)).await?;
        match raw.as_deref().and_then(loads_str) {
            Some(root) => {
                self.roots.insert(commit.to_string(), root.clone());
                Ok(root)
            }
            None => Ok(EMPTY_HASH.clone()),
        }
    }

    /// Our side: the worktree's changes as a commit on its current one.
    async fn build_ours(&mut self, info: Option<&Value>) -> Result<String, Error> {
        let a = self.args;
        let mut writes = BTreeMap::new();
        for (_, entry) in &a.updates {
            writes.insert(entry.blob.clone(), Write::Held);
        }
        if !a.chunks.is_empty() {
            stamp_version_at_least(self.io, CHUNK_STORAGE_VERSION).await?;
            for chunk in &a.chunks {
                writes.insert(chunk.clone(), Write::Held);
            }
        }
        let parent = self.current.clone();
        let commit = self
            .build(
                &[parent.as_str()],
                a.updates.clone(),
                a.removals.clone(),
                writes,
                info,
            )
            .await?;
        for (key, entry) in &a.updates {
            self.changes.insert(key.clone(), Some(entry.clone()));
        }
        for key in &a.removals {
            self.changes.insert(key.clone(), None);
        }
        self.current = commit.clone();
        Ok(commit)
    }

    /// A commit on `parents` whose keyset is the current one's with
    /// `updates` set and `removals` removed, landed with `writes`.
    async fn build(
        &mut self,
        parents: &[&str],
        updates: Vec<(String, Entry)>,
        removals: Vec<String>,
        mut writes: BTreeMap<String, Write>,
        info: Option<&Value>,
    ) -> Result<String, Error> {
        let current = self.current.clone();
        let root = self.root_of(&current).await?;
        let shape = self.args.shape.clone();
        let encoded = updates.into_iter().map(|(k, e)| (k, e.encode())).collect();
        let updated = tree::update(
            self.io.clone(),
            shape.clone(),
            root,
            encoded,
            removals,
            HashMap::new(),
        )
        .await?;
        // Once landed, the nodes are stored like any other, and the
        // shape's memo or the content cache holds them, decoded, for the
        // reads that follow.
        let mut landed_nodes = Vec::with_capacity(updated.nodes.len());
        let mut landed_bytes = 0;
        for ((hash, bytes), node) in updated.nodes.into_iter().zip(updated.decoded) {
            let key = format!("{}{hash}", shape.prefix);
            landed_bytes += key.len() + bytes.len();
            landed_nodes.push((hash, key.clone(), node, bytes.len()));
            writes.insert(key, Write::Bytes(bytes));
        }
        let parents: Vec<String> = parents.iter().map(|p| p.to_string()).collect();
        let gens_of_parents = self.parent_generations(&parents).await?;
        let created = self.io.now().await?;
        let refs: Vec<&str> = parents.iter().map(String::as_str).collect();
        let hash = commit_hash(&refs, &updated.root, created, info);
        let strings =
            |items: &[String]| Value::Array(items.iter().map(|s| Value::Str(s.clone())).collect());
        let gens = Value::Array(
            gens_of_parents
                .iter()
                .map(|g| Value::Int(*g as i64))
                .collect(),
        );
        writes.insert(commit_root(&hash), Write::Bytes(dumps_str(&updated.root)));
        writes.insert(
            parent_commit(&hash),
            Write::Bytes(dumps(&strings(&parents))),
        );
        writes.insert(parent_gens(&hash), Write::Bytes(dumps(&gens)));
        writes.insert(
            commit_time(&hash),
            Write::Bytes(dumps(&Value::Float(created))),
        );
        if let Some(info) = info {
            writes.insert(info_key(&hash), Write::Bytes(dumps(info)));
        }
        if !self.stamped {
            stamp_version_at_least(self.io, BLOB_STORAGE_VERSION).await?;
            self.stamped = true;
        }
        self.land(&hash, writes).await?;
        if let Some(memo) = &shape.memo {
            let mut memo = memo.lock().unwrap();
            for (hash, _, node, size) in landed_nodes {
                memo.hold_decoded(&hash, node, size);
            }
        } else if crate::cache::is_node(&shape.prefix) {
            self.io.remember_landed_nodes(
                landed_nodes
                    .into_iter()
                    .map(|(_, key, node, _)| (key, node))
                    .collect(),
                landed_bytes,
            );
        }
        let generation = 1 + gens_of_parents.iter().max().copied().unwrap_or(0);
        self.roots.insert(hash.clone(), updated.root.clone());
        self.gens.insert(hash.clone(), generation);
        self.new_roots.push((hash.clone(), updated.root));
        self.new_generations.push((hash.clone(), generation));
        Ok(hash)
    }

    /// The generation of each parent of a commit about to be written.
    async fn parent_generations(&mut self, parents: &[String]) -> Result<Vec<u64>, Error> {
        generations(self.io, parents, &mut self.gens).await
    }

    /// Land a commit's batch, marked in flight, while no sweep runs. It
    /// expects the lease record read after waiting any live sweep out,
    /// so it lands before a sweep starts or after one ends, never
    /// during; landing before is safe because the marker makes every
    /// later sweep keep the commit until it is published. The marker's
    /// expiry counts from the attempt that lands, not from before any
    /// wait, so a long wait can't land a marker already lapsed.
    async fn land(&mut self, commit: &str, writes: BTreeMap<String, Write>) -> Result<(), Error> {
        let mut seen = self.lease_hint.take();
        let (lease, marker) = loop {
            let lease = wait_for_gc(self.io, seen.take()).await?;
            let marker = dumps(&Value::Float(self.io.wall() + IN_FLIGHT_TTL));
            let mut batch = writes.clone();
            batch.insert(in_flight_key(commit), Write::Bytes(marker.clone()));
            let batch = Cas {
                expect: vec![],
                writes: batch.into_iter().collect(),
                removes: vec![],
            };
            if self.io.cas(under_lease(lease.clone(), batch)).await? {
                break (lease, marker);
            }
        };
        self.in_flight.push((commit.to_string(), marker));
        self.lease_seen = lease;
        Ok(())
    }

    /// Publish `new` as the branch's head if the head is `expected`.
    ///
    /// One write moves the head, records `expected` as its backup and
    /// drops the markers of the commits being published, so the backup
    /// always names the head just replaced, and a published commit
    /// never keeps a marker nor loses it before its head lands. The
    /// write expects the lease the latest batch landed against, so it
    /// can't land while a sweep runs (a sweep decides from the heads
    /// and markers it read at its start), and each marker as written:
    /// one that lapsed and was reaped may have taken its commit, so the
    /// publish fails rather than name it. A write that fails against a
    /// damaged head heals the head and tries again.
    async fn publish(&mut self, expected: &str, new: &str) -> Result<bool, Error> {
        let branch = &self.args.branch;
        let head_key = branch_head(branch);
        let expected_bytes = dumps_str(expected);
        let markers: Vec<(String, Vec<u8>)> = self
            .in_flight
            .iter()
            .map(|(c, m)| (in_flight_key(c), m.clone()))
            .collect();
        let mut lease = if markers.is_empty() {
            wait_for_gc(self.io, None).await?
        } else {
            self.lease_seen.clone()
        };
        loop {
            let mut expect = vec![(head_key.clone(), Some(expected_bytes.clone()))];
            expect.extend(markers.iter().map(|(k, m)| (k.clone(), Some(m.clone()))));
            let cas = Cas {
                expect,
                writes: vec![
                    (head_key.clone(), Write::Bytes(dumps_str(new))),
                    (
                        branch_head_prev(branch),
                        Write::Bytes(expected_bytes.clone()),
                    ),
                ],
                removes: markers.iter().map(|(k, _)| k.clone()).collect(),
            };
            if self.io.cas(under_lease(lease.clone(), cas)).await? {
                break;
            }
            if self.io.get_one(GC_LEASE_KEY).await? != lease {
                // A sweep ran, or is running: wait it out and try again.
                // A marker it reaped fails the next attempt on its own.
                lease = wait_for_gc(self.io, None).await?;
                continue;
            }
            let keys = markers.iter().map(|(k, _)| k.clone()).collect();
            let present = self.io.get(keys).await?;
            if markers.iter().any(|(k, m)| present.get(k) != Some(m)) {
                return Ok(false);
            }
            if !heal_head(self.io, branch, &expected_bytes, &mut self.notes).await? {
                return Ok(false);
            }
        }
        self.in_flight.clear();
        Ok(true)
    }

    /// Merge our commit with `theirs`, the branch's head, and publish
    /// the merge over it.
    async fn three_way(&mut self, theirs: &str) -> Result<Committed, Error> {
        let ours = self.current.clone();
        let Some(base) = merge_base(self.io, &ours, theirs, &mut self.gens).await? else {
            return self.no_ancestor(Strategy::ThreeWay).await;
        };
        let onto = Onto {
            base,
            parents: vec![theirs, ours.as_str()],
            over: theirs,
            strategy: Strategy::ThreeWay,
            may_be_nothing: false,
            post_check: false,
        };
        self.merge_onto(theirs, onto).await
    }

    /// Two commits share no history: nothing to merge from.
    async fn no_ancestor(&mut self, strategy: Strategy) -> Result<Committed, Error> {
        self.roll_back().await;
        if self.args.on_conflict == OnConflict::Abandon {
            return Ok(Self::abandoned(strategy));
        }
        Err(Error::Concurrency(
            "No common ancestor found between current commit and HEAD.".into(),
        ))
    }

    /// Merge, three ways from `onto.base`, the current commit and
    /// `theirs`, and publish the merge over `onto.over`.
    async fn merge_onto(&mut self, theirs: &str, onto: Onto<'_>) -> Result<Committed, Error> {
        let a = self.args;
        let ours = self.current.clone();
        // Each side's changes since the ancestor, as structural diffs:
        // subtrees the two share are never read.
        let our_changes = self.changes_between(&onto.base, &ours).await?;
        let their_changes = self.changes_between(&onto.base, theirs).await?;
        let mut resolved = resolve(self.io, &our_changes, &their_changes, &a.rules).await;
        if onto.post_check
            && let Ok(resolution) = &resolved
        {
            let values = resolution
                .merged
                .iter()
                .map(|(key, merged)| match merged {
                    Merged::Held { blob, .. } => (key.clone(), blob.clone(), None),
                    Merged::Made(bytes) => (key.clone(), merged.blob().0, Some(bytes.clone())),
                })
                .collect();
            let mut refused = self.io.check(values).await?;
            if !refused.is_empty() {
                refused.sort();
                resolved = Err(Error::MergeConflict {
                    keys: refused,
                    failures: vec![],
                });
            }
        }
        let resolution = match resolved {
            Ok(resolution) => resolution,
            Err(e @ Error::MergeConflict { .. }) => {
                self.roll_back().await;
                if a.on_conflict == OnConflict::Abandon {
                    return Ok(Self::abandoned(onto.strategy));
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        if onto.may_be_nothing && !resolution.changes_anything() {
            return self.live_no_op(&ours).await;
        }

        let mut writes = BTreeMap::new();
        let mut updates: Vec<(String, Entry)> = resolution.updates.clone().into_iter().collect();
        for (key, merged) in &resolution.merged {
            let (blob, size) = merged.blob();
            let write = match merged {
                Merged::Held { .. } => Write::Held,
                Merged::Made(bytes) => Write::Bytes(bytes.clone()),
            };
            writes.insert(blob.clone(), write);
            updates.push((key.clone(), Entry::new(blob, size, vec![])));
        }
        let removals: Vec<String> = resolution.removals.iter().cloned().collect();
        let merge = self
            .build(
                &onto.parents,
                updates.clone(),
                removals.clone(),
                writes,
                a.info.as_ref(),
            )
            .await?;
        for (key, entry) in updates {
            self.changes.insert(key, Some(entry));
        }
        for key in removals {
            self.changes.insert(key, None);
        }
        self.current = merge.clone();

        if self.publish(onto.over, &merge).await? {
            return Ok(self.landed(
                onto.strategy,
                merge,
                resolution.auto_merged,
                resolution.carried,
            ));
        }
        self.roll_back().await;
        if a.on_conflict == OnConflict::Abandon {
            return Ok(Self::abandoned(onto.strategy));
        }
        Err(Error::Concurrency(
            "HEAD changed during three-way merge. Refresh and retry.".into(),
        ))
    }

    /// Merge another head, or apply a change, as `m` asks.
    async fn merge(&mut self, m: &MergeArgs) -> Result<Committed, Error> {
        let ours = m.ours.clone();
        let mut named: Vec<&str> = m.from.iter().map(String::as_str).collect();
        named.push(&m.theirs);
        self.require(&named).await?;
        if let Some(from) = &m.from {
            let onto = Onto {
                base: from.clone(),
                parents: vec![ours.as_str()],
                over: &ours,
                strategy: Strategy::Apply,
                may_be_nothing: true,
                post_check: m.post_check,
            };
            return self.merge_onto(&m.theirs, onto).await;
        }
        let base = merge_base(self.io, &ours, &m.theirs, &mut self.gens).await?;
        let Some(base) = base else {
            return self.no_ancestor(Strategy::ThreeWay).await;
        };
        if base == m.theirs {
            return self.live_no_op(&ours).await;
        }
        if base == ours && m.fast_forward {
            return self.fast_forward(&ours, &m.theirs).await;
        }
        let onto = Onto {
            base,
            parents: vec![ours.as_str(), m.theirs.as_str()],
            over: &ours,
            strategy: Strategy::ThreeWay,
            may_be_nothing: false,
            post_check: m.post_check,
        };
        self.merge_onto(&m.theirs, onto).await
    }

    /// Fail for the first of `commits` that isn't in the store.
    async fn require(&mut self, commits: &[&str]) -> Result<(), Error> {
        let keys = commits.iter().map(|c| commit_root(c)).collect();
        let found = self.io.get(keys).await?;
        for commit in commits {
            match found.get(&commit_root(commit)).and_then(|r| loads_str(r)) {
                Some(root) => {
                    self.roots.insert(commit.to_string(), root);
                }
                None if found.contains_key(&commit_root(commit)) => {}
                None => {
                    return Err(Error::UnknownCommit {
                        commit: commit.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// A merge or apply that changes nothing, at `ours`. Nothing is
    /// written, so no publish guards the answer: it holds only while the
    /// branch is still at our head.
    async fn live_no_op(&mut self, ours: &str) -> Result<Committed, Error> {
        match self.head(vec![]).await? {
            None => Err(Error::head_gone(&self.args.branch)),
            Some(live) if live != ours => {
                if self.args.on_conflict == OnConflict::Abandon {
                    return Ok(Self::abandoned(Strategy::NoOp));
                }
                Err(Error::Concurrency(
                    "HEAD changed during merge. Refresh and retry.".into(),
                ))
            }
            Some(_) => Ok(self.landed(Strategy::NoOp, ours.to_string(), vec![], vec![])),
        }
    }

    /// Move the head from `ours` to `theirs`, a descendant of it, writing
    /// no commit. That theirs is here and the head names it are decided
    /// against one lease record, so a sweep can't take theirs between
    /// them. The write expects our head: a branch that moved meanwhile
    /// fails it, and a deleted one isn't made again.
    async fn fast_forward(&mut self, ours: &str, theirs: &str) -> Result<Committed, Error> {
        let changes = self.changes_between(ours, theirs).await?;
        let branch = self.args.branch.clone();
        let head_key = branch_head(&branch);
        let expected = dumps_str(ours);
        loop {
            let lease = wait_for_gc(self.io, None).await?;
            if self.io.get_one(&commit_root(theirs)).await?.is_none() {
                return Err(Error::UnknownCommit {
                    commit: theirs.to_string(),
                });
            }
            let cas = Cas {
                expect: vec![(head_key.clone(), Some(expected.clone()))],
                writes: vec![
                    (head_key.clone(), Write::Bytes(dumps_str(theirs))),
                    (branch_head_prev(&branch), Write::Bytes(expected.clone())),
                ],
                removes: vec![],
            };
            match try_land(self.io, lease, cas).await? {
                None => continue,
                Some(true) => break,
                Some(false) => {
                    if self.io.get_one(&head_key).await?.is_none() {
                        return Err(Error::UnknownBranch { branch });
                    }
                    if heal_head(self.io, &branch, &expected, &mut self.notes).await? {
                        continue;
                    }
                    if self.args.on_conflict == OnConflict::Abandon {
                        return Ok(Self::abandoned(Strategy::FastForward));
                    }
                    return Err(Error::Concurrency(
                        "HEAD changed during merge. Refresh and retry.".into(),
                    ));
                }
            }
        }
        let carried: Vec<String> = changes.keys().cloned().collect();
        for (key, change) in changes {
            self.changes.insert(key, change.new);
        }
        // The worktree is at theirs now, a commit it didn't write but
        // whose root it may keep.
        if let Some(root) = self.roots.get(theirs) {
            self.new_roots.push((theirs.to_string(), root.clone()));
        }
        self.current = theirs.to_string();
        Ok(self.landed(Strategy::FastForward, theirs.to_string(), vec![], carried))
    }

    /// Each key whose value differs from commit `a` to commit `b`. A key
    /// whose entry changed only in metadata, keeping its blob, is not a
    /// change.
    async fn changes_between(
        &mut self,
        a: &str,
        b: &str,
    ) -> Result<BTreeMap<String, Change>, Error> {
        let (ra, rb) = (self.root_of(a).await?, self.root_of(b).await?);
        history::changes(self.io, self.args.shape.clone(), ra, rb).await
    }

    /// Abandon what this attempt wrote: its commits become ordinary
    /// orphans once their markers are withdrawn, and the worktree's
    /// keyset is where it started. Each marker goes only if it still
    /// holds the bytes this attempt wrote: a writer that made the very
    /// same commit wrote its own over it, and keeps it until it
    /// publishes. Best effort: a marker left behind lapses.
    async fn roll_back(&mut self) {
        self.current = self.args.current.clone();
        self.changes.clear();
        for (commit, marker) in std::mem::take(&mut self.in_flight) {
            let key = in_flight_key(&commit);
            let cas = Cas {
                expect: vec![(key.clone(), Some(marker))],
                writes: vec![],
                removes: vec![key],
            };
            if self.io.cas(cas).await.is_err() {
                return;
            }
        }
    }
}
