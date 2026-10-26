//! A worktree's committed state: the root of the keyset it is at, and a
//! key index built when something first lists it.
//!
//! Opening a worktree costs one read whatever the keyset's size; a read
//! is a tree path through the repository's content cache, and a commit
//! rewrites only the paths it changes. Listing the whole tree on every
//! call would be too slow on a large one, so the first listing builds
//! an index, the sorted keys and nothing else, and later ones read it.
//! A commit or merge carries the index to its new root with its own
//! changes; moving to a root the worktree didn't write updates it by a
//! structural diff, which reads only the subtrees that differ, or drops
//! it if the store no longer holds the old tree. Reads
//! never consult the index for a value, only to answer that a key is
//! absent.
//!
//! The host holds the worktree's pending changes, which are values, and
//! lays them over what this answers.

use crate::error::Error;
use crate::history::decode_record;
use crate::keyset::Entry;
use crate::layout::{EMPTY_HASH, commit_root, parent_commit, parent_gens};
use crate::machine::Io;
use crate::store::loads_str;
use crate::tree::{self, Shape};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// A worktree's committed state.
#[derive(Clone, Debug)]
pub struct Tree {
    root: String,
    index: Option<BTreeSet<String>>,
}

/// A worktree's state as its host object and the operations it runs
/// share it.
pub type Shared = Arc<Mutex<Tree>>;

impl Tree {
    pub fn new(root: String) -> Tree {
        Tree { root, index: None }
    }

    pub fn shared(root: String) -> Shared {
        Arc::new(Mutex::new(Tree::new(root)))
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    /// Whether a listing has built the index.
    pub fn indexed(&self) -> bool {
        self.index.is_some()
    }

    /// The keys, if the index is built.
    pub fn index(&self) -> Option<&BTreeSet<String>> {
        self.index.as_ref()
    }

    /// Move to `root`, which a commit or merge from here wrote, with the
    /// changes it made to the keyset.
    pub fn landed(&mut self, root: String, changes: &[(String, Option<Entry>)]) {
        self.root = root;
        if let Some(index) = &mut self.index {
            for (key, entry) in changes {
                match entry {
                    Some(_) => index.insert(key.clone()),
                    None => index.remove(key),
                };
            }
        }
    }
}

fn root_of(tree: &Shared) -> String {
    tree.lock().unwrap().root.clone()
}

/// The entries of those of `keys` the tree holds. With the index built,
/// a key it lacks is known absent without a read.
pub async fn lookup(
    io: &Io,
    shape: Shape,
    tree: &Shared,
    keys: Vec<String>,
) -> Result<Vec<(String, Entry)>, Error> {
    let (root, keys) = {
        let t = tree.lock().unwrap();
        let keys: Vec<String> = match &t.index {
            Some(index) => keys.into_iter().filter(|k| index.contains(k)).collect(),
            None => keys,
        };
        (t.root.clone(), keys)
    };
    if keys.is_empty() {
        return Ok(vec![]);
    }
    let found = tree::get_many(io.clone(), shape, root, keys).await?;
    found
        .into_iter()
        .map(|(k, raw)| Ok((k, Entry::decode(&raw).map_err(Error::Malformed)?)))
        .collect()
}

/// Whether the tree holds `key`.
pub async fn contains(io: &Io, shape: Shape, tree: &Shared, key: &str) -> Result<bool, Error> {
    if let Some(index) = tree.lock().unwrap().index() {
        return Ok(index.contains(key));
    }
    Ok(!lookup(io, shape, tree, vec![key.to_string()])
        .await?
        .is_empty())
}

/// Every key, sorted, building the index with one walk if it isn't.
pub async fn keys(io: &Io, shape: Shape, tree: &Shared) -> Result<Vec<String>, Error> {
    if let Some(index) = tree.lock().unwrap().index() {
        return Ok(index.iter().cloned().collect());
    }
    let root = root_of(tree);
    let mut keys = tree::keys(io.clone(), shape, root.clone()).await?;
    keys.sort();
    let mut t = tree.lock().unwrap();
    // A move while the walk ran leaves the index unbuilt: it would
    // describe the old root.
    if t.root == root {
        t.index = Some(keys.iter().cloned().collect());
    }
    Ok(keys)
}

/// Move to `root`, a root this worktree didn't write: a checkout, a
/// refresh, a reset. A built index follows by a structural diff, unless
/// the store lacks a node the diff needs: the old root may have been
/// swept, and a diff from a tree it can't read would leave that tree's
/// keys behind. The index is then dropped, for the next listing to
/// build again.
pub async fn move_to(io: &Io, shape: Shape, tree: &Shared, root: String) -> Result<(), Error> {
    let (old, indexed) = {
        let t = tree.lock().unwrap();
        (t.root.clone(), t.indexed())
    };
    if old == root {
        return Ok(());
    }
    if !indexed {
        tree.lock().unwrap().root = root;
        return Ok(());
    }
    let diff = tree::diff_whole(io.clone(), shape, old.clone(), root.clone()).await?;
    let mut t = tree.lock().unwrap();
    let Some(diff) = diff.filter(|_| t.root == old) else {
        // Moved again meanwhile, or a tree the diff couldn't read:
        // what it found doesn't describe the move.
        t.index = None;
        t.root = root;
        return Ok(());
    };
    if let Some(index) = &mut t.index {
        for (key, _) in diff.added {
            index.insert(key);
        }
        for (key, _) in diff.removed {
            index.remove(&key);
        }
    }
    t.root = root;
    Ok(())
}

/// What a checkout found of the commit it moved to.
pub struct CheckedOut {
    /// The commit's root; `None` for a commit the store doesn't hold,
    /// which the worktree reads as the empty tree.
    pub root: Option<String>,
    /// The commit's generation, if it stores one.
    pub generation: Option<u64>,
}

/// Move to `commit`: its root, its parents and their generations in one
/// read, then [`move_to`] its root.
pub async fn checkout(
    io: &Io,
    shape: Shape,
    tree: &Shared,
    commit: &str,
) -> Result<CheckedOut, Error> {
    let (root_key, parents_key, gens_key) = (
        commit_root(commit),
        parent_commit(commit),
        parent_gens(commit),
    );
    let found = io
        .get(vec![
            root_key.clone(),
            parents_key.clone(),
            gens_key.clone(),
        ])
        .await?;
    let root = found.get(&root_key).and_then(|raw| loads_str(raw));
    // A record that doesn't parse costs the generation, not the move.
    let generation = root.as_ref().and_then(|_| {
        decode_record(
            found.get(&parents_key).map(Vec::as_slice),
            found.get(&gens_key).map(Vec::as_slice),
        )
        .ok()?
        .generation()
    });
    let to = root.clone().unwrap_or_else(|| EMPTY_HASH.clone());
    move_to(io, shape, tree, to).await?;
    Ok(CheckedOut { root, generation })
}
