//! The content-addressed hash array mapped trie a keyset is laid out in.
//!
//! A tree maps string keys to byte values. Each node is JSON stored
//! under its SHA-256, so an unchanged subtree is shared between versions
//! by hash alone, and a tree is named by its root's hash. A leaf holds
//! up to `bucket_max` entries, `{"items":{<key>:<base64 value>},"kind":"leaf"}`;
//! past that it splits into a branch, `{"children":{<nibble>:<hash>},"kind":"branch"}`,
//! that sends each key on by the next hex digit of the key's SHA-256.
//! The empty tree is [`EMPTY_HASH`], whose node is never written.
//!
//! Each operation is an `async fn` that reads nodes through [`Io`], one
//! batched read per tree level, and is driven by a [`Machine`]. A node
//! the store lacks reads as absent where Python kvgit treats it so: a
//! lookup misses, a walk skips it, a diff takes it for empty, an insert
//! below it starts a fresh leaf.
//!
//! [`Machine`]: crate::machine::Machine

use crate::base64;
use crate::hash::sha256_hex;
use crate::json::{self, Value, write_string};
use crate::layout::EMPTY_HASH;
use crate::machine::Io;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

/// Hex digits in a key's hash, and so the deepest a tree can go.
const HASH_LEN: usize = 64;

/// Where a tree's nodes are stored and how full a leaf may get.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shape {
    /// The prefix of each node's store key, before its hash.
    pub prefix: String,
    /// The most entries a leaf holds before it splits.
    pub bucket_max: usize,
}

/// A node, with its entries and children in the order they are written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Node {
    /// Key to the base64 of its value.
    Leaf(BTreeMap<String, String>),
    /// Hex digit to the child's hash.
    Branch(BTreeMap<String, String>),
}

/// Why a tree operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeError {
    /// A stored node that isn't a node.
    Malformed { hash: String, why: String },
    /// A shape no tree can have: one whose leaves hold nothing.
    BadShape,
}

impl std::fmt::Display for TreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeError::Malformed { hash, why } => write!(f, "malformed tree node {hash}: {why}"),
            TreeError::BadShape => write!(f, "bucket_max must be at least 1"),
        }
    }
}

impl std::error::Error for TreeError {}

impl Node {
    fn empty() -> Node {
        Node::Leaf(BTreeMap::new())
    }

    /// The node's bytes: `json.dumps(node, sort_keys=True, separators=(",", ":"))`.
    pub fn encode(&self) -> Vec<u8> {
        let (field, map, kind) = match self {
            Node::Leaf(items) => ("items", items, "leaf"),
            Node::Branch(children) => ("children", children, "branch"),
        };
        let mut out = String::with_capacity(32 + map.len() * 80);
        out.push_str("{\"");
        out.push_str(field);
        out.push_str("\":{");
        for (i, (k, v)) in map.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_string(&mut out, k);
            out.push(':');
            write_string(&mut out, v);
        }
        out.push_str("},\"kind\":\"");
        out.push_str(kind);
        out.push_str("\"}");
        out.into_bytes()
    }

    /// Read a stored node; `hash` names it in the error.
    pub fn decode(raw: &[u8], hash: &str) -> Result<Node, TreeError> {
        let bad = |why: &str| TreeError::Malformed {
            hash: hash.to_string(),
            why: why.to_string(),
        };
        let value = json::loads(raw).map_err(|e| bad(&e.to_string()))?;
        let strings = |field: &str| -> Result<BTreeMap<String, String>, TreeError> {
            match value.get(field) {
                Some(Value::Object(pairs)) => pairs
                    .iter()
                    .map(|(k, v)| match v {
                        Value::Str(s) => Ok((k.clone(), s.clone())),
                        _ => Err(bad(&format!("{field} holds a non-string"))),
                    })
                    .collect(),
                _ => Err(bad(&format!("no {field}"))),
            }
        };
        match value.get("kind").and_then(Value::as_str) {
            Some("leaf") => Ok(Node::Leaf(strings("items")?)),
            Some("branch") => Ok(Node::Branch(strings("children")?)),
            _ => Err(bad("no kind")),
        }
    }
}

fn key_hash(key: &str) -> String {
    sha256_hex(key.as_bytes())
}

/// The hex digit of `key_hash` a branch at `depth` sends a key on by;
/// none past the last, which only a planted tree reaches.
fn nibble(key_hash: &str, depth: usize) -> String {
    key_hash
        .get(depth..depth + 1)
        .unwrap_or_default()
        .to_string()
}

fn decode_value(key: &str, encoded: &str) -> Result<Vec<u8>, TreeError> {
    base64::decode(encoded).ok_or_else(|| TreeError::Malformed {
        hash: String::new(),
        why: format!("the value of {key:?} isn't base64"),
    })
}

// --- Nodes as one operation sees them ---

/// A node lookup within an operation.
enum Found {
    Node(Rc<Node>),
    /// The store doesn't have it.
    Missing,
    /// Not read yet.
    Unknown,
}

/// The nodes one operation has read or made.
struct Nodes {
    io: Io,
    shape: Shape,
    read: HashMap<String, Option<Rc<Node>>>,
    /// Nodes this operation made, and the caller's unwritten ones, by
    /// hash, with their bytes.
    unwritten: HashMap<String, (Rc<Node>, Vec<u8>)>,
}

impl Nodes {
    fn new(io: Io, shape: Shape, unwritten: HashMap<String, Vec<u8>>) -> Result<Nodes, TreeError> {
        if shape.bucket_max == 0 {
            return Err(TreeError::BadShape);
        }
        let unwritten = unwritten
            .into_iter()
            .map(|(hash, raw)| Ok((hash.clone(), (Rc::new(Node::decode(&raw, &hash)?), raw))))
            .collect::<Result<_, TreeError>>()?;
        Ok(Nodes {
            io,
            shape,
            read: HashMap::new(),
            unwritten,
        })
    }

    fn find(&self, hash: &str) -> Found {
        if hash == EMPTY_HASH.as_str() {
            return Found::Node(Rc::new(Node::empty()));
        }
        if let Some((node, _)) = self.unwritten.get(hash) {
            return Found::Node(node.clone());
        }
        match self.read.get(hash) {
            Some(Some(node)) => Found::Node(node.clone()),
            Some(None) => Found::Missing,
            None => Found::Unknown,
        }
    }

    fn known(&self, hash: &str) -> bool {
        !matches!(self.find(hash), Found::Unknown)
    }

    /// Read every node of `hashes` not yet at hand, in one request.
    async fn fetch<'a>(
        &mut self,
        hashes: impl IntoIterator<Item = &'a String>,
    ) -> Result<(), TreeError> {
        let mut wanted: Vec<String> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        for hash in hashes {
            if !self.known(hash) && seen.insert(hash) {
                wanted.push(hash.clone());
            }
        }
        if wanted.is_empty() {
            return Ok(());
        }
        let keys: Vec<String> = wanted
            .iter()
            .map(|h| format!("{}{h}", self.shape.prefix))
            .collect();
        let mut got = self.io.get(keys.clone()).await;
        for (hash, key) in wanted.into_iter().zip(keys) {
            let node = match got.remove(&key) {
                Some(raw) => Some(Rc::new(Node::decode(&raw, &hash)?)),
                None => None,
            };
            self.read.insert(hash, node);
        }
        Ok(())
    }

    /// Keep `node` as one this operation made; its hash.
    fn make(&mut self, node: Node) -> String {
        let raw = node.encode();
        let hash = sha256_hex(&raw);
        self.unwritten
            .entry(hash.clone())
            .or_insert((Rc::new(node), raw));
        hash
    }

    fn leaf(&mut self, items: BTreeMap<String, String>) -> String {
        self.make(Node::Leaf(items))
    }

    fn branch(&mut self, children: BTreeMap<String, String>) -> String {
        self.make(Node::Branch(children))
    }

    /// The unwritten nodes `root` reaches without passing through a
    /// stored one: what a write of the new tree must store.
    fn unwritten_under(&self, root: &str) -> Vec<(String, Vec<u8>)> {
        let mut out = BTreeMap::new();
        let mut queue = vec![root.to_string()];
        while let Some(hash) = queue.pop() {
            if out.contains_key(&hash) {
                continue;
            }
            let Some((node, raw)) = self.unwritten.get(&hash) else {
                continue;
            };
            out.insert(hash, raw.clone());
            if let Node::Branch(children) = node.as_ref() {
                queue.extend(children.values().cloned());
            }
        }
        out.into_iter().collect()
    }
}

/// A node this step needs but hasn't read: read these, then retry.
struct Need(Vec<String>);

// --- Reads ---

/// The values of those of `keys` the tree at `root` holds, reading the
/// paths to all of them level by level.
pub async fn get_many(
    io: Io,
    shape: Shape,
    root: String,
    keys: Vec<String>,
) -> Result<Vec<(String, Vec<u8>)>, TreeError> {
    let mut nodes = Nodes::new(io, shape, HashMap::new())?;
    let mut found = Vec::new();
    let mut seen = HashSet::new();
    // Each key's search: its hash, and the node and depth it has reached.
    let mut searches: Vec<(String, String, String, usize)> = keys
        .into_iter()
        .filter(|k| seen.insert(k.clone()))
        .map(|k| {
            let kh = key_hash(&k);
            (k, kh, root.clone(), 0)
        })
        .collect();
    while !searches.is_empty() {
        let mut waiting = Vec::new();
        for (key, kh, mut at, mut depth) in searches {
            loop {
                match nodes.find(&at) {
                    Found::Unknown => {
                        waiting.push((key, kh, at, depth));
                        break;
                    }
                    Found::Missing => break,
                    Found::Node(node) => match node.as_ref() {
                        Node::Leaf(items) => {
                            if let Some(encoded) = items.get(&key) {
                                let value = decode_value(&key, encoded)?;
                                found.push((key, value));
                            }
                            break;
                        }
                        Node::Branch(children) => match children.get(&nibble(&kh, depth)) {
                            Some(child) => {
                                at = child.clone();
                                depth += 1;
                            }
                            _ => break,
                        },
                    },
                }
            }
        }
        let hashes: Vec<String> = waiting.iter().map(|(_, _, at, _)| at.clone()).collect();
        nodes.fetch(&hashes).await?;
        searches = waiting;
    }
    Ok(found)
}

/// Everything the tree at `root` holds, and the hash of every node it
/// is made of, in one walk, level by level. A node `skip` holds for is
/// taken as already walked: neither it nor anything under it is read or
/// returned. Skipping the nodes of earlier walks shares the work between
/// trees that share structure.
pub async fn walk(
    io: Io,
    shape: Shape,
    root: String,
    skip: impl Fn(&str) -> bool,
) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>), TreeError> {
    let mut nodes = Nodes::new(io, shape, HashMap::new())?;
    let mut items = Vec::new();
    let mut visited = Vec::new();
    let mut level = if root == EMPTY_HASH.as_str() || skip(&root) {
        vec![]
    } else {
        vec![root]
    };
    while !level.is_empty() {
        let wanted: Vec<String> = level.iter().filter(|h| !skip(h)).cloned().collect();
        nodes.fetch(&wanted).await?;
        let mut next = Vec::new();
        for hash in wanted {
            if hash == EMPTY_HASH.as_str() {
                continue;
            }
            let Found::Node(node) = nodes.find(&hash) else {
                continue;
            };
            visited.push(hash);
            match node.as_ref() {
                Node::Leaf(entries) => {
                    for (k, v) in entries {
                        items.push((k.clone(), decode_value(k, v)?));
                    }
                }
                Node::Branch(children) => next.extend(children.values().cloned()),
            }
        }
        level = next;
    }
    Ok((items, visited))
}

/// What changed between two trees.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TreeDiff {
    pub added: Vec<(String, Vec<u8>)>,
    pub removed: Vec<(String, Vec<u8>)>,
    /// Key, old value, new value.
    pub modified: Vec<(String, Vec<u8>, Vec<u8>)>,
}

/// One side of a diff at some depth: a node, or the entries of a leaf
/// split to line up with the other side's branch.
#[derive(Clone)]
enum Side {
    Hash(String),
    Items(BTreeMap<String, String>),
}

/// What changed from the tree at `a` to the tree at `b`. Identical
/// subtrees are never read, so the cost follows the size of the change;
/// both trees are read a level at a time.
pub async fn diff(io: Io, shape: Shape, a: String, b: String) -> Result<TreeDiff, TreeError> {
    let mut nodes = Nodes::new(io, shape, HashMap::new())?;
    let mut out = TreeDiff::default();
    let mut level: Vec<(Side, Side, usize)> = if a == b {
        vec![]
    } else {
        vec![(Side::Hash(a), Side::Hash(b), 0)]
    };
    while !level.is_empty() {
        let hashes: Vec<String> = level
            .iter()
            .flat_map(|(a, b, _)| [a, b])
            .filter_map(|s| match s {
                Side::Hash(h) => Some(h.clone()),
                Side::Items(_) => None,
            })
            .collect();
        nodes.fetch(&hashes).await?;
        let mut next = Vec::new();
        for (a, b, depth) in level {
            let a_node = side_node(&nodes, a);
            let b_node = side_node(&nodes, b);
            match (&a_node, &b_node) {
                (Node::Leaf(a_items), Node::Leaf(b_items)) => {
                    diff_items(a_items, b_items, &mut out)?
                }
                _ => {
                    let a_children = children_at(&a_node, depth);
                    let b_children = children_at(&b_node, depth);
                    let chunks: BTreeSet<&String> =
                        a_children.keys().chain(b_children.keys()).collect();
                    for chunk in chunks {
                        let a_child = a_children
                            .get(chunk)
                            .cloned()
                            .unwrap_or_else(|| Side::Hash(EMPTY_HASH.clone()));
                        let b_child = b_children
                            .get(chunk)
                            .cloned()
                            .unwrap_or_else(|| Side::Hash(EMPTY_HASH.clone()));
                        if let (Side::Hash(x), Side::Hash(y)) = (&a_child, &b_child)
                            && x == y
                        {
                            continue;
                        }
                        next.push((a_child, b_child, depth + 1));
                    }
                }
            }
        }
        level = next;
    }
    out.added.sort();
    out.removed.sort();
    out.modified.sort();
    Ok(out)
}

/// A side's node; one the store lacks reads as empty.
fn side_node(nodes: &Nodes, side: Side) -> Node {
    match side {
        Side::Items(items) => Node::Leaf(items),
        Side::Hash(hash) => match nodes.find(&hash) {
            Found::Node(node) => node.as_ref().clone(),
            _ => Node::empty(),
        },
    }
}

/// A node's children, or a leaf's entries grouped as a branch at
/// `depth` would hold them.
fn children_at(node: &Node, depth: usize) -> BTreeMap<String, Side> {
    match node {
        Node::Branch(children) => children
            .iter()
            .map(|(k, h)| (k.clone(), Side::Hash(h.clone())))
            .collect(),
        Node::Leaf(items) => {
            let mut groups: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
            for (k, v) in items {
                groups
                    .entry(nibble(&key_hash(k), depth))
                    .or_default()
                    .insert(k.clone(), v.clone());
            }
            groups
                .into_iter()
                .map(|(k, items)| (k, Side::Items(items)))
                .collect()
        }
    }
}

fn diff_items(
    a: &BTreeMap<String, String>,
    b: &BTreeMap<String, String>,
    out: &mut TreeDiff,
) -> Result<(), TreeError> {
    for (k, v) in a {
        match b.get(k) {
            None => out.removed.push((k.clone(), decode_value(k, v)?)),
            Some(w) if w != v => {
                out.modified
                    .push((k.clone(), decode_value(k, v)?, decode_value(k, w)?))
            }
            Some(_) => {}
        }
    }
    for (k, v) in b {
        if !a.contains_key(k) {
            out.added.push((k.clone(), decode_value(k, v)?));
        }
    }
    Ok(())
}

// --- Writes ---

/// A tree after an update: its root, and the nodes to store for it.
#[derive(Debug, PartialEq, Eq)]
pub struct Updated {
    pub root: String,
    /// Node hash and bytes, by hash; store each under the shape's prefix.
    pub nodes: Vec<(String, Vec<u8>)>,
}

/// The tree at `root` with `updates` set and then `removals` removed,
/// in the order given. `unwritten` holds nodes, by hash, that the
/// caller made but hasn't stored yet; those the new tree reaches come
/// back with its own.
pub async fn update(
    io: Io,
    shape: Shape,
    root: String,
    updates: Vec<(String, Vec<u8>)>,
    removals: Vec<String>,
    unwritten: HashMap<String, Vec<u8>>,
) -> Result<Updated, TreeError> {
    let mut nodes = Nodes::new(io, shape, unwritten)?;
    let updated: Vec<String> = updates.iter().map(|(k, _)| k.clone()).collect();
    prefetch_paths(&mut nodes, &root, &updated, &removals).await?;
    let mut current = root;
    for (key, value) in &updates {
        let encoded = base64::encode(value);
        let kh = key_hash(key);
        current = loop {
            let attempt = if current == EMPTY_HASH.as_str() {
                Ok(nodes.leaf(BTreeMap::from([(key.clone(), encoded.clone())])))
            } else {
                insert_at(&mut nodes, &current, 0, &kh, key, &encoded)
            };
            match attempt {
                Ok(root) => break root,
                Err(Need(hashes)) => nodes.fetch(&hashes).await?,
            }
        };
    }
    for key in &removals {
        if current == EMPTY_HASH.as_str() {
            continue;
        }
        let kh = key_hash(key);
        current = loop {
            match delete_at(&mut nodes, &current, 0, &kh, key) {
                Ok(root) => break root.unwrap_or_else(|| EMPTY_HASH.clone()),
                Err(Need(hashes)) => nodes.fetch(&hashes).await?,
            }
        };
    }
    let written = nodes.unwritten_under(&current);
    Ok(Updated {
        root: current,
        nodes: written,
    })
}

fn found_or_need(nodes: &Nodes, hash: &str) -> Result<Option<Rc<Node>>, Need> {
    match nodes.find(hash) {
        Found::Node(node) => Ok(Some(node)),
        Found::Missing => Ok(None),
        Found::Unknown => Err(Need(vec![hash.to_string()])),
    }
}

fn insert_at(
    nodes: &mut Nodes,
    hash: &str,
    depth: usize,
    kh: &str,
    key: &str,
    encoded: &str,
) -> Result<String, Need> {
    let Some(node) = found_or_need(nodes, hash)? else {
        // A dangling reference: the entry starts a leaf of its own.
        return Ok(nodes.leaf(BTreeMap::from([(key.to_string(), encoded.to_string())])));
    };
    match node.as_ref() {
        Node::Leaf(items) => {
            if items.get(key).map(String::as_str) == Some(encoded) {
                return Ok(hash.to_string());
            }
            let mut items = items.clone();
            items.insert(key.to_string(), encoded.to_string());
            if items.len() <= nodes.shape.bucket_max {
                Ok(nodes.leaf(items))
            } else {
                Ok(split_leaf(nodes, items, depth))
            }
        }
        Node::Branch(children) => {
            let chunk = nibble(kh, depth);
            let mut children = children.clone();
            match children.get(&chunk).cloned() {
                Some(child) => {
                    let new_child = insert_at(nodes, &child, depth + 1, kh, key, encoded)?;
                    if new_child == child {
                        return Ok(hash.to_string());
                    }
                    children.insert(chunk, new_child);
                }
                None => {
                    let leaf = nodes.leaf(BTreeMap::from([(key.to_string(), encoded.to_string())]));
                    children.insert(chunk, leaf);
                }
            }
            Ok(nodes.branch(children))
        }
    }
}

/// An overflowing leaf at `depth`, as a branch.
fn split_leaf(nodes: &mut Nodes, items: BTreeMap<String, String>, depth: usize) -> String {
    if depth >= HASH_LEN {
        // The keys' hashes are equal to the last digit: they stay in one
        // oversized leaf.
        return nodes.leaf(items);
    }
    let mut groups: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (k, v) in items {
        groups
            .entry(nibble(&key_hash(&k), depth))
            .or_default()
            .insert(k, v);
    }
    if groups.len() == 1 {
        let (chunk, items) = groups.into_iter().next().expect("one group");
        let child = split_leaf(nodes, items, depth + 1);
        return nodes.branch(BTreeMap::from([(chunk, child)]));
    }
    let mut children = BTreeMap::new();
    for (chunk, items) in groups {
        let child = if items.len() <= nodes.shape.bucket_max {
            nodes.leaf(items)
        } else {
            split_leaf(nodes, items, depth + 1)
        };
        children.insert(chunk, child);
    }
    nodes.branch(children)
}

/// The subtree at `hash` without `key`: its new hash, or `None` once
/// it is empty.
fn delete_at(
    nodes: &mut Nodes,
    hash: &str,
    depth: usize,
    kh: &str,
    key: &str,
) -> Result<Option<String>, Need> {
    let Some(node) = found_or_need(nodes, hash)? else {
        return Ok(Some(hash.to_string()));
    };
    match node.as_ref() {
        Node::Leaf(items) => {
            if !items.contains_key(key) {
                return Ok(Some(hash.to_string()));
            }
            let mut items = items.clone();
            items.remove(key);
            Ok(if items.is_empty() {
                None
            } else {
                Some(nodes.leaf(items))
            })
        }
        Node::Branch(children) => {
            let chunk = nibble(kh, depth);
            let Some(child) = children.get(&chunk).cloned() else {
                return Ok(Some(hash.to_string()));
            };
            let new_child = delete_at(nodes, &child, depth + 1, kh, key)?;
            if new_child.as_deref() == Some(child.as_str()) {
                return Ok(Some(hash.to_string()));
            }
            let mut children = children.clone();
            match new_child {
                None => {
                    children.remove(&chunk);
                }
                Some(new_child) => {
                    children.insert(chunk, new_child);
                }
            }
            if children.is_empty() {
                return Ok(None);
            }
            // The same contents make the same tree: a branch whose
            // children are all leaves that fit in one becomes that leaf.
            if let Some(leaf) = collapse(nodes, &children)? {
                return Ok(Some(leaf));
            }
            Ok(Some(nodes.branch(children)))
        }
    }
}

fn collapse(
    nodes: &mut Nodes,
    children: &BTreeMap<String, String>,
) -> Result<Option<String>, Need> {
    let unknown: Vec<String> = children
        .values()
        .filter(|h| !nodes.known(h))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        return Err(Need(unknown));
    }
    let mut merged: BTreeMap<String, String> = BTreeMap::new();
    for child in children.values() {
        let Found::Node(node) = nodes.find(child) else {
            return Ok(None);
        };
        let Node::Leaf(items) = node.as_ref() else {
            return Ok(None);
        };
        for (k, v) in items {
            merged.entry(k.clone()).or_insert_with(|| v.clone());
            if merged.len() > nodes.shape.bucket_max {
                return Ok(None);
            }
        }
    }
    Ok(Some(nodes.leaf(merged)))
}

/// Read every stored node on the paths from `root` to the keys an
/// update touches, a level at a time. A removal can collapse a branch
/// it passes through, which reads all of that branch's children, so
/// those are read with the path.
async fn prefetch_paths(
    nodes: &mut Nodes,
    root: &str,
    updated: &[String],
    removed: &[String],
) -> Result<(), TreeError> {
    if root == EMPTY_HASH.as_str() {
        return Ok(());
    }
    let mut frontier: BTreeMap<String, Vec<(String, bool)>> = BTreeMap::new();
    for (keys, removing) in [(updated, false), (removed, true)] {
        for key in keys {
            frontier
                .entry(root.to_string())
                .or_default()
                .push((key_hash(key), removing));
        }
    }
    let mut extra: BTreeSet<String> = BTreeSet::new();
    let mut depth = 0;
    while !frontier.is_empty() && depth < HASH_LEN {
        let wanted: Vec<String> = frontier.keys().chain(extra.iter()).cloned().collect();
        nodes.fetch(&wanted).await?;
        let mut next: BTreeMap<String, Vec<(String, bool)>> = BTreeMap::new();
        let mut next_extra: BTreeSet<String> = BTreeSet::new();
        for (hash, paths) in &frontier {
            let Found::Node(node) = nodes.find(hash) else {
                continue;
            };
            let Node::Branch(children) = node.as_ref() else {
                continue;
            };
            if paths.iter().any(|(_, removing)| *removing) {
                next_extra.extend(
                    children
                        .values()
                        .filter(|h| *h != EMPTY_HASH.as_str())
                        .cloned(),
                );
            }
            for (kh, removing) in paths {
                if let Some(child) = children.get(&nibble(kh, depth))
                    && child != EMPTY_HASH.as_str()
                {
                    next.entry(child.clone())
                        .or_default()
                        .push((kh.clone(), *removing));
                }
            }
        }
        extra = next_extra
            .into_iter()
            .filter(|h| !next.contains_key(h))
            .collect();
        frontier = next;
        depth += 1;
    }
    if !extra.is_empty() {
        nodes.fetch(&extra.into_iter().collect::<Vec<_>>()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::Machine;
    use std::cell::RefCell;

    fn shape() -> Shape {
        Shape {
            prefix: "t:".into(),
            bucket_max: 2,
        }
    }

    /// A store the tests drive machines against, counting reads.
    #[derive(Default)]
    struct Store {
        data: RefCell<HashMap<String, Vec<u8>>>,
        reads: RefCell<usize>,
    }

    impl Store {
        fn run<T: 'static>(&self, machine: Machine<T>) -> T {
            machine.run(|keys| {
                *self.reads.borrow_mut() += 1;
                let data = self.data.borrow();
                keys.iter()
                    .filter_map(|k| data.get(k).map(|v| (k.clone(), v.clone())))
                    .collect()
            })
        }

        fn update(&self, root: &str, updates: &[(&str, &str)], removals: &[&str]) -> String {
            let updates = updates
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect();
            let removals = removals.iter().map(|k| k.to_string()).collect();
            let root = root.to_string();
            let out = self
                .run(Machine::new(move |io| {
                    update(io, shape(), root, updates, removals, HashMap::new())
                }))
                .unwrap();
            let mut data = self.data.borrow_mut();
            for (hash, raw) in out.nodes {
                data.insert(format!("t:{hash}"), raw);
            }
            out.root
        }

        fn items(&self, root: &str) -> BTreeMap<String, Vec<u8>> {
            let root = root.to_string();
            let (items, _) = self
                .run(Machine::new(move |io| walk(io, shape(), root, |_| false)))
                .unwrap();
            items.into_iter().collect()
        }
    }

    fn keys(n: usize) -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("key/{i}"), format!("value {i}")))
            .collect()
    }

    #[test]
    fn the_same_contents_make_the_same_tree_however_built() {
        let store = Store::default();
        let all = keys(40);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let at_once = store.update(&EMPTY_HASH, &pairs, &[]);
        let mut one_by_one = EMPTY_HASH.clone();
        for pair in pairs.iter().rev() {
            one_by_one = store.update(&one_by_one, &[*pair], &[]);
        }
        assert_eq!(at_once, one_by_one);
        // Grown past and shrunk back.
        let extra = keys(60);
        let more: Vec<(&str, &str)> = extra[40..]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let grown = store.update(&at_once, &more, &[]);
        let gone: Vec<&str> = more.iter().map(|(k, _)| *k).collect();
        assert_eq!(store.update(&grown, &[], &gone), at_once);
        // Emptied.
        let every: Vec<&str> = pairs.iter().map(|(k, _)| *k).collect();
        assert_eq!(store.update(&at_once, &[], &every), *EMPTY_HASH);
    }

    #[test]
    fn reads_find_what_was_written() {
        let store = Store::default();
        let all = keys(30);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let root = store.update(&EMPTY_HASH, &pairs, &[]);
        assert_eq!(store.items(&root).len(), 30);
        let wanted = vec![
            "key/3".to_string(),
            "key/29".to_string(),
            "absent".to_string(),
            "key/3".to_string(),
        ];
        let r = root.clone();
        *store.reads.borrow_mut() = 0;
        let found = store
            .run(Machine::new(move |io| get_many(io, shape(), r, wanted)))
            .unwrap();
        assert_eq!(
            found.into_iter().collect::<BTreeMap<_, _>>(),
            BTreeMap::from([
                ("key/29".to_string(), b"value 29".to_vec()),
                ("key/3".to_string(), b"value 3".to_vec())
            ])
        );
        // One read per level, not per key.
        let depth = *store.reads.borrow();
        assert!(depth <= 6, "{depth} reads");
    }

    #[test]
    fn a_diff_reports_each_change_once() {
        let store = Store::default();
        let all = keys(30);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let a = store.update(&EMPTY_HASH, &pairs, &[]);
        let b = store.update(
            &a,
            &[("key/1", "changed"), ("new", "added")],
            &["key/2", "key/3"],
        );
        let d = store
            .run(Machine::new(move |io| diff(io, shape(), a, b)))
            .unwrap();
        assert_eq!(d.added, vec![("new".to_string(), b"added".to_vec())]);
        assert_eq!(
            d.removed,
            vec![
                ("key/2".to_string(), b"value 2".to_vec()),
                ("key/3".to_string(), b"value 3".to_vec())
            ]
        );
        assert_eq!(
            d.modified,
            vec![(
                "key/1".to_string(),
                b"value 1".to_vec(),
                b"changed".to_vec()
            )]
        );
    }

    #[test]
    fn a_diff_lines_a_leaf_up_with_a_branch() {
        let store = Store::default();
        let a = store.update(&EMPTY_HASH, &[("x", "1")], &[]);
        let all = keys(10);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let b = store.update(&a, &pairs, &[]);
        let d = store
            .run(Machine::new(move |io| diff(io, shape(), a, b)))
            .unwrap();
        assert_eq!(d.added.len(), 10);
        assert!(d.removed.is_empty() && d.modified.is_empty());
    }

    #[test]
    fn a_walk_skips_what_it_was_told_it_has_seen() {
        let store = Store::default();
        let all = keys(20);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let a = store.update(&EMPTY_HASH, &pairs, &[]);
        let b = store.update(&a, &[("key/0", "changed")], &[]);
        let ra = a.clone();
        let (_, seen) = store
            .run(Machine::new(move |io| walk(io, shape(), ra, |_| false)))
            .unwrap();
        let seen: HashSet<String> = seen.into_iter().collect();
        let (items, nodes) = store
            .run(Machine::new(move |io| {
                walk(io, shape(), b, move |h| seen.contains(h))
            }))
            .unwrap();
        assert!(items.len() < 20 && items.iter().any(|(k, _)| k == "key/0"));
        assert!(!nodes.is_empty());
    }

    #[test]
    fn a_missing_node_reads_as_absent() {
        let store = Store::default();
        let all = keys(20);
        let pairs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let root = store.update(&EMPTY_HASH, &pairs, &[]);
        let victim = store
            .data
            .borrow()
            .keys()
            .find(|k| !k.ends_with(&root))
            .cloned()
            .unwrap();
        store.data.borrow_mut().remove(&victim);
        assert!(store.items(&root).len() < 20);
        // An insert under the hole starts again from a leaf.
        let after = store.update(&root, &[("key/0", "again")], &[]);
        assert!(!store.items(&after).is_empty());
    }

    #[test]
    fn a_leaf_must_hold_something() {
        let store = Store::default();
        let shape = Shape {
            bucket_max: 0,
            ..shape()
        };
        let out = store.run(Machine::new(move |io| {
            update(
                io,
                shape,
                EMPTY_HASH.clone(),
                vec![("k".into(), b"v".to_vec())],
                vec![],
                HashMap::new(),
            )
        }));
        assert_eq!(out, Err(TreeError::BadShape));
    }

    #[test]
    fn a_malformed_node_is_an_error() {
        let store = Store::default();
        store
            .data
            .borrow_mut()
            .insert("t:bad".into(), b"{\"kind\":\"leaf\"}".to_vec());
        let out = store.run(Machine::new(|io| {
            walk(io, shape(), "bad".into(), |_| false)
        }));
        assert!(matches!(out, Err(TreeError::Malformed { .. })));
    }

    #[test]
    fn nodes_encode_as_python_writes_them() {
        let leaf = Node::Leaf(BTreeMap::from([
            ("b".into(), "AA==".into()),
            ("\u{e9}".into(), "AQ==".into()),
        ]));
        let want = "{\"items\":{\"b\":\"AA==\",\"\\u00e9\":\"AQ==\"},\"kind\":\"leaf\"}";
        assert_eq!(String::from_utf8(leaf.encode()).unwrap(), want);
        assert_eq!(Node::decode(&leaf.encode(), "x").unwrap(), leaf);
        assert_eq!(Node::empty().encode(), crate::layout::EMPTY_LEAF.as_bytes());
    }
}
