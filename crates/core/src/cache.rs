//! A repository's memory of what its store can never change.
//!
//! Tree nodes and a commit's records (its parents, their generations,
//! its time and its info) are written once, under keys named by content
//! or by commit, and never rewritten. A read of one can be answered from
//! memory for as long as the store still holds it, which saves the round
//! trip that most reads of them are on a networked backend.
//!
//! What the cache must not outlive is a deletion. Only a sweep deletes
//! these keys, and every sweep rewrites the GC lease record, which each
//! commit reads before it builds. So the cache remembers the record it
//! last saw and empties itself when a read finds another: any process's
//! sweep is noticed before this one builds on what it may have removed.
//! A sweep run through this cache evicts what it removes as it does so.
//!
//! A tree node is held decoded, never also as stored, and charged what
//! it takes in memory, so the budget bounds both kinds together.
//!
//! A commit's root is never held: it is the store's answer to whether a
//! commit exists. Absence is never held either: a key missing now may be
//! written by the next commit.

use crate::hash::sha256_hex;
use crate::layout::{
    COMMIT_TIME_PREFIX, INFO_PREFIX, KEYSET_PREFIX, PARENT_COMMIT_PREFIX, PARENT_GENS_PREFIX,
};
use crate::tree::Node;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// The default budget, in bytes: tens of thousands of tree nodes.
pub const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// What an entry costs beyond its value: its key, its slot, the
/// headers. An estimate, so the budget bounds memory rather than just
/// payload.
const ENTRY_OVERHEAD: usize = 96;

const CACHEABLE: &[&str] = &[
    KEYSET_PREFIX,
    PARENT_COMMIT_PREFIX,
    PARENT_GENS_PREFIX,
    COMMIT_TIME_PREFIX,
    INFO_PREFIX,
];

/// Whether `key` is one the store never rewrites.
pub fn cacheable(key: &str) -> bool {
    CACHEABLE.iter().any(|p| key.starts_with(p))
}

/// Whether `key` names a keyset tree node, which the cache holds
/// decoded.
pub fn is_node(key: &str) -> bool {
    key.starts_with(KEYSET_PREFIX)
}

/// A cache several operations, and threads, share.
pub type Shared = Arc<Mutex<Cache>>;

/// A value held: a tree node decoded, so a read through the cache
/// doesn't parse it again, or anything else as stored.
#[derive(Debug)]
enum Held {
    Bytes(Vec<u8>),
    Node(Arc<Node>),
}

#[derive(Debug)]
struct Slot {
    held: Held,
    cost: usize,
    used: u64,
}

/// Least-recently-used memory of write-once keys, bounded in bytes.
/// Tree nodes and commit records share the one budget and the one
/// order of use.
#[derive(Debug)]
pub struct Cache {
    max_bytes: usize,
    entries: HashMap<String, Slot>,
    /// Each entry's key by when it was last used, oldest first.
    order: BTreeMap<u64, String>,
    tick: u64,
    bytes: usize,
    /// The lease record last seen: `None` before any.
    lease: Option<Option<Vec<u8>>>,
    /// Reads answered from memory.
    pub hits: u64,
    /// Reads of cacheable keys that went to the store.
    pub misses: u64,
    /// How often a sweep emptied the cache.
    pub clears: u64,
}

fn cost_of(key: &str, held: &Held) -> usize {
    key.len()
        + ENTRY_OVERHEAD
        + match held {
            Held::Bytes(value) => value.len(),
            Held::Node(node) => node.footprint(),
        }
}

impl Cache {
    pub fn new(max_bytes: usize) -> Cache {
        Cache {
            max_bytes,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
            bytes: 0,
            lease: None,
            hits: 0,
            misses: 0,
            clears: 0,
        }
    }

    pub fn shared(max_bytes: usize) -> Shared {
        Arc::new(Mutex::new(Cache::new(max_bytes)))
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// What the entries held cost, by the budget's estimate.
    pub fn size_bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    /// The entry under `key`, now the most recently used.
    fn touch(&mut self, key: &str) -> Option<&Held> {
        let tick = self.tick + 1;
        let slot = self.entries.get_mut(key)?;
        self.order.remove(&slot.used);
        slot.used = tick;
        self.order.insert(tick, key.to_string());
        self.tick = tick;
        Some(&slot.held)
    }

    /// The values of those `keys` held here, each now the most recently
    /// used. A node is answered in its stored bytes when encoding it
    /// again gives them, which it does for every node this library
    /// writes.
    pub fn lookup<'k>(
        &mut self,
        keys: impl IntoIterator<Item = &'k str>,
    ) -> HashMap<String, Vec<u8>> {
        let mut found = HashMap::new();
        for key in keys {
            let value = match self.touch(key) {
                Some(Held::Bytes(value)) => value.clone(),
                Some(Held::Node(node)) => {
                    let raw = node.encode();
                    if sha256_hex(&raw) != key[KEYSET_PREFIX.len()..] {
                        continue;
                    }
                    raw
                }
                None => continue,
            };
            found.insert(key.to_string(), value);
        }
        found
    }

    /// The tree nodes held under those of `keys` held here, each now the
    /// most recently used.
    pub fn nodes<'k>(
        &mut self,
        keys: impl IntoIterator<Item = &'k str>,
    ) -> HashMap<String, Arc<Node>> {
        let mut found = HashMap::new();
        for key in keys {
            if let Some(Held::Node(node)) = self.touch(key) {
                found.insert(key.to_string(), node.clone());
            }
        }
        found
    }

    pub fn count(&mut self, hits: u64, misses: u64) {
        self.hits += hits;
        self.misses += misses;
    }

    /// Hold these values, evicting the least recently used to fit. A
    /// tree node is held decoded, and one that doesn't decode isn't
    /// held; nor is one larger than the whole budget.
    pub fn remember<'k>(&mut self, items: impl IntoIterator<Item = (&'k str, &'k [u8])>) {
        for (key, value) in items {
            let held = if is_node(key) {
                match Node::decode(value, &key[KEYSET_PREFIX.len()..]) {
                    Ok(node) => Held::Node(Arc::new(node)),
                    Err(_) => continue,
                }
            } else {
                Held::Bytes(value.to_vec())
            };
            self.hold(key, held);
        }
        self.fit();
    }

    /// Hold these decoded tree nodes, evicting the least recently used
    /// to fit.
    pub fn remember_nodes(&mut self, nodes: impl IntoIterator<Item = (String, Arc<Node>)>) {
        for (key, node) in nodes {
            self.hold(&key, Held::Node(node));
        }
        self.fit();
    }

    fn hold(&mut self, key: &str, held: Held) {
        let cost = cost_of(key, &held);
        if cost > self.max_bytes {
            return;
        }
        self.forget([key]);
        self.tick += 1;
        self.entries.insert(
            key.to_string(),
            Slot {
                held,
                cost,
                used: self.tick,
            },
        );
        self.order.insert(self.tick, key.to_string());
        self.bytes += cost;
    }

    fn fit(&mut self) {
        while self.bytes > self.max_bytes {
            let Some((_, key)) = self.order.pop_first() else {
                break;
            };
            if let Some(slot) = self.entries.remove(&key) {
                self.bytes -= slot.cost;
            }
        }
    }

    pub fn forget<'k>(&mut self, keys: impl IntoIterator<Item = &'k str>) {
        for key in keys {
            if let Some(slot) = self.entries.remove(key) {
                self.order.remove(&slot.used);
                self.bytes -= slot.cost;
            }
        }
    }

    /// Note the lease record a read or write found: another than last
    /// time means a sweep has run, and what it removed may be held here.
    /// So does a first record, if the cache was filled before it: the
    /// sweep that wrote it may have run since. Only a store never swept
    /// has no record.
    pub fn saw_lease(&mut self, record: Option<&[u8]>) {
        let record = record.map(<[u8]>::to_vec);
        let swept = match &self.lease {
            Some(seen) => *seen != record,
            None => record.is_some() && !self.entries.is_empty(),
        };
        if swept {
            self.clear();
            self.clears += 1;
        }
        self.lease = Some(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(n: u8) -> String {
        format!("{PARENT_COMMIT_PREFIX}{n:02}")
    }

    /// A stored leaf and the key it is stored under.
    fn leaf(key: &str) -> (String, Vec<u8>) {
        let node = Node::Leaf(BTreeMap::from([(key.to_string(), "dg==".to_string())]));
        let raw = node.encode();
        (format!("{KEYSET_PREFIX}{}", sha256_hex(&raw)), raw)
    }

    fn one(key: &str, value: &[u8]) -> Cache {
        let mut cache = Cache::new(DEFAULT_CACHE_BYTES);
        cache.remember([(key, value)]);
        cache
    }

    #[test]
    fn the_least_recently_used_goes_first() {
        let one = cost_of(&record(1), &Held::Bytes(vec![0; 10]));
        let mut cache = Cache::new(2 * one);
        cache.remember([
            (record(1).as_str(), &[0u8; 10][..]),
            (record(2).as_str(), &[0; 10]),
        ]);
        cache.lookup([record(1).as_str()]);
        cache.remember([(record(3).as_str(), &[0u8; 10][..])]);
        let held = cache.lookup([record(1).as_str(), record(2).as_str(), record(3).as_str()]);
        assert!(held.contains_key(&record(1)) && held.contains_key(&record(3)));
        assert!(!held.contains_key(&record(2)));
        assert_eq!(cache.size_bytes(), 2 * one);
    }

    #[test]
    fn a_node_is_held_decoded_and_answered_in_its_stored_bytes() {
        let (key, raw) = leaf("k");
        let mut cache = one(&key, &raw);
        let nodes = cache.nodes([key.as_str()]);
        assert_eq!(nodes[&key].encode(), raw);
        assert_eq!(cache.lookup([key.as_str()])[&key], raw);
        assert_eq!(
            cache.size_bytes(),
            key.len() + ENTRY_OVERHEAD + nodes[&key].footprint()
        );
    }

    #[test]
    fn a_node_that_does_not_decode_is_not_held() {
        let cache = one(&format!("{KEYSET_PREFIX}00"), b"not a node");
        assert!(cache.is_empty());
    }

    #[test]
    fn nodes_and_records_share_the_budget() {
        let (key, raw) = leaf("k");
        let node_cost = one(&key, &raw).size_bytes();
        let record_cost = cost_of(&record(1), &Held::Bytes(vec![0; 10]));
        let mut cache = Cache::new(node_cost + record_cost);
        cache.remember([(key.as_str(), raw.as_slice())]);
        cache.remember([(record(1).as_str(), &[0u8; 10][..])]);
        assert_eq!(cache.len(), 2);
        cache.remember([(record(2).as_str(), &[0u8; 10][..])]);
        assert!(cache.nodes([key.as_str()]).is_empty());
        assert_eq!(cache.size_bytes(), 2 * record_cost);
    }

    #[test]
    fn a_new_lease_record_empties_it() {
        let mut cache = Cache::new(DEFAULT_CACHE_BYTES);
        cache.saw_lease(None);
        cache.remember([(record(1).as_str(), &b"n"[..])]);
        cache.saw_lease(None);
        assert_eq!(cache.len(), 1);
        cache.saw_lease(Some(b"a sweep"));
        assert!(cache.is_empty());
        assert_eq!(cache.clears, 1);
    }

    #[test]
    fn a_first_lease_record_empties_what_was_held_before_it() {
        // Filled before any read of the lease: a sweep may have run since.
        let mut cache = one(&record(1), b"n");
        cache.saw_lease(Some(b"a sweep"));
        assert!(cache.is_empty());
        assert_eq!(cache.clears, 1);

        // Decoded nodes alone count as filled.
        let (key, raw) = leaf("k");
        let mut cache = one(&key, &raw);
        cache.saw_lease(Some(b"a sweep"));
        assert!(cache.nodes([key.as_str()]).is_empty());

        // A store never swept has no record, so nothing is cleared.
        let mut cache = one(&record(1), b"n");
        cache.saw_lease(None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn only_write_once_keys_are_cacheable() {
        assert!(cacheable(&record(1)));
        assert!(cacheable(&format!("{KEYSET_PREFIX}abc")));
        assert!(!cacheable("__commit_root__abc"));
        assert!(!cacheable("__branch_head__main"));
        assert!(!cacheable("kvgit:blob:abc"));
    }
}
