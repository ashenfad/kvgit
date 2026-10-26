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
//! A commit's root is never held: it is the store's answer to whether a
//! commit exists. Absence is never held either: a key missing now may be
//! written by the next commit.

use crate::layout::{
    COMMIT_TIME_PREFIX, INFO_PREFIX, KEYSET_PREFIX, PARENT_COMMIT_PREFIX, PARENT_GENS_PREFIX,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// The default budget, in bytes: tens of thousands of tree nodes.
pub const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// What an entry costs beyond its bytes: its key, its slot, the
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

/// A cache several operations, and threads, share.
pub type Shared = Arc<Mutex<Cache>>;

/// Least-recently-used memory of write-once keys, bounded in bytes.
#[derive(Debug)]
pub struct Cache {
    max_bytes: usize,
    entries: HashMap<String, (Vec<u8>, u64)>,
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

fn cost(key: &str, value: &[u8]) -> usize {
    key.len() + value.len() + ENTRY_OVERHEAD
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

    /// The values of those `keys` held here, each now the most recently
    /// used.
    pub fn lookup<'k>(
        &mut self,
        keys: impl IntoIterator<Item = &'k str>,
    ) -> HashMap<String, Vec<u8>> {
        let mut found = HashMap::new();
        for key in keys {
            let tick = self.tick + 1;
            if let Some((value, used)) = self.entries.get_mut(key) {
                self.order.remove(used);
                *used = tick;
                self.order.insert(tick, key.to_string());
                self.tick = tick;
                found.insert(key.to_string(), value.clone());
            }
        }
        found
    }

    pub fn count(&mut self, hits: u64, misses: u64) {
        self.hits += hits;
        self.misses += misses;
    }

    /// Hold these values, evicting the least recently used to fit. One
    /// larger than the whole budget isn't held.
    pub fn remember<'k>(&mut self, items: impl IntoIterator<Item = (&'k str, &'k [u8])>) {
        for (key, value) in items {
            let cost = cost(key, value);
            if cost > self.max_bytes {
                continue;
            }
            self.forget([key]);
            self.tick += 1;
            self.entries
                .insert(key.to_string(), (value.to_vec(), self.tick));
            self.order.insert(self.tick, key.to_string());
            self.bytes += cost;
        }
        while self.bytes > self.max_bytes {
            let Some((_, key)) = self.order.pop_first() else {
                break;
            };
            if let Some((value, _)) = self.entries.remove(&key) {
                self.bytes -= cost(&key, &value);
            }
        }
    }

    pub fn forget<'k>(&mut self, keys: impl IntoIterator<Item = &'k str>) {
        for key in keys {
            if let Some((value, used)) = self.entries.remove(key) {
                self.order.remove(&used);
                self.bytes -= cost(key, &value);
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

    fn node(n: u8) -> String {
        format!("{KEYSET_PREFIX}{n:02}")
    }

    #[test]
    fn the_least_recently_used_goes_first() {
        let one = cost(&node(1), &[0; 10]);
        let mut cache = Cache::new(2 * one);
        cache.remember([
            (node(1).as_str(), &[0u8; 10][..]),
            (node(2).as_str(), &[0; 10]),
        ]);
        cache.lookup([node(1).as_str()]);
        cache.remember([(node(3).as_str(), &[0u8; 10][..])]);
        let held = cache.lookup([node(1).as_str(), node(2).as_str(), node(3).as_str()]);
        assert!(held.contains_key(&node(1)) && held.contains_key(&node(3)));
        assert!(!held.contains_key(&node(2)));
        assert_eq!(cache.size_bytes(), 2 * one);
    }

    #[test]
    fn a_new_lease_record_empties_it() {
        let mut cache = Cache::new(DEFAULT_CACHE_BYTES);
        cache.saw_lease(None);
        cache.remember([(node(1).as_str(), &b"n"[..])]);
        cache.saw_lease(None);
        assert_eq!(cache.len(), 1);
        cache.saw_lease(Some(b"a sweep"));
        assert!(cache.is_empty());
        assert_eq!(cache.clears, 1);
    }

    #[test]
    fn a_first_lease_record_empties_what_was_held_before_it() {
        // Filled before any read of the lease: a sweep may have run since.
        let mut cache = Cache::new(DEFAULT_CACHE_BYTES);
        cache.remember([(node(1).as_str(), &b"n"[..])]);
        cache.saw_lease(Some(b"a sweep"));
        assert!(cache.is_empty());
        assert_eq!(cache.clears, 1);

        // A store never swept has no record, so nothing is cleared.
        let mut cache = Cache::new(DEFAULT_CACHE_BYTES);
        cache.remember([(node(1).as_str(), &b"n"[..])]);
        cache.saw_lease(None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn only_write_once_keys_are_cacheable() {
        assert!(cacheable(&node(1)));
        assert!(cacheable("__parent_commit__abc"));
        assert!(!cacheable("__commit_root__abc"));
        assert!(!cacheable("__branch_head__main"));
        assert!(!cacheable("kvgit:blob:abc"));
    }
}
