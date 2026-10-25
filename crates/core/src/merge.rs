//! Three-way merge resolution over keyset changes.
//!
//! Only keys a side changed since the common ancestor are looked at: a
//! key neither changed is the same on both and stays, so the cost
//! follows the size of the change. The answer is a change to our side.
//!
//! A key is governed by its most specific registration: its exact key,
//! else the longest registered prefix it starts with, else the default.
//! What the registration holds decides its reach. A side, [`Rule::Ours`]
//! or [`Rule::Theirs`], is a standing policy: that side's state stands
//! for every key it covers that either side changed. A merge function
//! is consulted only where both sides changed a key, and differently.
//! The text merge runs in the core; any other function is the host's,
//! asked by [`MergeAsk`].

use crate::error::Error;
use crate::keyset::Entry;
use crate::layout::{BLOB_PREFIX, blob_key};
use crate::machine::Io;
use crate::text::TextRule;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One key's change from the common ancestor to one side: its entry
/// there and here, `None` where it is absent.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub old: Option<Entry>,
    pub new: Option<Entry>,
}

/// What a registration holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rule {
    /// Our side, for every key covered that either side changed.
    Ours,
    /// Their side, likewise.
    Theirs,
    /// The text merge, for keys both sides changed.
    Text(TextRule),
    /// The host's merge function by its number, for keys both sides
    /// changed.
    Host(u32),
}

/// The registrations a merge resolves by.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rules {
    pub keys: HashMap<String, Rule>,
    pub prefixes: Vec<(String, Rule)>,
    pub default: Option<Rule>,
}

impl Rules {
    /// The registration that governs `key`, if any.
    pub fn pick(&self, key: &str) -> Option<&Rule> {
        if let Some(rule) = self.keys.get(key) {
            return Some(rule);
        }
        let mut best: Option<(&str, &Rule)> = None;
        for (prefix, rule) in &self.prefixes {
            if key.starts_with(prefix.as_str()) && best.is_none_or(|(b, _)| prefix.len() > b.len())
            {
                best = Some((prefix, rule));
            }
        }
        best.map(|(_, rule)| rule).or(self.default.as_ref())
    }
}

/// A host merge function to run on one key's three sides, each named
/// by its blob key, `None` where the key is absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeAsk {
    pub rule: u32,
    pub key: String,
    pub old: Option<String>,
    pub ours: Option<String>,
    pub theirs: Option<String>,
}

/// A merge function's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeAnswer {
    /// A value: the host has hashed its bytes as `blob` and holds them
    /// for the write.
    Value {
        blob: String,
        size: u64,
    },
    /// That side's value, as it stands.
    Ours,
    Theirs,
    /// The function refused the key, for this reason: a conflict.
    Failed(String),
}

/// A value a merge produced, to be written as a blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Merged {
    /// A host function's, whose bytes the host holds.
    Held { blob: String, size: u64 },
    /// The text merge's.
    Made(Vec<u8>),
}

impl Merged {
    /// The value's blob key and size.
    pub fn blob(&self) -> (String, u64) {
        match self {
            Merged::Held { blob, size } => (blob.clone(), *size),
            Merged::Made(bytes) => (blob_key(bytes), bytes.len() as u64),
        }
    }
}

/// A resolved merge, as changes to our side. Nothing here restates a
/// key whose merged state is already ours.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resolution {
    pub updates: BTreeMap<String, Entry>,
    pub removals: BTreeSet<String>,
    /// Values merge functions produced, each still to be written.
    pub merged: BTreeMap<String, Merged>,
    /// Keys a rule decided: contested keys a function resolved, and
    /// keys under a side's policy that either side changed. Sorted.
    pub auto_merged: Vec<String>,
    /// Keys only the other side changed, taken as they were. Sorted.
    pub carried: Vec<String>,
}

impl Resolution {
    /// Whether the merge changes our side at all.
    pub fn changes_anything(&self) -> bool {
        !(self.updates.is_empty() && self.removals.is_empty() && self.merged.is_empty())
    }

    /// Make `entry` the merged state of `key`, whose state on our side
    /// is `ours`.
    fn take(&mut self, key: &str, entry: Option<&Entry>, ours: Option<&Entry>) {
        if entry == ours {
            return;
        }
        match entry {
            None => {
                self.removals.insert(key.to_string());
            }
            Some(entry) => {
                self.updates.insert(key.to_string(), entry.clone());
            }
        }
    }
}

/// A key both sides changed, differently.
struct Contested<'a> {
    key: &'a str,
    ancestor: Option<&'a Entry>,
    ours: Option<&'a Entry>,
    theirs: Option<&'a Entry>,
    rule: Option<&'a Rule>,
}

/// Resolve a merge from each side's changes since the common ancestor.
pub async fn resolve(
    io: &Io,
    ours: &BTreeMap<String, Change>,
    theirs: &BTreeMap<String, Change>,
    rules: &Rules,
) -> Result<Resolution, Error> {
    let mut out = Resolution::default();
    let mut auto: BTreeSet<String> = BTreeSet::new();
    let mut contested: Vec<Contested> = Vec::new();
    let keys: BTreeSet<&String> = ours.keys().chain(theirs.keys()).collect();
    for key in keys {
        let our_change = ours.get(key);
        let their_change = theirs.get(key);
        let ancestor = our_change.or(their_change).and_then(|c| c.old.as_ref());
        let our_entry = our_change.map_or(ancestor, |c| c.new.as_ref());
        let their_entry = their_change.map_or(ancestor, |c| c.new.as_ref());
        let rule = rules.pick(key);

        // A side's policy: its state stands, touched or not. Nothing is
        // read.
        if let Some(Rule::Ours | Rule::Theirs) = rule {
            if rule == Some(&Rule::Theirs) {
                out.take(key, their_entry, our_entry);
            }
            auto.insert(key.clone());
            continue;
        }
        if their_change.is_none() {
            continue; // changed only by us: ours stands
        }
        if our_change.is_none() {
            out.take(key, their_entry, our_entry); // changed only by them
            out.carried.push(key.clone());
            continue;
        }
        match (our_entry, their_entry) {
            (None, None) => continue,                           // both removed it
            (Some(o), Some(t)) if o.blob == t.blob => continue, // the same change
            _ => {}
        }
        contested.push(Contested {
            key,
            ancestor,
            ours: our_entry,
            theirs: their_entry,
            rule,
        });
    }

    // A blob written before storage v4 is keyed by the commit that wrote
    // it, so equal bytes can sit under two keys: the same change after
    // all, taking their key. Content-keyed blobs under different keys
    // differ, so only the older kind is read. Two missing blobs are
    // damage, not agreement, and stay contested.
    let legacy = |e: Option<&Entry>| e.is_some_and(|e| !e.blob.starts_with(BLOB_PREFIX));
    let to_compare: Vec<String> = contested
        .iter()
        .filter(|c| c.ours.is_some() && c.theirs.is_some() && (legacy(c.ours) || legacy(c.theirs)))
        .flat_map(|c| [c.ours, c.theirs])
        .map(|e| e.expect("both sides present").blob.clone())
        .collect();
    let compared = io.get(to_compare).await?;
    contested.retain(|c| {
        let (Some(o), Some(t)) = (c.ours, c.theirs) else {
            return true;
        };
        match (compared.get(&o.blob), compared.get(&t.blob)) {
            (Some(a), Some(b)) if a == b => {
                out.take(c.key, c.theirs, c.ours);
                false
            }
            _ => true,
        }
    });

    let mut conflicts: BTreeSet<String> = BTreeSet::new();
    let mut failures: Vec<(String, String)> = Vec::new();
    let blob_of = |e: Option<&Entry>| e.map(|e| e.blob.clone());

    // The text merge reads the three sides of each key it merges, all
    // in one read.
    let text_blobs: Vec<String> = contested
        .iter()
        .filter(|c| matches!(c.rule, Some(Rule::Text(_))))
        .flat_map(|c| [c.ancestor, c.ours, c.theirs])
        .flatten()
        .map(|e| e.blob.clone())
        .collect();
    let values = io.get(text_blobs).await?;
    let value_of = |e: Option<&Entry>| e.and_then(|e| values.get(&e.blob)).map(Vec::as_slice);

    let mut asks: Vec<MergeAsk> = Vec::new();
    let mut asked: Vec<&Contested> = Vec::new();
    for c in &contested {
        match c.rule {
            None => {
                conflicts.insert(c.key.to_string());
            }
            Some(Rule::Text(text)) => {
                match text.merge(value_of(c.ancestor), value_of(c.ours), value_of(c.theirs)) {
                    Ok(bytes) => {
                        out.merged.insert(c.key.to_string(), Merged::Made(bytes));
                        auto.insert(c.key.to_string());
                    }
                    Err(e) => {
                        conflicts.insert(c.key.to_string());
                        failures.push((c.key.to_string(), e.to_string()));
                    }
                }
            }
            Some(Rule::Host(rule)) => {
                asks.push(MergeAsk {
                    rule: *rule,
                    key: c.key.to_string(),
                    old: blob_of(c.ancestor),
                    ours: blob_of(c.ours),
                    theirs: blob_of(c.theirs),
                });
                asked.push(c);
            }
            Some(Rule::Ours | Rule::Theirs) => unreachable!("a side's policy is decided above"),
        }
    }
    let answers = io.merge(asks).await?;
    for (c, answer) in asked.into_iter().zip(answers) {
        match answer {
            MergeAnswer::Failed(why) => {
                conflicts.insert(c.key.to_string());
                failures.push((c.key.to_string(), why));
                continue;
            }
            // That side's committed value: its blob as it stands, or the
            // key removed where that side removed it.
            MergeAnswer::Theirs => out.take(c.key, c.theirs, c.ours),
            MergeAnswer::Ours => {}
            MergeAnswer::Value { blob, size } => {
                out.merged
                    .insert(c.key.to_string(), Merged::Held { blob, size });
            }
        }
        auto.insert(c.key.to_string());
    }

    if !conflicts.is_empty() {
        failures.sort();
        return Err(Error::MergeConflict {
            keys: conflicts.into_iter().collect(),
            failures,
        });
    }
    out.auto_merged = auto.into_iter().collect();
    Ok(out)
}
