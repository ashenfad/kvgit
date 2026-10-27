//! Ancestry over hand-built histories: the delta between tips, whether
//! one commit is in another's history, and what a receiver holds above
//! a generation, each the same with stored generations as without, and
//! a search on a long history reading only the stretch it is about.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, dumps};
use kvgit_core::layout::{parent_commit, parent_gens};
use kvgit_core::machine::{Reply, Request, Step};
use kvgit_core::ops::{self, Out};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Default)]
struct Store {
    data: BTreeMap<String, Vec<u8>>,
    gens: HashMap<String, i64>,
    reads: usize,
}

impl Store {
    /// A commit with these parents, its parents' generations stored or
    /// not.
    fn commit(&mut self, hash: &str, parents: &[&str], generations: bool) {
        let gens: Vec<i64> = parents.iter().map(|p| self.gens[*p]).collect();
        self.gens
            .insert(hash.into(), gens.iter().map(|g| g + 1).max().unwrap_or(0));
        let list = |items: Vec<Value>| dumps(&Value::Array(items), false).into_bytes();
        self.data.insert(
            parent_commit(hash),
            list(parents.iter().map(|p| Value::Str(p.to_string())).collect()),
        );
        if generations && !parents.is_empty() {
            self.data.insert(
                parent_gens(hash),
                list(gens.into_iter().map(Value::Int).collect()),
            );
        }
    }

    fn call(&mut self, name: &str, args: &[(&str, Value)]) -> Result<Value, Error> {
        let args = args
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let mut op = ops::call(name, Value::Object(args)).unwrap();
        let mut step = op.resume_at(0.0, None);
        loop {
            match step {
                Step::Done(done) => {
                    return done.result.map(|out| match out {
                        Out::Value(v) => v,
                        Out::Committed(_) => panic!("committed"),
                    });
                }
                Step::Request(Request::Get(keys)) => {
                    self.reads += 1;
                    let found = keys
                        .into_iter()
                        .filter_map(|k| self.data.get(&k).map(|v| (k, v.clone())))
                        .collect();
                    step = op.resume_at(0.0, Some(Reply::Values(found)));
                }
                Step::Request(r) => panic!("unexpected request {r:?}"),
            }
        }
    }

    fn delta(&mut self, want: &str, have: &[&str]) -> Result<BTreeSet<String>, Error> {
        let found = self.call("ancestry_delta", &[("want", s(want)), ("have", strs(have))])?;
        let Value::Array(items) = found else {
            panic!("a list")
        };
        Ok(items
            .into_iter()
            .map(|item| match item {
                Value::Array(pair) => pair[0].as_str().unwrap().to_string(),
                _ => panic!("a pair"),
            })
            .collect())
    }

    fn is_ancestor(&mut self, ancestor: &str, from: &str) -> bool {
        match self.call(
            "is_ancestor",
            &[("ancestor", s(ancestor)), ("from", s(from))],
        ) {
            Ok(Value::Bool(b)) => b,
            other => panic!("{other:?}"),
        }
    }

    fn above(&mut self, have: &[&str], floor: i64) -> BTreeSet<String> {
        match self.call(
            "ancestors_above",
            &[("have", strs(have)), ("floor", Value::Int(floor))],
        ) {
            Ok(Value::Array(items)) => items
                .into_iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
            other => panic!("{other:?}"),
        }
    }
}

fn s(text: &str) -> Value {
    Value::Str(text.into())
}

fn strs(items: &[&str]) -> Value {
    Value::Array(items.iter().map(|t| s(t)).collect())
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|t| t.to_string()).collect()
}

/// root ← a ← b ← m, root ← c ← m: two lines merged, then d on top.
fn diamond(generations: bool) -> Store {
    let mut store = Store::default();
    store.commit("root", &[], generations);
    store.commit("a", &["root"], generations);
    store.commit("b", &["a"], generations);
    store.commit("c", &["root"], generations);
    store.commit("m", &["b", "c"], generations);
    store.commit("d", &["m"], generations);
    store
}

#[test]
fn the_delta_is_what_want_has_and_have_lacks() {
    for generations in [true, false] {
        let mut store = diamond(generations);
        assert_eq!(
            store.delta("d", &[]).unwrap(),
            set(&["root", "a", "b", "c", "m", "d"])
        );
        assert_eq!(store.delta("d", &["b"]).unwrap(), set(&["c", "m", "d"]));
        assert_eq!(store.delta("d", &["b", "c"]).unwrap(), set(&["m", "d"]));
        assert_eq!(store.delta("b", &["d"]).unwrap(), set(&[]));
        assert_eq!(store.delta("c", &["b"]).unwrap(), set(&["c"]));
        // a commit of have the store lacks contributes nothing
        assert_eq!(
            store.delta("b", &["gone"]).unwrap(),
            set(&["root", "a", "b"])
        );
    }
}

#[test]
fn a_missing_commit_on_wants_side_is_an_unknown_commit() {
    for generations in [true, false] {
        let mut store = diamond(generations);
        let missing = store.delta("gone", &[]);
        assert!(matches!(missing, Err(Error::UnknownCommit { commit }) if commit == "gone"));
        store.data.remove(&parent_commit("c"));
        let hollow = store.delta("d", &["b"]);
        assert!(matches!(hollow, Err(Error::UnknownCommit { commit }) if commit == "c"));
    }
}

#[test]
fn ancestry_reads_each_commits_history() {
    for generations in [true, false] {
        let mut store = diamond(generations);
        assert!(store.is_ancestor("root", "d"));
        assert!(store.is_ancestor("c", "m"));
        assert!(store.is_ancestor("d", "d"));
        assert!(!store.is_ancestor("d", "m"));
        assert!(!store.is_ancestor("b", "c"));
        assert!(!store.is_ancestor("gone", "d"));
        assert!(!store.is_ancestor("root", "gone"));
        assert!(!store.is_ancestor("gone", "gone"));
    }
}

#[test]
fn above_a_floor_is_what_have_holds_that_high() {
    let mut store = diamond(true);
    // generations: root 0, a 1, c 1, b 2, m 3, d 4
    assert_eq!(store.above(&["d"], 2), set(&["b", "m", "d"]));
    assert_eq!(store.above(&["m", "c"], 1), set(&["a", "b", "c", "m"]));
    assert_eq!(store.above(&["gone"], 0), set(&[]));
    // without generations, everything have reaches
    let mut old = diamond(false);
    assert_eq!(old.above(&["b"], 2), set(&["root", "a", "b"]));
}

#[test]
fn a_search_on_a_long_history_reads_the_stretch_it_is_about() {
    let mut store = Store::default();
    store.commit("c0", &[], true);
    for i in 1..500 {
        store.commit(&format!("c{i}"), &[&format!("c{}", i - 1)], true);
    }
    store.reads = 0;
    assert_eq!(
        store.delta("c499", &["c497"]).unwrap(),
        set(&["c498", "c499"])
    );
    assert!(store.reads <= 4, "{} reads", store.reads);
    store.reads = 0;
    assert!(store.is_ancestor("c490", "c499"));
    assert!(store.reads <= 12, "{} reads", store.reads);
}
