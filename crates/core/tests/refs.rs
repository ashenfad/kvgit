//! Branches, tags and head recovery under the interleavings the corpus
//! can't stage: a write that loses to another writer between its read
//! and its write, a sweep's lease held across a create, and the commit
//! scan's choice among equals.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, dumps};
use kvgit_core::layout::{
    EMPTY_HASH, GC_LEASE_KEY, branch_head, commit_root, commit_time, parent_commit, tag_info_key,
};
use kvgit_core::machine::{Cas, Reply, Request, Step};
use kvgit_core::ops::{self, Op, Out};
use std::collections::BTreeMap;

const T0: f64 = 1_700_000_000.5;

struct Store {
    data: BTreeMap<String, Vec<u8>>,
    clock: f64,
}

impl Store {
    fn new() -> Store {
        Store {
            data: BTreeMap::new(),
            clock: T0,
        }
    }

    fn put(&mut self, key: String, value: &Value) {
        self.data.insert(key, dumps(value, false).into_bytes());
    }

    /// A commit with these parents, at `time`.
    fn commit(&mut self, hash: &str, parents: &[&str], time: f64) {
        self.put(commit_root(hash), &Value::Str(EMPTY_HASH.clone()));
        let parents = parents.iter().map(|p| Value::Str(p.to_string())).collect();
        self.put(parent_commit(hash), &Value::Array(parents));
        self.put(commit_time(hash), &Value::Float(time));
    }

    fn head(&mut self, branch: &str, commit: &str) {
        self.put(branch_head(branch), &Value::Str(commit.into()));
    }

    fn perform(&mut self, request: Request) -> Reply {
        match request {
            Request::Get(keys) => Reply::Values(
                keys.into_iter()
                    .filter_map(|k| self.data.get(&k).map(|v| (k, v.clone())))
                    .collect(),
            ),
            Request::Scan(prefix) => Reply::Keys(
                self.data
                    .keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect(),
            ),
            Request::Cas(Cas {
                expect,
                writes,
                removes,
            }) => {
                if expect.iter().any(|(k, v)| self.data.get(k) != v.as_ref()) {
                    return Reply::Landed(false);
                }
                for (key, write) in writes {
                    let kvgit_core::machine::Write::Bytes(bytes) = write else {
                        panic!("no held bytes here")
                    };
                    self.data.insert(key, bytes);
                }
                for key in removes {
                    self.data.remove(&key);
                }
                Reply::Landed(true)
            }
            Request::Wait(seconds) => {
                self.clock += seconds;
                Reply::Waited
            }
            Request::Now => Reply::Now(self.clock),
            Request::Recover(_) => Reply::Recovered(None),
            Request::Merge(_) => panic!("no merges here"),
        }
    }

    /// Run `op`, calling `before` with each request before performing
    /// it, so a test can act as another writer at that point.
    fn run(
        &mut self,
        mut op: Op,
        mut before: impl FnMut(&mut Store, &Request),
    ) -> Result<Value, Error> {
        let mut step = op.resume_at(self.clock, None);
        loop {
            match step {
                Step::Done(done) => {
                    return done.result.map(|out| match out {
                        Out::Value(v) => v,
                        Out::Committed(_) => panic!("committed"),
                    });
                }
                Step::Request(r) => {
                    before(self, &r);
                    let reply = self.perform(r);
                    step = op.resume_at(self.clock, Some(reply));
                }
            }
        }
    }
}

fn call(name: &str, args: &[(&str, Value)]) -> Op {
    let args = args
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    ops::call(name, Value::Object(args)).unwrap()
}

fn s(text: &str) -> Value {
    Value::Str(text.into())
}

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccc";

#[test]
fn a_delete_leaves_a_branch_that_moved_after_it_was_read() {
    let mut store = Store::new();
    store.commit(A, &[], T0);
    store.commit(B, &[A], T0 + 1.0);
    store.head("main", A);
    let op = call("delete_branch", &[("name", s("main"))]);
    let result = store.run(op, |store, r| {
        if matches!(r, Request::Cas(_)) {
            store.head("main", B);
        }
    });
    assert!(matches!(result, Err(Error::Concurrency(_))));
    assert!(store.data.contains_key(&branch_head("main")));
}

#[test]
fn a_delete_leaves_a_tag_made_again_at_the_same_commit() {
    let mut store = Store::new();
    store.commit(A, &[], T0);
    let create = |store: &mut Store| {
        let op = call(
            "create_tag",
            &[("name", s("v1")), ("target", s(A)), ("info", Value::Null)],
        );
        store.run(op, |_, _| {}).unwrap();
    };
    create(&mut store);
    let op = call("delete_tag", &[("name", s("v1"))]);
    let result = store.run(op, |store, r| {
        if matches!(r, Request::Cas(_)) {
            // Another writer deletes the tag and makes it again, later.
            store.data.remove(&branch_head("refs/tags/v1"));
            store.data.remove(&tag_info_key("v1"));
            store.clock += 1.0;
            create(store);
        }
    });
    assert!(matches!(result, Err(Error::Concurrency(_))));
    assert!(store.data.contains_key(&tag_info_key("v1")));
}

#[test]
fn a_branch_is_made_after_a_live_sweep_ends() {
    let mut store = Store::new();
    store.commit(A, &[], T0);
    let expires = T0 + 30.0;
    store.put(
        GC_LEASE_KEY.into(),
        &Value::Object(vec![
            ("owner".into(), s("gc")),
            ("expires".into(), Value::Float(expires)),
        ]),
    );
    let op = call("create_branch", &[("name", s("dev")), ("target", s(A))]);
    let mut landed_at = None;
    store
        .run(op, |store, r| {
            if let Request::Cas(cas) = r
                && cas.writes.iter().any(|(k, _)| *k == branch_head("dev"))
            {
                landed_at = Some(store.clock);
            }
        })
        .unwrap();
    assert!(landed_at.unwrap() >= expires);
}

#[test]
fn the_commit_scan_takes_the_newest_unclaimed_tip_and_breaks_ties_by_hash() {
    let mut store = Store::new();
    // `other` claims A; B and C are unclaimed tips made at one time.
    store.commit(A, &[], T0);
    store.commit(B, &[A], T0 + 5.0);
    store.commit(C, &[A], T0 + 5.0);
    store.head("other", A);
    store.put(branch_head("main"), &s("garbage"));
    let op = call("recover_by_commit_scan", &[("branch", s("main"))]);
    assert_eq!(store.run(op, |_, _| {}).unwrap(), s(B));
}
