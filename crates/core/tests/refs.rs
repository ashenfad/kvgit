//! Branches, tags, head recovery, history and the lease under what the
//! corpus can't stage: a write that loses to another writer between its
//! read and its write, a sweep's lease held across a create, the commit
//! scan's choice among equals, the order and reads of a log, and the
//! version stamp's writes.

use kvgit_core::error::Error;
use kvgit_core::json::loads;
use kvgit_core::json::{Value, dumps};
use kvgit_core::layout::STORAGE_VERSION_KEY;
use kvgit_core::layout::{
    EMPTY_HASH, GC_LEASE_KEY, branch_head, commit_root, commit_time, parent_commit, tag_info_key,
};
use kvgit_core::lease::{stamp_version_at_least, wait_for_gc};
use kvgit_core::machine::{Cas, Machine, Reply, Request, Step};
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
            Request::Emit(_) => Reply::Next,
            Request::Check(_) => Reply::Refused(vec![]),
            Request::Owner => Reply::Owner("lease-1".into()),
            Request::Merge(_) => panic!("no merges here"),
        }
    }

    /// Run `op`, calling `before` with each request before performing
    /// it, so a test can act as another writer at that point.
    fn run(&mut self, op: Op, before: impl FnMut(&mut Store, &Request)) -> Result<Value, Error> {
        self.run_emitting(op, before).0
    }

    /// [`run`](Self::run), with the items of the parts it emitted.
    fn run_emitting(
        &mut self,
        mut op: Op,
        mut before: impl FnMut(&mut Store, &Request),
    ) -> (Result<Value, Error>, Vec<Value>) {
        let mut emitted = Vec::new();
        let mut step = op.resume_at(self.clock, None);
        loop {
            match step {
                Step::Done(done) => {
                    let result = done.result.map(|out| match out {
                        Out::Value(v) => v,
                        Out::Committed(_) => panic!("committed"),
                    });
                    return (result, emitted);
                }
                Step::Request(Request::Emit(Value::Array(part))) => {
                    emitted.extend(part);
                    step = op.resume_at(self.clock, Some(Reply::Next));
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

const D: &str = "dddddddddddddddddddddddddddddddddddddddd";

/// A diamond: A, then B and C on it, then D merging C into B.
fn diamond() -> Store {
    let mut store = Store::new();
    store.commit(A, &[], T0);
    store.commit(B, &[A], T0 + 1.0);
    store.commit(C, &[A], T0 + 2.0);
    store.commit(D, &[B, C], T0 + 3.0);
    store
}

fn log(
    store: &mut Store,
    limit: Value,
    first_parent: bool,
) -> (Result<Value, Error>, Vec<String>, usize) {
    let op = call(
        "log",
        &[
            ("start", s(D)),
            ("limit", limit),
            ("first_parent", Value::Bool(first_parent)),
        ],
    );
    let mut reads = 0;
    let (result, records) = store.run_emitting(op, |_, r| {
        if matches!(r, Request::Get(_)) {
            reads += 1;
        }
    });
    let hashes = records
        .iter()
        .map(|r| r.get("hash").unwrap().as_str().unwrap().to_string())
        .collect();
    (result, hashes, reads)
}

#[test]
fn a_log_lists_breadth_first_reading_a_level_at_a_time() {
    let mut store = diamond();
    let (result, hashes, reads) = log(&mut store, Value::Null, false);
    assert_eq!(result.unwrap(), Value::Null);
    assert_eq!(hashes, [D, B, C, A]);
    assert_eq!(reads, 3);
}

#[test]
fn a_first_parent_log_follows_the_branch_itself() {
    let mut store = diamond();
    let (_, hashes, _) = log(&mut store, Value::Null, true);
    assert_eq!(hashes, [D, B, A]);
}

#[test]
fn a_log_stops_reading_at_its_limit() {
    let mut store = diamond();
    let (_, hashes, reads) = log(&mut store, Value::Int(1), false);
    assert_eq!(hashes, [D]);
    assert_eq!(reads, 1);
}

#[test]
fn a_log_fails_where_a_missing_commit_would_be_listed() {
    let mut store = diamond();
    store.data.remove(&commit_root(C));
    let (result, hashes, _) = log(&mut store, Value::Null, false);
    assert_eq!(hashes, [D, B]);
    assert!(matches!(result, Err(Error::UnknownCommit { commit }) if commit == C));
}

#[test]
fn a_log_hands_over_its_newest_commit_alone_then_doubling_batches() {
    let mut store = Store::new();
    let hashes: Vec<String> = (0..10).map(|i| format!("{i:040}")).collect();
    for (i, hash) in hashes.iter().enumerate() {
        let parents: Vec<&str> = if i == 0 { vec![] } else { vec![&hashes[i - 1]] };
        store.commit(hash, &parents, T0 + i as f64);
    }
    let mut op = call(
        "log",
        &[
            ("start", s(&hashes[9])),
            ("limit", Value::Null),
            ("first_parent", Value::Bool(true)),
        ],
    );
    let mut parts = Vec::new();
    let mut step = op.resume_at(store.clock, None);
    loop {
        match step {
            Step::Done(done) => {
                assert!(done.result.is_ok());
                break;
            }
            Step::Request(Request::Emit(Value::Array(part))) => {
                parts.push(part.len());
                step = op.resume_at(store.clock, Some(Reply::Next));
            }
            Step::Request(r) => {
                let reply = store.perform(r);
                step = op.resume_at(store.clock, Some(reply));
            }
        }
    }
    assert_eq!(parts, [1, 2, 4, 3]);
}

#[test]
fn a_damaged_root_fails_a_read_rather_than_passing_for_an_empty_tree() {
    let mut store = diamond();
    store.data.insert(commit_root(B), b"not json".to_vec());
    store.put(commit_root(C), &Value::Int(7));
    for commit in [B, C] {
        let op = call("get_commit", &[("commit", s(commit))]);
        assert!(matches!(store.run(op, |_, _| {}), Err(Error::Malformed(_))));
        let op = call(
            "resolve_ref",
            &[("commit", s(commit)), ("recover", Value::Bool(false))],
        );
        assert!(matches!(store.run(op, |_, _| {}), Err(Error::Malformed(_))));
    }
}

/// Run `machine` against `store`, calling `before` with each request
/// before performing it.
fn drive<T: 'static>(
    store: &mut Store,
    mut machine: Machine<T>,
    mut before: impl FnMut(&mut Store, &Request),
) -> T {
    let mut step = machine.resume_at(store.clock, None);
    loop {
        match step {
            Step::Done(out) => return out,
            Step::Request(r) => {
                before(store, &r);
                let reply = store.perform(r);
                step = machine.resume_at(store.clock, Some(reply));
            }
        }
    }
}

fn stamp(store: &mut Store, version: i64, before: impl FnMut(&mut Store, &Request)) {
    let machine = Machine::new(move |io| async move { stamp_version_at_least(&io, version).await });
    drive(store, machine, before).unwrap();
}

fn version(store: &Store) -> Value {
    loads(&store.data[STORAGE_VERSION_KEY]).unwrap()
}

#[test]
fn a_stamp_only_rises() {
    let mut store = Store::new();
    store.put(STORAGE_VERSION_KEY.into(), &Value::Int(3));
    stamp(&mut store, 2, |_, _| {});
    assert_eq!(version(&store), Value::Int(3));
    let mut fresh = Store::new();
    stamp(&mut fresh, 3, |_, _| {});
    assert_eq!(version(&fresh), Value::Int(3));
}

#[test]
fn a_stale_stamp_loses_to_a_higher_one() {
    // Another writer raises the stamp between this one's read and its
    // write: the write expects what it read, fails, and reads again.
    let mut store = Store::new();
    store.put(STORAGE_VERSION_KEY.into(), &Value::Int(2));
    let mut raced = false;
    stamp(&mut store, 3, |store, r| {
        if matches!(r, Request::Cas(_)) && !raced {
            raced = true;
            store.put(STORAGE_VERSION_KEY.into(), &Value::Int(4));
        }
    });
    assert!(raced);
    assert_eq!(version(&store), Value::Int(4));
}

#[test]
fn no_lease_costs_one_read() {
    let mut store = Store::new();
    let mut reads = 0;
    let machine = Machine::new(|io| async move { wait_for_gc(&io, None).await });
    let lease = drive(&mut store, machine, |_, r| {
        if matches!(r, Request::Get(_)) {
            reads += 1;
        }
    });
    assert_eq!(lease, Ok(None));
    assert_eq!(reads, 1);
}
