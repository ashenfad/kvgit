//! The wire walk and replay between two stores this test holds: a
//! history walked from one replays into the other record for record,
//! a replay of a commit already there only re-marks it, and a commit
//! whose values, parents or generations don't check out is refused
//! with nothing of it written.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, dumps, loads};
use kvgit_core::keyset::{Entry, shape};
use kvgit_core::layout::{
    EMPTY_HASH, ROOT_COMMIT, STORAGE_VERSION_KEY, blob_key, branch_head, commit_root, commit_time,
    in_flight_key, info_key, parent_commit, parent_gens,
};
use kvgit_core::machine::{Cas, Machine, Reply, Request, Step, Write};
use kvgit_core::merge::Rules;
use kvgit_core::ops::{self, Out};
use kvgit_core::write::{CommitArgs, OnConflict, OnMoved, commit};
use std::collections::{BTreeMap, HashMap};

const T0: f64 = 1_700_000_000.5;

#[derive(Default)]
struct Store {
    data: BTreeMap<String, Vec<u8>>,
    clock: f64,
    held: HashMap<String, Vec<u8>>,
}

impl Store {
    /// A store with `main` at the root commit.
    fn new() -> Store {
        let mut store = Store {
            clock: T0,
            ..Store::default()
        };
        store.put(STORAGE_VERSION_KEY.into(), &Value::Int(4));
        store.put(commit_root(ROOT_COMMIT), &Value::Str(EMPTY_HASH.clone()));
        store.put(parent_commit(ROOT_COMMIT), &Value::Array(vec![]));
        store.put(commit_time(ROOT_COMMIT), &Value::Float(T0));
        store.put(branch_head("main"), &Value::Str(ROOT_COMMIT.into()));
        store
    }

    fn put(&mut self, key: String, value: &Value) {
        self.data.insert(key, dumps(value, false).into_bytes());
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
                    let bytes = match write {
                        Write::Bytes(bytes) => bytes,
                        Write::Held => self.held[&key].clone(),
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
            Request::Emit(_) => Reply::Next,
            Request::Owner => Reply::Owner("lease-1".into()),
            r => panic!("unexpected request {r:?}"),
        }
    }

    /// Commit `sets` and `removals` on `main`; the commit's hash.
    fn commit(&mut self, sets: &[(&str, &str)], removals: &[&str], info: Option<Value>) -> String {
        self.clock += 1.0;
        let head = loads(&self.data[&branch_head("main")])
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let updates = sets
            .iter()
            .map(|(k, v)| {
                let blob = blob_key(v.as_bytes());
                self.held.insert(blob.clone(), v.as_bytes().to_vec());
                (k.to_string(), Entry::new(blob, v.len() as u64, vec![]))
            })
            .collect();
        let args = CommitArgs {
            branch: "main".into(),
            base: head.clone(),
            current: head,
            shape: shape(),
            updates,
            removals: removals.iter().map(|k| k.to_string()).collect(),
            chunks: vec![],
            info,
            on_moved: OnMoved::Merge,
            on_conflict: OnConflict::Raise,
            rules: Rules::default(),
            recover: false,
            stamped: true,
            roots: vec![],
            generations: vec![],
        };
        let mut machine = Machine::new(move |io| commit(io, args));
        let mut step = machine.resume_at(self.clock, None);
        loop {
            match step {
                Step::Done(done) => return done.result.unwrap().commit.unwrap(),
                Step::Request(r) => {
                    let reply = self.perform(r);
                    step = machine.resume_at(self.clock, Some(reply));
                }
            }
        }
    }

    fn call(&mut self, name: &str, args: Value) -> (Result<Value, Error>, Vec<Value>) {
        let mut op = ops::call(name, args).unwrap();
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
                    let reply = self.perform(r);
                    step = op.resume_at(self.clock, Some(reply));
                }
            }
        }
    }

    fn walk(&mut self, want: &str, have: &[&str]) -> Vec<Value> {
        let have = Value::Array(have.iter().map(|h| Value::Str(h.to_string())).collect());
        let args = obj(vec![("want", Value::Str(want.into())), ("have", have)]);
        let (result, emitted) = self.call("wire_walk", args);
        result.unwrap();
        emitted
    }

    /// Replay one walked commit, its values' bytes read from `from`.
    fn replay(&mut self, walked: &Value, from: &Store) -> Result<Value, Error> {
        let field = |name: &str| walked.get(name).unwrap().clone();
        let record = |name: &str| match field(name) {
            Value::Str(text) => loads(text.as_bytes()).unwrap(),
            other => other,
        };
        let Value::Array(walked_updates) = field("updates") else {
            panic!("updates")
        };
        let mut updates = Vec::new();
        for item in walked_updates {
            let Value::Array(pair) = item else { panic!() };
            let (key, blob) = (pair[0].as_str().unwrap(), pair[1].as_str().unwrap());
            let bytes = from.data[blob].clone();
            updates.push(Value::Array(vec![
                Value::Str(key.into()),
                Value::Str(blob.into()),
                Value::Int(bytes.len() as i64),
            ]));
            self.held.insert(blob.into(), bytes);
        }
        let args = obj(vec![
            ("hash", field("hash")),
            ("parents", field("parents")),
            ("parent_gens", field("parent_gens")),
            ("time", record("time")),
            ("info", record("info")),
            ("updates", Value::Array(updates)),
            ("removals", field("removals")),
        ]);
        self.call("wire_replay", args).0
    }
}

fn obj(fields: Vec<(&str, Value)>) -> Value {
    Value::Object(fields.into_iter().map(|(k, v)| (k.into(), v)).collect())
}

fn hash(walked: &Value) -> String {
    walked.get("hash").unwrap().as_str().unwrap().to_string()
}

/// A history of three commits on the root: a set, a change and a
/// removal, the last with info.
fn history() -> (Store, Vec<String>) {
    let mut src = Store::new();
    let a = src.commit(&[("x", "one"), ("y", "two")], &[], None);
    let b = src.commit(&[("x", "uno")], &[], None);
    let info = obj(vec![
        ("why", Value::Str("tidy".into())),
        ("n", Value::Float(1.0)),
    ]);
    let c = src.commit(&[("z", "three")], &["y"], Some(info));
    (src, vec![a, b, c])
}

#[test]
fn a_history_replays_record_for_record() {
    let (src, commits) = history();
    let mut src = src;
    let walked = src.walk(&commits[2], &[]);
    let order: Vec<String> = walked.iter().map(hash).collect();
    let mut expected = vec![ROOT_COMMIT.to_string()];
    expected.extend(commits.iter().cloned());
    assert_eq!(order, expected);

    let mut dst = Store::new();
    dst.data.clear();
    dst.put(STORAGE_VERSION_KEY.into(), &Value::Int(4));
    for w in &walked {
        assert_eq!(dst.replay(w, &src).unwrap(), Value::Str("applied".into()));
    }
    for c in &expected {
        for key in [
            commit_root(c),
            parent_commit(c),
            parent_gens(c),
            commit_time(c),
            info_key(c),
        ] {
            assert_eq!(dst.data.get(&key), src.data.get(&key), "{key}");
        }
        assert!(dst.data.contains_key(&in_flight_key(c)));
    }
    // and replayed again, each is already there
    for w in &walked {
        assert_eq!(dst.replay(w, &src).unwrap(), Value::Str("present".into()));
    }
}

#[test]
fn a_walk_from_what_the_receiver_has_sends_the_rest() {
    let (mut src, commits) = history();
    let walked = src.walk(&commits[2], &[&commits[0]]);
    let order: Vec<String> = walked.iter().map(hash).collect();
    assert_eq!(order, commits[1..].to_vec());
    // the removal travels as a removal, the change as its blob
    let last = &walked[1];
    assert_eq!(
        last.get("removals").unwrap(),
        &Value::Array(vec![Value::Str("y".into())])
    );
    let updates = last.get("updates").unwrap();
    assert_eq!(
        updates,
        &Value::Array(vec![Value::Array(vec![
            Value::Str("z".into()),
            Value::Str(blob_key(b"three")),
        ])])
    );
    assert!(src.walk(&commits[2], &[&commits[2]]).is_empty());
}

/// A destination holding everything up to `commits[0]`.
fn partway(src: &mut Store, commits: &[String]) -> Store {
    let mut dst = Store::new();
    for w in src.walk(&commits[0], &[]) {
        dst.replay(&w, src).unwrap();
    }
    dst
}

#[test]
fn a_commit_that_does_not_check_out_is_refused_unwritten() {
    let (mut src, commits) = history();
    let walked = src.walk(&commits[2], &[&commits[0]]);

    // a value that isn't the one the commit has
    let mut dst = partway(&mut src, &commits);
    let mut tampered = src.data.clone();
    tampered.insert(blob_key(b"uno"), b"evil".to_vec());
    let liar = Store {
        data: tampered,
        ..Store::default()
    };
    let Err(Error::Integrity(why)) = dst.replay(&walked[0], &liar) else {
        panic!("refused")
    };
    assert!(why.contains("integrity check failed"), "{why}");
    assert!(!dst.data.contains_key(&commit_root(&commits[1])));

    // generations other than the parents'
    let mut wrong = walked[0].clone();
    if let Value::Object(fields) = &mut wrong {
        for (name, value) in fields.iter_mut() {
            if name == "parent_gens" {
                *value = Value::Array(vec![Value::Int(7)]);
            }
        }
    }
    let Err(Error::Integrity(why)) = dst.replay(&wrong, &src) else {
        panic!("refused")
    };
    assert!(why.contains("parent generations"), "{why}");
    assert!(!dst.data.contains_key(&commit_root(&commits[1])));

    // a child before its parent
    let Err(Error::Integrity(why)) = dst.replay(&walked[1], &src) else {
        panic!("refused")
    };
    assert!(why.contains("sends parents first"), "{why}");
}
