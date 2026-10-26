//! The commit protocol under interleavings: two machines over one
//! store, advanced in the order each test chooses, a sweep's lease
//! taken and given back between two writes, and a host that fails.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, dumps, loads};
use kvgit_core::keyset::{Entry, shape};
use kvgit_core::layout::{
    EMPTY_HASH, GC_LEASE_KEY, IN_FLIGHT_PREFIX, ROOT_COMMIT, STORAGE_VERSION_KEY, blob_key,
    branch_head, commit_root, commit_time, parent_commit,
};
use kvgit_core::lease::IN_FLIGHT_TTL;
use kvgit_core::machine::{Cas, Machine, Reply, Request, Step, Write};
use kvgit_core::merge::{MergeAnswer, Rule, Rules};
use kvgit_core::text::TextRule;
use kvgit_core::write::{CommitArgs, Committed, Done, OnConflict, OnMoved, Strategy, commit};
use std::collections::{BTreeMap, HashMap};

const T0: f64 = 1_700_000_000.5;

struct Store {
    data: BTreeMap<String, Vec<u8>>,
    clock: f64,
    /// Every value any writer holds, by blob key.
    held: HashMap<String, Vec<u8>>,
}

impl Store {
    /// A store with `main` at the root commit.
    fn new() -> Store {
        let mut data = BTreeMap::new();
        let mut put = |k: String, v: &Value| data.insert(k, dumps(v, false).into_bytes());
        put(STORAGE_VERSION_KEY.into(), &Value::Int(4));
        put(commit_root(ROOT_COMMIT), &Value::Str(EMPTY_HASH.clone()));
        put(parent_commit(ROOT_COMMIT), &Value::Array(vec![]));
        put(commit_time(ROOT_COMMIT), &Value::Float(T0));
        put(branch_head("main"), &Value::Str(ROOT_COMMIT.into()));
        Store {
            data,
            clock: T0,
            held: HashMap::new(),
        }
    }

    fn head(&self) -> String {
        loads(&self.data[&branch_head("main")])
            .unwrap()
            .as_str()
            .unwrap()
            .into()
    }

    fn markers(&self) -> Vec<&String> {
        self.data
            .keys()
            .filter(|k| k.starts_with(IN_FLIGHT_PREFIX))
            .collect()
    }

    fn clock(&self) -> f64 {
        self.clock
    }

    fn perform(&mut self, request: Request) -> Reply {
        match request {
            Request::Get(keys) => Reply::Values(
                keys.into_iter()
                    .filter_map(|k| self.data.get(&k).map(|v| (k, v.clone())))
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
            Request::Merge(asks) => Reply::Merged(
                asks.iter()
                    .map(|ask| {
                        // `concat`: theirs then ours.
                        let side = |b: &Option<String>| b.as_ref().map(|b| self.data[b].clone());
                        let mut merged = side(&ask.theirs).unwrap_or_default();
                        merged.extend(side(&ask.ours).unwrap_or_default());
                        let blob = blob_key(&merged);
                        let size = merged.len() as u64;
                        self.held.insert(blob.clone(), merged);
                        MergeAnswer::Value { blob, size }
                    })
                    .collect(),
            ),
            Request::Scan(prefix) => Reply::Keys(
                self.data
                    .keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect(),
            ),
            Request::Recover(_) => Reply::Recovered(None),
            Request::Now => Reply::Now(self.clock),
        }
    }
}

/// A commit of `sets` on `main`, from `base`.
fn committing(
    store: &mut Store,
    base: &str,
    sets: &[(&str, &str)],
    rules: Rules,
) -> Machine<Done<Committed>> {
    committing_with(store, base, sets, rules, OnMoved::Merge)
}

fn committing_with(
    store: &mut Store,
    base: &str,
    sets: &[(&str, &str)],
    rules: Rules,
    on_moved: OnMoved,
) -> Machine<Done<Committed>> {
    let updates = sets
        .iter()
        .map(|(k, v)| {
            let blob = blob_key(v.as_bytes());
            store.held.insert(blob.clone(), v.as_bytes().to_vec());
            (k.to_string(), Entry::new(blob, v.len() as u64, vec![]))
        })
        .collect();
    let args = CommitArgs {
        branch: "main".into(),
        base: base.into(),
        current: base.into(),
        shape: shape(),
        updates,
        removals: vec![],
        chunks: vec![],
        info: None,
        on_moved,
        on_conflict: OnConflict::Raise,
        rules,
        recover: false,
        stamped: true,
        roots: vec![],
        generations: vec![],
    };
    Machine::new(move |io| commit(io, args))
}

/// Run `machine` until `stop` holds for the request it makes, which is
/// returned unperformed; `None` if it finished first.
fn run_until(
    store: &mut Store,
    machine: &mut Machine<Done<Committed>>,
    mut step: Step<Done<Committed>>,
    stop: impl Fn(&Request) -> bool,
) -> Result<Request, Box<Done<Committed>>> {
    loop {
        match step {
            Step::Done(done) => return Err(Box::new(done)),
            Step::Request(r) if stop(&r) => return Ok(r),
            Step::Request(r) => {
                let reply = store.perform(r);
                step = machine.resume_at(store.clock(), Some(reply));
            }
        }
    }
}

fn finish(
    store: &mut Store,
    mut machine: Machine<Done<Committed>>,
    step: Step<Done<Committed>>,
) -> Done<Committed> {
    match run_until(store, &mut machine, step, |_| false) {
        Err(done) => *done,
        Ok(_) => unreachable!(),
    }
}

fn is_publish(r: &Request) -> bool {
    matches!(r, Request::Cas(c) if c.writes.iter().any(|(k, _)| k == &branch_head("main")))
}

fn read(store: &Store, commit: &str, key: &str) -> Option<Vec<u8>> {
    let root = loads(&store.data[&commit_root(commit)])
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let data = &store.data;
    let found = Machine::new(|io| kvgit_core::tree::get_many(io, shape(), root, vec![key.into()]))
        .run(|keys| {
            keys.iter()
                .filter_map(|k| data.get(k).map(|v| (k.clone(), v.clone())))
                .collect()
        })
        .unwrap();
    found
        .into_iter()
        .next()
        .map(|(_, raw)| store.data[&Entry::decode(&raw).unwrap().blob].clone())
}

#[test]
fn a_sweep_between_batch_and_publish_is_waited_out() {
    let mut store = Store::new();
    let mut machine = committing(&mut store, ROOT_COMMIT, &[("k", "v")], Rules::default());
    let first = machine.resume_at(store.clock(), None);
    let publish = run_until(&mut store, &mut machine, first, is_publish).unwrap();

    // A sweep takes the lease after the batch landed, its marker in place.
    assert_eq!(store.markers().len(), 1);
    let lease = br#"{"owner":"lease-1","expires":1700000600.5}"#.to_vec();
    store.data.insert(GC_LEASE_KEY.into(), lease);
    let reply = store.perform(publish);
    assert_eq!(reply, Reply::Landed(false));

    // The publish waits; the sweep ends after a few polls.
    let mut waits = 0;
    let mut step = machine.resume_at(store.clock(), Some(reply));
    loop {
        match step {
            Step::Done(done) => {
                let committed = done.result.unwrap();
                assert_eq!(committed.strategy, Strategy::FastForward);
                assert_eq!(store.head(), committed.commit.unwrap());
                break;
            }
            Step::Request(r) => {
                if let Request::Wait(_) = r {
                    waits += 1;
                    if waits == 3 {
                        let done = br#"{"owner":"lease-1","expires":0}"#.to_vec();
                        store.data.insert(GC_LEASE_KEY.into(), done);
                    }
                }
                let reply = store.perform(r);
                step = machine.resume_at(store.clock(), Some(reply));
            }
        }
    }
    assert_eq!(waits, 3);
    assert!(store.markers().is_empty());
}

#[test]
fn a_marker_counts_from_after_a_long_wait() {
    let mut store = Store::new();
    // A sweep holds the lease past the in-flight lifetime.
    let expires = T0 + IN_FLIGHT_TTL + 100.0;
    let lease = format!(r#"{{"owner":"lease-1","expires":{expires}}}"#);
    store.data.insert(GC_LEASE_KEY.into(), lease.into_bytes());
    let mut machine = committing(&mut store, ROOT_COMMIT, &[("k", "v")], Rules::default());
    let first = machine.resume_at(store.clock(), None);
    run_until(&mut store, &mut machine, first, is_publish).unwrap();

    assert!(store.clock() >= expires);
    let marker = &store.data[store.markers()[0]];
    let Value::Float(lapses) = loads(marker).unwrap() else {
        panic!("a marker holds its expiry")
    };
    assert!(lapses >= store.clock() + IN_FLIGHT_TTL - 1.0);
}

#[test]
fn a_lost_publish_refused_withdraws_its_marker() {
    let mut store = Store::new();
    let mut alice = committing_with(
        &mut store,
        ROOT_COMMIT,
        &[("x", "alice")],
        Rules::default(),
        OnMoved::Raise,
    );
    let first = alice.resume_at(store.clock(), None);
    let publish = run_until(&mut store, &mut alice, first, is_publish).unwrap();

    let mut bob = committing(&mut store, ROOT_COMMIT, &[("y", "bob")], Rules::default());
    let first = bob.resume_at(store.clock(), None);
    let bobs = finish(&mut store, bob, first)
        .result
        .unwrap()
        .commit
        .unwrap();

    let reply = store.perform(publish);
    let step = alice.resume_at(store.clock(), Some(reply));
    match finish(&mut store, alice, step).result {
        Err(Error::BranchMoved { head, .. }) => assert_eq!(head, bobs),
        other => panic!("{other:?}"),
    }
    assert_eq!(store.head(), bobs);
    assert!(store.markers().is_empty());
}

#[test]
fn a_host_failure_mid_publish_leaves_no_marker() {
    let mut store = Store::new();
    let mut machine = committing(&mut store, ROOT_COMMIT, &[("k", "v")], Rules::default());
    let first = machine.resume_at(store.clock(), None);
    let _publish = run_until(&mut store, &mut machine, first, is_publish).unwrap();
    assert_eq!(store.markers().len(), 1);
    let step = machine.resume_at(store.clock(), Some(Reply::Failed));
    let done = finish(&mut store, machine, step);
    assert_eq!(done.result, Err(Error::Host));
    assert!(store.markers().is_empty());
    assert_eq!(store.head(), ROOT_COMMIT);
}

/// Two writers change `k` from the root; the second merges by `rule`.
fn contested(rule: Rule) -> (Store, Done<Committed>) {
    let mut store = Store::new();
    let first = committing(&mut store, ROOT_COMMIT, &[("k", "a\n")], Rules::default());
    let mut first = first;
    let step = first.resume_at(store.clock(), None);
    finish(&mut store, first, step).result.unwrap();
    let rules = Rules {
        keys: HashMap::from([("k".to_string(), rule)]),
        ..Rules::default()
    };
    let mut second = committing(&mut store, ROOT_COMMIT, &[("k", "b\n")], rules);
    let step = second.resume_at(store.clock(), None);
    let done = finish(&mut store, second, step);
    (store, done)
}

#[test]
fn a_host_merge_function_resolves_a_contested_key() {
    let (store, done) = contested(Rule::Host(0));
    let c = done.result.unwrap();
    assert_eq!(
        (c.strategy, c.auto_merged.as_slice()),
        (Strategy::ThreeWay, &["k".to_string()][..])
    );
    assert_eq!(
        read(&store, &c.commit.unwrap(), "k"),
        Some(b"a\nb\n".to_vec())
    );
}

#[test]
fn the_text_merge_marks_a_contested_key() {
    let (store, done) = contested(Rule::Text(TextRule::default()));
    let c = done.result.unwrap();
    assert_eq!(
        read(&store, &c.commit.unwrap(), "k").unwrap(),
        b"<<<<<<< ours\nb\n=======\na\n>>>>>>> theirs\n"
    );
}

#[test]
fn a_strict_text_merge_refuses_and_leaves_no_marker() {
    let strict = TextRule {
        strict: true,
        ..TextRule::default()
    };
    let (store, done) = contested(Rule::Text(strict));
    match done.result {
        Err(Error::MergeConflict { keys, failures }) => {
            assert_eq!(keys, ["k"]);
            assert!(failures[0].1.contains("strict"));
        }
        other => panic!("{other:?}"),
    }
    assert!(store.markers().is_empty());
}

#[test]
fn a_contested_key_without_a_rule_conflicts() {
    let mut store = Store::new();
    let first = committing(&mut store, ROOT_COMMIT, &[("k", "a")], Rules::default());
    let mut first = first;
    let step = first.resume_at(store.clock(), None);
    let landed = finish(&mut store, first, step).result.unwrap();
    let mut second = committing(&mut store, ROOT_COMMIT, &[("k", "b")], Rules::default());
    let step = second.resume_at(store.clock(), None);
    let done = finish(&mut store, second, step);
    assert!(matches!(done.result, Err(Error::MergeConflict { ref keys, .. }) if keys == &["k"]));
    assert_eq!(store.head(), landed.commit.unwrap());
    assert!(store.markers().is_empty());
}
