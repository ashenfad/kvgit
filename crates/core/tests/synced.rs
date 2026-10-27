//! Sync heads per remote, the sync mark, deleting a branch unless it
//! syncs, and publishing a transferred head, against a store this test
//! holds: the keys each writes, and what each reads back.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, dumps, loads};
use kvgit_core::layout::{
    EMPTY_HASH, branch_head, branch_head_prev, commit_root, in_flight_key, sync_head, sync_head_at,
    sync_heads_of, sync_mark,
};
use kvgit_core::machine::{Cas, Reply, Request, Step, Write};
use kvgit_core::ops::{self, Out};
use std::collections::BTreeMap;

#[derive(Default)]
struct Store {
    data: BTreeMap<String, Vec<u8>>,
}

impl Store {
    fn put(&mut self, key: String, value: &str) {
        self.data
            .insert(key, dumps(&Value::Str(value.into()), false).into_bytes());
    }

    fn read(&self, key: &str) -> Option<Value> {
        self.data.get(key).map(|raw| loads(raw).unwrap())
    }

    fn call(&mut self, name: &str, args: &[(&str, Value)]) -> Result<Value, Error> {
        let args = args
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let mut op = ops::call(name, Value::Object(args)).unwrap();
        let mut step = op.resume_at(0.0, None);
        loop {
            let reply = match step {
                Step::Done(done) => {
                    return done.result.map(|out| match out {
                        Out::Value(v) => v,
                        Out::Committed(_) => panic!("committed"),
                    });
                }
                Step::Request(Request::Get(keys)) => Reply::Values(
                    keys.into_iter()
                        .filter_map(|k| self.data.get(&k).map(|v| (k, v.clone())))
                        .collect(),
                ),
                Step::Request(Request::Scan(prefix)) => Reply::Keys(
                    self.data
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect(),
                ),
                Step::Request(Request::Cas(Cas {
                    expect,
                    writes,
                    removes,
                })) => {
                    if expect.iter().any(|(k, v)| self.data.get(k) != v.as_ref()) {
                        Reply::Landed(false)
                    } else {
                        for (key, write) in writes {
                            let Write::Bytes(bytes) = write else {
                                panic!("no held bytes here")
                            };
                            self.data.insert(key, bytes);
                        }
                        for key in removes {
                            self.data.remove(&key);
                        }
                        Reply::Landed(true)
                    }
                }
                Step::Request(Request::Now) => Reply::Now(0.0),
                Step::Request(r) => panic!("unexpected request {r:?}"),
            };
            step = op.resume_at(0.0, Some(reply));
        }
    }
}

fn s(text: &str) -> Value {
    Value::Str(text.into())
}

fn remote(id: Option<&str>) -> Value {
    Value::Object(vec![("id".into(), id.map_or(Value::Null, s))])
}

const NONE: Value = Value::Null;
const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[test]
fn per_remote_keys_escape_as_encode_uri_component_does() {
    assert_eq!(sync_heads_of("a/b c"), "__sync_head_at__a%2Fb%20c/");
    assert_eq!(
        sync_head_at("é!~*'()-_.", "github.com/o/r"),
        "__sync_head_at__%C3%A9!~*'()-_./github.com%2Fo%2Fr"
    );
    assert_eq!(sync_head("main"), "__sync_head__main");
    assert_eq!(sync_mark("main"), "__sync_mark__main");
}

#[test]
fn each_remote_keeps_its_own_sync_head() {
    let mut store = Store::default();
    let head = |store: &mut Store, owner: Value| {
        store
            .call("sync_head", &[("branch", s("main")), ("remote", owner)])
            .unwrap()
    };
    store
        .call(
            "set_sync_head",
            &[
                ("branch", s("main")),
                ("commit", s(A)),
                ("remote", remote(Some("r1"))),
            ],
        )
        .unwrap();
    assert_eq!(store.read(&sync_mark("main")), Some(Value::Bool(true)));
    assert_eq!(head(&mut store, remote(Some("r1"))), s(A));
    assert_eq!(head(&mut store, remote(Some("r2"))), NONE);
    assert_eq!(head(&mut store, remote(None)), NONE);
    // with no remote named, the one remote's head
    assert_eq!(head(&mut store, NONE), s(A));
    store
        .call(
            "set_sync_head",
            &[
                ("branch", s("main")),
                ("commit", s(B)),
                ("remote", remote(Some("r2"))),
            ],
        )
        .unwrap();
    // and with two, none
    assert_eq!(head(&mut store, NONE), NONE);
    assert_eq!(
        store
            .call("has_sync_head", &[("branch", s("main"))])
            .unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn the_first_remote_to_claim_a_shared_head_takes_it_up() {
    let mut store = Store::default();
    store.put(sync_head("main"), A);
    let claim = |store: &mut Store, id: &str| {
        store
            .call(
                "claim_sync_head",
                &[("branch", s("main")), ("remote", remote(Some(id)))],
            )
            .unwrap()
    };
    assert_eq!(claim(&mut store, "r1"), s(A));
    assert_eq!(store.read(&sync_head_at("main", "r1")), Some(s(A)));
    assert_eq!(store.read(&sync_head("main")), None);
    assert_eq!(claim(&mut store, "r2"), NONE);
    assert_eq!(claim(&mut store, "r1"), s(A));
}

#[test]
fn clearing_the_last_head_drops_the_mark() {
    let mut store = Store::default();
    for id in ["r1", "r2"] {
        store
            .call(
                "set_sync_head",
                &[
                    ("branch", s("main")),
                    ("commit", s(A)),
                    ("remote", remote(Some(id))),
                ],
            )
            .unwrap();
    }
    let clear = |store: &mut Store, owner: Value| {
        store
            .call(
                "clear_sync_head",
                &[("branch", s("main")), ("remote", owner)],
            )
            .unwrap();
    };
    clear(&mut store, remote(Some("r1")));
    assert!(store.data.contains_key(&sync_mark("main")));
    clear(&mut store, remote(Some("r2")));
    assert!(!store.data.contains_key(&sync_mark("main")));

    // with no remote named, every remote's
    for id in ["r1", "r2"] {
        store
            .call(
                "set_sync_head",
                &[
                    ("branch", s("main")),
                    ("commit", s(A)),
                    ("remote", remote(Some(id))),
                ],
            )
            .unwrap();
    }
    clear(&mut store, NONE);
    let left: Vec<&String> = store.data.keys().collect();
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn a_branch_that_syncs_is_not_deleted() {
    let mut store = Store::default();
    let delete = |store: &mut Store, name: &str| {
        store.call("delete_branch_unless_synced", &[("name", s(name))])
    };
    assert!(matches!(
        delete(&mut store, "main"),
        Err(Error::UnknownBranch { .. })
    ));
    assert!(matches!(delete(&mut store, ""), Err(Error::Invalid(_))));
    store.put(branch_head("main"), A);
    store.put(branch_head_prev("main"), B);
    store
        .call("mark_syncing", &[("branch", s("main"))])
        .unwrap();
    assert_eq!(delete(&mut store, "main").unwrap(), Value::Bool(false));
    store.data.remove(&sync_mark("main"));
    assert_eq!(delete(&mut store, "main").unwrap(), Value::Bool(true));
    assert!(store.data.is_empty());
}

#[test]
fn publishing_moves_the_head_with_its_backup_and_drops_markers() {
    let mut store = Store::default();
    for c in [A, B] {
        store.put(commit_root(c), &EMPTY_HASH);
        store.put(in_flight_key(c), "marker");
    }
    let publish = |store: &mut Store, expected: Value, head: &str| {
        store.call(
            "publish_head",
            &[
                ("branch", s("main")),
                ("expected", expected),
                ("head", s(head)),
                ("in_flight", Value::Array(vec![s(head)])),
            ],
        )
    };
    store.put(branch_head_prev("main"), "stale");
    assert_eq!(publish(&mut store, NONE, A).unwrap(), Value::Bool(true));
    assert_eq!(store.read(&branch_head("main")), Some(s(A)));
    assert_eq!(store.read(&branch_head_prev("main")), None);
    assert!(!store.data.contains_key(&in_flight_key(A)));
    // a create of a branch that exists, or a move from elsewhere, fails
    assert_eq!(publish(&mut store, NONE, B).unwrap(), Value::Bool(false));
    assert_eq!(publish(&mut store, s(B), B).unwrap(), Value::Bool(false));
    assert!(store.data.contains_key(&in_flight_key(B)));
    assert_eq!(publish(&mut store, s(A), B).unwrap(), Value::Bool(true));
    assert_eq!(store.read(&branch_head("main")), Some(s(B)));
    assert_eq!(store.read(&branch_head_prev("main")), Some(s(A)));
    assert!(!store.data.contains_key(&in_flight_key(B)));
    // the head must be in the store
    let gone = "cccccccccccccccccccccccccccccccccccccccc";
    assert!(matches!(
        publish(&mut store, s(B), gone),
        Err(Error::UnknownCommit { commit }) if commit == gone
    ));
}
