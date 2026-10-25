//! The stores the corpus recorded (`corpus/scenarios/**`, each one's
//! `final`), read back through the core: every commit made during a
//! scenario rehashes to its name, and every keyset node hashes to its
//! key and re-encodes to its bytes.

use kvgit_core::commit::commit_hash;
use kvgit_core::hash::sha256_hex;
use kvgit_core::json::{Value, dumps, loads};
use kvgit_core::layout::{
    COMMIT_ROOT_PREFIX, KEYSET_PREFIX, ROOT_COMMIT, commit_time, info_key, parent_commit,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn scenarios(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("the corpus directory") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            scenarios(&path, out);
        } else if path.extension().is_some_and(|e| e == "json") {
            out.push(path);
        }
    }
}

/// A store as a scenario writes it, keeping only values that are text:
/// every record and node is.
fn text_store(value: Option<&Value>) -> BTreeMap<String, String> {
    let Some(Value::Object(pairs)) = value else {
        return BTreeMap::new();
    };
    pairs
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

/// A scenario's path, the store it starts from and the one it recorded.
type Recorded = (PathBuf, BTreeMap<String, String>, BTreeMap<String, String>);

fn corpus() -> Vec<Recorded> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../corpus/scenarios");
    let mut paths = Vec::new();
    scenarios(&root, &mut paths);
    paths.sort();
    assert!(!paths.is_empty(), "no scenarios under {}", root.display());
    paths
        .into_iter()
        .map(|path| {
            let scenario = loads(&fs::read(&path).expect("a scenario")).expect("a scenario parses");
            let before = text_store(scenario.get("store"));
            let after = text_store(scenario.get("final"));
            (path, before, after)
        })
        .collect()
}

#[test]
fn every_new_commit_rehashes_to_its_name() {
    let mut checked = 0;
    for (path, before, after) in corpus() {
        for (key, root) in &after {
            let Some(commit) = key.strip_prefix(COMMIT_ROOT_PREFIX) else {
                continue;
            };
            // The root commit's name is fixed, and a commit the scenario
            // started with may predate this hash.
            if commit == ROOT_COMMIT || before.contains_key(key) {
                continue;
            }
            let root = loads(root.as_bytes()).unwrap();
            let parents = loads(after[&parent_commit(commit)].as_bytes()).unwrap();
            let Value::Array(parents) = parents else {
                panic!("{commit}: parents aren't a list")
            };
            let parents: Vec<&str> = parents.iter().map(|p| p.as_str().unwrap()).collect();
            let time = match loads(after[&commit_time(commit)].as_bytes()).unwrap() {
                Value::Float(t) => t,
                other => panic!("{commit}: time {other:?}"),
            };
            let info = after
                .get(&info_key(commit))
                .map(|raw| loads(raw.as_bytes()).unwrap());
            assert_eq!(
                commit_hash(&parents, root.as_str().unwrap(), time, info.as_ref()),
                commit,
                "{}",
                path.display()
            );
            checked += 1;
        }
    }
    assert!(checked > 50, "only {checked} commits checked");
}

#[test]
fn every_keyset_node_hashes_to_its_key_and_reencodes() {
    let mut checked = 0;
    for (path, _, after) in corpus() {
        for (key, raw) in &after {
            let Some(hash) = key.strip_prefix(KEYSET_PREFIX) else {
                continue;
            };
            let Ok(node) = loads(raw.as_bytes()) else {
                continue;
            };
            // A scenario may plant a node under a name it doesn't hash
            // to: leftovers for a deep sweep to find.
            if sha256_hex(raw.as_bytes()) != hash {
                continue;
            }
            assert_eq!(dumps(&node, true), *raw, "{}", path.display());
            checked += 1;
        }
    }
    assert!(checked > 50, "only {checked} nodes checked");
}
