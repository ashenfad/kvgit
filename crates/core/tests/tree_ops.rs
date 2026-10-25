//! The tree, against Python kvgit's HAMT: the same batches of sets and
//! removals reach the same root after every batch
//! (`fixtures/tree-ops.json`, from `make_tree_ops.py`).

use kvgit_core::json::{Value, loads};
use kvgit_core::layout::EMPTY_HASH;
use kvgit_core::machine::Machine;
use kvgit_core::tree::{Shape, update, walk};
use std::collections::{BTreeMap, HashMap};

fn run<T: 'static>(store: &HashMap<String, Vec<u8>>, machine: Machine<T>) -> T {
    machine.run(|keys| {
        keys.iter()
            .filter_map(|k| store.get(k).map(|v| (k.clone(), v.clone())))
            .collect()
    })
}

#[test]
fn every_batch_reaches_the_root_python_reached() {
    let runs = loads(include_bytes!("fixtures/tree-ops.json")).unwrap();
    let Value::Array(runs) = runs else {
        panic!("a list of runs")
    };
    let mut checked = 0;
    for r in runs {
        let Some(Value::Int(bucket_max)) = r.get("bucket_max") else {
            panic!("bucket_max")
        };
        let shape = Shape {
            prefix: "t:".into(),
            bucket_max: *bucket_max as usize,
            memo: None,
        };
        let Some(Value::Array(batches)) = r.get("batches") else {
            panic!("batches")
        };
        let mut store: HashMap<String, Vec<u8>> = HashMap::new();
        let mut root = EMPTY_HASH.clone();
        let mut model: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (n, batch) in batches.iter().enumerate() {
            let Some(Value::Object(sets)) = batch.get("set") else {
                panic!("set")
            };
            let Some(Value::Array(removals)) = batch.get("remove") else {
                panic!("remove")
            };
            let sets: Vec<(String, Vec<u8>)> = sets
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().as_bytes().to_vec()))
                .collect();
            let removals: Vec<String> = removals
                .iter()
                .map(|k| k.as_str().unwrap().to_string())
                .collect();
            for (k, v) in &sets {
                model.insert(k.clone(), v.clone());
            }
            for k in &removals {
                model.remove(k);
            }
            let (s, rm, at, sh) = (sets.clone(), removals.clone(), root.clone(), shape.clone());
            let out = run(
                &store,
                Machine::new(move |io| update(io, sh, at, s, rm, HashMap::new())),
            )
            .unwrap();
            for (hash, raw) in out.nodes {
                store.insert(format!("t:{hash}"), raw);
            }
            root = out.root;
            assert_eq!(
                root,
                batch.get("root").unwrap().as_str().unwrap(),
                "bucket_max {bucket_max}, batch {n}"
            );
            let (at, sh) = (root.clone(), shape.clone());
            let (items, _) =
                run(&store, Machine::new(move |io| walk(io, sh, at, |_| false))).unwrap();
            assert_eq!(
                items.into_iter().collect::<BTreeMap<_, _>>(),
                model,
                "batch {n}"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 700);
}
