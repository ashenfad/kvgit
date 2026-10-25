//! The text merge, against Python kvgit's: the same three sides make
//! the same bytes and the same conflict flag
//! (`fixtures/text-merge.json`, from `make_text_merge.py`).

use kvgit_core::json::{Value, loads};
use kvgit_core::text::text_merge;

fn side(case: &Value, name: &str) -> Option<Vec<u8>> {
    match case.get(name) {
        Some(Value::Str(s)) => Some(s.as_bytes().to_vec()),
        Some(Value::Null) => None,
        other => panic!("{name}: {other:?}"),
    }
}

#[test]
fn every_merge_makes_what_python_made() {
    let Value::Array(cases) = loads(include_bytes!("fixtures/text-merge.json")).unwrap() else {
        panic!("a list of cases")
    };
    assert!(cases.len() > 1000);
    for (n, case) in cases.iter().enumerate() {
        let Some(Value::Array(labels)) = case.get("labels") else {
            panic!("labels")
        };
        let label = |i: usize| labels[i].as_str().unwrap();
        let (old, ours, theirs) = (side(case, "old"), side(case, "ours"), side(case, "theirs"));
        let (merged, conflicted) = text_merge(
            old.as_deref(),
            ours.as_deref(),
            theirs.as_deref(),
            label(0),
            label(1),
        )
        .unwrap();
        let expected = case.get("merged").and_then(Value::as_str).unwrap();
        assert_eq!(String::from_utf8(merged).unwrap(), expected, "case {n}");
        assert_eq!(
            Some(&Value::Bool(conflicted)),
            case.get("conflicted"),
            "case {n}"
        );
    }
}
