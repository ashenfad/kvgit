//! Commit records and the hash that names a commit.

use crate::hash::sha256_hex;
use crate::json::{Value, dumps};
use crate::layout::COMMIT_HASH_LEN;

/// The 40-hex hash naming a commit: SHA-256 over its parents, keyset
/// root, time and info, truncated. Every `__commit_*__<hash>` key is
/// written once, so a hash names exactly one root; two writers making
/// the same change at the same clock instant make the very same commit.
pub fn commit_hash(parents: &[&str], root: &str, time: f64, info: Option<&Value>) -> String {
    let payload = Value::Array(vec![
        Value::Str("kvgit/4".into()),
        Value::Array(parents.iter().map(|p| Value::Str((*p).into())).collect()),
        Value::Str(root.into()),
        Value::Float(time),
        info.cloned().unwrap_or(Value::Null),
    ]);
    let mut hash = sha256_hex(dumps(&payload, true).as_bytes());
    hash.truncate(COMMIT_HASH_LEN);
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::loads;
    use crate::layout::ROOT_COMMIT;

    #[test]
    fn info_is_hashed_with_sorted_keys() {
        let a = loads(br#"{"b":1,"a":2}"#).unwrap();
        let b = loads(br#"{"a":2,"b":1}"#).unwrap();
        assert_eq!(
            commit_hash(&[ROOT_COMMIT], "r", 1.5, Some(&a)),
            commit_hash(&[ROOT_COMMIT], "r", 1.5, Some(&b))
        );
        assert_ne!(
            commit_hash(&[ROOT_COMMIT], "r", 1.5, Some(&a)),
            commit_hash(&[ROOT_COMMIT], "r", 1.5, None)
        );
    }
}
