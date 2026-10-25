//! Records as the store holds them: compact JSON, written the way
//! Python's `json.dumps(value, separators=(",", ":"))` writes them.

use crate::json::{self, Value};

/// A record's bytes.
pub fn dumps(value: &Value) -> Vec<u8> {
    json::dumps(value, false).into_bytes()
}

/// A string's record: a branch head, a commit's root.
pub fn dumps_str(s: &str) -> Vec<u8> {
    dumps(&Value::Str(s.to_string()))
}

/// The value stored, or `None` for bytes that aren't JSON: a store may
/// hold damage, partial writes or another version's records.
pub fn safe_loads(raw: &[u8]) -> Option<Value> {
    json::loads(raw).ok()
}

/// The string stored, or `None` for anything else.
pub fn loads_str(raw: &[u8]) -> Option<String> {
    match safe_loads(raw)? {
        Value::Str(s) => Some(s),
        _ => None,
    }
}
