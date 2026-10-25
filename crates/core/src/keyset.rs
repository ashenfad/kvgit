//! A keyset: the tree a commit's keys live in, each holding an entry
//! that points at the key's value.
//!
//! An entry is `[<blob key>, <meta>]`, compact JSON in that order, the
//! meta's fields in the order `size`, `created_at`, `chunks`. `size` is
//! always written; `chunks`, the chunk keys a chunked value refers to,
//! only when there are some. `created_at` appears only on entries
//! written before storage v4 and is never written on a new one: it is
//! kept so that an untouched subtree re-encodes to its stored bytes.

use crate::json::{self, Value, dumps};
use crate::layout::KEYSET_PREFIX;
use crate::tree::Shape;

/// The shape every keyset has.
pub fn shape() -> Shape {
    Shape {
        prefix: KEYSET_PREFIX.to_string(),
        bucket_max: 8,
        memo: None,
    }
}

/// One key's entry.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// The key of the value's blob: `kvgit:blob:<sha256>`, or
    /// `<commit>:<key>` on entries written before storage v4.
    pub blob: String,
    /// The value's size in bytes, as stored (`null` when it wasn't).
    pub size: Value,
    pub created_at: Option<Value>,
    pub chunks: Vec<String>,
}

impl Entry {
    /// An entry for a value of `size` bytes in `blob`.
    pub fn new(blob: String, size: u64, chunks: Vec<String>) -> Entry {
        let size = i64::try_from(size).map_or_else(|_| Value::BigInt(size.to_string()), Value::Int);
        Entry {
            blob,
            size,
            created_at: None,
            chunks,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut meta = vec![("size".to_string(), self.size.clone())];
        if let Some(created_at) = &self.created_at {
            meta.push(("created_at".to_string(), created_at.clone()));
        }
        if !self.chunks.is_empty() {
            let chunks = self.chunks.iter().map(|c| Value::Str(c.clone())).collect();
            meta.push(("chunks".to_string(), Value::Array(chunks)));
        }
        dumps(
            &Value::Array(vec![Value::Str(self.blob.clone()), Value::Object(meta)]),
            false,
        )
        .into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<Entry, String> {
        let value = json::loads(raw).map_err(|e| format!("a keyset entry: {e}"))?;
        let Value::Array(parts) = value else {
            return Err("a keyset entry isn't a list".into());
        };
        let [Value::Str(blob), meta @ Value::Object(_)] = parts.as_slice() else {
            return Err("a keyset entry isn't [blob, meta]".into());
        };
        let chunks = match meta.get("chunks") {
            None | Some(Value::Null) => vec![],
            Some(Value::Array(chunks)) => chunks
                .iter()
                .map(|c| {
                    c.as_str()
                        .map(str::to_string)
                        .ok_or("a chunk key isn't a string")
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err("a keyset entry's chunks aren't a list".into()),
        };
        Ok(Entry {
            blob: blob.clone(),
            size: meta.get("size").cloned().unwrap_or(Value::Null),
            created_at: meta
                .get("created_at")
                .filter(|v| **v != Value::Null)
                .cloned(),
            chunks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_encode_as_python_writes_them() {
        let entry = Entry::new("kvgit:blob:ab".into(), 3, vec![]);
        assert_eq!(entry.encode(), br#"["kvgit:blob:ab",{"size":3}]"#);
        let chunked = Entry::new("x".into(), 1, vec!["c1".into(), "c2".into()]);
        assert_eq!(
            chunked.encode(),
            br#"["x",{"size":1,"chunks":["c1","c2"]}]"#
        );
        assert_eq!(Entry::decode(&chunked.encode()).unwrap(), chunked);
    }

    #[test]
    fn entries_written_before_v4_reencode_to_their_bytes() {
        for text in [
            r#"["c1:k",{"size":26,"created_at":1790533742.0508668}]"#,
            r#"["c1:k",{"size":2,"created_at":1790533742.0}]"#,
            r#"["c1:k",{"size":2,"created_at":1790533742}]"#,
            r#"["c1:k",{"size":2,"created_at":1790533742.5,"chunks":["c1","c2"]}]"#,
            r#"["c1:k",{"size":null}]"#,
        ] {
            assert_eq!(
                Entry::decode(text.as_bytes()).unwrap().encode(),
                text.as_bytes(),
                "{text}"
            );
        }
    }

    #[test]
    fn meta_is_rewritten_in_its_canonical_order() {
        let entry =
            Entry::decode(br#"["b",{"chunks":[],"created_at":null,"size":4,"extra":1}]"#).unwrap();
        assert_eq!(entry.encode(), br#"["b",{"size":4}]"#);
    }

    #[test]
    fn what_isnt_an_entry() {
        for text in [
            &b"{}"[..],
            b"[\"b\"]",
            b"[1,{}]",
            b"[\"b\",{\"chunks\":[1]}]",
            b"nope",
        ] {
            assert!(Entry::decode(text).is_err());
        }
    }
}
