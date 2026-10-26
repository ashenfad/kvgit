//! The WebAssembly binding of the kvgit core, loaded by the TypeScript
//! package.
//!
//! What crosses the boundary crosses in bulk: a list of strings as one
//! JSON text, many byte strings as one buffer and their lengths, so a
//! level of a tree is a handful of calls whatever its width.

use kvgit_core::json::{Value, write_string};
use kvgit_core::machine::{Machine, Reply, Request, Step};
use kvgit_core::tree::{self, Memo, Shape, TreeDiff, TreeError, Updated};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use wasm_bindgen::prelude::*;

mod op;
pub use op::Op;

/// The version of the core, which is the version of the package.
#[wasm_bindgen]
pub fn version() -> String {
    kvgit_core::version().to_string()
}

/// The root of the empty keyset.
#[wasm_bindgen(js_name = emptyHash)]
pub fn empty_hash() -> String {
    kvgit_core::layout::EMPTY_HASH.clone()
}

/// The 40-hex hash naming a commit. `info` is the info's JSON text, or
/// absent for none.
#[wasm_bindgen(js_name = commitHash)]
pub fn commit_hash(
    parents_json: &str,
    root: &str,
    time: f64,
    info: Option<String>,
) -> Result<String, JsError> {
    let parents = strings(parents_json)?;
    let info = info
        .map(|text| kvgit_core::json::loads(text.as_bytes()))
        .transpose()
        .map_err(to_js)?;
    let parents: Vec<&str> = parents.iter().map(String::as_str).collect();
    Ok(kvgit_core::commit::commit_hash(
        &parents,
        root,
        time,
        info.as_ref(),
    ))
}

/// What the text merge made of a value's sides.
#[wasm_bindgen]
pub struct TextMerged {
    #[wasm_bindgen(getter_with_clone)]
    pub merged: Vec<u8>,
    pub conflicted: bool,
    /// Why the values can't be marker-merged, when they can't; then
    /// `merged` is empty.
    #[wasm_bindgen(getter_with_clone)]
    pub refused: Option<String>,
}

/// The three-way text merge of a value's sides, absent where the key
/// is. A label holding a line break throws.
#[wasm_bindgen(js_name = textMerge)]
pub fn text_merge(
    old: Option<Vec<u8>>,
    ours: Option<Vec<u8>>,
    theirs: Option<Vec<u8>>,
    ours_label: &str,
    theirs_label: &str,
) -> Result<TextMerged, JsError> {
    use kvgit_core::text::{self, TextError};
    match text::text_merge(
        old.as_deref(),
        ours.as_deref(),
        theirs.as_deref(),
        ours_label,
        theirs_label,
    ) {
        Ok((merged, conflicted)) => Ok(TextMerged {
            merged,
            conflicted,
            refused: None,
        }),
        Err(TextError::CantMark(reason)) => Ok(TextMerged {
            merged: Vec::new(),
            conflicted: false,
            refused: Some(reason),
        }),
        Err(e) => Err(to_js(e)),
    }
}

/// The most bytes, all sides together, the text merge will mark.
#[wasm_bindgen(js_name = maxMarkBytes)]
pub fn max_mark_bytes() -> usize {
    kvgit_core::text::MAX_MARK_BYTES
}

/// The reason `name` can't be a branch, or nothing if it can.
#[wasm_bindgen(js_name = branchNameProblem)]
pub fn branch_name_problem(name: &str) -> Option<String> {
    kvgit_core::refs::check_branch_name(name)
        .err()
        .map(|e| e.to_string())
}

/// The reason `name` can't be a tag, or nothing if it can.
#[wasm_bindgen(js_name = tagNameProblem)]
pub fn tag_name_problem(name: &str) -> Option<String> {
    kvgit_core::refs::check_tag_name(name)
        .err()
        .map(|e| e.to_string())
}

/// A repository's memory of write-once keys (tree nodes, commit
/// records), least recently used first out, bounded in bytes, and
/// emptied when a read finds the GC lease record changed.
#[wasm_bindgen]
pub struct ContentCache {
    inner: kvgit_core::cache::Shared,
}

#[wasm_bindgen]
impl ContentCache {
    #[wasm_bindgen(constructor)]
    pub fn new(max_bytes: usize) -> ContentCache {
        ContentCache {
            inner: kvgit_core::cache::Cache::shared(max_bytes),
        }
    }

    /// `{hits, misses, clears, entries, bytes}`, as JSON.
    pub fn stats(&self) -> String {
        let c = self.inner.lock().unwrap();
        format!(
            r#"{{"hits":{},"misses":{},"clears":{},"entries":{},"bytes":{}}}"#,
            c.hits,
            c.misses,
            c.clears,
            c.len(),
            c.size_bytes()
        )
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

impl ContentCache {
    pub(crate) fn shared(&self) -> kvgit_core::cache::Shared {
        self.inner.clone()
    }
}

fn to_js(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

/// A JSON list of strings.
fn strings(json: &str) -> Result<Vec<String>, JsError> {
    match kvgit_core::json::loads(json.as_bytes()).map_err(to_js)? {
        Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Str(s) => Ok(s),
                _ => Err(JsError::new("expected a list of strings")),
            })
            .collect(),
        _ => Err(JsError::new("expected a list of strings")),
    }
}

fn strings_json(items: &[String]) -> String {
    let mut out = String::from("[");
    for (i, s) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_string(&mut out, s);
    }
    out.push(']');
    out
}

/// Byte strings laid end to end in `data`, `lengths[i]` long each.
fn unpack(data: &[u8], lengths: &[u32]) -> Result<Vec<Vec<u8>>, JsError> {
    let mut out = Vec::with_capacity(lengths.len());
    let mut at = 0usize;
    for &n in lengths {
        let end = at + n as usize;
        let chunk = data
            .get(at..end)
            .ok_or_else(|| JsError::new("lengths overrun the data"))?;
        out.push(chunk.to_vec());
        at = end;
    }
    if at != data.len() {
        return Err(JsError::new("lengths don't cover the data"));
    }
    Ok(out)
}

fn pairs(keys_json: &str, data: &[u8], lengths: &[u32]) -> Result<Vec<(String, Vec<u8>)>, JsError> {
    let keys = strings(keys_json)?;
    let values = unpack(data, lengths)?;
    if keys.len() != values.len() {
        return Err(JsError::new("as many keys as values"));
    }
    Ok(keys.into_iter().zip(values).collect())
}

enum Out {
    Found(Vec<(String, Vec<u8>)>),
    Keys(Vec<String>),
    Walked(Vec<(String, Vec<u8>)>, Vec<String>),
    Diffed(TreeDiff),
    Updated(Updated),
}

/// A tree operation in progress.
///
/// `resume` returns the keys the operation needs, as a JSON list, or
/// `undefined` once it is done; it is answered with the stored values
/// of those keys, leaving out absent ones. The result is then read with
/// the `result*` methods.
#[wasm_bindgen]
pub struct TreeOp {
    machine: Option<Machine<Result<Out, TreeError>>>,
    out: Option<Out>,
}

/// A memo with no room holds nothing, and so stands for none.
fn shape(prefix: String, bucket_max: usize, memo: &NodeMemo) -> Shape {
    let memo = (memo.0.borrow().max_bytes() > 0).then(|| memo.0.clone());
    Shape {
        prefix,
        bucket_max,
        memo,
    }
}

/// Stored tree nodes, parsed, held between operations: bounded by their
/// stored bytes, least recently used evicted first.
#[wasm_bindgen]
pub struct NodeMemo(Memo);

#[wasm_bindgen]
impl NodeMemo {
    #[wasm_bindgen(constructor)]
    pub fn new(max_bytes: usize) -> NodeMemo {
        NodeMemo(Rc::new(RefCell::new(tree::NodeMemo::new(max_bytes))))
    }

    /// `{hits, misses, evictions, entries, bytes}`, as JSON.
    pub fn stats(&self) -> String {
        let s = self.0.borrow().stats();
        format!(
            "{{\"hits\":{},\"misses\":{},\"evictions\":{},\"entries\":{},\"bytes\":{}}}",
            s.hits, s.misses, s.evictions, s.entries, s.bytes
        )
    }

    /// Hold a node the host has stored, from its bytes.
    pub fn hold(&self, hash: &str, raw: &[u8]) -> Result<(), JsError> {
        self.0.borrow_mut().hold(hash, raw).map_err(to_js)
    }

    #[wasm_bindgen(js_name = resetStats)]
    pub fn reset_stats(&self) {
        self.0.borrow_mut().reset_stats();
    }

    pub fn clear(&self) {
        self.0.borrow_mut().clear();
    }
}

fn op(machine: Machine<Result<Out, TreeError>>) -> TreeOp {
    TreeOp {
        machine: Some(machine),
        out: None,
    }
}

/// Rows of values that are keyset entries, as JSON: `[[key, entry], ...]`.
/// An entry is JSON already, so it is written as it is stored.
fn entry_rows(rows: &[(String, Vec<u8>)]) -> Result<String, JsError> {
    let mut out = String::from("[");
    for (i, (k, v)) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('[');
        write_string(&mut out, k);
        out.push(',');
        out.push_str(std::str::from_utf8(v).map_err(to_js)?);
        out.push(']');
    }
    out.push(']');
    Ok(out)
}

#[wasm_bindgen]
impl TreeOp {
    /// The values of those of `keys` (a JSON list) the tree holds.
    #[wasm_bindgen(js_name = getMany)]
    pub fn get_many(
        prefix: String,
        bucket_max: usize,
        root: String,
        keys_json: &str,
        memo: &NodeMemo,
    ) -> Result<TreeOp, JsError> {
        let keys = strings(keys_json)?;
        let shape = shape(prefix, bucket_max, memo);
        Ok(op(Machine::new(move |io| async move {
            tree::get_many(io, shape, root, keys).await.map(Out::Found)
        })))
    }

    /// Everything the tree holds and every node it is made of, skipping
    /// the nodes `skip.has` holds for, and all under them.
    pub fn walk(
        prefix: String,
        bucket_max: usize,
        root: String,
        skip: Option<js_sys::Set>,
        memo: &NodeMemo,
    ) -> TreeOp {
        let shape = shape(prefix, bucket_max, memo);
        let skipped = move |hash: &str| {
            skip.as_ref()
                .is_some_and(|s| s.has(&JsValue::from_str(hash)))
        };
        op(Machine::new(move |io| async move {
            tree::walk(io, shape, root, skipped)
                .await
                .map(|(items, nodes)| Out::Walked(items, nodes))
        }))
    }

    /// Every key the tree holds, without its value.
    pub fn keys(prefix: String, bucket_max: usize, root: String, memo: &NodeMemo) -> TreeOp {
        let shape = shape(prefix, bucket_max, memo);
        op(Machine::new(move |io| async move {
            tree::keys(io, shape, root).await.map(Out::Keys)
        }))
    }

    /// What changed from the tree at `a` to the tree at `b`.
    pub fn diff(
        prefix: String,
        bucket_max: usize,
        a: String,
        b: String,
        memo: &NodeMemo,
    ) -> TreeOp {
        let shape = shape(prefix, bucket_max, memo);
        op(Machine::new(move |io| async move {
            tree::diff(io, shape, a, b).await.map(Out::Diffed)
        }))
    }

    /// The tree at `root` with the updates set (keys as a JSON list,
    /// values packed), then `removals` (a JSON list) removed. The
    /// caller's unstored nodes come packed the same way, keyed by hash.
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        prefix: String,
        bucket_max: usize,
        root: String,
        keys_json: &str,
        data: &[u8],
        lengths: &[u32],
        removals_json: &str,
        unwritten_json: &str,
        unwritten_data: &[u8],
        unwritten_lengths: &[u32],
        memo: &NodeMemo,
    ) -> Result<TreeOp, JsError> {
        let sets = pairs(keys_json, data, lengths)?;
        let removals = strings(removals_json)?;
        let unwritten: HashMap<String, Vec<u8>> =
            pairs(unwritten_json, unwritten_data, unwritten_lengths)?
                .into_iter()
                .collect();
        let shape = shape(prefix, bucket_max, memo);
        Ok(op(Machine::new(move |io| async move {
            tree::update(io, shape, root, sets, removals, unwritten)
                .await
                .map(Out::Updated)
        })))
    }

    /// Run until the operation needs the store (the keys, as a JSON
    /// list) or is done (`undefined`). The first call passes no reply.
    pub fn resume(
        &mut self,
        keys_json: Option<String>,
        data: Option<Vec<u8>>,
        lengths: Option<Vec<u32>>,
    ) -> Result<Option<String>, JsError> {
        let machine = self
            .machine
            .as_mut()
            .ok_or_else(|| JsError::new("the operation is done"))?;
        let reply = match keys_json {
            None => None,
            Some(keys) => {
                let found = pairs(
                    &keys,
                    &data.unwrap_or_default(),
                    &lengths.unwrap_or_default(),
                )?;
                Some(Reply::Values(found.into_iter().collect()))
            }
        };
        match machine.resume(reply) {
            Step::Request(Request::Get(keys)) => Ok(Some(strings_json(&keys))),
            Step::Request(other) => unreachable!("a tree only reads, not {other:?}"),
            Step::Done(result) => {
                self.machine = None;
                self.out = Some(result.map_err(to_js)?);
                Ok(None)
            }
        }
    }

    fn rows(&self) -> Result<&[(String, Vec<u8>)], JsError> {
        match &self.out {
            Some(Out::Found(rows)) | Some(Out::Walked(rows, _)) => Ok(rows),
            Some(Out::Updated(u)) => Ok(&u.nodes),
            _ => Err(JsError::new("no rows")),
        }
    }

    /// The result's keys (for an update, its nodes' hashes), as a JSON
    /// list.
    #[wasm_bindgen(js_name = resultKeys)]
    pub fn result_keys(&self) -> Result<String, JsError> {
        if let Some(Out::Keys(keys)) = &self.out {
            return Ok(strings_json(keys));
        }
        Ok(strings_json(
            &self
                .rows()?
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
        ))
    }

    /// The result's values, end to end.
    #[wasm_bindgen(js_name = resultData)]
    pub fn result_data(&self) -> Result<Vec<u8>, JsError> {
        Ok(self
            .rows()?
            .iter()
            .flat_map(|(_, v)| v.iter().copied())
            .collect())
    }

    /// The lengths of the result's values.
    #[wasm_bindgen(js_name = resultLengths)]
    pub fn result_lengths(&self) -> Result<Vec<u32>, JsError> {
        Ok(self.rows()?.iter().map(|(_, v)| v.len() as u32).collect())
    }

    /// The result's rows when its values are keyset entries, as JSON:
    /// `[[key, entry], ...]`.
    #[wasm_bindgen(js_name = resultEntries)]
    pub fn result_entries(&self) -> Result<String, JsError> {
        entry_rows(self.rows()?)
    }

    /// A walk's nodes, as a JSON list.
    #[wasm_bindgen(js_name = resultNodes)]
    pub fn result_nodes(&self) -> Result<String, JsError> {
        match &self.out {
            Some(Out::Walked(_, nodes)) => Ok(strings_json(nodes)),
            _ => Err(JsError::new("not a walk")),
        }
    }

    /// An update's root.
    #[wasm_bindgen(js_name = resultRoot)]
    pub fn result_root(&self) -> Result<String, JsError> {
        match &self.out {
            Some(Out::Updated(u)) => Ok(u.root.clone()),
            _ => Err(JsError::new("not an update")),
        }
    }

    /// A diff whose values are keyset entries, as JSON:
    /// `{"added": [[key, entry]], "removed": [...], "modified": [[key, old, new]]}`.
    #[wasm_bindgen(js_name = resultDiffEntries)]
    pub fn result_diff_entries(&self) -> Result<String, JsError> {
        let Some(Out::Diffed(d)) = &self.out else {
            return Err(JsError::new("not a diff"));
        };
        let mut modified = String::from("[");
        for (i, (k, old, new)) in d.modified.iter().enumerate() {
            if i > 0 {
                modified.push(',');
            }
            modified.push('[');
            write_string(&mut modified, k);
            modified.push(',');
            modified.push_str(std::str::from_utf8(old).map_err(to_js)?);
            modified.push(',');
            modified.push_str(std::str::from_utf8(new).map_err(to_js)?);
            modified.push(']');
        }
        modified.push(']');
        Ok(format!(
            "{{\"added\":{},\"removed\":{},\"modified\":{modified}}}",
            entry_rows(&d.added)?,
            entry_rows(&d.removed)?
        ))
    }

    /// A diff of byte values, as rows: which change each is (0 added,
    /// 1 removed, 2 modified), its key, and its values packed: one for
    /// an addition or removal, the old then the new for a modification.
    #[wasm_bindgen(js_name = resultDiffKinds)]
    pub fn result_diff_kinds(&self) -> Result<Vec<u8>, JsError> {
        let Some(Out::Diffed(d)) = &self.out else {
            return Err(JsError::new("not a diff"));
        };
        Ok(std::iter::repeat_n(0, d.added.len())
            .chain(std::iter::repeat_n(1, d.removed.len()))
            .chain(std::iter::repeat_n(2, d.modified.len()))
            .collect())
    }

    #[wasm_bindgen(js_name = resultDiffKeys)]
    pub fn result_diff_keys(&self) -> Result<String, JsError> {
        let Some(Out::Diffed(d)) = &self.out else {
            return Err(JsError::new("not a diff"));
        };
        let keys: Vec<String> = d
            .added
            .iter()
            .chain(&d.removed)
            .map(|(k, _)| k.clone())
            .chain(d.modified.iter().map(|(k, _, _)| k.clone()))
            .collect();
        Ok(strings_json(&keys))
    }

    #[wasm_bindgen(js_name = resultDiffData)]
    pub fn result_diff_data(&self) -> Result<Vec<u8>, JsError> {
        let Some(Out::Diffed(d)) = &self.out else {
            return Err(JsError::new("not a diff"));
        };
        let mut out = Vec::new();
        for (_, v) in d.added.iter().chain(&d.removed) {
            out.extend_from_slice(v);
        }
        for (_, old, new) in &d.modified {
            out.extend_from_slice(old);
            out.extend_from_slice(new);
        }
        Ok(out)
    }

    #[wasm_bindgen(js_name = resultDiffLengths)]
    pub fn result_diff_lengths(&self) -> Result<Vec<u32>, JsError> {
        let Some(Out::Diffed(d)) = &self.out else {
            return Err(JsError::new("not a diff"));
        };
        let mut out = Vec::new();
        for (_, v) in d.added.iter().chain(&d.removed) {
            out.push(v.len() as u32);
        }
        for (_, old, new) in &d.modified {
            out.push(old.len() as u32);
            out.push(new.len() as u32);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_cross_as_json() {
        let items = vec![
            "a".to_string(),
            "nul\u{0}inside".to_string(),
            "\u{1f600}".to_string(),
        ];
        assert_eq!(strings(&strings_json(&items)).unwrap(), items);
    }
}
