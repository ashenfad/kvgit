//! The core's operations across the boundary.
//!
//! An operation's arguments come in as one JSON text; each request goes
//! out as its kind, a JSON text and, for a write, its bytes end to end;
//! each reply comes back through the method for its kind.

use super::{NodeMemo, pairs, shape, strings_json, to_js};
use kvgit_core::error::Error;
use kvgit_core::head::Note;
use kvgit_core::json::{self, Value, loads, write_string};
use kvgit_core::keyset::{self, Entry};
use kvgit_core::machine::{Machine, Reply, Request, Step, Write};
use kvgit_core::merge::{MergeAnswer, Rule, Rules};
use kvgit_core::ops::{self, Out};
use kvgit_core::text::TextRule;
use kvgit_core::write::{CommitArgs, Committed, Done, MergeArgs, OnConflict, OnMoved};
use std::collections::HashMap;
use std::fmt::Write as _;
use wasm_bindgen::prelude::*;

/// An operation of the core in progress, started by name (see
/// `kvgit_core::ops`) or by `Op.commit`. Each `reply*` call (and the
/// first, `start`), passing wall time, returns the kind of the next
/// request (`"get"`, `"scan"`, `"cas"`, `"wait"`, `"merge"`,
/// `"recover"`, `"now"`, `"emit"`) or `"done"`; `requestJson` and `requestData`
/// describe the request, and `outcome` the result.
#[wasm_bindgen]
pub struct Op {
    machine: Option<Machine<Done<Out>>>,
    asked: Option<Request>,
    done: Option<Done<Out>>,
}

fn field<'a>(args: &'a Value, name: &str) -> Result<&'a Value, JsError> {
    args.get(name)
        .ok_or_else(|| JsError::new(&format!("commit arguments lack {name}")))
}

fn text(value: &Value, what: &str) -> Result<String, JsError> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| JsError::new(&format!("{what} isn't a string")))
}

fn list<'a>(value: &'a Value, what: &str) -> Result<&'a [Value], JsError> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err(JsError::new(&format!("{what} isn't a list"))),
    }
}

fn texts(value: &Value, what: &str) -> Result<Vec<String>, JsError> {
    list(value, what)?.iter().map(|v| text(v, what)).collect()
}

fn number(value: &Value, what: &str) -> Result<u64, JsError> {
    match value {
        Value::Int(n) if *n >= 0 => Ok(*n as u64),
        Value::Float(x) if *x >= 0.0 && x.fract() == 0.0 => Ok(*x as u64),
        _ => Err(JsError::new(&format!("{what} isn't a count"))),
    }
}

/// A rule: `"ours"`, `"theirs"`, a host function's number, or `["text",
/// ours_label, theirs_label, strict]`.
fn rule(value: &Value) -> Result<Rule, JsError> {
    match value {
        Value::Str(side) if side == "ours" => Ok(Rule::Ours),
        Value::Str(side) if side == "theirs" => Ok(Rule::Theirs),
        Value::Int(_) | Value::Float(_) => Ok(Rule::Host(number(value, "a rule")? as u32)),
        Value::Array(parts) => match parts.as_slice() {
            [Value::Str(kind), ours, theirs, Value::Bool(strict)] if kind == "text" => {
                Ok(Rule::Text(TextRule {
                    ours_label: text(ours, "a label")?,
                    theirs_label: text(theirs, "a label")?,
                    strict: *strict,
                }))
            }
            _ => Err(JsError::new("not a merge rule")),
        },
        _ => Err(JsError::new("not a merge rule")),
    }
}

fn pairs_of<T>(
    value: &Value,
    what: &str,
    mut each: impl FnMut(&Value, &Value) -> Result<T, JsError>,
) -> Result<Vec<T>, JsError> {
    list(value, what)?
        .iter()
        .map(|pair| match pair {
            Value::Array(two) if two.len() == 2 => each(&two[0], &two[1]),
            _ => Err(JsError::new(&format!("{what} isn't a list of pairs"))),
        })
        .collect()
}

fn args_of(json: &str, info: Option<String>, memo: &NodeMemo) -> Result<CommitArgs, JsError> {
    let args = loads(json.as_bytes()).map_err(to_js)?;
    let updates = list(field(&args, "updates")?, "updates")?
        .iter()
        .map(|u| match u {
            Value::Array(parts) if parts.len() == 4 => Ok((
                text(&parts[0], "a key")?,
                Entry::new(
                    text(&parts[1], "a blob")?,
                    number(&parts[2], "a size")?,
                    match &parts[3] {
                        Value::Null => vec![],
                        chunks => texts(chunks, "chunks")?,
                    },
                ),
            )),
            _ => Err(JsError::new("an update isn't [key, blob, size, chunks]")),
        })
        .collect::<Result<_, _>>()?;
    let keyset = keyset::shape();
    Ok(CommitArgs {
        branch: text(field(&args, "branch")?, "branch")?,
        base: text(field(&args, "base")?, "base")?,
        current: text(field(&args, "current")?, "current")?,
        shape: shape(keyset.prefix, keyset.bucket_max, memo),
        updates,
        removals: texts(field(&args, "removals")?, "removals")?,
        chunks: texts(field(&args, "chunks")?, "chunks")?,
        info: info
            .map(|text| loads(text.as_bytes()))
            .transpose()
            .map_err(to_js)?,
        on_moved: match field(&args, "onMoved")?.as_str() {
            Some("raise") => OnMoved::Raise,
            _ => OnMoved::Merge,
        },
        on_conflict: on_conflict_of(&args)?,
        rules: rules_of(&args)?,
        recover: field(&args, "recover")? == &Value::Bool(true),
        stamped: field(&args, "stamped")? == &Value::Bool(true),
        roots: roots_of(&args)?,
        generations: generations_of(&args)?,
    })
}

/// The merge rules in `args`: `mergeFns`, `mergePrefixes` and
/// `defaultMerge`.
fn rules_of(args: &Value) -> Result<Rules, JsError> {
    let default = match field(args, "defaultMerge")? {
        Value::Null => None,
        r => Some(rule(r)?),
    };
    Ok(Rules {
        keys: pairs_of(field(args, "mergeFns")?, "mergeFns", |k, r| {
            Ok((text(k, "a key")?, rule(r)?))
        })?
        .into_iter()
        .collect(),
        prefixes: pairs_of(field(args, "mergePrefixes")?, "mergePrefixes", |p, r| {
            Ok((text(p, "a prefix")?, rule(r)?))
        })?,
        default,
    })
}

fn roots_of(args: &Value) -> Result<Vec<(String, String)>, JsError> {
    pairs_of(field(args, "roots")?, "roots", |c, r| {
        Ok((text(c, "a commit")?, text(r, "a root")?))
    })
}

fn generations_of(args: &Value) -> Result<Vec<(String, u64)>, JsError> {
    pairs_of(field(args, "generations")?, "generations", |c, g| {
        Ok((text(c, "a commit")?, number(g, "a generation")?))
    })
}

fn on_conflict_of(args: &Value) -> Result<OnConflict, JsError> {
    Ok(match field(args, "onConflict")?.as_str() {
        Some("abandon") => OnConflict::Abandon,
        _ => OnConflict::Raise,
    })
}

fn merge_args_of(json: &str, info: Option<String>, memo: &NodeMemo) -> Result<MergeArgs, JsError> {
    let args = loads(json.as_bytes()).map_err(to_js)?;
    let keyset = keyset::shape();
    Ok(MergeArgs {
        branch: text(field(&args, "branch")?, "branch")?,
        ours: text(field(&args, "ours")?, "ours")?,
        theirs: text(field(&args, "theirs")?, "theirs")?,
        from: match field(&args, "from")? {
            Value::Null => None,
            from => Some(text(from, "from")?),
        },
        fast_forward: field(&args, "fastForward")? == &Value::Bool(true),
        shape: shape(keyset.prefix, keyset.bucket_max, memo),
        rules: rules_of(&args)?,
        on_conflict: on_conflict_of(&args)?,
        info: info
            .map(|text| loads(text.as_bytes()))
            .transpose()
            .map_err(to_js)?,
        post_check: field(&args, "postCheck")? == &Value::Bool(true),
        recover: field(&args, "recover")? == &Value::Bool(true),
        stamped: field(&args, "stamped")? == &Value::Bool(true),
        roots: roots_of(&args)?,
        generations: generations_of(&args)?,
    })
}

fn kind(request: &Request) -> &'static str {
    match request {
        Request::Get(_) => "get",
        Request::Scan(_) => "scan",
        Request::Cas(_) => "cas",
        Request::Wait(_) => "wait",
        Request::Merge(_) => "merge",
        Request::Recover(_) => "recover",
        Request::Now => "now",
        Request::Emit(_) => "emit",
        Request::Check(_) => "check",
        Request::Owner => "owner",
    }
}

fn json_opt(out: &mut String, s: Option<&str>) {
    match s {
        Some(s) => write_string(out, s),
        None => out.push_str("null"),
    }
}

fn json_list(out: &mut String, items: &[String]) {
    out.push_str(&strings_json(items));
}

#[wasm_bindgen]
impl Op {
    /// Operation `name` on `args_json`, an object.
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str, args_json: &str) -> Result<Op, JsError> {
        let args = loads(args_json.as_bytes()).map_err(to_js)?;
        Ok(Op {
            machine: Some(ops::call(name, args).map_err(to_js)?),
            asked: None,
            done: None,
        })
    }

    /// A commit of `args_json`, with `info` its info's JSON text, if
    /// any, reading tree nodes through `memo` (0 bytes: none).
    pub fn commit(args_json: &str, info: Option<String>, memo: &NodeMemo) -> Result<Op, JsError> {
        let args = args_of(args_json, info, memo)?;
        Ok(Op {
            machine: Some(ops::commit(args)),
            asked: None,
            done: None,
        })
    }

    fn step(&mut self, wall: f64, reply: Option<Reply>) -> Result<String, JsError> {
        let machine = self
            .machine
            .as_mut()
            .ok_or_else(|| JsError::new("the operation is done"))?;
        match machine.resume_at(wall, reply) {
            Step::Request(request) => {
                let kind = kind(&request);
                self.asked = Some(request);
                Ok(kind.into())
            }
            Step::Done(done) => {
                self.machine = None;
                self.asked = None;
                self.done = Some(done);
                Ok("done".into())
            }
        }
    }

    /// Read and write through `cache`; given before `start`.
    #[wasm_bindgen(js_name = useCache)]
    pub fn use_cache(&mut self, cache: &super::ContentCache) -> Result<(), JsError> {
        self.machine
            .as_mut()
            .ok_or_else(|| JsError::new("the operation is done"))?
            .use_cache(cache.shared());
        Ok(())
    }

    /// Read from, and move, `state`; given before `start`.
    #[wasm_bindgen(js_name = useTree)]
    pub fn use_tree(&mut self, state: &super::WorktreeState) -> Result<(), JsError> {
        self.machine
            .as_mut()
            .ok_or_else(|| JsError::new("the operation is done"))?
            .use_tree(state.shared());
        Ok(())
    }

    pub fn start(&mut self, wall: f64) -> Result<String, JsError> {
        self.step(wall, None)
    }

    /// The time to stamp a commit with.
    #[wasm_bindgen(js_name = replyNow)]
    pub fn reply_now(&mut self, wall: f64, now: f64) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Now(now)))
    }

    /// The stored values asked for: the keys found, as a JSON list, and
    /// their values packed.
    #[wasm_bindgen(js_name = replyValues)]
    pub fn reply_values(
        &mut self,
        wall: f64,
        keys_json: &str,
        data: &[u8],
        lengths: &[u32],
    ) -> Result<String, JsError> {
        let found: HashMap<String, Vec<u8>> =
            pairs(keys_json, data, lengths)?.into_iter().collect();
        self.step(wall, Some(Reply::Values(found)))
    }

    /// The stored keys asked for, as a JSON list.
    #[wasm_bindgen(js_name = replyKeys)]
    pub fn reply_keys(&mut self, wall: f64, keys_json: &str) -> Result<String, JsError> {
        let keys = loads(keys_json.as_bytes()).map_err(to_js)?;
        let keys = texts(&keys, "keys")?;
        self.step(wall, Some(Reply::Keys(keys)))
    }

    /// A merge of `args_json` (`theirs` into the branch at `ours`, or,
    /// given `from`, an apply of the change from `from` to `theirs`),
    /// with `info` its info's JSON text, if any.
    pub fn merge(args_json: &str, info: Option<String>, memo: &NodeMemo) -> Result<Op, JsError> {
        let args = merge_args_of(args_json, info, memo)?;
        Ok(Op {
            machine: Some(ops::merge(args)),
            asked: None,
            done: None,
        })
    }

    /// A fresh name for a GC lease.
    #[wasm_bindgen(js_name = replyOwner)]
    pub fn reply_owner(&mut self, wall: f64, owner: String) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Owner(owner)))
    }

    /// The keys a check refuses, as a JSON list.
    #[wasm_bindgen(js_name = replyRefused)]
    pub fn reply_refused(&mut self, wall: f64, keys_json: &str) -> Result<String, JsError> {
        let keys = loads(keys_json.as_bytes()).map_err(to_js)?;
        let keys = texts(&keys, "keys")?;
        self.step(wall, Some(Reply::Refused(keys)))
    }

    /// Go on, past part of the answer emitted early.
    #[wasm_bindgen(js_name = replyNext)]
    pub fn reply_next(&mut self, wall: f64) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Next))
    }

    #[wasm_bindgen(js_name = replyLanded)]
    pub fn reply_landed(&mut self, wall: f64, landed: bool) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Landed(landed)))
    }

    #[wasm_bindgen(js_name = replyWaited)]
    pub fn reply_waited(&mut self, wall: f64) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Waited))
    }

    /// One answer per merge asked, as JSON: `"ours"`, `"theirs"`,
    /// `["value", blob, size]` or `["failed", reason]`.
    #[wasm_bindgen(js_name = replyMerged)]
    pub fn reply_merged(&mut self, wall: f64, answers_json: &str) -> Result<String, JsError> {
        let answers = loads(answers_json.as_bytes()).map_err(to_js)?;
        let answers = list(&answers, "answers")?
            .iter()
            .map(|a| match a {
                Value::Str(side) if side == "ours" => Ok(MergeAnswer::Ours),
                Value::Str(side) if side == "theirs" => Ok(MergeAnswer::Theirs),
                Value::Array(parts) => match parts.as_slice() {
                    [Value::Str(k), blob, size] if k == "value" => Ok(MergeAnswer::Value {
                        blob: text(blob, "a blob")?,
                        size: number(size, "a size")?,
                    }),
                    [Value::Str(k), why] if k == "failed" => {
                        Ok(MergeAnswer::Failed(text(why, "a reason")?))
                    }
                    _ => Err(JsError::new("not a merge answer")),
                },
                _ => Err(JsError::new("not a merge answer")),
            })
            .collect::<Result<_, _>>()?;
        self.step(wall, Some(Reply::Merged(answers)))
    }

    #[wasm_bindgen(js_name = replyRecovered)]
    pub fn reply_recovered(
        &mut self,
        wall: f64,
        commit: Option<String>,
    ) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Recovered(commit)))
    }

    /// The host couldn't perform the request; it keeps the reason.
    #[wasm_bindgen(js_name = replyFailed)]
    pub fn reply_failed(&mut self, wall: f64) -> Result<String, JsError> {
        self.step(wall, Some(Reply::Failed))
    }

    /// The pending request, as JSON: a read's keys; a write's `{expect,
    /// writes, removes}`, each expected or written value its length in
    /// `requestData` (`null`: absent, or for a write the bytes the host
    /// holds); a wait's seconds; the merges asked, each `[rule, key,
    /// old, ours, theirs]`; the branch to recover; a scan's prefix; the part
    /// of the answer emitted; a check's `[key, blob, length]` values, each
    /// length that of its bytes in `requestData`, or `null` where the
    /// host holds the blob.
    #[wasm_bindgen(js_name = requestJson)]
    pub fn request_json(&self) -> Result<String, JsError> {
        let asked = self
            .asked
            .as_ref()
            .ok_or_else(|| JsError::new("no request"))?;
        let mut out = String::new();
        match asked {
            Request::Get(keys) => json_list(&mut out, keys),
            Request::Scan(prefix) => write_string(&mut out, prefix),
            Request::Cas(cas) => {
                out.push_str("{\"expect\":[");
                for (i, (k, v)) in cas.expect.iter().enumerate() {
                    out.push_str(if i > 0 { ",[" } else { "[" });
                    write_string(&mut out, k);
                    match v {
                        Some(b) => {
                            let _ = write!(out, ",{}]", b.len());
                        }
                        None => out.push_str(",null]"),
                    }
                }
                out.push_str("],\"writes\":[");
                for (i, (k, w)) in cas.writes.iter().enumerate() {
                    out.push_str(if i > 0 { ",[" } else { "[" });
                    write_string(&mut out, k);
                    match w {
                        Write::Bytes(b) => {
                            let _ = write!(out, ",{}]", b.len());
                        }
                        Write::Held => out.push_str(",null]"),
                    }
                }
                out.push_str("],\"removes\":");
                json_list(&mut out, &cas.removes);
                out.push('}');
            }
            Request::Wait(seconds) => {
                let _ = write!(out, "{seconds}");
            }
            Request::Merge(asks) => {
                out.push('[');
                for (i, a) in asks.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = write!(out, "[{},", a.rule);
                    write_string(&mut out, &a.key);
                    for side in [&a.old, &a.ours, &a.theirs] {
                        out.push(',');
                        json_opt(&mut out, side.as_deref());
                    }
                    out.push(']');
                }
                out.push(']');
            }
            Request::Recover(branch) => write_string(&mut out, branch),
            Request::Now | Request::Owner => out.push_str("null"),
            Request::Emit(part) => out.push_str(&json::dumps(part, false)),
            Request::Check(values) => {
                out.push('[');
                for (i, (key, blob, made)) in values.iter().enumerate() {
                    out.push_str(if i > 0 { ",[" } else { "[" });
                    write_string(&mut out, key);
                    out.push(',');
                    write_string(&mut out, blob);
                    match made {
                        Some(b) => {
                            let _ = write!(out, ",{}]", b.len());
                        }
                        None => out.push_str(",null]"),
                    }
                }
                out.push(']');
            }
        }
        Ok(out)
    }

    /// A write's bytes end to end: the expected values, then the
    /// written ones, in the order `requestJson` lists them.
    #[wasm_bindgen(js_name = requestData)]
    pub fn request_data(&self) -> Vec<u8> {
        if let Some(Request::Check(values)) = &self.asked {
            return values
                .iter()
                .filter_map(|(_, _, b)| b.clone())
                .flatten()
                .collect();
        }
        let Some(Request::Cas(cas)) = &self.asked else {
            return vec![];
        };
        let mut out = Vec::new();
        for b in cas.expect.iter().filter_map(|(_, v)| v.as_ref()) {
            out.extend_from_slice(b);
        }
        for (_, w) in &cas.writes {
            if let Write::Bytes(b) = w {
                out.extend_from_slice(b);
            }
        }
        out
    }

    /// The outcome, as JSON: `{"ok": value}` (a commit's value an
    /// object of what it committed) or `{"error": {"kind", "message",
    /// ...}}`, with the notes made on the way.
    pub fn outcome(&self) -> Result<String, JsError> {
        let done = self
            .done
            .as_ref()
            .ok_or_else(|| JsError::new("the operation isn't done"))?;
        let mut out = String::from("{");
        match &done.result {
            Ok(Out::Value(v)) => {
                out.push_str("\"ok\":");
                out.push_str(&json::dumps(v, false));
            }
            Ok(Out::Committed(c)) => committed_json(&mut out, c)?,
            Err(e) => {
                out.push_str("\"error\":{\"kind\":");
                write_string(&mut out, e.kind());
                out.push_str(",\"message\":");
                write_string(&mut out, &e.to_string());
                for (name, v) in e.fields() {
                    let _ = write!(out, ",\"{name}\":");
                    write_string(&mut out, v);
                }
                if let Error::MergeConflict { keys, failures } = e {
                    out.push_str(",\"keys\":");
                    json_list(&mut out, keys);
                    out.push_str(",\"failures\":[");
                    for (i, (k, why)) in failures.iter().enumerate() {
                        out.push_str(if i > 0 { ",[" } else { "[" });
                        write_string(&mut out, k);
                        out.push(',');
                        write_string(&mut out, why);
                        out.push(']');
                    }
                    out.push(']');
                }
                out.push('}');
            }
        }
        notes_json(&mut out, &done.notes);
        Ok(out)
    }
}

/// What a commit committed, as the outcome's `ok` field.
fn committed_json(out: &mut String, c: &Committed) -> Result<(), JsError> {
    out.push_str("\"ok\":{\"merged\":");
    out.push_str(if c.merged { "true" } else { "false" });
    out.push_str(",\"commit\":");
    json_opt(out, c.commit.as_deref());
    let _ = write!(
        out,
        ",\"strategy\":\"{}\",\"autoMerged\":",
        c.strategy.name()
    );
    json_list(out, &c.auto_merged);
    out.push_str(",\"carried\":");
    json_list(out, &c.carried);
    out.push_str(",\"changes\":[");
    for (i, (k, e)) in c.changes.iter().enumerate() {
        out.push_str(if i > 0 { ",[" } else { "[" });
        write_string(out, k);
        out.push(',');
        match e {
            Some(e) => out.push_str(std::str::from_utf8(&e.encode()).map_err(to_js)?),
            None => out.push_str("null"),
        }
        out.push(']');
    }
    out.push_str("],\"roots\":[");
    for (i, (commit, root)) in c.roots.iter().enumerate() {
        out.push_str(if i > 0 { ",[" } else { "[" });
        write_string(out, commit);
        out.push(',');
        write_string(out, root);
        out.push(']');
    }
    out.push_str("],\"generations\":[");
    for (i, (commit, g)) in c.generations.iter().enumerate() {
        out.push_str(if i > 0 { ",[" } else { "[" });
        write_string(out, commit);
        let _ = write!(out, ",{g}]");
    }
    let _ = write!(out, "],\"stamped\":{}}}", c.stamped);
    Ok(())
}

/// The notes an operation made, as the outcome's `notes` field.
fn notes_json(out: &mut String, notes: &[Note]) {
    out.push_str(",\"notes\":[");
    for (i, note) in notes.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let (kind, branch, answer) = match note {
            Note::FromBackup { branch } => ("from_backup", branch, None),
            Note::FromRecoverer { branch } => ("from_recoverer", branch, None),
            Note::RecovererMissed { branch, answer } => ("recoverer_missed", branch, Some(answer)),
            Note::Healed { branch } => ("healed", branch, None),
            Note::LeaseOverrun { overrun, ttl } => {
                let _ = write!(
                    out,
                    "{{\"kind\":\"lease_overrun\",\"overrun\":{overrun},\"ttl\":{ttl}}}"
                );
                continue;
            }
        };
        let _ = write!(out, "{{\"kind\":\"{kind}\",\"branch\":");
        write_string(out, branch);
        out.push_str(",\"answer\":");
        json_opt(out, answer.map(String::as_str));
        out.push('}');
    }
    out.push_str("]}");
}
