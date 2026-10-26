//! Operations by name, for hosts: each takes its arguments as one JSON
//! object and answers with a JSON value, so a host binds them all
//! through one class rather than one per operation. A commit answers
//! with what it committed instead.

use crate::error::Error;
use crate::head::{self, Notes, Resolved};
use crate::history;
use crate::json::Value;
use crate::keyset;
use crate::layout::commit_root;
use crate::machine::{Io, Machine};
use crate::refs::{self, Ref};
use crate::store::{dumps_str, loads_str};
use crate::write::{self, CommitArgs, Committed, Done, MergeArgs};
use std::collections::HashMap;

/// What an operation answers with.
#[derive(Debug)]
pub enum Out {
    Value(Value),
    Committed(Box<Committed>),
}

/// An operation in progress.
pub type Op = Machine<Done<Out>>;

/// The operations [`call`] knows, each with the arguments it takes:
///
/// - `resolve_head {branch, recover}`: `{commit, root}`, or null.
/// - `head_of {branch, recover}`: `{commit, root}` of a branch that
///   must exist and resolve.
/// - `repair_head {branch, recover}`: the commit the head now names, or
///   null.
/// - `heal_head {branch, commit}`: whether a damaged head was replaced.
/// - `recover_by_commit_scan {branch}`: the guess, or null.
/// - `branches {}`: the names, sorted.
/// - `branch_exists {name}`, `create_branch {name, target}`,
///   `delete_branch {name}`.
/// - `resolve_tag {name}`: the commit, or null.
/// - `tags {}`: `[name, commit]` pairs, sorted by name.
/// - `tag_info {name}`: `{name, commit, time, info, dangling}`, or null.
/// - `create_tag {name, target, info}`, `delete_tag {name}`.
/// - `resolve_ref {commit | branch | tag, recover}`: `{commit, root}` of
///   a commit that must be in the store, with a root that is a string.
/// - `get_commit {commit}`: `{hash, parents, time, info, root}`.
/// - `log {start, limit, first_parent}`: emits lists of records, newest
///   first; answers null.
/// - `diff {a, b}`: `{added, removed, modified}`, each a sorted list of
///   keys, between two commits in the store.
/// - `merge_base {a, b, check}`: `{base, generations}`, the base or
///   null and `[commit, generation]` pairs worked out on the way; with
///   `check`, both commits must be in the store.
pub const NAMES: &[&str] = &[
    "resolve_head",
    "head_of",
    "repair_head",
    "heal_head",
    "recover_by_commit_scan",
    "branches",
    "branch_exists",
    "create_branch",
    "delete_branch",
    "resolve_tag",
    "tags",
    "tag_info",
    "create_tag",
    "delete_tag",
    "resolve_ref",
    "get_commit",
    "log",
    "diff",
    "merge_base",
];

/// Start operation `name` on `args`, an object.
pub fn call(name: &str, args: Value) -> Result<Op, Error> {
    if !NAMES.contains(&name) {
        return Err(Error::Invalid(format!("no operation is named {name:?}")));
    }
    let name = name.to_string();
    Ok(Machine::new(move |io| async move {
        let mut notes = Notes::new();
        let result = run(&io, &name, &args, &mut notes).await.map(Out::Value);
        Done { result, notes }
    }))
}

/// Start a commit.
pub fn commit(args: CommitArgs) -> Op {
    Machine::new(move |io| async move {
        let done = write::commit(io, args).await;
        Done {
            result: done.result.map(|c| Out::Committed(Box::new(c))),
            notes: done.notes,
        }
    })
}

/// Start a merge or an apply.
pub fn merge(args: MergeArgs) -> Op {
    Machine::new(move |io| async move {
        let done = write::merge(io, args).await;
        Done {
            result: done.result.map(|c| Out::Committed(Box::new(c))),
            notes: done.notes,
        }
    })
}

fn arg<'a>(args: &'a Value, name: &str) -> Result<&'a Value, Error> {
    args.get(name)
        .ok_or_else(|| Error::Invalid(format!("the arguments lack {name:?}")))
}

fn text<'a>(args: &'a Value, name: &str) -> Result<&'a str, Error> {
    arg(args, name)?
        .as_str()
        .ok_or_else(|| Error::Invalid(format!("{name:?} isn't a string")))
}

fn flag(args: &Value, name: &str) -> Result<bool, Error> {
    match arg(args, name)? {
        Value::Bool(b) => Ok(*b),
        _ => Err(Error::Invalid(format!("{name:?} isn't true or false"))),
    }
}

fn number(args: &Value, name: &str) -> Result<Option<u64>, Error> {
    match arg(args, name)? {
        Value::Null => Ok(None),
        Value::Int(n) if *n >= 0 => Ok(Some(*n as u64)),
        _ => Err(Error::Invalid(format!("{name:?} isn't a count"))),
    }
}

/// The keyset root of a commit that must be in the store.
async fn root_of(io: &Io, commit: &str) -> Result<String, Error> {
    match io.get_one(&commit_root(commit)).await? {
        Some(raw) => Ok(loads_str(&raw).unwrap_or_else(|| crate::layout::EMPTY_HASH.clone())),
        None => Err(Error::UnknownCommit {
            commit: commit.to_string(),
        }),
    }
}

fn opt_str(s: Option<String>) -> Value {
    s.map_or(Value::Null, Value::Str)
}

fn resolved(r: Resolved) -> Value {
    Value::Object(vec![
        ("commit".into(), Value::Str(r.commit)),
        ("root".into(), opt_str(r.root)),
    ])
}

fn strings(items: Vec<String>) -> Value {
    Value::Array(items.into_iter().map(Value::Str).collect())
}

async fn run(io: &Io, name: &str, args: &Value, notes: &mut Notes) -> Result<Value, Error> {
    Ok(match name {
        "resolve_head" => {
            let found = head::resolve(
                io,
                text(args, "branch")?,
                flag(args, "recover")?,
                None,
                notes,
            )
            .await?;
            found.map_or(Value::Null, resolved)
        }
        "head_of" => {
            resolved(head::head_of(io, text(args, "branch")?, flag(args, "recover")?, notes).await?)
        }
        "repair_head" => opt_str(
            head::repair_head(io, text(args, "branch")?, flag(args, "recover")?, notes).await?,
        ),
        "heal_head" => {
            let recovered = dumps_str(text(args, "commit")?);
            Value::Bool(head::heal_head(io, text(args, "branch")?, &recovered, notes).await?)
        }
        "recover_by_commit_scan" => {
            opt_str(head::recover_by_commit_scan(io, text(args, "branch")?).await?)
        }
        "branches" => strings(refs::branches(io).await?),
        "branch_exists" => Value::Bool(refs::branch_exists(io, text(args, "name")?).await?),
        "create_branch" => {
            refs::create_branch(io, text(args, "name")?, text(args, "target")?).await?;
            Value::Null
        }
        "delete_branch" => {
            refs::delete_branch(io, text(args, "name")?).await?;
            Value::Null
        }
        "resolve_tag" => opt_str(refs::resolve_tag(io, text(args, "name")?).await?),
        "tags" => Value::Array(
            refs::tags(io)
                .await?
                .into_iter()
                .map(|(name, commit)| Value::Array(vec![Value::Str(name), Value::Str(commit)]))
                .collect(),
        ),
        "tag_info" => match refs::tag_info(io, text(args, "name")?).await? {
            None => Value::Null,
            Some(t) => Value::Object(vec![
                ("name".into(), Value::Str(t.name)),
                ("commit".into(), Value::Str(t.commit)),
                ("time".into(), t.time.map_or(Value::Null, Value::Float)),
                ("info".into(), t.info.unwrap_or(Value::Null)),
                ("dangling".into(), Value::Bool(t.dangling)),
            ]),
        },
        "create_tag" => {
            let info = match arg(args, "info")? {
                Value::Null => None,
                info @ Value::Object(_) => Some(info.clone()),
                _ => return Err(Error::Invalid("a tag's info must be an object".into())),
            };
            refs::create_tag(io, text(args, "name")?, text(args, "target")?, info).await?;
            Value::Null
        }
        "delete_tag" => {
            refs::delete_tag(io, text(args, "name")?).await?;
            Value::Null
        }
        "resolve_ref" => {
            let given: Vec<Ref> = [
                ("commit", Ref::Commit as fn(String) -> Ref),
                ("branch", Ref::Branch),
                ("tag", Ref::Tag),
            ]
            .into_iter()
            .filter_map(|(name, make)| args.get(name)?.as_str().map(|s| make(s.to_string())))
            .collect();
            let [r] = given.as_slice() else {
                return Err(Error::Invalid(
                    "name exactly one of commit, branch or tag".into(),
                ));
            };
            let (commit, root) = refs::resolve_ref(io, r, flag(args, "recover")?, notes).await?;
            Value::Object(vec![
                ("commit".into(), Value::Str(commit)),
                ("root".into(), Value::Str(root)),
            ])
        }
        "get_commit" => history::get_commit(io, text(args, "commit")?)
            .await?
            .to_value(),
        "log" => {
            let (start, limit) = (text(args, "start")?, number(args, "limit")?);
            history::log(io, start, limit, flag(args, "first_parent")?).await?;
            Value::Null
        }
        "diff" => {
            let (a, b) = (
                root_of(io, text(args, "a")?).await?,
                root_of(io, text(args, "b")?).await?,
            );
            let mut added = Vec::new();
            let mut removed = Vec::new();
            let mut modified = Vec::new();
            for (key, change) in history::changes(io, keyset::shape(), a, b).await? {
                match (change.old, change.new) {
                    (None, _) => added.push(key),
                    (_, None) => removed.push(key),
                    _ => modified.push(key),
                }
            }
            Value::Object(vec![
                ("added".into(), strings(added)),
                ("removed".into(), strings(removed)),
                ("modified".into(), strings(modified)),
            ])
        }
        "merge_base" => {
            let (a, b) = (text(args, "a")?, text(args, "b")?);
            if flag(args, "check")? {
                root_of(io, a).await?;
                root_of(io, b).await?;
            }
            let mut known = HashMap::new();
            let base = history::merge_base(io, a, b, &mut known).await?;
            let mut generations: Vec<(String, u64)> = known.into_iter().collect();
            generations.sort();
            Value::Object(vec![
                ("base".into(), opt_str(base)),
                (
                    "generations".into(),
                    Value::Array(
                        generations
                            .into_iter()
                            .map(|(c, g)| Value::Array(vec![Value::Str(c), Value::Int(g as i64)]))
                            .collect(),
                    ),
                ),
            ])
        }
        _ => unreachable!("checked against NAMES"),
    })
}
