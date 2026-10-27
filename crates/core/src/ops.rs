//! Operations by name, for hosts: each takes its arguments as one JSON
//! object and answers with a JSON value, so a host binds them all
//! through one class rather than one per operation. A commit answers
//! with what it committed instead.

use crate::ancestry;
use crate::error::Error;
use crate::gc::{self, Gc};
use crate::head::{self, Notes, Resolved};
use crate::history;
use crate::json::Value;
use crate::keyset;
use crate::layout::commit_root;
use crate::machine::{Io, Machine};
use crate::refs::{self, Ref};
use crate::store::{dumps_str, loads_str};
use crate::version;
use crate::worktree;
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
/// - `latest_head {branch, base, recover}`: the commit the branch's head
///   resolves to, or null; one read when it is still `base`.
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
/// - `ancestry_delta {want, have}`: `[commit, parents]` for each commit
///   reachable from `want` and from none of `have`, a list; `want`'s
///   side must be in the store.
/// - `is_ancestor {ancestor, from}`: whether `ancestor` is `from` or in
///   its history; false for a commit the store lacks.
/// - `ancestors_above {have, floor}`: the commits reachable from `have`
///   of generation `floor` and above, sorted; on history without
///   generations, all of them.
/// - `check_version {stamp}`: refuse a store of another storage layout;
///   with `stamp`, stamp a fresh one. Null.
/// - `gc {min_age, deep, wait, lease_ttl}`: the number of commits swept,
///   after the same check, which stamps nothing.
/// - `reset_branch {name, target}`: whether the branch moved; false when
///   `target` isn't in the store.
///
/// On the worktree the operation is given:
///
/// - `wt_lookup {keys}`: `[key, entry]` for each of the keys it holds,
///   each entry `[blob, size, chunks]`.
/// - `wt_contains {key}`: whether it holds the key.
/// - `wt_keys {}`: every key, sorted.
/// - `wt_move {root}`: move it to a root it didn't write; null.
/// - `wt_checkout {commit}`: move it to a commit, reading the commit's
///   root and records in one read; `{root, generation}`, either null
///   when the store holds none.
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
    "ancestry_delta",
    "is_ancestor",
    "ancestors_above",
    "check_version",
    "gc",
    "reset_branch",
    "wt_lookup",
    "wt_contains",
    "wt_keys",
    "wt_move",
    "wt_checkout",
    "latest_head",
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

/// Start a commit. Given a worktree, one that lands moves it.
pub fn commit(args: CommitArgs) -> Op {
    Machine::new(move |io| async move {
        let tree = io.tree();
        let done = write::commit(io, args).await;
        landed(tree, done)
    })
}

/// Move a worktree to what a commit or merge landed.
fn landed(tree: Option<worktree::Shared>, done: Done<Committed>) -> Done<Out> {
    if let (Some(tree), Ok(c)) = (tree, &done.result)
        && c.merged
        && let Some(commit) = &c.commit
        && let Some((_, root)) = c.roots.iter().find(|(r, _)| r == commit)
    {
        tree.lock().unwrap().landed(root.clone(), &c.changes);
    }
    Done {
        result: done.result.map(|c| Out::Committed(Box::new(c))),
        notes: done.notes,
    }
}

/// Start a merge or an apply. Given a worktree, one that lands moves it.
pub fn merge(args: MergeArgs) -> Op {
    Machine::new(move |io| async move {
        let tree = io.tree();
        let done = write::merge(io, args).await;
        landed(tree, done)
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

fn seconds(args: &Value, name: &str) -> Result<f64, Error> {
    match arg(args, name)? {
        Value::Int(n) => Ok(*n as f64),
        Value::Float(x) => Ok(*x),
        _ => Err(Error::Invalid(format!("{name:?} isn't a number"))),
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

fn texts(args: &Value, name: &str) -> Result<Vec<String>, Error> {
    let invalid = || Error::Invalid(format!("{name:?} isn't a list of strings"));
    match arg(args, name)? {
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_str().map(str::to_string).ok_or_else(invalid))
            .collect(),
        _ => Err(invalid()),
    }
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
        "reset_branch" => {
            Value::Bool(refs::reset_branch(io, text(args, "name")?, text(args, "target")?).await?)
        }
        "wt_lookup" | "wt_contains" | "wt_keys" | "wt_move" | "wt_checkout" => {
            let tree = io
                .tree()
                .ok_or_else(|| Error::Invalid(format!("{name} needs a worktree")))?;
            let shape = keyset::shape();
            match name {
                "wt_lookup" => {
                    let keys = match arg(args, "keys")? {
                        Value::Array(keys) => keys
                            .iter()
                            .map(|k| {
                                k.as_str()
                                    .map(String::from)
                                    .ok_or_else(|| Error::Invalid("a key isn't a string".into()))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        _ => return Err(Error::Invalid("\"keys\" isn't a list".into())),
                    };
                    let found = worktree::lookup(io, shape, &tree, keys).await?;
                    Value::Array(
                        found
                            .into_iter()
                            .map(|(k, e)| {
                                let chunks = strings(e.chunks.clone());
                                Value::Array(vec![
                                    Value::Str(k),
                                    Value::Array(vec![Value::Str(e.blob), e.size, chunks]),
                                ])
                            })
                            .collect(),
                    )
                }
                "wt_contains" => {
                    Value::Bool(worktree::contains(io, shape, &tree, text(args, "key")?).await?)
                }
                "wt_keys" => strings(worktree::keys(io, shape, &tree).await?),
                "wt_checkout" => {
                    let found = worktree::checkout(io, shape, &tree, text(args, "commit")?).await?;
                    Value::Object(vec![
                        ("root".into(), found.root.map_or(Value::Null, Value::Str)),
                        (
                            "generation".into(),
                            found
                                .generation
                                .map_or(Value::Null, |g| Value::Int(g as i64)),
                        ),
                    ])
                }
                _ => {
                    let root = text(args, "root")?.to_string();
                    worktree::move_to(io, shape, &tree, root).await?;
                    Value::Null
                }
            }
        }
        "latest_head" => opt_str(
            head::latest_head(
                io,
                text(args, "branch")?,
                text(args, "base")?,
                flag(args, "recover")?,
                vec![],
                notes,
            )
            .await?
            .head,
        ),
        "ancestry_delta" => Value::Array(
            ancestry::delta(io, text(args, "want")?, &texts(args, "have")?)
                .await?
                .into_iter()
                .map(|(commit, parents)| Value::Array(vec![Value::Str(commit), strings(parents)]))
                .collect(),
        ),
        "is_ancestor" => Value::Bool(
            ancestry::is_ancestor(io, text(args, "ancestor")?, text(args, "from")?).await?,
        ),
        "ancestors_above" => {
            let floor = number(args, "floor")?.unwrap_or(0);
            let mut found: Vec<String> = ancestry::above(io, &texts(args, "have")?, floor)
                .await?
                .into_iter()
                .collect();
            found.sort();
            strings(found)
        }
        "check_version" => {
            version::check(io, flag(args, "stamp")?).await?;
            Value::Null
        }
        "gc" => {
            // Before the lease, not after: a store this code must not
            // touch comes out of the call untouched, where taking the
            // lease and then failing would leave a record behind.
            version::check(io, false).await?;
            let sweep = Gc {
                min_age: seconds(args, "min_age")?,
                deep: flag(args, "deep")?,
                wait: flag(args, "wait")?,
                lease_ttl: seconds(args, "lease_ttl")?,
            };
            Value::Int(gc::gc(io, keyset::shape(), &sweep, notes).await? as i64)
        }
        _ => unreachable!("checked against NAMES"),
    })
}
