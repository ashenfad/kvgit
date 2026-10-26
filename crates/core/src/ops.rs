//! Operations by name, for hosts: each takes its arguments as one JSON
//! object and answers with a JSON value, so a host binds them all
//! through one class rather than one per operation. A commit answers
//! with what it committed instead.

use crate::error::Error;
use crate::head::{self, Notes, Resolved};
use crate::json::Value;
use crate::machine::{Io, Machine};
use crate::refs;
use crate::store::dumps_str;
use crate::write::{self, CommitArgs, Committed, Done};

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
        _ => unreachable!("checked against NAMES"),
    })
}
