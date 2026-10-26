//! The corpus's commit scenarios, run by the core: every commit goes
//! through the core's commit operation over one in-memory store, two
//! machines interleaved where a scenario holds a commit at its batch or
//! its publish, and the store must end byte for byte as Python left it.
//!
//! Until the rest of the engine is in the core, this runner keeps the
//! worktree state and the reads a scenario checks by itself, and runs
//! only the scenarios made of the steps it knows. Branches, tags and
//! heads go through the core's operations by name, as the hosts call
//! them.

use kvgit_core::error::Error;
use kvgit_core::json::{Value, loads};
use kvgit_core::keyset::{Entry, shape};
use kvgit_core::layout::{
    ROOT_COMMIT, STORAGE_VERSION_KEY, blob_key, branch_head, commit_root, commit_time, info_key,
    parent_commit,
};
use kvgit_core::machine::{Cas, Machine, Reply, Request, Step, Write};
use kvgit_core::merge::{MergeAnswer, MergeAsk, Rule, Rules};
use kvgit_core::ops::{self, Out};
use kvgit_core::text::{TextRule, text_merge};
use kvgit_core::tree::walk;
use kvgit_core::write::{CommitArgs, Committed, Done, OnConflict, OnMoved, commit};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

/// The steps this runner knows.
const KNOWN: &[&str] = &[
    "open",
    "worktree",
    "set",
    "delete",
    "get",
    "keys",
    "head",
    "discard",
    "refresh",
    "commit",
    "clock",
    "branches",
    "get_commit",
    "log",
    "snapshot",
    "text_merge",
    "start",
    "resume",
    "store_set",
    "store_delete",
    "branch_create",
    "branch_delete",
    "tag_create",
    "tag_delete",
    "tags",
    "repair_head",
];

/// The scenarios it must run: a scenario that gains a step it doesn't
/// know fails here rather than quietly dropping out.
const EXPECTED: &[&str] = &[
    "basics/first-commit",
    "basics/lost-race",
    "history/commit-scan-recovery",
    "history/head-recovery",
    "history/refs",
    "merges/text",
    "races/lost-before-batch",
];

fn scenarios(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scenarios(&path, out);
        } else if path.extension().is_some_and(|e| e == "json") {
            out.push(path);
        }
    }
}

fn steps_of(scenario: &Value) -> Vec<Value> {
    let Some(Value::Array(steps)) = scenario.get("steps") else {
        panic!("steps")
    };
    steps.clone()
}

fn op(step: &Value) -> &str {
    step.get("op").and_then(Value::as_str).expect("op")
}

fn runs_here(scenario: &Value) -> bool {
    if let Some(Value::Array(only)) = scenario.get("only")
        && !only.iter().any(|o| o.as_str() == Some("python"))
    {
        return false;
    }
    steps_of(scenario).iter().all(|s| {
        KNOWN.contains(&op(s)) && s.get("step").is_none_or(|inner| KNOWN.contains(&op(inner)))
    })
}

#[test]
fn the_commit_scenarios_end_where_python_left_the_store() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../corpus/scenarios");
    let mut paths = Vec::new();
    scenarios(&root, &mut paths);
    paths.sort();
    let mut ran = Vec::new();
    for path in paths {
        let scenario = loads(&fs::read(&path).unwrap()).unwrap();
        if !runs_here(&scenario) {
            continue;
        }
        let name = path
            .strip_prefix(&root)
            .unwrap()
            .with_extension("")
            .to_string_lossy()
            .into_owned();
        Run::new(&scenario).play(&name, &scenario);
        ran.push(name);
    }
    assert_eq!(ran, EXPECTED);
}

/// A value as a scenario writes bytes.
fn bytes_of(value: &Value) -> Vec<u8> {
    match value {
        Value::Str(s) => s.as_bytes().to_vec(),
        Value::Object(_) => {
            let b64 = value.get("b64").and_then(Value::as_str).expect("b64");
            kvgit_core::base64::decode(b64).expect("base64")
        }
        other => panic!("bytes: {other:?}"),
    }
}

/// Bytes as a scenario writes them.
fn bytes_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(s) => Value::Str(s.into()),
        Err(_) => Value::Object(vec![(
            "b64".into(),
            Value::Str(kvgit_core::base64::encode(bytes)),
        )]),
    }
}

fn strings(items: &[String]) -> Value {
    Value::Array(items.iter().map(|s| Value::Str(s.clone())).collect())
}

/// A worktree: its branch, the commit it's at, its keyset and its
/// pending changes.
#[derive(Clone)]
struct Wt {
    branch: String,
    /// Whether its repo has a recoverer.
    recover: bool,
    base: String,
    keys: BTreeMap<String, Entry>,
    pending: BTreeMap<String, Option<Vec<u8>>>,
    stamped: bool,
}

/// A commit held at its pause point.
struct Job {
    machine: Machine<Done<Committed>>,
    held: Request,
    wt: String,
    values: HashMap<String, Vec<u8>>,
    recover: bool,
}

struct Run {
    /// Each repo, and whether it recovers heads by a commit scan.
    repos: HashMap<String, bool>,
    store: BTreeMap<String, Vec<u8>>,
    clock: f64,
    vars: HashMap<String, Value>,
    wts: HashMap<String, Wt>,
    snapshots: HashMap<String, BTreeMap<String, Entry>>,
    jobs: HashMap<String, Job>,
}

impl Run {
    fn new(scenario: &Value) -> Run {
        let mut store = BTreeMap::new();
        if let Some(Value::Object(pairs)) = scenario.get("store") {
            for (k, v) in pairs {
                store.insert(k.clone(), bytes_of(v));
            }
        }
        let clock = match scenario.get("clock") {
            Some(Value::Float(x)) => *x,
            Some(Value::Int(n)) => *n as f64,
            other => panic!("clock: {other:?}"),
        };
        Run {
            repos: HashMap::new(),
            store,
            clock,
            vars: HashMap::from([("$root".into(), Value::Str(ROOT_COMMIT.into()))]),
            wts: HashMap::new(),
            snapshots: HashMap::new(),
            jobs: HashMap::new(),
        }
    }

    fn play(&mut self, name: &str, scenario: &Value) {
        for (i, step) in steps_of(scenario).iter().enumerate() {
            let context = format!("{name} step {i} ({})", op(step));
            let found = self.step(step, &context);
            if let Some(expect) = step.get("expect") {
                self.expect(expect, &found, &context);
            } else if let Err(e) = &found {
                panic!("{context}: failed with {e:?}");
            }
        }
        assert!(self.jobs.is_empty(), "{name}: a job was never resumed");
        let Some(Value::Object(fin)) = scenario.get("final") else {
            panic!("{name}: no final store")
        };
        let expected: BTreeMap<String, Vec<u8>> =
            fin.iter().map(|(k, v)| (k.clone(), bytes_of(v))).collect();
        for (key, bytes) in &expected {
            assert_eq!(
                self.store
                    .get(key)
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                Some(String::from_utf8_lossy(bytes).into_owned()),
                "{name}: {key}"
            );
        }
        let extra: Vec<&String> = self
            .store
            .keys()
            .filter(|k| !expected.contains_key(*k))
            .collect();
        assert!(
            extra.is_empty(),
            "{name}: keys Python didn't write: {extra:?}"
        );
    }

    fn arg(&self, step: &Value, field: &str) -> String {
        let raw = step
            .get(field)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{field}"));
        match self.vars.get(raw) {
            Some(Value::Str(bound)) => bound.clone(),
            _ if raw.starts_with('$') => panic!("{raw} is unbound"),
            _ => raw.to_string(),
        }
    }

    fn expect(&mut self, expect: &Value, found: &Result<Value, String>, context: &str) {
        if let Some(Value::Str(error)) = expect.get("error") {
            assert_eq!(found.as_ref().err(), Some(error), "{context}");
            return;
        }
        let found = found
            .as_ref()
            .unwrap_or_else(|e| panic!("{context}: failed with {e}"));
        assert!(
            self.matches(expect, found),
            "{context}: expected {expect:?}, found {found:?}"
        );
    }

    /// Whether `found` is what `expected` describes, binding variables.
    /// An object expectation checks only the fields it gives.
    fn matches(&mut self, expected: &Value, found: &Value) -> bool {
        match (expected, found) {
            (Value::Str(var), _) if var.starts_with('$') => match self.vars.get(var) {
                Some(bound) => bound == found,
                None => {
                    self.vars.insert(var.clone(), found.clone());
                    true
                }
            },
            (Value::Object(fields), Value::Object(_)) if expected.get("b64").is_none() => fields
                .iter()
                .all(|(k, v)| found.get(k).is_some_and(|f| self.matches(v, &f.clone()))),
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| self.matches(x, y))
            }
            (Value::Int(a), Value::Float(b)) => *a as f64 == *b,
            _ => expected == found,
        }
    }

    fn step(&mut self, step: &Value, context: &str) -> Result<Value, String> {
        match op(step) {
            "open" => {
                if !self.store.contains_key(STORAGE_VERSION_KEY) {
                    self.store.insert(STORAGE_VERSION_KEY.into(), b"4".to_vec());
                }
                let scan = step.get("recover").and_then(Value::as_str) == Some("commit_scan");
                self.repos.insert(self.arg(step, "as"), scan);
                Ok(Value::Null)
            }
            "worktree" => {
                let branch = self.arg(step, "branch");
                let recover = self.repos[&self.arg(step, "repo")];
                let head = match self.head_of(&branch, recover) {
                    Err(e)
                        if e == "UnknownBranchError"
                            && step.get("create") == Some(&Value::Bool(true)) =>
                    {
                        match self.call(
                            "create_branch",
                            args(&[("name", &branch), ("target", ROOT_COMMIT)]),
                            false,
                        ) {
                            Ok(_) => {}
                            Err(e) if e == "BranchExistsError" => {}
                            Err(e) => return Err(e),
                        }
                        self.head_of(&branch, recover)?
                    }
                    other => other?,
                };
                let wt = Wt {
                    branch,
                    recover,
                    keys: self.keyset(&head),
                    base: head,
                    pending: BTreeMap::new(),
                    stamped: false,
                };
                self.wts.insert(self.arg(step, "as"), wt);
                Ok(Value::Null)
            }
            "snapshot" => {
                let commit = match step.get("branch") {
                    Some(_) => self.head_of(&self.arg(step, "branch"), false)?,
                    None => self.arg(step, "commit"),
                };
                let keys = self.keyset(&commit);
                self.snapshots.insert(self.arg(step, "as"), keys);
                Ok(Value::Null)
            }
            "set" => {
                let value = bytes_of(step.get("value").expect("value"));
                let (name, key) = (self.arg(step, "wt"), self.arg(step, "key"));
                self.wts
                    .get_mut(&name)
                    .unwrap()
                    .pending
                    .insert(key, Some(value));
                Ok(Value::Null)
            }
            "delete" => {
                let (name, key) = (self.arg(step, "wt"), self.arg(step, "key"));
                let wt = self.wts.get_mut(&name).unwrap();
                if wt.keys.contains_key(&key) {
                    wt.pending.insert(key, None);
                } else {
                    wt.pending.remove(&key);
                }
                Ok(Value::Null)
            }
            "discard" => {
                let name = self.arg(step, "wt");
                self.wts.get_mut(&name).unwrap().pending.clear();
                Ok(Value::Null)
            }
            "refresh" => {
                let name = self.arg(step, "wt");
                let (branch, recover) = (self.wts[&name].branch.clone(), self.wts[&name].recover);
                let head = self.head_of(&branch, recover)?;
                let keys = self.keyset(&head);
                let wt = self.wts.get_mut(&name).unwrap();
                (wt.base, wt.keys) = (head, keys);
                wt.pending.clear();
                Ok(Value::Null)
            }
            "get" => {
                let from = self.arg(step, "from");
                let key = self.arg(step, "key");
                let (keys, pending) = match self.wts.get(&from) {
                    Some(wt) => (&wt.keys, Some(&wt.pending)),
                    None => (&self.snapshots[&from], None),
                };
                let value = match pending.and_then(|p| p.get(&key)) {
                    Some(pending) => pending.clone(),
                    None => keys.get(&key).map(|e| self.store[&e.blob].clone()),
                };
                Ok(value.map_or(Value::Null, |v| bytes_value(&v)))
            }
            "keys" => {
                let from = self.arg(step, "from");
                let mut keys: BTreeMap<String, bool> = match self.wts.get(&from) {
                    Some(wt) => {
                        let mut keys: BTreeMap<String, bool> =
                            wt.keys.keys().map(|k| (k.clone(), true)).collect();
                        for (k, v) in &wt.pending {
                            keys.insert(k.clone(), v.is_some());
                        }
                        keys
                    }
                    None => self.snapshots[&from]
                        .keys()
                        .map(|k| (k.clone(), true))
                        .collect(),
                };
                keys.retain(|_, present| *present);
                Ok(strings(&keys.into_keys().collect::<Vec<_>>()))
            }
            "head" => Ok(Value::Str(self.wts[&self.arg(step, "wt")].base.clone())),
            "clock" => {
                let number = |v: &Value| match v {
                    Value::Float(x) => *x,
                    Value::Int(n) => *n as f64,
                    other => panic!("a time: {other:?}"),
                };
                if let Some(at) = step.get("set") {
                    self.clock = number(at);
                }
                if let Some(by) = step.get("advance") {
                    self.clock += number(by);
                }
                Ok(Value::Null)
            }
            "branches" => {
                let recover = self.repos[&self.arg(step, "repo")];
                let Value::Array(names) = self.call("branches", args(&[]), false)? else {
                    panic!("branches")
                };
                let mut out = Vec::new();
                for name in names {
                    let name = name.as_str().unwrap().to_string();
                    let head = self.head_of(&name, recover)?;
                    out.push((name, Value::Str(head)));
                }
                Ok(Value::Object(out))
            }
            "branch_create" => {
                let name = self.arg(step, "name");
                let target = match step.get("at") {
                    Some(_) => self.arg(step, "at"),
                    None => ROOT_COMMIT.to_string(),
                };
                self.call(
                    "create_branch",
                    args(&[("name", &name), ("target", &target)]),
                    false,
                )?;
                Ok(Value::Str(target))
            }
            "branch_delete" => {
                let name = self.arg(step, "name");
                self.call("delete_branch", args(&[("name", &name)]), false)
            }
            "tag_create" => {
                let mut fields = vec![
                    ("name".to_string(), Value::Str(self.arg(step, "name"))),
                    ("target".to_string(), Value::Str(self.arg(step, "commit"))),
                ];
                fields.push((
                    "info".into(),
                    step.get("info").cloned().unwrap_or(Value::Null),
                ));
                self.call("create_tag", Value::Object(fields), false)
            }
            "tag_delete" => {
                let name = self.arg(step, "name");
                self.call("delete_tag", args(&[("name", &name)]), false)
            }
            "tags" => {
                let Value::Array(pairs) = self.call("tags", args(&[]), false)? else {
                    panic!("tags")
                };
                Ok(Value::Object(
                    pairs
                        .into_iter()
                        .map(|pair| match pair {
                            Value::Array(two) => {
                                (two[0].as_str().unwrap().to_string(), two[1].clone())
                            }
                            other => panic!("a tag: {other:?}"),
                        })
                        .collect(),
                ))
            }
            "repair_head" => {
                let recover = self.repos[&self.arg(step, "repo")];
                let name = self.arg(step, "name");
                let mut fields = vec![("branch".to_string(), Value::Str(name))];
                fields.push(("recover".into(), Value::Bool(recover)));
                self.call("repair_head", Value::Object(fields), recover)
            }
            "get_commit" => {
                let c = self.arg(step, "commit");
                let read = |key: String| self.store.get(&key).map(|raw| loads(raw).unwrap());
                let mut fields = vec![
                    (
                        "parents".into(),
                        read(parent_commit(&c)).ok_or("UnknownCommitError")?,
                    ),
                    ("root".into(), read(commit_root(&c)).unwrap_or(Value::Null)),
                    ("time".into(), read(commit_time(&c)).unwrap_or(Value::Null)),
                ];
                fields.push(("info".into(), read(info_key(&c)).unwrap_or(Value::Null)));
                Ok(Value::Object(fields))
            }
            "log" => {
                let start = match step.get("branch") {
                    Some(_) => self.head_of(&self.arg(step, "branch"), false)?,
                    None => self.arg(step, "commit"),
                };
                let mut out = Vec::new();
                let mut queue = std::collections::VecDeque::from([start]);
                while let Some(c) = queue.pop_front() {
                    if out.contains(&c) {
                        continue;
                    }
                    let parents = match self
                        .store
                        .get(&parent_commit(&c))
                        .map(|r| loads(r).unwrap())
                    {
                        Some(Value::Array(ps)) => ps,
                        _ => vec![],
                    };
                    queue.extend(parents.iter().filter_map(|p| p.as_str().map(String::from)));
                    out.push(c);
                }
                Ok(strings(&out))
            }
            "text_merge" => {
                let side = |name: &str| match step.get(name) {
                    None | Some(Value::Null) => None,
                    Some(v) => Some(bytes_of(v)),
                };
                let label = |name: &str, default: &str| {
                    step.get(name)
                        .and_then(Value::as_str)
                        .unwrap_or(default)
                        .to_string()
                };
                let rule = TextRule {
                    ours_label: label("ours_label", "ours"),
                    theirs_label: label("theirs_label", "theirs"),
                    strict: step.get("strict") == Some(&Value::Bool(true)),
                };
                let (old, ours, theirs) = (side("old"), side("ours"), side("theirs"));
                let (old, ours, theirs) = (old.as_deref(), ours.as_deref(), theirs.as_deref());
                if rule.strict {
                    return match rule.merge(old, ours, theirs) {
                        Ok(merged) => {
                            Ok(Value::Object(vec![("merged".into(), bytes_value(&merged))]))
                        }
                        Err(_) => Err("CantMark".into()),
                    };
                }
                match text_merge(old, ours, theirs, &rule.ours_label, &rule.theirs_label) {
                    Ok((merged, conflicted)) => Ok(Value::Object(vec![
                        ("merged".into(), bytes_value(&merged)),
                        ("conflicted".into(), Value::Bool(conflicted)),
                    ])),
                    Err(_) => Err("CantMark".into()),
                }
            }
            "store_set" => {
                let key = self.arg(step, "key");
                self.store.insert(key, bytes_of(step.get("value").unwrap()));
                Ok(Value::Null)
            }
            "store_delete" => {
                self.store.remove(&self.arg(step, "key"));
                Ok(Value::Null)
            }
            "commit" => {
                let name = self.arg(step, "wt");
                let (machine, values) = self.start_commit(step, &name);
                let recover = self.wts[&name].recover;
                let done = self.drive(machine, &values, recover);
                self.finish(&name, done)
            }
            "start" => {
                let inner = step.get("step").unwrap().clone();
                assert_eq!(op(&inner), "commit", "{context}: only commits are held");
                let name = self.arg(&inner, "wt");
                let pause = step
                    .get("pause")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string();
                let (machine, values) = self.start_commit(&inner, &name);
                let branch = self.wts[&name].branch.clone();
                let recover = self.wts[&name].recover;
                let held = Box::new(move |r: &Request| holds(r, &pause, &branch));
                let mut machine = machine;
                let mut step_now = machine.resume_at(self.clock_now(), None);
                loop {
                    match step_now {
                        Step::Done(_) => panic!("{context}: the job never reached its pause"),
                        Step::Request(r) if held(&r) => {
                            let job = Job {
                                machine,
                                held: r,
                                wt: name,
                                values,
                                recover,
                            };
                            self.jobs.insert(self.arg(step, "job"), job);
                            return Ok(Value::Null);
                        }
                        Step::Request(r) => {
                            let reply = self.perform(r, &values, recover);
                            step_now = machine.resume_at(self.clock_now(), Some(reply));
                        }
                    }
                }
            }
            "resume" => {
                let job = self
                    .jobs
                    .remove(&self.arg(step, "job"))
                    .expect("a held job");
                let Job {
                    mut machine,
                    held,
                    wt,
                    values,
                    recover,
                } = job;
                let reply = self.perform(held, &values, recover);
                let first = machine.resume_at(self.clock_now(), Some(reply));
                let done = self.drive_from(machine, first, &values, recover);
                self.finish(&wt, done)
            }
            other => panic!("{context}: {other} is not a known step"),
        }
    }

    fn clock_now(&self) -> f64 {
        self.clock
    }

    /// A branch's head, resolved as a repo resolves it.
    fn head_of(&mut self, branch: &str, recover: bool) -> Result<String, String> {
        let mut fields = vec![("branch".to_string(), Value::Str(branch.into()))];
        fields.push(("recover".into(), Value::Bool(recover)));
        let found = self.call("head_of", Value::Object(fields), recover)?;
        Ok(found.get("commit").unwrap().as_str().unwrap().to_string())
    }

    /// Run the core's operation `name` against the store.
    fn call(&mut self, name: &str, args: Value, recover: bool) -> Result<Value, String> {
        let mut machine = ops::call(name, args).map_err(|e| error_name(&e))?;
        let mut step = machine.resume_at(self.clock_now(), None);
        loop {
            match step {
                Step::Done(done) => {
                    return match done.result {
                        Ok(Out::Value(v)) => Ok(v),
                        Ok(Out::Committed(_)) => panic!("{name} committed"),
                        Err(e) => Err(error_name(&e)),
                    };
                }
                Step::Request(r) => {
                    let reply = self.perform(r, &HashMap::new(), recover);
                    step = machine.resume_at(self.clock_now(), Some(reply));
                }
            }
        }
    }

    /// A commit's keyset, read through the core's walk.
    fn keyset(&self, commit: &str) -> BTreeMap<String, Entry> {
        let Some(raw) = self.store.get(&commit_root(commit)) else {
            return BTreeMap::new();
        };
        let root = loads(raw).unwrap().as_str().unwrap().to_string();
        let store = &self.store;
        let (items, _) = Machine::new(|io| walk(io, shape(), root, |_| false))
            .run(|keys| {
                keys.iter()
                    .filter_map(|k| store.get(k).map(|v| (k.clone(), v.clone())))
                    .collect()
            })
            .unwrap();
        items
            .into_iter()
            .map(|(k, raw)| (k, Entry::decode(&raw).unwrap()))
            .collect()
    }

    fn start_commit(
        &self,
        step: &Value,
        name: &str,
    ) -> (Machine<Done<Committed>>, HashMap<String, Vec<u8>>) {
        let wt = &self.wts[name];
        let mut values = HashMap::new();
        let mut updates = Vec::new();
        let mut removals = Vec::new();
        for (key, pending) in &wt.pending {
            match pending {
                Some(value) => {
                    let blob = blob_key(value);
                    values.insert(blob.clone(), value.clone());
                    updates.push((key.clone(), Entry::new(blob, value.len() as u64, vec![])));
                }
                None => removals.push(key.clone()),
            }
        }
        let on_moved = match step.get("on_moved").and_then(Value::as_str) {
            Some("raise") => OnMoved::Raise,
            _ => OnMoved::Merge,
        };
        let on_conflict = match step.get("on_conflict").and_then(Value::as_str) {
            Some("abandon") => OnConflict::Abandon,
            _ => OnConflict::Raise,
        };
        let args = CommitArgs {
            branch: wt.branch.clone(),
            base: wt.base.clone(),
            current: wt.base.clone(),
            shape: shape(),
            updates,
            removals,
            chunks: vec![],
            info: step.get("info").cloned(),
            on_moved,
            on_conflict,
            rules: rules(step),
            recover: wt.recover,
            stamped: wt.stamped,
            roots: vec![],
            generations: vec![],
        };
        (Machine::new(move |io| commit(io, args)), values)
    }

    fn drive(
        &mut self,
        mut machine: Machine<Done<Committed>>,
        values: &HashMap<String, Vec<u8>>,
        recover: bool,
    ) -> Done<Committed> {
        let first = machine.resume_at(self.clock_now(), None);
        self.drive_from(machine, first, values, recover)
    }

    fn drive_from(
        &mut self,
        mut machine: Machine<Done<Committed>>,
        mut step: Step<Done<Committed>>,
        values: &HashMap<String, Vec<u8>>,
        recover: bool,
    ) -> Done<Committed> {
        loop {
            match step {
                Step::Done(done) => return done,
                Step::Request(r) => {
                    let reply = self.perform(r, values, recover);
                    step = machine.resume_at(self.clock_now(), Some(reply));
                }
            }
        }
    }

    /// Perform a request against the store. `values` are the bytes the
    /// commit's driver holds, by blob key; `recover`, whether a head is
    /// recovered by the commit scan.
    fn perform(
        &mut self,
        request: Request,
        values: &HashMap<String, Vec<u8>>,
        recover: bool,
    ) -> Reply {
        match request {
            Request::Get(keys) => Reply::Values(
                keys.into_iter()
                    .filter_map(|k| self.store.get(&k).map(|v| (k, v.clone())))
                    .collect(),
            ),
            Request::Scan(prefix) => Reply::Keys(
                self.store
                    .keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect(),
            ),
            Request::Cas(Cas {
                expect,
                writes,
                removes,
            }) => {
                if expect.iter().any(|(k, v)| self.store.get(k) != v.as_ref()) {
                    return Reply::Landed(false);
                }
                for (key, write) in writes {
                    let bytes = match write {
                        Write::Bytes(bytes) => bytes,
                        Write::Held => values[&key].clone(),
                    };
                    self.store.insert(key, bytes);
                }
                for key in removes {
                    self.store.remove(&key);
                }
                Reply::Landed(true)
            }
            Request::Wait(seconds) => {
                self.clock += seconds;
                Reply::Waited
            }
            Request::Merge(asks) => {
                Reply::Merged(asks.iter().map(|a| self.host_merge(a)).collect())
            }
            Request::Recover(branch) if recover => {
                let guess = self
                    .call(
                        "recover_by_commit_scan",
                        args(&[("branch", &branch)]),
                        false,
                    )
                    .unwrap();
                Reply::Recovered(guess.as_str().map(String::from))
            }
            Request::Recover(_) => Reply::Recovered(None),
            Request::Now => Reply::Now(self.clock),
        }
    }

    fn host_merge(&self, ask: &MergeAsk) -> MergeAnswer {
        let side = |blob: &Option<String>| blob.as_ref().map(|b| self.store[b].clone());
        match HOST_RULES[ask.rule as usize] {
            "pick_ours" => MergeAnswer::Ours,
            "pick_theirs" => MergeAnswer::Theirs,
            "concat" => {
                let mut merged = side(&ask.theirs).unwrap_or_default();
                merged.extend(side(&ask.ours).unwrap_or_default());
                MergeAnswer::Value {
                    blob: blob_key(&merged),
                    size: merged.len() as u64,
                }
            }
            _ => MergeAnswer::Failed("refused".into()),
        }
    }

    fn finish(&mut self, name: &str, done: Done<Committed>) -> Result<Value, String> {
        let c = done.result.map_err(|e| error_name(&e))?;
        let wt = self.wts.get_mut(name).unwrap();
        if c.merged {
            for (key, entry) in &c.changes {
                match entry {
                    Some(entry) => wt.keys.insert(key.clone(), entry.clone()),
                    None => wt.keys.remove(key),
                };
            }
            wt.base = c.commit.clone().unwrap();
            wt.pending.clear();
            wt.stamped = c.stamped;
        }
        Ok(Value::Object(vec![
            ("merged".into(), Value::Bool(c.merged)),
            ("commit".into(), c.commit.map_or(Value::Null, Value::Str)),
            ("strategy".into(), Value::Str(c.strategy.name().into())),
            ("auto_merged_keys".into(), strings(&c.auto_merged)),
            ("carried_keys".into(), strings(&c.carried)),
        ]))
    }
}

/// The host merge functions a scenario may name, by number.
const HOST_RULES: &[&str] = &["pick_ours", "pick_theirs", "concat", "fail"];

fn rule(name: &str) -> Rule {
    match name {
        "ours" => Rule::Ours,
        "theirs" => Rule::Theirs,
        "text" => Rule::Text(TextRule::default()),
        "text_strict" => Rule::Text(TextRule {
            strict: true,
            ..TextRule::default()
        }),
        host => Rule::Host(HOST_RULES.iter().position(|r| *r == host).expect("a rule") as u32),
    }
}

fn rules(step: &Value) -> Rules {
    let map = |field: &str| match step.get(field) {
        Some(Value::Object(pairs)) => pairs
            .iter()
            .map(|(k, v)| (k.clone(), rule(v.as_str().unwrap())))
            .collect(),
        _ => vec![],
    };
    Rules {
        keys: map("merge_fns").into_iter().collect(),
        prefixes: map("merge_prefixes"),
        default: step.get("default_merge").and_then(Value::as_str).map(rule),
    }
}

/// Whether `request` is the write a job pauses before: its batch, which
/// lands an in-flight marker, or its publish, which moves its head.
fn holds(request: &Request, pause: &str, branch: &str) -> bool {
    let Request::Cas(cas) = request else {
        return false;
    };
    let head = branch_head(branch);
    cas.writes.iter().any(|(key, _)| match pause {
        "batch" => key.starts_with("__inflight__"),
        "publish" => *key == head,
        other => panic!("pause {other}"),
    })
}

fn error_name(e: &Error) -> String {
    match e {
        Error::BranchMoved { .. } => "BranchMovedError",
        Error::UnknownBranch { .. } | Error::HeadGone { .. } => "UnknownBranchError",
        Error::CorruptHead { .. } => "CorruptHeadError",
        Error::UnknownTag { .. } => "UnknownTagError",
        Error::UnknownCommit { .. } => "UnknownCommitError",
        Error::BranchExists { .. } => "BranchExistsError",
        Error::TagExists { .. } => "TagExistsError",
        Error::Concurrency(_) => "ConcurrencyError",
        Error::MergeConflict { .. } => "MergeConflict",
        Error::Malformed(_) | Error::Host | Error::Invalid(_) => "invalid",
    }
    .into()
}

/// Arguments for an operation, all strings.
fn args(pairs: &[(&str, &str)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Str(v.to_string())))
            .collect(),
    )
}
