//! The Python binding of the kvgit core, imported as `kvgit._core`.

use pyo3::prelude::*;

#[pymodule]
mod _core {
    use kvgit_core::cache::{self, Cache, Shared};
    use kvgit_core::error::Error;
    use kvgit_core::head::Note;
    use kvgit_core::json::Value;
    use kvgit_core::keyset::Entry;
    use kvgit_core::machine::{Cas, Machine, Reply, Request, Step, Write};
    use kvgit_core::merge::{MergeAnswer, Rule, Rules};
    use kvgit_core::ops::{self, Out as OpOut};
    use kvgit_core::refs;
    use kvgit_core::text::{self, TextError, TextRule};
    use kvgit_core::tree::{self, Shape, TreeDiff, TreeError, Updated};
    use kvgit_core::write::{CommitArgs, Committed, MergeArgs, OnConflict, OnMoved};
    use pyo3::exceptions::{PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{
        PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple, PyType,
    };
    use pyo3::{IntoPyObjectExt, PyTypeInfo};
    use std::collections::HashMap;

    /// The version of the core, which is the version of the package.
    #[pyfunction]
    fn version() -> &'static str {
        kvgit_core::version()
    }

    /// The root of the empty keyset.
    #[pyfunction]
    fn empty_hash() -> &'static str {
        kvgit_core::layout::EMPTY_HASH.as_str()
    }

    /// The 40-hex hash naming a commit.
    #[pyfunction]
    #[pyo3(signature = (parents, root, time, info=None))]
    fn commit_hash(
        parents: Vec<String>,
        root: &str,
        time: f64,
        info: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let info = info.filter(|v| !v.is_none()).map(to_value).transpose()?;
        let parents: Vec<&str> = parents.iter().map(String::as_str).collect();
        Ok(kvgit_core::commit::commit_hash(
            &parents,
            root,
            time,
            info.as_ref(),
        ))
    }

    /// The three-way text merge of a value's sides, and whether it
    /// marked a conflict. Values that can't be marked raise `cant_mark`;
    /// a label holding a line break raises `ValueError`.
    #[pyfunction]
    fn text_merge<'py>(
        py: Python<'py>,
        old: Option<&[u8]>,
        ours: Option<&[u8]>,
        theirs: Option<&[u8]>,
        ours_label: &str,
        theirs_label: &str,
        cant_mark: &Bound<'py, PyType>,
    ) -> PyResult<(Bound<'py, PyBytes>, bool)> {
        match text::text_merge(old, ours, theirs, ours_label, theirs_label) {
            Ok((merged, conflicted)) => Ok((PyBytes::new(py, &merged), conflicted)),
            Err(TextError::CantMark(message)) => Err(PyErr::from_type(cant_mark.clone(), message)),
            Err(e) => Err(PyValueError::new_err(e.to_string())),
        }
    }

    /// Raise `ValueError` if either label holds a line break.
    #[pyfunction]
    fn check_text_labels(ours_label: &str, theirs_label: &str) -> PyResult<()> {
        text::check_labels(ours_label, theirs_label)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pymodule_export]
    const MAX_MARK_BYTES: usize = text::MAX_MARK_BYTES;

    /// Raise `ValueError` for a name that can't be a branch.
    #[pyfunction]
    fn check_branch_name(name: &str) -> PyResult<()> {
        refs::check_branch_name(name).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Raise `ValueError` for a name that can't be a tag.
    #[pyfunction]
    fn check_tag_name(name: &str) -> PyResult<()> {
        refs::check_tag_name(name).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// A Python value as JSON, as `json.dumps` would read it. Object
    /// keys must be strings: `json.dumps` writes others as strings, so
    /// they wouldn't read back as the keys that were hashed.
    fn to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
        to_value_at(obj, 0)
    }

    fn to_value_at(obj: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
        if obj.is_none() {
            return Ok(Value::Null);
        }
        if let Ok(b) = obj.cast::<PyBool>() {
            return Ok(Value::Bool(b.is_true()));
        }
        if let Ok(n) = obj.cast::<PyInt>() {
            return Ok(match n.extract::<i64>() {
                Ok(n) => Value::Int(n),
                Err(_) => Value::BigInt(n.str()?.to_string()),
            });
        }
        if let Ok(x) = obj.cast::<PyFloat>() {
            return Ok(Value::Float(x.value()));
        }
        if let Ok(s) = obj.cast::<PyString>() {
            return Ok(Value::Str(s.to_str()?.to_string()));
        }
        if depth == kvgit_core::json::MAX_DEPTH {
            return Err(PyValueError::new_err("nested too deeply"));
        }
        if let Ok(d) = obj.cast::<PyDict>() {
            let mut pairs: Vec<(String, Value)> = Vec::with_capacity(d.len());
            for (k, v) in d.iter() {
                let key = k.cast::<PyString>().map_err(|_| {
                    PyTypeError::new_err(format!(
                        "keys must be str, not {}",
                        k.get_type()
                            .name()
                            .map(|n| n.to_string())
                            .unwrap_or_default()
                    ))
                })?;
                pairs.push((key.to_str()?.to_string(), to_value_at(&v, depth + 1)?));
            }
            return Ok(Value::Object(pairs));
        }
        if let Ok(l) = obj.cast::<PyList>() {
            return l
                .iter()
                .map(|v| to_value_at(&v, depth + 1))
                .collect::<PyResult<_>>()
                .map(Value::Array);
        }
        if let Ok(t) = obj.cast::<PyTuple>() {
            return t
                .iter()
                .map(|v| to_value_at(&v, depth + 1))
                .collect::<PyResult<_>>()
                .map(Value::Array);
        }
        Err(PyTypeError::new_err(format!(
            "Object of type {} is not JSON serializable",
            obj.get_type().name()?
        )))
    }

    /// JSON as the Python value `json.loads` would make of it.
    fn from_value(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
        Ok(match value {
            Value::Null => py.None(),
            Value::Bool(b) => b.into_py_any(py)?,
            Value::Int(n) => n.into_py_any(py)?,
            Value::BigInt(digits) => PyInt::type_object(py).call1((digits.as_str(),))?.unbind(),
            Value::Float(x) => x.into_py_any(py)?,
            Value::Str(s) => s.into_py_any(py)?,
            Value::Array(items) => {
                let list = PyList::empty(py);
                for item in items {
                    list.append(from_value(py, item)?)?;
                }
                list.into_any().unbind()
            }
            Value::Object(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    dict.set_item(k, from_value(py, v)?)?;
                }
                dict.into_any().unbind()
            }
        })
    }

    /// Per-key metadata stored beside a blob pointer in a keyset.
    ///
    /// `chunks` lists the chunk keys a chunked value refers to, or is
    /// `None`. `created_at` is set only on entries written before
    /// storage v4, and kept so their subtrees re-encode to their bytes.
    #[pyclass(module = "kvgit._core")]
    struct MetaEntry {
        #[pyo3(get, set)]
        size: Py<PyAny>,
        #[pyo3(get, set)]
        created_at: Py<PyAny>,
        #[pyo3(get, set)]
        chunks: Py<PyAny>,
    }

    #[pymethods]
    impl MetaEntry {
        #[new]
        #[pyo3(signature = (size, created_at=None, chunks=None))]
        fn new(
            py: Python<'_>,
            size: Py<PyAny>,
            created_at: Option<Py<PyAny>>,
            chunks: Option<Py<PyAny>>,
        ) -> Self {
            MetaEntry {
                size,
                created_at: created_at.unwrap_or_else(|| py.None()),
                chunks: chunks.unwrap_or_else(|| py.None()),
            }
        }

        fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let Ok(other) = other.cast::<MetaEntry>() else {
                return Ok(py.NotImplemented());
            };
            let other = other.borrow();
            let same = self.size.bind(py).eq(&other.size)?
                && self.created_at.bind(py).eq(&other.created_at)?
                && self.chunks.bind(py).eq(&other.chunks)?;
            same.into_py_any(py)
        }

        fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
            Ok(format!(
                "MetaEntry(size={}, created_at={}, chunks={})",
                self.size.bind(py).repr()?,
                self.created_at.bind(py).repr()?,
                self.chunks.bind(py).repr()?
            ))
        }
    }

    /// One entry in a keyset: a blob pointer and its metadata.
    #[pyclass(module = "kvgit._core", frozen)]
    struct KeysetEntry {
        #[pyo3(get)]
        blob: String,
        #[pyo3(get)]
        meta: Py<MetaEntry>,
    }

    #[pymethods]
    impl KeysetEntry {
        #[new]
        fn new(blob: String, meta: Py<MetaEntry>) -> Self {
            KeysetEntry { blob, meta }
        }

        fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let Ok(other) = other.cast::<KeysetEntry>() else {
                return Ok(py.NotImplemented());
            };
            let other = other.get();
            let same = self.blob == other.blob && self.meta.bind(py).eq(other.meta.bind(py))?;
            same.into_py_any(py)
        }

        fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
            Ok(format!(
                "KeysetEntry(blob={:?}, meta={})",
                self.blob,
                self.meta.bind(py).repr()?
            ))
        }
    }

    fn entry_to_py(py: Python<'_>, entry: &Entry) -> PyResult<Py<KeysetEntry>> {
        let chunks = if entry.chunks.is_empty() {
            py.None()
        } else {
            PyList::new(py, &entry.chunks)?.into_any().unbind()
        };
        let meta = MetaEntry {
            size: from_value(py, &entry.size)?,
            created_at: match &entry.created_at {
                Some(v) => from_value(py, v)?,
                None => py.None(),
            },
            chunks,
        };
        Py::new(
            py,
            KeysetEntry {
                blob: entry.blob.clone(),
                meta: Py::new(py, meta)?,
            },
        )
    }

    fn entry_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Entry> {
        let py = obj.py();
        let entry = obj.cast::<KeysetEntry>()?.get();
        let meta = entry.meta.borrow(py);
        let created_at = meta.created_at.bind(py);
        let chunks = meta.chunks.bind(py);
        Ok(Entry {
            blob: entry.blob.clone(),
            size: to_value(meta.size.bind(py))?,
            created_at: if created_at.is_none() {
                None
            } else {
                Some(to_value(created_at)?)
            },
            chunks: if chunks.is_none() {
                vec![]
            } else {
                chunks.extract()?
            },
        })
    }

    /// An entry's bytes, as both packages write them.
    #[pyfunction]
    fn encode_entry<'py>(
        py: Python<'py>,
        entry: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, &entry_from_py(entry)?.encode()))
    }

    /// The entry stored as `raw`.
    #[pyfunction]
    fn decode_entry(py: Python<'_>, raw: &[u8]) -> PyResult<Py<KeysetEntry>> {
        let entry = Entry::decode(raw).map_err(PyValueError::new_err)?;
        entry_to_py(py, &entry)
    }

    enum Out {
        Found(Vec<(String, Vec<u8>)>),
        Walked(Vec<(String, Vec<u8>)>, Vec<String>),
        Diffed(TreeDiff),
        Updated(Updated),
    }

    /// A tree operation in progress. `resume` returns `("get", keys)`
    /// while it needs nodes, to be answered with the stored values of
    /// those keys (a dict, leaving out absent ones), and `("done",
    /// result)` once it is finished.
    #[pyclass(unsendable)]
    struct TreeOp {
        machine: Machine<Result<Out, TreeError>>,
        /// Whether values are keyset entries, read and written as
        /// `KeysetEntry`, rather than bytes.
        entries: bool,
    }

    fn shape(prefix: String, bucket_max: usize) -> Shape {
        Shape {
            prefix,
            bucket_max,
            memo: None,
        }
    }

    fn bytes_of(d: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Vec<u8>>> {
        let mut out = HashMap::with_capacity(d.len());
        for (k, v) in d.iter() {
            let key = k.cast::<PyString>()?.to_str()?.to_string();
            out.insert(key, v.cast::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(out)
    }

    /// A value as Python sees it: its bytes, or its decoded entry.
    fn value_to_py(py: Python<'_>, raw: &[u8], entries: bool) -> PyResult<Py<PyAny>> {
        if entries {
            let entry = Entry::decode(raw).map_err(PyValueError::new_err)?;
            Ok(entry_to_py(py, &entry)?.into_any())
        } else {
            Ok(PyBytes::new(py, raw).into_any().unbind())
        }
    }

    fn pairs_to_dict<'py>(
        py: Python<'py>,
        pairs: Vec<(String, Vec<u8>)>,
        entries: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        // The keys first, all together: made between their values, they
        // would lie scattered among them in memory, and a pass over the
        // keys alone would run at half speed.
        let keys: Vec<Bound<'py, PyString>> =
            pairs.iter().map(|(k, _)| PyString::new(py, k)).collect();
        let d = PyDict::new(py);
        for (key, (_, v)) in keys.into_iter().zip(&pairs) {
            d.set_item(key, value_to_py(py, v, entries)?)?;
        }
        Ok(d)
    }

    #[pymethods]
    impl TreeOp {
        /// The values of those of `keys` the tree at `root` holds.
        #[staticmethod]
        #[pyo3(signature = (prefix, bucket_max, root, keys, entries=false))]
        fn get_many(
            prefix: String,
            bucket_max: usize,
            root: String,
            keys: Vec<String>,
            entries: bool,
        ) -> TreeOp {
            let shape = shape(prefix, bucket_max);
            TreeOp {
                machine: Machine::new(move |io| async move {
                    tree::get_many(io, shape, root, keys).await.map(Out::Found)
                }),
                entries,
            }
        }

        /// Everything the tree holds and every node it is made of,
        /// skipping the nodes in `skip` and all under them.
        #[staticmethod]
        #[pyo3(signature = (prefix, bucket_max, root, skip, entries=false))]
        fn walk(
            prefix: String,
            bucket_max: usize,
            root: String,
            skip: Py<PySet>,
            entries: bool,
        ) -> TreeOp {
            let shape = shape(prefix, bucket_max);
            // Asked node by node, so a large skip set is never copied.
            let skipped = move |hash: &str| {
                Python::attach(|py| skip.bind(py).contains(hash).unwrap_or(false))
            };
            TreeOp {
                machine: Machine::new(move |io| async move {
                    tree::walk(io, shape, root, skipped)
                        .await
                        .map(|(items, nodes)| Out::Walked(items, nodes))
                }),
                entries,
            }
        }

        /// What changed from the tree at `a` to the tree at `b`.
        #[staticmethod]
        #[pyo3(signature = (prefix, bucket_max, a, b, entries=false))]
        fn diff(prefix: String, bucket_max: usize, a: String, b: String, entries: bool) -> TreeOp {
            let shape = shape(prefix, bucket_max);
            TreeOp {
                machine: Machine::new(move |io| async move {
                    tree::diff(io, shape, a, b).await.map(Out::Diffed)
                }),
                entries,
            }
        }

        /// The tree at `root` with `updates` set, then `removals`
        /// removed. `unwritten` holds the caller's unstored nodes by
        /// hash.
        #[staticmethod]
        #[pyo3(signature = (prefix, bucket_max, root, updates, removals, unwritten=None, entries=false))]
        #[allow(clippy::too_many_arguments)]
        fn update(
            prefix: String,
            bucket_max: usize,
            root: String,
            updates: &Bound<'_, PyList>,
            removals: Vec<String>,
            unwritten: Option<&Bound<'_, PyDict>>,
            entries: bool,
        ) -> PyResult<TreeOp> {
            let shape = shape(prefix, bucket_max);
            let mut sets = Vec::with_capacity(updates.len());
            for item in updates.iter() {
                let item = item.cast::<PyTuple>()?;
                let key = item.get_item(0)?.cast::<PyString>()?.to_str()?.to_string();
                let value = item.get_item(1)?;
                let raw = if entries {
                    entry_from_py(&value)?.encode()
                } else {
                    value.cast::<PyBytes>()?.as_bytes().to_vec()
                };
                sets.push((key, raw));
            }
            let unwritten = unwritten.map(bytes_of).transpose()?.unwrap_or_default();
            Ok(TreeOp {
                machine: Machine::new(move |io| async move {
                    tree::update(io, shape, root, sets, removals, unwritten)
                        .await
                        .map(Out::Updated)
                }),
                entries,
            })
        }

        /// Read through `cache`; given before the first `resume`.
        fn use_cache(&mut self, cache: &ContentCache) {
            self.machine.use_cache(cache.inner.clone());
        }

        #[pyo3(signature = (reply=None))]
        fn resume<'py>(
            &mut self,
            py: Python<'py>,
            reply: Option<&Bound<'py, PyDict>>,
        ) -> PyResult<Bound<'py, PyTuple>> {
            let reply = reply.map(bytes_of).transpose()?.map(Reply::Values);
            let entries = self.entries;
            match self.machine.resume(reply) {
                Step::Request(Request::Get(keys)) => {
                    ("get", PyList::new(py, keys)?).into_pyobject(py)
                }
                Step::Request(other) => unreachable!("a tree only reads, not {other:?}"),
                Step::Done(Err(e)) => Err(PyValueError::new_err(e.to_string())),
                Step::Done(Ok(out)) => {
                    let result: Bound<'py, PyAny> = match out {
                        Out::Found(pairs) => pairs_to_dict(py, pairs, entries)?.into_any(),
                        Out::Walked(items, nodes) => {
                            (pairs_to_dict(py, items, entries)?, PySet::new(py, nodes)?)
                                .into_pyobject(py)?
                                .into_any()
                        }
                        Out::Diffed(d) => {
                            let modified = PyDict::new(py);
                            for (k, old, new) in d.modified {
                                modified.set_item(
                                    k,
                                    (
                                        value_to_py(py, &old, entries)?,
                                        value_to_py(py, &new, entries)?,
                                    ),
                                )?;
                            }
                            (
                                pairs_to_dict(py, d.added, entries)?,
                                pairs_to_dict(py, d.removed, entries)?,
                                modified,
                            )
                                .into_pyobject(py)?
                                .into_any()
                        }
                        Out::Updated(u) => (u.root, pairs_to_dict(py, u.nodes, false)?)
                            .into_pyobject(py)?
                            .into_any(),
                    };
                    ("done", result).into_pyobject(py)
                }
            }
        }
    }

    /// A repository's memory of write-once keys (tree nodes, commit
    /// records), least recently used first out, bounded in bytes, and
    /// emptied when a read finds the GC lease record changed. Shared by
    /// every operation that is given it, from any thread.
    #[pyclass(module = "kvgit._core", frozen)]
    struct ContentCache {
        inner: Shared,
    }

    #[pymethods]
    impl ContentCache {
        #[new]
        fn new(max_bytes: usize) -> ContentCache {
            ContentCache {
                inner: Cache::shared(max_bytes),
            }
        }

        #[getter]
        fn max_bytes(&self) -> usize {
            self.inner.lock().unwrap().max_bytes()
        }

        #[getter]
        fn size_bytes(&self) -> usize {
            self.inner.lock().unwrap().size_bytes()
        }

        #[getter]
        fn hits(&self) -> u64 {
            self.inner.lock().unwrap().hits
        }

        #[getter]
        fn misses(&self) -> u64 {
            self.inner.lock().unwrap().misses
        }

        #[getter]
        fn clears(&self) -> u64 {
            self.inner.lock().unwrap().clears
        }

        fn __len__(&self) -> usize {
            self.inner.lock().unwrap().len()
        }

        fn clear(&self) {
            self.inner.lock().unwrap().clear();
        }

        /// The values held of those `keys`.
        fn lookup<'py>(&self, py: Python<'py>, keys: Vec<String>) -> PyResult<Bound<'py, PyDict>> {
            let found = self
                .inner
                .lock()
                .unwrap()
                .lookup(keys.iter().map(String::as_str));
            let out = PyDict::new(py);
            for (k, v) in found {
                out.set_item(k, PyBytes::new(py, &v))?;
            }
            Ok(out)
        }

        fn count(&self, hits: u64, misses: u64) {
            self.inner.lock().unwrap().count(hits, misses);
        }

        /// Hold these values, the least recently used going to fit.
        fn remember(&self, items: &Bound<'_, PyDict>) -> PyResult<()> {
            let items = bytes_of(items)?;
            self.inner
                .lock()
                .unwrap()
                .remember(items.iter().map(|(k, v)| (k.as_str(), v.as_slice())));
            Ok(())
        }

        fn forget(&self, keys: Vec<String>) {
            self.inner
                .lock()
                .unwrap()
                .forget(keys.iter().map(String::as_str));
        }

        /// Note the lease record a read or write found.
        fn saw_lease(&self, record: Option<&[u8]>) {
            self.inner.lock().unwrap().saw_lease(record);
        }
    }

    /// Whether `key` is one the store never rewrites.
    #[pyfunction]
    fn cacheable(key: &str) -> bool {
        cache::cacheable(key)
    }

    /// An operation of the core in progress, started by name (see
    /// `kvgit_core::ops`) with its arguments as a dict, or by
    /// `Op.commit`. It is driven like a tree operation: `resume` returns
    /// the next request as a tuple, to be answered by the next call,
    /// until it returns `("done", outcome, notes)`.
    ///
    /// - `("get", keys)`: a dict of the stored values, absent ones left
    ///   out.
    /// - `("scan", prefix)`: the stored keys starting with `prefix`.
    /// - `("cas", expect, writes, removes)`: whether the batch landed.
    ///   `expect` and `writes` are lists of `(key, bytes)`; an expected
    ///   `None` means absent, and a write of `None` writes the bytes the
    ///   host holds for that key.
    /// - `("wait", seconds)`: `None`, once slept.
    /// - `("merge", asks)`: one answer per `(rule, key, old, ours,
    ///   theirs)`, each side a blob key or `None`: `("value", blob,
    ///   size)`, `"ours"`, `"theirs"` or `("failed", reason)`.
    /// - `("recover", branch)`: the recoverer's guess, or `None`.
    /// - `("now",)`: the time to stamp a commit with.
    /// - `("emit", part)`: part of the answer, ready early; `None`, to
    ///   go on.
    /// - `("owner",)`: a fresh name for a GC lease.
    /// - `("check", values)`: the keys refused of `(key, blob, bytes)`,
    ///   `bytes` given where the host doesn't hold the blob.
    ///
    /// Every call passes wall time; `failed=True` answers a request the
    /// host couldn't perform.
    #[pyclass]
    struct Op {
        machine: Option<ops::Op>,
        asked: Option<Request>,
    }

    // SAFETY: a machine is neither `Send` nor `Sync` only for the `Rc`s
    // between it and its operation, and every clone of them lives inside
    // this object: a commit's shape has no memo, and no operation's
    // arguments hold any. Moved to another thread, the object takes them
    // all along. Python reaches the machine only through `resume`, which
    // takes the object mutably, so no two threads ever touch them at once.
    unsafe impl Send for Op {}
    unsafe impl Sync for Op {}

    /// A merge rule as Python hands one over: `"ours"`, `"theirs"`, a
    /// host function's number, or `("text", ours_label, theirs_label,
    /// strict)`.
    fn rule_of(obj: &Bound<'_, PyAny>) -> PyResult<Rule> {
        if let Ok(n) = obj.extract::<u32>() {
            return Ok(Rule::Host(n));
        }
        if let Ok(side) = obj.extract::<String>() {
            return match side.as_str() {
                "ours" => Ok(Rule::Ours),
                "theirs" => Ok(Rule::Theirs),
                other => Err(PyValueError::new_err(format!(
                    "not a merge rule: {other:?}"
                ))),
            };
        }
        let (kind, ours_label, theirs_label, strict): (String, String, String, bool) =
            obj.extract()?;
        if kind != "text" {
            return Err(PyValueError::new_err(format!("not a merge rule: {kind:?}")));
        }
        Ok(Rule::Text(TextRule {
            ours_label,
            theirs_label,
            strict,
        }))
    }

    /// Merge rules as Python hands them over: by key, by prefix, and a
    /// default.
    fn rules_of(
        merge_fns: &Bound<'_, PyDict>,
        merge_prefixes: &[(String, Bound<'_, PyAny>)],
        default_merge: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Rules> {
        let mut keys = HashMap::new();
        for (k, r) in merge_fns.iter() {
            keys.insert(k.extract::<String>()?, rule_of(&r)?);
        }
        Ok(Rules {
            keys,
            prefixes: merge_prefixes
                .iter()
                .map(|(p, r)| Ok((p.clone(), rule_of(r)?)))
                .collect::<PyResult<_>>()?,
            default: default_merge
                .filter(|d| !d.is_none())
                .map(rule_of)
                .transpose()?,
        })
    }

    fn reply_of(asked: &Request, reply: Option<&Bound<'_, PyAny>>) -> PyResult<Reply> {
        match asked {
            Request::Wait(_) => return Ok(Reply::Waited),
            Request::Emit(_) => return Ok(Reply::Next),
            Request::Recover(_) => {
                return Ok(Reply::Recovered(
                    reply.map(|r| r.extract()).transpose()?.flatten(),
                ));
            }
            _ => {}
        }
        let reply = reply.ok_or_else(|| PyValueError::new_err("a request needs a reply"))?;
        Ok(match asked {
            Request::Get(_) => Reply::Values(bytes_of(reply.cast::<PyDict>()?)?),
            Request::Check(_) => Reply::Refused(reply.extract()?),
            Request::Owner => Reply::Owner(reply.extract()?),
            Request::Scan(_) => Reply::Keys(
                reply
                    .try_iter()?
                    .filter_map(|k| k.and_then(|k| k.extract::<String>()).ok())
                    .collect(),
            ),
            Request::Cas(_) => Reply::Landed(reply.extract()?),
            Request::Wait(_) | Request::Recover(_) | Request::Emit(_) => {
                unreachable!("answered above")
            }
            Request::Now => Reply::Now(reply.extract()?),
            Request::Merge(_) => {
                let mut answers = Vec::new();
                for answer in reply.try_iter()? {
                    let answer = answer?;
                    answers.push(if let Ok(side) = answer.extract::<String>() {
                        match side.as_str() {
                            "ours" => MergeAnswer::Ours,
                            "theirs" => MergeAnswer::Theirs,
                            other => {
                                return Err(PyValueError::new_err(format!(
                                    "not an answer: {other:?}"
                                )));
                            }
                        }
                    } else if let Ok((_, blob, size)) = answer.extract::<(String, String, u64)>() {
                        MergeAnswer::Value { blob, size }
                    } else {
                        let (_, reason): (String, String) = answer.extract()?;
                        MergeAnswer::Failed(reason)
                    });
                }
                Reply::Merged(answers)
            }
        })
    }

    fn request_to_py<'py>(py: Python<'py>, request: &Request) -> PyResult<Bound<'py, PyTuple>> {
        let bytes = |b: &Option<Vec<u8>>| b.as_ref().map(|b| PyBytes::new(py, b));
        match request {
            Request::Get(keys) => ("get", PyList::new(py, keys)?).into_pyobject(py),
            Request::Scan(prefix) => ("scan", prefix).into_pyobject(py),
            Request::Cas(Cas {
                expect,
                writes,
                removes,
            }) => {
                let expect = PyList::new(
                    py,
                    expect
                        .iter()
                        .map(|(k, v)| (k, bytes(v)))
                        .collect::<Vec<_>>(),
                )?;
                let writes = PyList::new(
                    py,
                    writes
                        .iter()
                        .map(|(k, w)| match w {
                            Write::Bytes(b) => (k, Some(PyBytes::new(py, b))),
                            Write::Held => (k, None),
                        })
                        .collect::<Vec<_>>(),
                )?;
                ("cas", expect, writes, PyList::new(py, removes)?).into_pyobject(py)
            }
            Request::Wait(seconds) => ("wait", *seconds).into_pyobject(py),
            Request::Merge(asks) => {
                let asks = asks
                    .iter()
                    .map(|a| (a.rule, &a.key, &a.old, &a.ours, &a.theirs))
                    .collect::<Vec<_>>();
                ("merge", PyList::new(py, asks)?).into_pyobject(py)
            }
            Request::Recover(branch) => ("recover", branch).into_pyobject(py),
            Request::Now => ("now",).into_pyobject(py),
            Request::Owner => ("owner",).into_pyobject(py),
            Request::Emit(part) => ("emit", from_value(py, part)?).into_pyobject(py),
            Request::Check(values) => {
                let values = values
                    .iter()
                    .map(|(k, blob, made)| (k, blob, bytes(made)))
                    .collect::<Vec<_>>();
                ("check", PyList::new(py, values)?).into_pyobject(py)
            }
        }
    }

    /// A note as `(kind, *args)`, the arguments its message takes.
    fn note_to_py<'py>(py: Python<'py>, note: &Note) -> PyResult<Bound<'py, PyTuple>> {
        match note {
            Note::FromBackup { branch } => ("from_backup", branch).into_pyobject(py),
            Note::FromRecoverer { branch } => ("from_recoverer", branch).into_pyobject(py),
            Note::RecovererMissed { branch, answer } => {
                ("recoverer_missed", branch, answer).into_pyobject(py)
            }
            Note::Healed { branch } => ("healed", branch).into_pyobject(py),
            Note::LeaseOverrun { overrun, ttl } => {
                ("lease_overrun", *overrun, *ttl).into_pyobject(py)
            }
        }
    }

    fn committed_to_py<'py>(py: Python<'py>, c: &Committed) -> PyResult<Bound<'py, PyAny>> {
        let fields = PyDict::new(py);
        fields.set_item("merged", c.merged)?;
        fields.set_item("commit", &c.commit)?;
        fields.set_item("strategy", c.strategy.name())?;
        fields.set_item("auto_merged_keys", PyTuple::new(py, &c.auto_merged)?)?;
        fields.set_item("carried_keys", PyTuple::new(py, &c.carried)?)?;
        let changes = PyList::empty(py);
        for (key, entry) in &c.changes {
            let entry = match entry {
                Some(e) => entry_to_py(py, e)?.into_any(),
                None => py.None(),
            };
            changes.append((key, entry))?;
        }
        fields.set_item("changes", changes)?;
        fields.set_item("roots", PyList::new(py, &c.roots)?)?;
        fields.set_item("generations", PyList::new(py, &c.generations)?)?;
        fields.set_item("stamped", c.stamped)?;
        Ok(fields.into_any())
    }

    /// An outcome as `("ok", value)`, or `("error", kind, fields)`, the
    /// fields always holding the message.
    fn outcome_to_py<'py>(
        py: Python<'py>,
        result: &Result<OpOut, Error>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        match result {
            Ok(OpOut::Committed(c)) => ("ok", committed_to_py(py, c)?).into_pyobject(py),
            Ok(OpOut::Value(v)) => ("ok", from_value(py, v)?).into_pyobject(py),
            Err(e) => {
                let fields = PyDict::new(py);
                for (name, value) in e.fields() {
                    fields.set_item(name, value)?;
                }
                if let Error::MergeConflict { keys, failures } = e {
                    fields.set_item("keys", PyList::new(py, keys)?)?;
                    fields.set_item(
                        "failures",
                        PyDict::from_sequence(&PyList::new(py, failures)?.into_any())?,
                    )?;
                }
                fields.set_item("message", e.to_string())?;
                ("error", e.kind(), fields).into_pyobject(py)
            }
        }
    }

    #[pymethods]
    impl Op {
        #[new]
        fn new(name: &str, args: &Bound<'_, PyAny>) -> PyResult<Op> {
            let machine = ops::call(name, to_value(args)?)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(Op {
                machine: Some(machine),
                asked: None,
            })
        }

        #[staticmethod]
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (
            branch, base, current, updates, removals, chunks, info, on_moved, on_conflict,
            merge_fns, merge_prefixes, default_merge, recover, stamped, roots, generations,
        ))]
        fn commit(
            branch: String,
            base: String,
            current: String,
            updates: Vec<(String, Bound<'_, PyAny>)>,
            removals: Vec<String>,
            chunks: Vec<String>,
            info: Option<&Bound<'_, PyAny>>,
            on_moved: &str,
            on_conflict: &str,
            merge_fns: &Bound<'_, PyDict>,
            merge_prefixes: Vec<(String, Bound<'_, PyAny>)>,
            default_merge: Option<&Bound<'_, PyAny>>,
            recover: bool,
            stamped: bool,
            roots: Vec<(String, String)>,
            generations: Vec<(String, u64)>,
        ) -> PyResult<Op> {
            let updates = updates
                .iter()
                .map(|(k, e)| Ok((k.clone(), entry_from_py(e)?)))
                .collect::<PyResult<_>>()?;
            let rules = rules_of(merge_fns, &merge_prefixes, default_merge)?;
            let args = CommitArgs {
                branch,
                base,
                current,
                shape: kvgit_core::keyset::shape(),
                updates,
                removals,
                chunks,
                info: info.filter(|v| !v.is_none()).map(to_value).transpose()?,
                on_moved: if on_moved == "raise" {
                    OnMoved::Raise
                } else {
                    OnMoved::Merge
                },
                on_conflict: if on_conflict == "abandon" {
                    OnConflict::Abandon
                } else {
                    OnConflict::Raise
                },
                rules,
                recover,
                stamped,
                roots,
                generations,
            };
            Ok(Op {
                machine: Some(ops::commit(args)),
                asked: None,
            })
        }

        /// A merge of `theirs` into the branch at `ours`, or, given
        /// `from_`, an apply of the change from `from_` to `theirs`.
        #[staticmethod]
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (
            branch, ours, theirs, from_, fast_forward, info, on_conflict, merge_fns,
            merge_prefixes, default_merge, post_check, recover, stamped, roots, generations,
        ))]
        fn merge(
            branch: String,
            ours: String,
            theirs: String,
            from_: Option<String>,
            fast_forward: bool,
            info: Option<&Bound<'_, PyAny>>,
            on_conflict: &str,
            merge_fns: &Bound<'_, PyDict>,
            merge_prefixes: Vec<(String, Bound<'_, PyAny>)>,
            default_merge: Option<&Bound<'_, PyAny>>,
            post_check: bool,
            recover: bool,
            stamped: bool,
            roots: Vec<(String, String)>,
            generations: Vec<(String, u64)>,
        ) -> PyResult<Op> {
            let args = MergeArgs {
                branch,
                ours,
                theirs,
                from: from_,
                fast_forward,
                shape: kvgit_core::keyset::shape(),
                rules: rules_of(merge_fns, &merge_prefixes, default_merge)?,
                on_conflict: if on_conflict == "abandon" {
                    OnConflict::Abandon
                } else {
                    OnConflict::Raise
                },
                info: info.filter(|v| !v.is_none()).map(to_value).transpose()?,
                post_check,
                recover,
                stamped,
                roots,
                generations,
            };
            Ok(Op {
                machine: Some(ops::merge(args)),
                asked: None,
            })
        }

        /// Read and write through `cache`; given before the first
        /// `resume`.
        fn use_cache(&mut self, cache: &ContentCache) -> PyResult<()> {
            self.machine
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("the operation is done"))?
                .use_cache(cache.inner.clone());
            Ok(())
        }

        #[pyo3(signature = (wall, reply=None, failed=false))]
        fn resume<'py>(
            &mut self,
            py: Python<'py>,
            wall: f64,
            reply: Option<&Bound<'py, PyAny>>,
            failed: bool,
        ) -> PyResult<Bound<'py, PyTuple>> {
            let reply = match (&self.asked, failed) {
                (None, _) => None,
                (Some(_), true) => Some(Reply::Failed),
                (Some(asked), false) => Some(reply_of(asked, reply)?),
            };
            let machine = self
                .machine
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("the operation is done"))?;
            match machine.resume_at(wall, reply) {
                Step::Request(request) => {
                    let out = request_to_py(py, &request);
                    self.asked = Some(request);
                    out
                }
                Step::Done(done) => {
                    self.machine = None;
                    self.asked = None;
                    let notes = done
                        .notes
                        .iter()
                        .map(|n| note_to_py(py, n))
                        .collect::<PyResult<Vec<_>>>()?;
                    (
                        "done",
                        outcome_to_py(py, &done.result)?,
                        PyList::new(py, notes)?,
                    )
                        .into_pyobject(py)
                }
            }
        }
    }
}
