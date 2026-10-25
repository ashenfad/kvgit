//! The Python binding of the kvgit core, imported as `kvgit._core`.

use pyo3::prelude::*;

#[pymodule]
mod _core {
    use kvgit_core::json::Value;
    use kvgit_core::keyset::Entry;
    use kvgit_core::machine::{Machine, Reply, Request, Step};
    use kvgit_core::tree::{self, Shape, TreeDiff, TreeError, Updated};
    use pyo3::exceptions::{PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple};
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
        Shape { prefix, bucket_max }
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
}
