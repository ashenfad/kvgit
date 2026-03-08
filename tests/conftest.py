import os

collect_ignore_glob = []

# IndexedDB tests require: --runtime chrome --dist-dir ./pyodide
# Skip them during normal `uv run pytest` runs.
if os.environ.get("KVGIT_PYODIDE_TESTS") != "1":
    collect_ignore_glob.append("**/test_indexeddb.py")
