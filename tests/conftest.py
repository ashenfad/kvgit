import glob
import os

collect_ignore_glob = []

# Pyodide-hosted tests require: --runtime chrome --dist-dir ./pyodide
# Skip them during normal `uv run pytest` runs.
if os.environ.get("KVGIT_PYODIDE_TESTS") != "1":
    collect_ignore_glob.append("**/test_indexeddb.py")
    collect_ignore_glob.append("**/test_pyodide_fs.py")


def pytest_configure(config):
    """Write the kvgit wheel filename to dist-dir so pyodide tests can find it."""
    dist_dir = getattr(config.option, "dist_dir", None)
    if dist_dir and os.path.isdir(dist_dir):
        wheels = glob.glob(os.path.join(dist_dir, "kvgit-*.whl"))
        if wheels:
            whl_name = os.path.basename(wheels[0])
            with open(os.path.join(dist_dir, "_kvgit_whl.txt"), "w") as f:
                f.write(whl_name)
