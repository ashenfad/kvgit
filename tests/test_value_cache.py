"""A worktree's memory of the values it reads: bounded, and never the
only copy of anything."""

import os
import threading
import tracemalloc

import pytest

import kvgit
from kvgit import Repo
from kvgit.cache import DEFAULT_VALUE_CACHE_BYTES, ValueCache
from kvgit.kv.memory import Memory

KB = 1 << 10
MB = 1 << 20


def _committed(n, size, **repo_kwargs):
    """A worktree whose head holds ``n`` distinct values of ``size`` bytes."""
    repo = Repo(Memory(), **repo_kwargs)
    wt = repo.worktree("main", create=True)
    values = {f"f{i}": os.urandom(size) for i in range(n)}
    for key, value in values.items():
        wt[key] = value
    wt.commit()
    return wt, values


# -- the bound -------------------------------------------------------------------


def test_a_read_only_worktree_retains_a_bounded_amount():
    """Reading many large values without ever committing must not keep
    them all alive."""
    wt, _ = _committed(64, MB)
    tracemalloc.start()
    try:
        for i in range(64):
            assert len(wt[f"f{i}"]) == MB
        retained, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert retained < DEFAULT_VALUE_CACHE_BYTES + 8 * MB


def test_the_cache_stays_under_its_budget_across_many_large_reads():
    wt, values = _committed(40, 100 * KB, value_cache_bytes=MB)
    for key in values:
        assert wt[key] == values[key]
        assert wt.cache.size_bytes <= MB
    assert 0 < len(wt.cache) < 40


def test_get_many_stays_under_the_budget_too():
    wt, values = _committed(40, 100 * KB, value_cache_bytes=MB)
    assert wt.get_many(*values) == values
    assert wt.cache.size_bytes <= MB


def test_a_value_over_a_quarter_of_the_budget_is_not_held():
    wt, values = _committed(2, 300 * KB, value_cache_bytes=MB)
    assert wt["f0"] == values["f0"]
    assert "f0" not in wt.cache
    assert wt.cache.size_bytes == 0
    assert wt["f0"] == values["f0"]


def test_least_recently_used_goes_first():
    # Each 240 KB value costs just under a quarter of 1 MB: four fit.
    wt, _ = _committed(5, 240 * KB, value_cache_bytes=MB)
    for key in ("f0", "f1", "f2", "f3"):
        wt[key]
    assert all(key in wt.cache for key in ("f0", "f1", "f2", "f3"))
    wt["f0"]  # f1 is now the least recently used
    wt["f4"]
    assert "f1" not in wt.cache
    assert all(key in wt.cache for key in ("f0", "f2", "f3", "f4"))


def test_an_evicted_value_reads_back_from_the_store():
    wt, values = _committed(5, 240 * KB, value_cache_bytes=MB)
    for key in values:
        wt[key]
    assert "f0" not in wt.cache
    assert wt["f0"] == values["f0"]
    assert wt.get_many("f0", "f1") == {"f0": values["f0"], "f1": values["f1"]}


def test_a_held_value_is_served_without_decoding_again():
    wt, _ = _committed(1, KB)
    assert wt["f0"] is wt["f0"]


def test_a_value_of_none_is_held_and_served():
    repo = Repo(Memory())
    wt = repo.worktree("main", create=True)
    wt["nothing"] = None
    wt.commit()
    assert wt.get("nothing", "default") is None
    assert "nothing" in wt.cache
    assert wt.get("nothing", "default") is None


def test_a_chunked_value_costs_its_chunks():
    np = pytest.importorskip("numpy")
    wt = kvgit.open(codec="scientific")
    wt["arr"] = np.zeros(128 * KB, dtype="uint8")
    wt.commit()
    wt["arr"]
    assert wt.cache.size_bytes >= 128 * KB


# -- pending changes -------------------------------------------------------------


def test_pending_writes_survive_any_amount_of_reading():
    wt, values = _committed(20, 200 * KB, value_cache_bytes=MB)
    pending = {f"p{i}": os.urandom(200 * KB) for i in range(20)}
    for key, value in pending.items():
        wt[key] = value
    for key in values:
        wt[key]
    for key, value in pending.items():
        assert wt[key] is value
    assert wt.get_many(*pending) == pending


def test_a_pending_write_shadows_a_held_value_until_discarded():
    wt, values = _committed(1, KB)
    assert wt["f0"] == values["f0"]
    assert "f0" in wt.cache
    wt["f0"] = b"pending"
    assert wt["f0"] == b"pending"
    assert wt.get_many("f0") == {"f0": b"pending"}
    del wt["f0"]
    assert wt.get("f0") is None
    wt.discard()
    assert wt["f0"] == values["f0"]


def test_with_no_budget_nothing_is_held_and_reads_still_work():
    wt, values = _committed(3, KB, value_cache_bytes=0)
    wt["p"] = b"pending"
    for key in values:
        assert wt[key] == values[key]
    assert len(wt.cache) == 0
    assert wt["p"] == b"pending"


def test_a_negative_budget_is_an_error():
    with pytest.raises(ValueError):
        Repo(Memory(), value_cache_bytes=-1)
    with pytest.raises(ValueError):
        ValueCache(-1)


def test_open_passes_the_budget_through():
    wt = kvgit.open(value_cache_bytes=123 * KB)
    assert wt.cache.max_bytes == 123 * KB


# -- when it empties -------------------------------------------------------------


def test_clearing_the_cache_keeps_pending_changes():
    wt, values = _committed(2, KB)
    wt["f0"]
    wt["p"] = b"pending"
    wt.cache.clear()
    assert len(wt.cache) == 0 and wt.cache.size_bytes == 0
    assert wt.status().updated == {"p"}
    assert wt["f0"] == values["f0"]


def test_a_commit_empties_the_cache():
    wt, _ = _committed(2, KB)
    wt["f0"]
    wt["f1"] = b"new"
    wt.commit()
    assert len(wt.cache) == 0
    assert wt["f1"] == b"new"


def test_discard_and_refresh_empty_the_cache():
    wt, _ = _committed(2, KB)
    wt["f0"]
    wt.discard()
    assert len(wt.cache) == 0
    wt["f0"]
    wt.refresh()
    assert len(wt.cache) == 0


def test_each_worktree_has_its_own_cache():
    wt, values = _committed(2, KB)
    other = wt.repo.worktree("main")
    wt["f0"]
    assert "f0" not in other.cache
    assert other["f0"] == values["f0"]


def test_concurrent_readers_keep_the_budget():
    wt, values = _committed(30, 100 * KB, value_cache_bytes=MB)
    keys = list(values)
    errors: list[BaseException] = []

    def read(offset):
        try:
            for i in range(300):
                key = keys[(i + offset) % len(keys)]
                assert wt[key] == values[key]
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=read, args=(n * 7,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert wt.cache.size_bytes <= MB
