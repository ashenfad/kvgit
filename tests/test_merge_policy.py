"""Tests for merge policy by prefix, side-picking merges, and byte equality."""

import pickle

import pytest

from kvgit import MergeChoice, MergeConflict, Staged, VersionedKV as Versioned
from kvgit.kv.memory import Memory
from kvgit.merges import ours, theirs
from kvgit.store import store


def _mark(tag: bytes):
    """A merge fn that resolves any key to one fixed marker value."""
    return lambda old, our, their: tag


def _branched_versioned(base: dict[str, bytes] | None = None):
    """Main + worker VersionedKV pair sharing one store (raw bytes)."""
    main = Versioned(Memory())
    main.commit(base if base is not None else {"seed": b"0"})
    worker = main.create_branch("worker")
    return main, worker


def _concurrent_versioned(base: dict[str, bytes] | None = None):
    """Two VersionedKV writers on one branch, both at the same base commit."""
    kv = Memory()
    first = Versioned(kv)
    first.commit(base if base is not None else {"seed": b"0"})
    second = Versioned(kv)
    return first, second


def _branched_staged(base_key: str = "seed", base_value=0):
    """Main + worker Staged pair sharing one memory store."""
    main = store(kind="memory", branch="main")
    main[base_key] = base_value
    main.commit()
    worker = main.create_branch("worker")
    return main, worker


def _concurrent_staged(base_key: str = "seed", base_value=0):
    """Two Staged writers on one branch, both at the same base commit."""
    kv = Memory()
    first = Staged(Versioned(kv))
    first[base_key] = base_value
    first.commit()
    second = Staged(Versioned(kv))
    return first, second


class TestMergePrefix:
    def test_prefix_resolves_key_registered_by_no_exact_fn(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-prefix"))
        main.commit({"runs/1": b"ours"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"by-prefix"

    def test_exact_key_beats_prefix(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-prefix"))
        main.set_merge_fn("runs/1", _mark(b"by-key"))
        main.commit({"runs/1": b"ours", "runs/2": b"ours"})
        worker.commit({"runs/1": b"theirs", "runs/2": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"by-key"
        assert main.get("runs/2") == b"by-prefix"

    def test_longest_prefix_wins(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"short"))
        main.set_merge_prefix("runs/hot/", _mark(b"long"))
        main.commit({"runs/hot/1": b"ours", "runs/cold/1": b"ours"})
        worker.commit({"runs/hot/1": b"theirs", "runs/cold/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/hot/1") == b"long"
        assert main.get("runs/cold/1") == b"short"

    def test_prefix_beats_default(self):
        main, worker = _branched_versioned()
        main.set_default_merge(_mark(b"by-default"))
        main.set_merge_prefix("runs/", _mark(b"by-prefix"))
        main.commit({"runs/1": b"ours", "other": b"ours"})
        worker.commit({"runs/1": b"theirs", "other": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"by-prefix"
        assert main.get("other") == b"by-default"

    def test_no_matching_prefix_conflicts_without_default(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-prefix"))
        main.commit({"other": b"ours"})
        worker.commit({"other": b"theirs"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit)
        assert exc_info.value.conflicting_keys == {"other"}

    def test_per_call_prefixes_override_instance_registration(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"instance"))
        main.commit({"runs/1": b"ours"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(
            worker.current_commit,
            merge_prefixes={"runs/": _mark(b"per-call")},
        )
        assert result.merged
        assert main.get("runs/1") == b"per-call"

    def test_per_call_prefixes_layer_over_instance_registration(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"instance"))
        main.commit({"runs/1": b"ours", "logs/1": b"ours"})
        worker.commit({"runs/1": b"theirs", "logs/1": b"theirs"})

        result = main.merge_heads(
            worker.current_commit,
            merge_prefixes={"logs/": _mark(b"per-call")},
        )
        assert result.merged
        assert main.get("runs/1") == b"instance"
        assert main.get("logs/1") == b"per-call"

    def test_prefix_applies_on_the_concurrent_commit_path(self):
        first, second = _concurrent_versioned()
        second.set_merge_prefix("runs/", _mark(b"by-prefix"))
        first.commit({"runs/1": b"theirs"})

        result = second.commit({"runs/1": b"ours"})
        assert result.merged
        assert second.get("runs/1") == b"by-prefix"

    def test_per_call_prefixes_on_the_concurrent_commit_path(self):
        first, second = _concurrent_versioned()
        second.set_merge_prefix("runs/", _mark(b"instance"))
        first.commit({"runs/1": b"theirs"})

        result = second.commit(
            {"runs/1": b"ours"},
            merge_prefixes={"runs/": _mark(b"per-call")},
        )
        assert result.merged
        assert second.get("runs/1") == b"per-call"

    def test_empty_prefix_matches_every_key(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("", _mark(b"catch-all"))
        main.commit({"anything": b"ours"})
        worker.commit({"anything": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("anything") == b"catch-all"


class TestOursTheirs:
    def test_theirs_keeps_their_pointer_and_writes_no_blob(self):
        main, worker = _branched_versioned()
        main.commit({"k": b"our-value"})
        worker.commit({"k": b"their-value"})
        their_pointer = worker._load_keyset(worker.current_commit)["k"]

        result = main.merge_heads(worker.current_commit, default_merge=theirs)
        assert result.merged
        assert main.get("k") == b"their-value"
        assert main._load_keyset(result.commit)["k"] == their_pointer
        # A new blob for the key would live under the merge commit's hash.
        assert f"{result.commit}:k" not in main.store.keys()

    def test_ours_keeps_our_pointer_and_writes_no_blob(self):
        main, worker = _branched_versioned()
        main.commit({"k": b"our-value"})
        worker.commit({"k": b"their-value"})
        our_pointer = main._load_keyset(main.current_commit)["k"]

        result = main.merge_heads(worker.current_commit, default_merge=ours)
        assert result.merged
        assert main.get("k") == b"our-value"
        assert main._load_keyset(result.commit)["k"] == our_pointer
        assert f"{result.commit}:k" not in main.store.keys()

    def test_side_pick_counts_as_auto_merged(self):
        main, worker = _branched_versioned()
        main.commit({"k": b"our-value"})
        worker.commit({"k": b"their-value"})

        result = main.merge_heads(worker.current_commit, default_merge=ours)
        assert result.auto_merged_keys == ("k",)

    def test_ours_removes_the_key_when_we_removed_it(self):
        main, worker = _branched_versioned({"k": b"base"})
        main.commit(removals={"k"})
        worker.commit({"k": b"their-value"})

        result = main.merge_heads(worker.current_commit, default_merge=ours)
        assert result.merged
        assert "k" not in main
        assert main.get("k") is None
        assert result.auto_merged_keys == ("k",)

    def test_theirs_removes_the_key_when_they_removed_it(self):
        main, worker = _branched_versioned({"k": b"base"})
        main.commit({"k": b"our-value"})
        worker.commit(removals={"k"})

        result = main.merge_heads(worker.current_commit, default_merge=theirs)
        assert result.merged
        assert "k" not in main
        assert main.get("k") is None

    def test_ours_keeps_our_value_when_they_removed_it(self):
        main, worker = _branched_versioned({"k": b"base"})
        main.commit({"k": b"our-value"})
        worker.commit(removals={"k"})

        result = main.merge_heads(worker.current_commit, default_merge=ours)
        assert result.merged
        assert main.get("k") == b"our-value"

    def test_side_pick_by_prefix_on_the_concurrent_commit_path(self):
        first, second = _concurrent_versioned()
        second.set_merge_prefix("keep/", ours)
        first.commit({"keep/1": b"theirs"})

        result = second.commit({"keep/1": b"ours"})
        assert result.merged
        assert second.get("keep/1") == b"ours"
        # The kept pointer belongs to our own commit, not the merge one.
        assert not second._load_keyset(result.commit)["keep/1"].startswith(
            f"{result.commit}:"
        )
        assert f"{result.commit}:keep/1" not in second.store.keys()


class TestByteEqualContested:
    def test_identical_bytes_merge_clean_across_branches(self):
        main, worker = _branched_versioned()
        # Both sides write the same bytes to "k"; the extra keys make the
        # two commit hashes differ, so the blob pointers differ too.
        main.commit({"k": b"same", "a": b"1"})
        worker.commit({"k": b"same", "b": b"2"})
        assert (
            main._load_keyset(main.current_commit)["k"]
            != worker._load_keyset(worker.current_commit)["k"]
        )

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("k") == b"same"
        assert "k" not in result.auto_merged_keys
        assert (
            main._load_keyset(result.commit)["k"]
            == worker._load_keyset(worker.current_commit)["k"]
        )

    def test_identical_bytes_merge_clean_on_the_concurrent_commit_path(self):
        first, second = _concurrent_versioned()
        first.commit({"k": b"same", "a": b"1"})

        result = second.commit({"k": b"same", "b": b"2"})
        assert result.merged
        assert second.get("k") == b"same"
        assert second.get("a") == b"1"
        assert second.get("b") == b"2"

    def test_identical_bytes_do_not_call_the_merge_fn(self):
        main, worker = _branched_versioned()
        calls: list[str] = []

        def spy(old, our, their):
            calls.append("called")
            return b"merged"

        main.commit({"k": b"same", "a": b"1"})
        worker.commit({"k": b"same", "b": b"2"})

        result = main.merge_heads(worker.current_commit, default_merge=spy)
        assert result.merged
        assert calls == []
        assert main.get("k") == b"same"

    def test_differing_bytes_still_conflict(self):
        main, worker = _branched_versioned()
        main.commit({"k": b"ours", "a": b"1"})
        worker.commit({"k": b"theirs", "b": b"2"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit)
        assert exc_info.value.conflicting_keys == {"k"}

    def test_removed_versus_modified_still_conflicts(self):
        main, worker = _branched_versioned({"k": b"base"})
        main.commit(removals={"k"})
        worker.commit({"k": b"their-value"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit)
        assert exc_info.value.conflicting_keys == {"k"}

    def test_modified_versus_removed_still_conflicts(self):
        main, worker = _branched_versioned({"k": b"base"})
        main.commit({"k": b"our-value"})
        worker.commit(removals={"k"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit)
        assert exc_info.value.conflicting_keys == {"k"}


class TestStagedMergePolicy:
    def test_prefix_registration_on_commit_path(self):
        first, second = _concurrent_staged()
        second.set_merge_prefix("runs/", lambda old, our, their: our + their)
        first["runs/1"] = "theirs"
        first.commit()

        second["runs/1"] = "ours"
        result = second.commit()
        assert result.merged
        assert second["runs/1"] == "ourstheirs"

    def test_exact_key_beats_prefix_on_commit_path(self):
        first, second = _concurrent_staged()
        second.set_merge_prefix("runs/", lambda old, our, their: "by-prefix")
        second.set_merge_fn("runs/1", lambda old, our, their: "by-key")
        first["runs/1"] = "theirs"
        first["runs/2"] = "theirs"
        first.commit()

        second["runs/1"] = "ours"
        second["runs/2"] = "ours"
        result = second.commit()
        assert result.merged
        assert second["runs/1"] == "by-key"
        assert second["runs/2"] == "by-prefix"

    def test_per_call_prefixes_override_instance_on_commit_path(self):
        first, second = _concurrent_staged()
        second.set_merge_prefix("runs/", lambda old, our, their: "instance")
        first["runs/1"] = "theirs"
        first.commit()

        second["runs/1"] = "ours"
        result = second.commit(
            merge_prefixes={"runs/": lambda old, our, their: "per-call"}
        )
        assert result.merged
        assert second["runs/1"] == "per-call"

    def test_prefix_registration_on_merge_path(self):
        main, worker = _branched_staged()
        main.set_merge_prefix("runs/", lambda old, our, their: "by-prefix")
        main["runs/1"] = "ours"
        main.commit()
        worker["runs/1"] = "theirs"
        worker.commit()

        result = main.merge(worker.current_commit)
        assert result.merged
        assert main["runs/1"] == "by-prefix"

    def test_per_call_prefixes_override_instance_on_merge_path(self):
        main, worker = _branched_staged()
        main.set_merge_prefix("runs/", lambda old, our, their: "instance")
        main["runs/1"] = "ours"
        main.commit()
        worker["runs/1"] = "theirs"
        worker.commit()

        result = main.merge(
            worker.current_commit,
            merge_prefixes={"runs/": lambda old, our, their: "per-call"},
        )
        assert result.merged
        assert main["runs/1"] == "per-call"

    def test_theirs_keeps_their_pointer_through_staged(self):
        main, worker = _branched_staged()
        main["k"] = "our-value"
        main.commit()
        worker["k"] = "their-value"
        worker.commit()
        their_pointer = worker.versioned._load_keyset(worker.current_commit)["k"]

        result = main.merge(worker.current_commit, default_merge=theirs)
        assert result.merged
        assert main["k"] == "their-value"
        keyset = main.versioned._load_keyset(result.commit)
        assert keyset["k"] == their_pointer
        assert f"{result.commit}:k" not in main.versioned.store.keys()

    def test_ours_keeps_our_pointer_through_staged_commit(self):
        first, second = _concurrent_staged()
        second.set_merge_prefix("keep/", ours)
        first["keep/1"] = "theirs"
        first.commit()

        second["keep/1"] = "ours"
        result = second.commit()
        assert result.merged
        assert second["keep/1"] == "ours"
        assert f"{result.commit}:keep/1" not in second.versioned.store.keys()

    def test_decoded_merge_fn_may_return_a_merge_choice(self):
        main, worker = _branched_staged()

        def keep_the_longer(old, our, their):
            return MergeChoice.OURS if len(our) >= len(their) else MergeChoice.THEIRS

        main.set_merge_prefix("doc/", keep_the_longer)
        main["doc/a"] = "a longer value"
        main["doc/b"] = "short"
        main.commit()
        worker["doc/a"] = "short"
        worker["doc/b"] = "a longer value"
        worker.commit()
        their_pointer = worker.versioned._load_keyset(worker.current_commit)["doc/b"]

        result = main.merge(worker.current_commit)
        assert result.merged
        assert main["doc/a"] == "a longer value"
        assert main["doc/b"] == "a longer value"
        assert main.versioned._load_keyset(result.commit)["doc/b"] == their_pointer
        assert f"{result.commit}:doc/b" not in main.versioned.store.keys()

    def test_identical_values_merge_clean_on_commit_path(self):
        first, second = _concurrent_staged()
        first["k"] = {"same": [1, 2, 3]}
        first["a"] = 1
        first.commit()

        second["k"] = {"same": [1, 2, 3]}
        second["b"] = 2
        result = second.commit()
        assert result.merged
        assert second["k"] == {"same": [1, 2, 3]}
        assert second["a"] == 1
        assert second["b"] == 2

    def test_identical_values_merge_clean_on_merge_path(self):
        main, worker = _branched_staged()
        main["k"] = {"same": [1, 2, 3]}
        main["a"] = 1
        main.commit()
        worker["k"] = {"same": [1, 2, 3]}
        worker["b"] = 2
        worker.commit()

        result = main.merge(worker.current_commit)
        assert result.merged
        assert main["k"] == {"same": [1, 2, 3]}
        assert main["b"] == 2

    def test_removed_versus_modified_still_conflicts_on_merge_path(self):
        main, worker = _branched_staged("k", "base")
        del main["k"]
        main.commit()
        worker["k"] = "their-value"
        worker.commit()

        with pytest.raises(MergeConflict) as exc_info:
            main.merge(worker.current_commit)
        assert exc_info.value.conflicting_keys == {"k"}


class TestMergeChoicePolicy:
    """A registered MergeChoice governs every key either side changed."""

    def test_their_added_key_is_dropped_under_an_ours_prefix(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"runs/mine": b"ours"})
        worker.commit({"runs/theirs": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert "runs/theirs" not in main
        assert main.get("runs/mine") == b"ours"
        assert "runs/theirs" in result.auto_merged_keys

    def test_their_removed_key_survives_under_an_ours_prefix(self):
        main, worker = _branched_versioned({"runs/1": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"other": b"1"})
        worker.commit(removals={"runs/1"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"base"

    def test_their_modified_key_is_ignored_under_an_ours_prefix(self):
        main, worker = _branched_versioned({"runs/1": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"other": b"1"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"base"

    def test_our_removal_stays_removed_under_an_ours_prefix(self):
        main, worker = _branched_versioned({"runs/1": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit(removals={"runs/1"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert "runs/1" not in main

    def test_our_added_key_is_dropped_under_a_theirs_prefix(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.THEIRS)
        main.commit({"runs/mine": b"ours"})
        worker.commit({"runs/theirs": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert "runs/mine" not in main
        assert main.get("runs/theirs") == b"theirs"

    def test_their_removal_removes_under_a_theirs_prefix(self):
        main, worker = _branched_versioned({"runs/1": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.THEIRS)
        main.commit({"runs/1": b"ours"})
        worker.commit(removals={"runs/1"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert "runs/1" not in main

    def test_our_modification_is_discarded_under_a_theirs_prefix(self):
        main, worker = _branched_versioned({"runs/1": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.THEIRS)
        main.commit({"runs/1": b"ours"})
        worker.commit({"other": b"1"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/1") == b"base"

    def test_policy_keys_are_never_read(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"runs/1": b"ours"})
        worker.commit({"runs/1": b"theirs", "runs/2": b"theirs"})

        reads: list[str] = []
        original = main._read_blob
        main._read_blob = lambda cid: (reads.append(cid), original(cid))[1]
        try:
            result = main.merge_heads(worker.current_commit)
        finally:
            main._read_blob = original
        assert result.merged
        assert reads == []

    def test_policy_writes_no_blob_and_keeps_our_pointer(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"runs/1": b"ours"})
        worker.commit({"runs/1": b"theirs"})
        our_pointer = main._load_keyset(main.current_commit)["runs/1"]

        result = main.merge_heads(worker.current_commit)
        assert main._load_keyset(result.commit)["runs/1"] == our_pointer
        assert f"{result.commit}:runs/1" not in main.store.keys()

    def test_untouched_keys_outside_the_prefix_are_unaffected(self):
        main, worker = _branched_versioned({"kept": b"base"})
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.commit({"runs/1": b"ours"})
        worker.commit({"outside": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("kept") == b"base"
        assert main.get("outside") == b"theirs"

    def test_exact_key_choice_beats_prefix_fn(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-prefix"))
        main.set_merge_fn("runs/1", MergeChoice.OURS)
        main.commit({"runs/1": b"ours", "runs/2": b"ours"})
        worker.commit({"runs/1": b"theirs", "runs/2": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert main.get("runs/1") == b"ours"
        assert main.get("runs/2") == b"by-prefix"
        assert result.merged

    def test_longer_fn_prefix_beats_shorter_choice_prefix(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.set_merge_prefix("runs/hot/", _mark(b"by-fn"))
        main.commit({"runs/cold/1": b"ours", "runs/hot/1": b"ours"})
        worker.commit({"runs/cold/1": b"theirs", "runs/hot/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/cold/1") == b"ours"
        assert main.get("runs/hot/1") == b"by-fn"

    def test_longer_choice_prefix_beats_shorter_fn_prefix(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-fn"))
        main.set_merge_prefix("runs/hot/", MergeChoice.THEIRS)
        main.commit({"runs/cold/1": b"ours", "runs/hot/1": b"ours"})
        worker.commit({"runs/cold/1": b"theirs", "runs/hot/1": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("runs/cold/1") == b"by-fn"
        assert main.get("runs/hot/1") == b"theirs"

    def test_a_longer_fn_prefix_still_sees_only_contested_keys(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.set_merge_prefix("runs/hot/", _mark(b"by-fn"))
        main.commit({"other": b"1"})
        worker.commit({"runs/hot/new": b"theirs"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        # Their-only add under a fn prefix lands as it always has.
        assert main.get("runs/hot/new") == b"theirs"

    def test_per_call_prefixes_accept_a_choice(self):
        main, worker = _branched_versioned()
        main.commit({"other": b"1"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(
            worker.current_commit,
            merge_prefixes={"runs/": MergeChoice.OURS},
        )
        assert result.merged
        assert "runs/1" not in main

    def test_per_call_choice_overrides_an_instance_fn(self):
        main, worker = _branched_versioned()
        main.set_merge_prefix("runs/", _mark(b"by-fn"))
        main.commit({"runs/1": b"ours"})
        worker.commit({"runs/1": b"theirs"})

        result = main.merge_heads(
            worker.current_commit,
            merge_prefixes={"runs/": MergeChoice.THEIRS},
        )
        assert result.merged
        assert main.get("runs/1") == b"theirs"

    def test_choice_as_default_merge_hands_over_every_changed_key(self):
        main, worker = _branched_versioned()
        main.commit({"mine": b"ours"})
        worker.commit({"theirs": b"theirs"})

        result = main.merge_heads(worker.current_commit, default_merge=MergeChoice.OURS)
        assert result.merged
        assert main.get("mine") == b"ours"
        assert "theirs" not in main

    def test_policy_applies_on_the_concurrent_commit_path(self):
        first, second = _concurrent_versioned()
        second.set_merge_prefix("runs/", MergeChoice.OURS)
        first.commit({"runs/1": b"theirs", "elsewhere": b"theirs"})

        result = second.commit({"other": b"1"})
        assert result.merged
        assert "runs/1" not in second
        assert second.get("elsewhere") == b"theirs"


class TestStagedMergeChoicePolicy:
    def test_their_added_key_is_dropped_under_an_ours_prefix(self):
        main, worker = _branched_staged()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main["runs/mine"] = "ours"
        main.commit()
        worker["runs/theirs"] = "theirs"
        worker.commit()

        result = main.merge(worker.current_commit)
        assert result.merged
        assert "runs/theirs" not in main
        assert main["runs/mine"] == "ours"

    def test_their_added_key_is_dropped_on_the_commit_path(self):
        first, second = _concurrent_staged()
        second.set_merge_prefix("runs/", MergeChoice.OURS)
        first["runs/theirs"] = "theirs"
        first.commit()

        second["other"] = 1
        result = second.commit()
        assert result.merged
        assert "runs/theirs" not in second

    def test_choice_registration_is_not_decoded(self):
        """A MergeChoice must reach the resolver without the decode wrapper."""
        main, worker = _branched_staged()
        decoded: list[bytes] = []

        def spy_decoder(raw):
            decoded.append(raw)
            return pickle.loads(raw)

        main = Staged(main.versioned, decoder=spy_decoder)
        main.set_merge_prefix("runs/", MergeChoice.THEIRS)
        main["runs/1"] = "ours"
        main.commit()
        worker["runs/1"] = "theirs"
        worker.commit()

        decoded.clear()
        result = main.merge(worker.current_commit)
        assert result.merged
        assert decoded == []
        assert main["runs/1"] == "theirs"

    def test_per_call_prefixes_accept_a_choice(self):
        main, worker = _branched_staged()
        main["other"] = 1
        main.commit()
        worker["runs/1"] = "theirs"
        worker.commit()

        result = main.merge(
            worker.current_commit, merge_prefixes={"runs/": MergeChoice.OURS}
        )
        assert result.merged
        assert "runs/1" not in main


class TestReadmeExamples:
    """The README's Merging examples, run exactly as they are written."""

    def test_text_merge_example(self):
        import kvgit
        from kvgit import text_merge

        main = kvgit.store()
        main["notes"] = "alpha\nbeta\n"
        main.commit()

        edits = main.create_branch("edits")
        edits["notes"] = "alpha\nBETA\n"
        edits.commit()

        main["notes"] = "ALPHA\nbeta\n"
        main.commit()

        main.merge(edits.current_commit, default_merge=text_merge())
        assert main["notes"] == "ALPHA\nBETA\n"

    def test_registration_example(self):
        import kvgit
        from kvgit import MergeChoice, text_merge

        main = kvgit.store()
        main.set_merge_prefix("runs/", MergeChoice.OURS)
        main.set_merge_fn("runs/index", text_merge())

        main["runs/index"] = "base\n"
        main["runs/1"] = "ours"
        main.commit()
        worker = main.create_branch("worker")
        worker["runs/index"] = "theirs\n"
        worker["runs/2"] = "theirs"
        worker.commit()
        main["runs/index"] = "ours\n"
        main.commit()

        result = main.merge(worker.current_commit)
        assert result.merged
        # The exact-key text merge marks the contested index...
        assert main["runs/index"] == (
            "<<<<<<< ours\nours\n=======\ntheirs\n>>>>>>> theirs\n"
        )
        # ...while the prefix policy keeps our runs/ and drops their new key.
        assert main["runs/1"] == "ours"
        assert "runs/2" not in main
