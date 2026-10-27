"""``Repo.resolve``: a three-way merge over values the caller names,
``Repo.merge_rule``, and ``EMPTY_ROOT``."""

import pytest

from kvgit import (
    EMPTY_ROOT,
    ROOT_COMMIT,
    MergeChoice,
    MergeConflict,
    Repo,
    counter,
    text_merge,
)
from kvgit.kv.memory import Memory


def repo(**options) -> Repo:
    return Repo(Memory(), **options)


class TestResolve:
    def test_each_sides_changes_are_taken(self):
        r = repo()
        base = {"a": 1, "b": 2, "gone": 3}
        ours = {"a": 10, "b": 2, "gone": 3}
        theirs = {"a": 1, "b": 20, "new": 4}
        out = r.resolve(base, ours, theirs)
        assert out.values == {"b": 20, "new": 4}
        assert out.removals == {"gone"}
        assert sorted(out.carried_keys) == ["b", "gone", "new"]
        assert out.auto_merged_keys == ()

    def test_changes_made_alike_agree(self):
        out = repo().resolve({"a": 1}, {"a": 2}, {"a": 2})
        assert out.values == {}
        assert not out.removals

    def test_a_contested_key_without_a_rule_conflicts(self):
        with pytest.raises(MergeConflict) as e:
            repo().resolve({"a": 1, "b": 1}, {"a": 2, "b": 2}, {"a": 3, "b": 2})
        assert e.value.conflicting_keys == {"a"}

    def test_a_merge_function_decides_a_contested_key(self):
        out = repo().resolve({"n": 1}, {"n": 3}, {"n": 5}, merge_fns={"n": counter()})
        assert out.values == {"n": 7}
        assert out.auto_merged_keys == ("n",)

    def test_a_side_policy_stands_for_every_key_it_covers(self):
        r = repo(merge_prefixes={"mine/": MergeChoice.OURS})
        out = r.resolve(
            {"mine/a": 1, "other": 1},
            {"mine/a": 2, "other": 1},
            {"mine/a": 3, "mine/b": 9, "other": 5},
        )
        # theirs' changes under the policy are not taken, the rest are
        assert out.values == {"other": 5}
        assert sorted(out.auto_merged_keys) == ["mine/a", "mine/b"]

    def test_the_call_s_rules_sit_over_the_repository_s(self):
        r = repo(default_merge=MergeChoice.OURS)
        assert r.resolve({"k": 1}, {"k": 2}, {"k": 3}).values == {}
        out = r.resolve({"k": 1}, {"k": 2}, {"k": 3}, default_merge=MergeChoice.THEIRS)
        assert out.values == {"k": 3}

    def test_a_rule_answering_a_side_takes_that_side(self):
        out = repo().resolve(
            {"k": 1},
            {"k": 2},
            {"k": 3},
            merge_fns={"k": lambda o, a, b: MergeChoice.THEIRS},
        )
        assert out.values == {"k": 3}

    def test_the_text_merge_merges_lines(self):
        r = repo(codec="bytes")
        base = {"doc": b"a\nb\nc\n"}
        ours = {"doc": b"A\nb\nc\n"}
        theirs = {"doc": b"a\nb\nC\n"}
        out = r.resolve(base, ours, theirs, merge_fns={"doc": text_merge()})
        assert out.values == {"doc": b"A\nb\nC\n"}

    def test_values_are_compared_by_their_encoding(self):
        # Equal values under the codec are the same value, whatever object
        # holds them.
        out = repo().resolve({"k": [1, 2]}, {"k": [1, 2]}, {"k": (1, 2)})
        assert out.values == {"k": (1, 2)}

    def test_nothing_is_read_or_written(self):
        store = Memory()
        r = Repo(store)
        before = dict(store.items())
        r.resolve({"k": 1}, {"k": 2}, {"k": 3}, merge_fns={"k": counter()})
        assert dict(store.items()) == before


class TestMergeRule:
    def test_exact_then_longest_prefix_then_default(self):
        r = repo(
            merge_fns={"runs/1": MergeChoice.THEIRS},
            merge_prefixes={"runs/": MergeChoice.OURS},
            default_merge=MergeChoice.THEIRS,
        )
        assert r.merge_rule("runs/1") is MergeChoice.THEIRS
        assert r.merge_rule("runs/2") is MergeChoice.OURS
        assert r.merge_rule("runs/x/y", merge_prefixes={"runs/x/": counter}) is counter
        assert r.merge_rule("other") is MergeChoice.THEIRS

    def test_none_without_a_rule(self):
        assert repo().merge_rule("k") is None


def test_empty_root_is_the_root_of_an_empty_keyset():
    r = repo()
    r.worktree("main", create=True)
    assert r.get_commit(ROOT_COMMIT).root == EMPTY_ROOT


class _FalsyRule:
    """A merge rule that is falsy, as a callable with ``__len__`` can be."""

    def __init__(self, answer):
        self.answer = answer

    def __call__(self, old, ours, theirs):
        return self.answer

    def __bool__(self) -> bool:
        return False


def test_a_falsy_rule_still_counts():
    rule = _FalsyRule(42)
    r = repo(default_merge=MergeChoice.OURS)
    assert r.merge_rule("k", default_merge=rule) is rule
    out = r.resolve({"k": 1}, {"k": 2}, {"k": 3}, default_merge=rule)
    assert out.values == {"k": 42}


def test_a_falsy_rule_counts_on_a_worktree_commit():
    store = Memory()
    r = Repo(store, default_merge=MergeChoice.OURS)
    a = r.worktree("main", create=True)
    a["k"] = 1
    a.commit()
    b = r.worktree("main")
    a["k"] = 2
    a.commit()
    b["k"] = 3
    b.commit(default_merge=_FalsyRule(42))
    assert r.snapshot(branch="main")["k"] == 42


def test_each_key_takes_back_its_own_object():
    # Two keys holding equal values share a blob; each gets its own.
    mine, theirs_obj = [1], [1]
    out = repo().resolve({"a": mine}, {"a": mine}, {"a": mine, "b": theirs_obj})
    assert out.values["b"] is theirs_obj
