"""What a merge reports, and the structural diff between commits."""

import random

import pytest
from support import fork, pointers, worktree

from kvgit import DiffResult, MergeChoice


def diff_keysets(a: dict[str, str], b: dict[str, str]) -> DiffResult:
    """Keys added, removed and modified from keyset ``a`` to ``b``,
    comparing their blob pointers."""
    return DiffResult(
        added=frozenset(b.keys() - a.keys()),
        removed=frozenset(a.keys() - b.keys()),
        modified=frozenset(k for k in a.keys() & b.keys() if a[k] != b[k]),
    )


# -- What a merge reports --


class TestReportedKeys:
    def _diverged(self):
        wt = worktree()
        wt["both"] = 0
        wt["mine"] = 0
        wt["yours"] = 0
        wt["owned/x"] = 0
        wt["same"] = 0
        wt.commit()
        dev = fork(wt, "dev")
        dev["both"] = 2
        dev["yours"] = 2
        dev["owned/x"] = 2
        dev["same"] = 9
        dev["new"] = 2
        dev.commit()
        wt["both"] = 1
        wt["mine"] = 1
        wt["same"] = 9
        wt.commit()
        return wt, dev

    def test_merge_reports_rule_resolved_and_carried_keys(self):
        wt, _ = self._diverged()
        wt.set_merge_prefix("owned/", MergeChoice.OURS)
        result = wt.merge(branch="dev", merge_fns={"both": lambda o, a, b: a + b})
        assert result.strategy == "three_way"
        assert set(result.auto_merged_keys) == {"both", "owned/x"}
        assert set(result.carried_keys) == {"yours", "new"}
        assert (wt["both"], wt["mine"], wt["yours"], wt["new"]) == (3, 1, 2, 2)
        assert (wt["owned/x"], wt["same"]) == (0, 9)

    def test_an_identical_change_is_reported_only_under_a_policy(self):
        wt = worktree()
        wt["owned/x"] = 0
        wt["plain"] = 0
        wt.commit()
        dev = fork(wt, "dev")
        dev["owned/x"] = 5
        dev["plain"] = 5
        dev.commit()
        wt["owned/x"] = 5
        wt["plain"] = 5
        wt.commit()
        result = wt.merge(branch="dev", merge_prefixes={"owned/": MergeChoice.OURS})
        assert result.auto_merged_keys == ("owned/x",)
        assert result.carried_keys == ()

    def test_a_plain_commit_carries_nothing(self):
        wt = worktree()
        wt["k"] = 1
        result = wt.commit()
        assert result.strategy == "fast_forward"
        assert result.carried_keys == () and result.auto_merged_keys == ()

    def test_a_fast_forward_merge_carries_what_their_side_changed(self):
        wt = worktree()
        wt["a"] = 1
        wt["b"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2
        dev["c"] = 3
        del dev["a"]
        dev.commit()
        result = wt.merge(branch="dev")
        assert result.strategy == "fast_forward"
        assert result.carried_keys == ("a", "b", "c")
        assert result.auto_merged_keys == ()

    def test_a_cherry_pick_carries_the_picked_change(self):
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2
        dev.commit()
        wt["c"] = 3
        wt.commit()
        result = wt.cherry_pick(dev.head)
        assert result.strategy == "apply"
        assert result.carried_keys == ("b",)
        assert (wt["a"], wt["b"], wt["c"]) == (1, 2, 3)


class TestRepoDiff:
    @pytest.mark.parametrize("seed", range(5))
    def test_matches_a_diff_of_the_full_keysets(self, seed):
        rng = random.Random(seed)
        wt = worktree()
        heads = []
        for _ in range(12):
            for _ in range(rng.randrange(1, 30)):
                key = f"k{rng.randrange(200)}"
                if key in wt and rng.random() < 0.3:
                    del wt[key]
                else:
                    wt[key] = rng.randrange(4)
            wt.commit()
            heads.append(wt.head)
        store = wt.repo.store
        for a in heads:
            for b in rng.sample(heads, 4):
                expected = diff_keysets(pointers(store, a), pointers(store, b))
                assert wt.repo.diff(a, b) == expected
