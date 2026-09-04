"""Tests for kvgit.merges: marker-merge with git-verified expectations."""

import random
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from kvgit import MergeConflict, VersionedKV as Versioned
from kvgit.kv.memory import Memory
from kvgit.merges import CantMark, make_text_merge, text

GIT = shutil.which("git")
needs_git = pytest.mark.skipif(GIT is None, reason="git binary not available")

_ALPHABET = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", ""]

BASE = b"a\nb\nc\nd\n"


class TestCleanMerges:
    def test_disjoint_edits_merge(self):
        assert text(BASE, b"a\nB\nc\nd\n", b"a\nb\nc\nD\n") == b"a\nB\nc\nD\n"

    def test_identical_edits_agree(self):
        assert text(BASE, b"a\nX\nc\nd\n", b"a\nX\nc\nd\n") == b"a\nX\nc\nd\n"

    def test_one_side_unchanged(self):
        assert text(BASE, BASE, b"a\nb\nc\nD\n") == b"a\nb\nc\nD\n"
        assert text(BASE, b"a\nB\nc\nd\n", BASE) == b"a\nB\nc\nd\n"

    def test_added_one_side(self):
        assert text(None, None, b"new\n") == b"new\n"
        assert text(None, b"new\n", None) == b"new\n"

    def test_both_deleted(self):
        assert text(BASE, None, None) == b""

    def test_deleted_vs_unchanged_deletes(self):
        assert text(BASE, None, BASE) == b""
        assert text(BASE, BASE, None) == b""

    def test_no_trailing_newline_preserved(self):
        assert text(b"a\nb", b"a\nB", b"a\nb") == b"a\nB"

    def test_empty_inputs(self):
        assert text(b"", b"", b"") == b""


class TestConflicts:
    def test_overlap_markers(self):
        out = text(BASE, b"a\nX\nc\nd\n", b"a\nY\nc\nd\n")
        assert out == b"a\n<<<<<<< ours\nX\n=======\nY\n>>>>>>> theirs\nc\nd\n"

    def test_added_added_different(self):
        out = text(None, b"hello\n", b"world\n")
        assert out == b"<<<<<<< ours\nhello\n=======\nworld\n>>>>>>> theirs\n"

    def test_added_added_same(self):
        assert text(None, b"same\n", b"same\n") == b"same\n"

    def test_delete_vs_modify(self):
        out = text(BASE, None, b"a\nb\nc\nD\n")
        assert out == (b"<<<<<<< ours\n=======\na\nb\nc\nD\n>>>>>>> theirs\n")

    def test_modify_vs_delete(self):
        out = text(BASE, b"a\nB\nc\nd\n", None)
        assert out == (b"<<<<<<< ours\na\nB\nc\nd\n=======\n>>>>>>> theirs\n")

    def test_custom_labels(self):
        fn = make_text_merge(ours_label="main", theirs_label="dev")
        out = fn(BASE, b"a\nX\nc\nd\n", b"a\nY\nc\nd\n")
        assert b"<<<<<<< main\n" in out
        assert b">>>>>>> dev\n" in out

    def test_label_newline_rejected(self):
        with pytest.raises(ValueError):
            make_text_merge(ours_label="a\nb")

    def test_deterministic(self):
        args = (BASE, b"a\nX\nc\nd\n", b"a\nY\nc\nd\n")
        assert text(*args) == text(*args)


class TestReviewRegression:
    """Regression tests for PR #34 review findings (all reproduced)."""

    def test_overlapping_groups_keep_full_span(self):
        """P1: overlapping hunks over different base spans must show each
        side's full content over the union — resolving to ours must not
        drop unchanged lines (here: `c`). Verified byte-identical to
        `git merge-file`."""
        out = text(b"a\nb\nc\n", b"a\nB\nc\n", b"a\nY\nZ\n")
        assert out == b"a\n<<<<<<< ours\nB\nc\n=======\nY\nZ\n>>>>>>> theirs\n"

    def test_empty_base_delete_vs_write_conflicts(self):
        """P2: an empty base file one side deletes and the other writes
        is modify/delete, not a clean add."""
        out = text(b"", None, b"x\n")
        assert out == b"<<<<<<< ours\n=======\nx\n>>>>>>> theirs\n"
        out = text(b"", b"x\n", None)
        assert out == b"<<<<<<< ours\nx\n=======\n>>>>>>> theirs\n"

    def test_unterminated_survivor_terminated(self):
        """P2: whole-file delete/modify where the survivor lacks a trailing
        newline must not glue content to the closing marker."""
        assert text(b"a", None, b"B") == b"<<<<<<< ours\n=======\nB\n>>>>>>> theirs\n"
        assert text(b"a", b"B", None) == b"<<<<<<< ours\nB\n=======\n>>>>>>> theirs\n"


class TestDocumentedDivergence:
    def test_adjacent_insert_merges_clean(self):
        """Ours changes line 2, theirs inserts right after it.

        git merge-file reports a conflict here (hunk-context overlap);
        the regions genuinely don't overlap, so we merge cleanly. Pinned
        as a known, safe-direction divergence: clean-where-git-conflicts
        can never silently lose content the way the reverse could.
        """
        out = text(BASE, b"a\nB\nc\nd\n", b"a\nb\nX\nc\nd\n")
        assert out == b"a\nB\nX\nc\nd\n"


class TestCantMark:
    def test_nul_byte(self):
        with pytest.raises(CantMark):
            text(BASE, b"a\x00b", BASE)

    def test_invalid_utf8(self):
        with pytest.raises(CantMark):
            text(BASE, b"\xff\xfe binary", BASE)

    def test_oversize(self):
        big = b"x" * (1024 * 1024 + 1)
        with pytest.raises(CantMark):
            text(b"", big, b"")


def _git_merge(old: bytes, ours: bytes, theirs: bytes) -> tuple[bytes, int]:
    """Run git merge-file; returns (stdout, exit code)."""
    assert GIT is not None
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for name, data in (("old", old), ("ours", ours), ("theirs", theirs)):
            p = Path(d) / name
            p.write_bytes(data)
            paths.append(str(p))
        proc = subprocess.run(
            [
                GIT,
                "merge-file",
                "-p",
                "-L",
                "ours",
                "-L",
                "base",
                "-L",
                "theirs",
                paths[1],
                paths[0],
                paths[2],
            ],
            capture_output=True,
            env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        )
        return proc.stdout, proc.returncode


@needs_git
class TestGitDifferential:
    """Ours must match git merge-file byte-exactly on agreement-zone inputs.

    The generator keeps edits separated so hunk contexts cannot overlap
    (the zone where both algorithms provably agree); trickier shapes live
    as pinned cases above or in the adversarial test below. Seed-pinned:
    failures reproduce deterministically.
    """

    def _triple(self, rng: random.Random) -> tuple[bytes, bytes, bytes]:
        n = rng.randint(2, 8)
        base = [rng.choice(_ALPHABET) + f"-{i}" for i in range(n)]
        ours, theirs = list(base), list(base)
        # Edits at distinct positions with at least one untouched line
        # between any two edit sites.
        positions = rng.sample(range(n), k=min(n, rng.randint(0, 3)))
        positions.sort()
        ok = True
        for prev, cur in zip([-10, *positions], positions, strict=False):
            if prev != -10 and cur - prev < 2:
                ok = False
        if not ok:
            return self._triple(rng)
        for p in positions:
            choice = rng.random()
            if choice < 0.4:
                ours[p] = f"ours-{p}"
                theirs[p] = f"ours-{p}"  # identical edit: must agree
            elif choice < 0.7:
                ours[p] = f"ours-{p}"
            else:
                theirs[p] = f"theirs-{p}"

        def join(lines: list[str]) -> bytes:
            return ("".join(line + "\n" for line in lines)).encode()

        return join(base), join(ours), join(theirs)

    def test_matches_git(self):
        rng = random.Random(0xC0FFEE)
        checked_clean = 0
        for _ in range(150):
            old, ours, theirs = self._triple(rng)
            git_out, code = _git_merge(old, ours, theirs)
            mine = text(old, ours, theirs)
            if code == 0:
                # Clean merges are uniquely determined: byte equality.
                assert mine == git_out
                checked_clean += 1
            else:
                # Conflict: agreement on conflict-hood.
                assert b"<<<<<<< ours\n" in mine
        assert checked_clean > 50  # generator must exercise clean merges

    def test_adversarial_properties(self):
        """Where we may legitimately diverge from git (adjacent edits,
        unterminated lines), assert the sound properties instead: clean
        git merges still match byte-exactly, and our output never
        invents lines or emits unbalanced markers."""
        rng = random.Random(0xBEEF)
        checked_clean = 0
        for _ in range(150):
            old, ours, theirs = self._triple(rng)
            if rng.random() < 0.5:
                # Strip a trailing newline: the adjacency trigger.
                if rng.random() < 0.5 and ours.endswith(b"\n"):
                    ours = ours.removesuffix(b"\n")
                elif theirs.endswith(b"\n"):
                    theirs = theirs.removesuffix(b"\n")
            git_out, code = _git_merge(old, ours, theirs)
            mine = text(old, ours, theirs)
            if code == 0:
                assert mine == git_out
                checked_clean += 1
                continue
            inputs = {
                ln.removesuffix("\n")
                for data in (old, ours, theirs)
                for ln in data.decode("utf-8").splitlines(keepends=True)
            }
            opens = closes = mids = 0
            for ln in mine.decode("utf-8").splitlines(keepends=True):
                if ln.startswith("<<<<<<< "):
                    opens += 1
                elif ln.startswith(">>>>>>> "):
                    closes += 1
                elif ln == "=======\n":
                    mids += 1
                else:
                    # No invented content (modulo the terminator): every
                    # emitted line comes verbatim from some input.
                    assert ln.removesuffix("\n") in inputs
            assert opens == mids == closes
        assert checked_clean > 0  # the net must catch clean merges too


class TestVersionedIntegration:
    def _two_branches(self):
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"doc": b"a\nb\nc\nd\n"})
        v2 = Versioned(store)
        return v1, v2

    def test_default_merge_automerges_text(self):
        from kvgit.merges import text as text_merge

        v1, v2 = self._two_branches()
        v1.commit({"doc": b"a\nB\nc\nd\n"})
        result = v2.commit({"doc": b"a\nb\nc\nD\n"}, default_merge=text_merge)
        assert result
        assert v2.get("doc") == b"a\nB\nc\nD\n"

    def test_conflict_filed_without_fn(self):
        v1, v2 = self._two_branches()
        v1.commit({"doc": b"a\nB\nc\nd\n"})
        with pytest.raises(MergeConflict) as exc_info:
            v2.commit({"doc": b"a\nb\nc\nD\n"})
        assert "doc" in exc_info.value.conflicting_keys

    def test_binary_conflict_carries_cantmark(self):
        from kvgit.merges import text as text_merge

        v1, v2 = self._two_branches()
        v1.commit({"doc": b"a\x00b"})
        with pytest.raises(MergeConflict) as exc_info:
            v2.commit({"doc": b"a\nb\nc\nD\n"}, default_merge=text_merge)
        assert "doc" in exc_info.value.conflicting_keys
        assert isinstance(exc_info.value.merge_errors["doc"], CantMark)
