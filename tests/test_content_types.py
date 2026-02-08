"""Tests for content types."""

from kvit import Versioned, counter, json_value, last_writer_wins
from kvit.kv.memory import Memory


class TestCounter:
    def test_encode_decode_round_trip(self):
        ct = counter()
        assert ct.decode(ct.encode(42)) == 42
        assert ct.decode(ct.encode(-7)) == -7
        assert ct.decode(ct.encode(0)) == 0

    def test_counter_merge(self):
        ct = counter()
        # old=5, ours=8, theirs=7 -> 8 + 7 - 5 = 10
        result = ct.merge(5, 8, 7)
        assert result == 10

    def test_counter_no_old(self):
        ct = counter()
        # old=None, ours=3, theirs=5 -> 3 + 5 - 0 = 8
        result = ct.merge(None, 3, 5)
        assert result == 8

    def test_as_merge_fn(self):
        ct = counter()
        fn = ct.as_merge_fn()
        old = ct.encode(10)
        ours = ct.encode(15)
        theirs = ct.encode(12)
        result = fn(old, ours, theirs)
        assert ct.decode(result) == 17  # 15 + 12 - 10


class TestLastWriterWins:
    def test_always_returns_theirs(self):
        ct = last_writer_wins()
        assert ct.merge(b"old", b"ours", b"theirs") == b"theirs"

    def test_as_merge_fn(self):
        ct = last_writer_wins()
        fn = ct.as_merge_fn()
        assert fn(b"old", b"ours", b"theirs") == b"theirs"


class TestJsonValue:
    def test_encode_decode_round_trip(self):
        ct = json_value()
        data = {"key": "value", "num": 42, "nested": [1, 2, 3]}
        assert ct.decode(ct.encode(data)) == data

    def test_default_merge_is_lww(self):
        ct = json_value()
        result = ct.merge({"old": True}, {"ours": True}, {"theirs": True})
        assert result == {"theirs": True}

    def test_custom_merge(self):
        def merge_dicts(old, ours, theirs):
            base = old or {}
            merged = dict(base)
            merged.update(ours or {})
            merged.update(theirs or {})
            return merged

        ct = json_value(merge_fn=merge_dicts)
        result = ct.merge(
            {"a": 1},
            {"a": 1, "b": 2},
            {"a": 1, "c": 3},
        )
        assert result == {"a": 1, "b": 2, "c": 3}


class TestContentTypeIntegration:
    def test_counter_end_to_end(self):
        """Full cycle: two branches increment counter, merge produces sum."""
        store = Memory()
        ct = counter()

        v1 = Versioned(store)
        v1.commit({"hits": ct.encode(10)})

        v2 = Versioned(store)
        v2.set_content_type("hits", ct)

        # v1 increments to 15
        v1.commit({"hits": ct.encode(15)})

        # v2 increments to 20
        # Three-way merge: 15 + 20 - 10 = 25
        assert v2.commit({"hits": ct.encode(20)})
        assert ct.decode(v2.get("hits")) == 25

    def test_set_content_type_registers_merge_fn(self):
        """set_content_type registers the merge function."""
        store = Memory()
        ct = counter()

        v1 = Versioned(store)
        v1.commit({"x": ct.encode(0)})

        v2 = Versioned(store)
        v2.set_content_type("x", ct)

        v1.commit({"x": ct.encode(5)})

        # Without content type this would be a MergeConflict
        assert v2.commit({"x": ct.encode(3)})
        assert ct.decode(v2.get("x")) == 8  # 5 + 3 - 0

    def test_json_end_to_end(self):
        """JSON values with custom merge."""

        def merge_lists(old, ours, theirs):
            base = set(old or [])
            return sorted(
                base | set(ours or []) | set(theirs or [])
            )

        ct = json_value(merge_fn=merge_lists)
        store = Memory()

        v1 = Versioned(store)
        v1.commit({"tags": ct.encode(["a", "b"])})

        v2 = Versioned(store)
        v2.set_content_type("tags", ct)

        v1.commit({"tags": ct.encode(["a", "b", "c"])})

        assert v2.commit({"tags": ct.encode(["a", "b", "d"])})
        assert ct.decode(v2.get("tags")) == ["a", "b", "c", "d"]
