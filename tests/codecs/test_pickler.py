"""Tests for the ChunkingPickler / ChunkingUnpickler glue."""

from __future__ import annotations

from typing import Any

import pickle

import pytest

from kvgit.codecs import compose
from kvgit.codecs.base import ChunkSink

from conftest import DictSink, reader_for  # noqa: E402


class TaggedThing:
    """Plain class — used to verify pickle handles unknown types."""

    def __init__(self, value: Any) -> None:
        self.value = value

    def __eq__(self, other) -> bool:
        return isinstance(other, TaggedThing) and self.value == other.value


class FakeBigBlob:
    """Test value externalized by FakeCodec into one chunk."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __eq__(self, other) -> bool:
        return isinstance(other, FakeBigBlob) and self.payload == other.payload


class FakeCodec:
    """Minimal codec for testing the pickler glue without numpy/pandas."""

    name = "fake"

    def __init__(self) -> None:
        self.externalize_calls = 0
        self.materialize_calls = 0

    def try_externalize(self, obj: Any, sink: ChunkSink):
        if not isinstance(obj, FakeBigBlob):
            return None
        self.externalize_calls += 1
        ref = sink.put(obj.payload)
        return {"ref": ref, "size": len(obj.payload)}

    def materialize(self, token, reader) -> FakeBigBlob:
        self.materialize_calls += 1
        return FakeBigBlob(reader.get(token["ref"]))


class TestPicklerBasics:
    def test_pass_through_for_unknown_types(self):
        encoder, decoder = compose(FakeCodec())
        sink = DictSink()
        blob = encoder({"a": 1, "b": [2, 3]}, sink)
        # No chunks should have been emitted.
        assert sink.chunks == {}
        # And it should round-trip.
        assert decoder(blob, reader_for(sink)) == {"a": 1, "b": [2, 3]}

    def test_externalizes_known_type(self):
        codec = FakeCodec()
        encoder, decoder = compose(codec)
        sink = DictSink()
        v = FakeBigBlob(b"x" * 4096)
        blob = encoder(v, sink)
        assert len(sink.chunks) == 1
        assert codec.externalize_calls == 1
        out = decoder(blob, reader_for(sink))
        assert out == v
        assert codec.materialize_calls == 1

    def test_nested_in_container(self):
        encoder, decoder = compose(FakeCodec())
        sink = DictSink()
        original = {
            "x": FakeBigBlob(b"AAAA"),
            "y": [FakeBigBlob(b"BBBB"), 42, "hello"],
            "z": (FakeBigBlob(b"CCCC"),),
        }
        blob = encoder(original, sink)
        assert len(sink.chunks) == 3
        out = decoder(blob, reader_for(sink))
        assert out == original

    def test_dedup_by_identity(self):
        """Same object visited twice in one pickle is externalized once."""
        encoder, decoder = compose(FakeCodec())
        sink = DictSink()
        shared = FakeBigBlob(b"shared payload")
        blob = encoder({"a": shared, "b": shared}, sink)
        assert sink.put_calls == 1  # id-memo hit
        assert len(sink.chunks) == 1
        out = decoder(blob, reader_for(sink))
        assert out["a"] == out["b"] == shared

    def test_dedup_by_content(self):
        """Two distinct objects with identical bytes share one chunk."""
        encoder, decoder = compose(FakeCodec())
        sink = DictSink()
        # Two FakeBigBlob instances with identical payload.
        a = FakeBigBlob(b"identical")
        b = FakeBigBlob(b"identical")
        encoder({"a": a, "b": b}, sink)
        # Both went through put (id memo doesn't apply across distinct ids).
        assert sink.put_calls == 2
        # But the sink dedups by hash → only one chunk stored.
        assert len(sink.chunks) == 1


class TestPicklerErrors:
    def test_unknown_codec_in_blob_raises(self):
        """A blob produced with a codec not present at decode raises clearly."""
        codec_a = FakeCodec()
        encoder_with, _ = compose(codec_a)
        sink = DictSink()
        blob = encoder_with(FakeBigBlob(b"data"), sink)

        # Decode without registering the codec.
        _, decoder_without = compose()
        with pytest.raises(pickle.UnpicklingError, match="codec 'fake' not registered"):
            decoder_without(blob, reader_for(sink))

    def test_corrupt_persistent_id_shape(self):
        """Hand-rolled bad pid raises a useful UnpicklingError."""
        # Build a pickle that emits a malformed persistent_id directly.
        import io

        class BadPickler(pickle.Pickler):
            def persistent_id(self, obj):
                if obj == "trigger":
                    return "this-is-not-a-tuple"
                return None

        buf = io.BytesIO()
        BadPickler(buf, protocol=pickle.HIGHEST_PROTOCOL).dump(["trigger"])
        _, decoder = compose(FakeCodec())
        with pytest.raises(pickle.UnpicklingError, match="persistent_id"):
            decoder(buf.getvalue(), reader_for(DictSink()))


class TestComposeOrdering:
    def test_first_codec_wins(self):
        """When two codecs claim the same type, the first one wins."""
        events: list[str] = []

        class CodecA:
            name = "a"

            def try_externalize(self, obj, sink):
                if isinstance(obj, FakeBigBlob):
                    events.append("a")
                    return {"ref": sink.put(obj.payload), "from": "a"}
                return None

            def materialize(self, token, reader):
                return FakeBigBlob(reader.get(token["ref"]))

        class CodecB:
            name = "b"

            def try_externalize(self, obj, sink):
                if isinstance(obj, FakeBigBlob):
                    events.append("b")
                    return {"ref": sink.put(obj.payload), "from": "b"}
                return None

            def materialize(self, token, reader):
                return FakeBigBlob(reader.get(token["ref"]))

        encoder, decoder = compose(CodecA(), CodecB())
        sink = DictSink()
        encoder(FakeBigBlob(b"data"), sink)
        assert events == ["a"]


class TestUnknownTypePassthrough:
    def test_custom_class_round_trips_via_pickle(self):
        encoder, decoder = compose(FakeCodec())
        sink = DictSink()
        v = TaggedThing(value=99)
        blob = encoder(v, sink)
        # No chunks for unknown type.
        assert not sink.chunks
        out = decoder(blob, reader_for(sink))
        assert out == v
