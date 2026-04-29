"""Shared fixtures for codec tests.

The directory is intentionally not a package (no ``__init__.py``) to
avoid colliding with stdlib ``codecs``. Tests pick up these helpers
through pytest's conftest auto-injection.
"""

from __future__ import annotations

import pytest

from kvgit.codecs._hash import hash_bytes


class DictSink:
    """In-memory ChunkSink that records every put for inspection."""

    def __init__(self) -> None:
        self.chunks: dict[str, bytes] = {}
        self.put_calls: int = 0

    def put(self, data) -> str:
        self.put_calls += 1
        ref = hash_bytes(data)
        if ref not in self.chunks:
            self.chunks[ref] = bytes(data) if isinstance(data, memoryview) else data
        return ref


class DictReader:
    """ChunkReader backed by a plain dict."""

    def __init__(self, chunks: dict[str, bytes]) -> None:
        self.chunks = chunks
        self.get_calls: int = 0
        self.get_many_calls: int = 0

    def get(self, ref: str) -> bytes:
        self.get_calls += 1
        return self.chunks[ref]

    def get_many(self, refs):
        self.get_many_calls += 1
        return {r: self.chunks[r] for r in refs if r in self.chunks}

    def prefetch(self, refs) -> None:
        return None


def reader_for(sink: DictSink) -> DictReader:
    return DictReader(sink.chunks)


@pytest.fixture
def dict_sink():
    return DictSink()


@pytest.fixture
def make_reader():
    return reader_for


# Re-export for direct import from test modules. Pytest's conftest
# auto-injection covers fixtures, but the classes/helpers we want
# in test bodies need a normal import path. Tests use:
#     from conftest import DictSink, reader_for
# which works because pytest adds each test directory to sys.path.
