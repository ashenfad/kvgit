"""Staged: buffered writes over a Versioned store."""

import inspect
import pickle
from collections.abc import Iterable, Iterator, MutableMapping
from typing import Any, Callable

from .content_types import MergeFn
from .versioned.kv import CHUNK_PREFIX, VersionedKV
from .versioned.protocol import BytesMergeFn, MergeResult, Versioned


class _ChunkSink:
    """Accumulates content-addressed chunks emitted during one encode.

    Built fresh per ``commit()`` and shared across all encoded keys in
    that commit so dedup naturally extends to "the same buffer
    appearing under multiple staged keys".
    """

    def __init__(self) -> None:
        from .codecs._hash import hash_bytes

        self._hash_bytes = hash_bytes
        self.chunks: dict[str, bytes] = {}
        # per-encode key tracking — set externally between encode calls
        self.current_key: str | None = None
        self.refs_by_key: dict[str, list[str]] = {}

    def put(self, data) -> str:
        ref = self._hash_bytes(data)
        if ref not in self.chunks:
            # Materialize once on first sight; later puts of the same
            # chunk hash are free (same ref returned, no new bytes).
            self.chunks[ref] = bytes(data) if isinstance(data, memoryview) else data
        if self.current_key is not None:
            self.refs_by_key.setdefault(self.current_key, []).append(ref)
        return ref


class _ChunkReader:
    """Fetches chunks from the underlying KVStore by content-addressed key."""

    def __init__(self, kv) -> None:
        self._kv = kv

    def get(self, ref: str) -> bytes:
        raw = self._kv.get(CHUNK_PREFIX + ref)
        if raw is None:
            raise KeyError(f"chunk not found: {ref!r}")
        return raw

    def get_many(self, refs):
        prefixed = [CHUNK_PREFIX + r for r in refs]
        raw = self._kv.get_many(*prefixed)
        # Strip the prefix back off so callers see the codec-level refs.
        return {k[len(CHUNK_PREFIX) :]: v for k, v in raw.items()}

    def prefetch(self, refs) -> None:
        # Default: a no-op. Backends with async fetch can override via
        # subclass; the in-process backends do nothing useful here.
        return None


def _is_chunk_aware(fn) -> bool:
    """Detect if an encoder/decoder takes a required sink/reader arg.

    Chunk-aware encoders/decoders (built via ``kvgit.codecs.compose``)
    have exactly two **required** positional parameters: the value/blob
    and the sink/reader. Stdlib ``pickle.dumps`` and friends have
    optional second args (``protocol=...``) and stay 1-arg.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    required = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        and p.default is inspect.Parameter.empty
    ]
    return len(required) >= 2


class Staged(MutableMapping[str, Any]):
    """Buffered write layer over a ``Versioned`` store.

    Writes are staged in memory. ``commit()`` flushes them to the
    underlying ``Versioned`` as a single atomic commit + merge.

    Values are encoded to bytes on commit using the configured encoder.
    Implements ``MutableMapping[str, Any]``.

    Encoder/decoder arity is autodetected:

    * 1-arg ``encoder(value) -> bytes`` and ``decoder(bytes) -> value``
      preserve the legacy pickle-style API. Stores remain v2-compatible.
    * 2-arg ``encoder(value, sink) -> bytes`` and
      ``decoder(bytes, reader) -> value`` enable chunked codecs from
      :mod:`kvgit.codecs`. The first chunked write upgrades the store
      to v3.
    """

    def __init__(
        self,
        versioned: Versioned,
        *,
        encoder: Callable[..., bytes] = pickle.dumps,
        decoder: Callable[..., Any] = pickle.loads,
    ) -> None:
        self._versioned = versioned
        self._encoder = encoder
        self._decoder = decoder
        self._encoder_chunked = _is_chunk_aware(encoder)
        self._decoder_chunked = _is_chunk_aware(decoder)
        self._chunk_reader = (
            _ChunkReader(versioned.store)
            if self._decoder_chunked and isinstance(versioned, VersionedKV)
            else None
        )
        self._updates: dict[str, Any] = {}
        self._removals: set[str] = set()
        self._cache: dict[str, Any] = {}
        self._merge_fns: dict[str, MergeFn] = {}
        self._default_merge: MergeFn | None = None

    def _decode(self, raw: bytes) -> Any:
        if self._decoder_chunked:
            return self._decoder(raw, self._chunk_reader)
        return self._decoder(raw)

    # -- Read operations --

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value, checking staged changes first."""
        if key in self._removals:
            return default
        if key in self._updates:
            return self._updates[key]
        if key in self._cache:
            return self._cache[key]
        raw = self._versioned.get(key)
        if raw is None:
            return default
        value = self._decode(raw)
        self._cache[key] = value
        return value

    def get_many(self, *keys: str) -> dict[str, Any]:
        """Get multiple values, respecting staged state."""
        result: dict[str, Any] = {}
        fetch: list[str] = []
        for key in keys:
            if key in self._removals:
                continue
            if key in self._updates:
                result[key] = self._updates[key]
            elif key in self._cache:
                result[key] = self._cache[key]
            else:
                fetch.append(key)
        if fetch:
            for key, raw in self._versioned.get_many(*fetch).items():
                value = self._decode(raw)
                self._cache[key] = value
                result[key] = value
        return result

    def keys(self) -> set[str]:  # type: ignore[override]
        """All keys visible in the current state (committed + staged)."""
        seen: set[str] = set()
        for key in self._versioned.keys():
            if key not in self._removals:
                seen.add(key)
        seen.update(self._updates.keys())
        return seen

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key in self._removals:
            return False
        if key in self._updates:
            return True
        return key in self._versioned

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise KeyError(key)
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._removals.discard(key)
        self._updates[key] = value

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        self._updates.pop(key, None)
        self._removals.add(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a specific key."""
        self._merge_fns[key] = fn

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function."""
        self._default_merge = fn

    def _wrap_merge_fn(self, fn: MergeFn) -> BytesMergeFn:
        """Wrap a user-level merge fn into a bytes-level merge fn.

        Decoding uses the configured (possibly chunked) decoder so the
        merge sees real Python values for both sides. Encoding the
        merge result, however, always falls back to plain ``pickle.dumps``
        — the bytes-level merge protocol has no place to land chunks
        (no commit context yet, no sink). Chunked dedup of merge
        outputs is not supported in v1; merge outputs are stored as
        opaque blobs. Subsequent commits that overwrite the merged key
        do go through the chunked path normally.
        """
        decode = self._decode

        def wrapped(
            old: bytes | None, ours: bytes | None, theirs: bytes | None
        ) -> bytes:
            old_val = decode(old) if old is not None else None
            ours_val = decode(ours) if ours is not None else None
            theirs_val = decode(theirs) if theirs is not None else None
            return pickle.dumps(fn(old_val, ours_val, theirs_val))

        return wrapped

    # -- Commit / reset --

    def commit(
        self,
        *,
        keys: set[str] | None = None,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergeFn] | None = None,
        default_merge: MergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Flush staged changes to the underlying Versioned store.

        Encodes staged values to bytes, wraps merge functions, and
        calls ``Versioned.commit()``. On success, clears the committed
        entries from the staging buffer.

        Args:
            keys: If provided, only commit these specific keys.
                Keys not in ``_updates`` or ``_removals`` are silently
                ignored. Uncommitted keys remain staged for a future
                commit. When ``None`` (default), all staged changes
                are committed.

        Returns:
            A MergeResult (truthy when committed).
        """
        # Encode staged updates to bytes — scoped to keys if provided.
        # When the encoder is chunk-aware, share one sink across all
        # encodes in this commit so chunks dedup across staged keys.
        sink = _ChunkSink() if self._encoder_chunked else None

        def _encode_one(key: str, value: Any) -> bytes:
            if sink is not None:
                sink.current_key = key
                try:
                    return self._encoder(value, sink)
                finally:
                    sink.current_key = None
            return self._encoder(value)

        encoded_updates: dict[str, bytes] | None = None
        if keys is not None:
            # Iterate the (typically small) keys set, not the full _updates dict
            matched_updates = {k: self._updates[k] for k in keys if k in self._updates}
            if matched_updates:
                encoded_updates = {
                    k: _encode_one(k, v) for k, v in matched_updates.items()
                }
            # .intersection() accepts any iterable, not just sets
            removals = self._removals.intersection(keys) or None
        else:
            if self._updates:
                encoded_updates = {
                    key: _encode_one(key, value) for key, value in self._updates.items()
                }
            removals = self._removals if self._removals else None

        chunks = sink.chunks if (sink is not None and sink.chunks) else None
        chunk_refs = (
            sink.refs_by_key if (sink is not None and sink.refs_by_key) else None
        )

        # Build effective merge fns and wrap to bytes-level
        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_default = default_merge or self._default_merge

        bytes_merge_fns: dict[str, BytesMergeFn] | None = None
        if effective_fns:
            bytes_merge_fns = {
                key: self._wrap_merge_fn(fn) for key, fn in effective_fns.items()
            }

        bytes_default: BytesMergeFn | None = None
        if effective_default:
            bytes_default = self._wrap_merge_fn(effective_default)

        result = self._versioned.commit(
            encoded_updates,
            removals,
            on_conflict=on_conflict,
            merge_fns=bytes_merge_fns,
            default_merge=bytes_default,
            info=info,
            chunks=chunks,
            chunk_refs=chunk_refs,
        )
        if result.merged:
            if keys is not None:
                # Only clear the committed keys from staging
                for k in keys:
                    self._updates.pop(k, None)
                    self._removals.discard(k)
            else:
                self._updates.clear()
                self._removals.clear()
            # Always clear the full read cache — HEAD moved, so cached
            # values from other keys may be stale after a merge.
            self._cache.clear()
        return result

    def reset(self) -> None:
        """Discard all staged changes."""
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()

    @property
    def has_changes(self) -> bool:
        """Whether there are staged changes."""
        return bool(self._updates or self._removals)

    def is_staged(self, key: str) -> bool:
        """Whether a specific key has a pending staged update or removal."""
        return key in self._updates or key in self._removals

    # -- Versioned pass-through --

    @property
    def versioned(self) -> Versioned:
        """The underlying Versioned instance."""
        return self._versioned

    @property
    def current_commit(self) -> str:
        return self._versioned.current_commit

    @property
    def base_commit(self) -> str:
        return self._versioned.base_commit

    @property
    def current_branch(self) -> str:
        """The name of the current branch."""
        return self._versioned.current_branch

    @property
    def initial_commit(self) -> str:
        return self._versioned.initial_commit

    @property
    def last_merge_result(self) -> MergeResult | None:
        return self._versioned.last_merge_result

    def create_branch(self, name: str, *, at: str | None = None) -> "Staged":
        """Fork a commit onto a new branch. Returns a new Staged."""
        return Staged(
            self._versioned.create_branch(name, at=at),
            encoder=self._encoder,
            decoder=self._decoder,
        )

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "Staged | None":
        """Create a new Staged at a specific commit. Returns None if not found."""
        v = self._versioned.checkout(commit_hash, branch=branch)
        if v is None:
            return None
        return Staged(v, encoder=self._encoder, decoder=self._decoder)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return self._versioned.list_branches()

    def delete_branch(self, name: str) -> None:
        """Delete a branch by name. Cannot delete the current branch."""
        self._versioned.delete_branch(name)

    def switch_branch(self, name: str) -> None:
        """Switch to a different branch in-place. Discards staged changes."""
        self._versioned.switch_branch(name)
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()

    def peek(self, key: str, *, branch: str) -> Any:
        """Read a key from another branch without switching. Returns None if not found."""
        raw = self._versioned.peek(key, branch=branch)
        if raw is None:
            return None
        return self._decode(raw)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit and clear staged changes.

        Returns True if the commit exists and reset succeeded.
        """
        ok = self._versioned.reset_to(commit_hash)
        if ok:
            self._updates.clear()
            self._removals.clear()
            self._cache.clear()
        return ok

    def history(
        self,
        commit_hash: str | None = None,
        *,
        all_parents: bool = False,
    ) -> "Iterable[str]":
        """Yield the commit chain from newest to oldest."""
        return self._versioned.history(commit_hash, all_parents=all_parents)

    def refresh(self) -> None:
        """Reload from HEAD and discard staged changes."""
        self._versioned.refresh()
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()
