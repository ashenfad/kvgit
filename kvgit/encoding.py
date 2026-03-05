"""JSON encoding helpers and metadata types."""

import json
from dataclasses import asdict, dataclass


def to_bytes(obj) -> bytes:
    """Encode a JSON-safe Python object to bytes."""
    return json.dumps(obj, separators=(",", ":")).encode()


def from_bytes(raw: bytes):
    """Decode bytes to a Python object."""
    return json.loads(raw)


@dataclass
class MetaEntry:
    """Metadata for a single key in versioned state."""

    last_touch: int
    size: int | None
    created_at: float


def meta_to_bytes(meta: dict[str, "MetaEntry"]) -> bytes:
    """Serialize the per-key metadata dict to JSON bytes."""
    return to_bytes({k: asdict(v) for k, v in meta.items()})


def meta_from_bytes(raw: bytes) -> dict[str, "MetaEntry"]:
    """Deserialize JSON bytes to a per-key metadata dict."""
    return {k: MetaEntry(**v) for k, v in from_bytes(raw).items()}
