"""JSON encoding helpers and metadata types."""

import json
from dataclasses import dataclass


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
