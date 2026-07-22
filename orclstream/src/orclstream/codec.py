"""Safe default conversion between Python values and OCI message bytes."""

from __future__ import annotations

import json
from typing import Any


def serialize(value: Any) -> bytes:
    """Convert bytes, strings, or JSON-compatible values to UTF-8 bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def deserialize(value: bytes) -> Any:
    """Decode UTF-8 then parse JSON when possible; otherwise return a string."""
    text = value.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

