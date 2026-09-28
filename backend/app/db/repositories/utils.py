from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from backend.app.core.sources import normalize_source_key, source_key_from_url

# SQLite caps bound parameters per statement; stay well under it.
_CHUNK = 500


def chunks(values: Iterable[str]) -> Iterator[list[str]]:
    """Distinct ``values`` in slices small enough to bind in one statement."""
    items = list(dict.fromkeys(str(value) for value in values))
    for start in range(0, len(items), _CHUNK):
        yield items[start : start + _CHUNK]


def marks(values: list[Any]) -> str:
    return ", ".join("?" for _ in values)


def _encode(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _decode(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _source_key(source_url: str) -> str:
    return source_key_from_url(source_url)


def _payload_source_key(payload: dict[str, Any], source_url: str) -> str:
    return normalize_source_key(payload.get("source_key")) or _source_key(source_url)
