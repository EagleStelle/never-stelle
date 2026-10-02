from __future__ import annotations

from collections.abc import Callable
from typing import Any

from backend.app.core.resolution import invalidate, resolved
from backend.app.db.repositories import (
    delete_learned_format_row,
    load_learned_formats_payload,
    merge_learned_formats_payload,
    save_learned_formats_payload,
)

LEARNED_FORMATS_KEY = "formats.learned"


def load_learned_formats() -> dict[str, Any]:
    # Template resolution runs per row, and each was re-querying the table.
    return resolved(LEARNED_FORMATS_KEY, load_learned_formats_payload)


def save_learned_formats(payload: dict[str, Any]) -> None:
    """Upsert the sources in ``payload``. Sources absent from it keep their rows."""
    save_learned_formats_payload(payload)
    invalidate(LEARNED_FORMATS_KEY)


def merge_learned_formats(
    update: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Learn on top of the stored formats as they are now, not a snapshot read earlier."""
    before, after = merge_learned_formats_payload(update)
    invalidate(LEARNED_FORMATS_KEY)
    return before, after


def forget_learned_format(source_key: str) -> None:
    delete_learned_format_row(source_key)
    invalidate(LEARNED_FORMATS_KEY)
