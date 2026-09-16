from __future__ import annotations

from typing import Any

from backend.app.core.coercion import safe_int

from .storage import load_saved_settings_file

MIN_INTERVAL_SECONDS = 3600
MAX_INTERVAL_SECONDS = 30 * 24 * 3600

# field -> (default, minimum, maximum)
_COUNT_FIELDS: dict[str, tuple[int, int, int]] = {
    # New entries one check handles before the next check continues with older ones.
    "page_size": (30, 1, 500),
    # Items already in the app, in a row, before a check stops scrolling; tolerates pinned posts.
    "stop_after": (20, 1, 500),
    # Check interval a new tracker starts with.
    "interval_seconds": (6 * 3600, MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS),
}


def normalize_tracker_settings(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, Any] = {}
    for field, (default, minimum, maximum) in _COUNT_FIELDS.items():
        value = source.get(field)
        number = default if value is None or isinstance(value, bool) else safe_int(value, default)
        out[field] = max(minimum, min(number, maximum))
    # Whether a new tracker downloads what its link already holds.
    out["backfill"] = bool(source.get("backfill", True))
    return out


def get_tracker_settings() -> dict[str, Any]:
    return normalize_tracker_settings(load_saved_settings_file().get("tracker_settings"))
