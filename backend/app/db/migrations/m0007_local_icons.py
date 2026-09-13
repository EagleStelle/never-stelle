"""Saved source profiles drop ``icon``, ``icon_url`` and ``iconUrl``.

Icons now live in ``<data>/icons/<source_key>.webp``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

_ICON_KEYS = ("icon", "icon_url", "iconUrl")


def _strip_icons(profiles: Any) -> bool:
    if isinstance(profiles, dict):
        entries: Any = list(profiles.values())
    elif isinstance(profiles, list):
        entries = profiles
    else:
        return False
    changed = False
    for profile in entries:
        if not isinstance(profile, dict):
            continue
        for name in _ICON_KEYS:
            if name in profile:
                profile.pop(name)
                changed = True
    return changed


def upgrade(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    if row is None:
        return
    try:
        payload = json.loads(row[0] or "{}")
    except (TypeError, ValueError):
        # Unreadable payload: the app rebuilds it from defaults on the next save.
        return
    if not isinstance(payload, dict) or not _strip_icons(payload.get("source_profiles")):
        return
    connection.execute(
        "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'app'",
        (json.dumps(payload, ensure_ascii=False), datetime.now(UTC).isoformat()),
    )
