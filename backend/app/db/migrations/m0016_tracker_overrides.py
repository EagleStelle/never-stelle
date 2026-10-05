"""Trackers keep only the settings they set themselves.

A tracker used to copy its check interval when it was added, so later changes to its source's or
the default interval never reached it. ``overrides`` holds the tracker's own interval and download
limits as JSON; whatever it leaves out follows its source and the defaults. An interval equal to the
one the tracker would follow now is dropped, so it follows from here on; any other is kept.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from backend.app.core.config import get_config_source_profiles, load_app_config
from backend.app.core.sources import merge_source_profiles, source_key_from_url
from backend.app.domains.settings.profiles import configured_source_profiles
from backend.app.domains.settings.trackers import normalize_source_tracker_settings, normalize_tracker_settings

from . import table_exists


def _settings_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    try:
        payload = json.loads(row[0] or "{}") if row else {}
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def upgrade(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "trackers"):
        return
    connection.execute("ALTER TABLE trackers ADD COLUMN overrides TEXT NOT NULL DEFAULT '{}'")
    payload = _settings_payload(connection) if table_exists(connection, "app_settings") else {}
    default = normalize_tracker_settings(payload.get("tracker_settings"))["interval_seconds"]
    sources = normalize_source_tracker_settings(payload.get("source_tracker_settings"))
    profiles = merge_source_profiles(
        get_config_source_profiles(load_app_config()), configured_source_profiles(payload)
    )
    rows = connection.execute("SELECT id, source_url, interval_seconds FROM trackers").fetchall()
    updates: list[tuple[str, str]] = []
    for tracker_id, source_url, interval in rows:
        key = source_key_from_url(str(source_url or ""), profiles)
        if interval != sources.get(key, {}).get("interval_seconds", default):
            updates.append((json.dumps({"interval_seconds": interval}), tracker_id))
    connection.executemany("UPDATE trackers SET overrides = ? WHERE id = ?", updates)
    connection.execute("ALTER TABLE trackers DROP COLUMN interval_seconds")
