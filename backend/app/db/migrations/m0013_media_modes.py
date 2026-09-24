"""Three media modes, with defaults remembered per mode.

The old ``video`` mode (video with its audio) is now ``merged``, and ``video`` now means
video without audio. Its separate ``video_audio_codec`` is gone: a Merged download's
audio follows ``audio_format`` and ``audio_bitrate``, so a stored codec moves into the
format and the bitrate stays uncapped, and a re-run keeps the audio it had.

Saved defaults change shape from one selection to one per mode, plus the mode new
downloads start in.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from . import table_exists

_VIDEO_KEYS = ("video_quality", "video_container", "video_codec")
_AUDIO_KEYS = ("audio_format", "audio_bitrate")
# Every place a selection was stored inside a JSON payload.
_PAYLOAD_COLUMNS = (
    ("download_tasks", "encoding"),
    ("download_history", "encoding"),
    ("download_enrichment_jobs", "payload"),
)


def _merged(selection: dict[str, Any]) -> dict[str, Any]:
    """An old video selection as its Merged equivalent."""
    rewritten = {key: value for key, value in selection.items() if key != "video_audio_codec"}
    rewritten["mode"] = "merged"
    rewritten["audio_format"] = selection.get("video_audio_codec") or "auto"
    rewritten["audio_bitrate"] = "best"
    return rewritten


def _rewritten(raw: Any) -> dict[str, Any] | None:
    """One stored selection in the new modes, or None when it needs no change."""
    if not isinstance(raw, dict):
        return None
    if str(raw.get("mode") or "").strip().lower() == "video":
        return _merged(raw)
    if "video_audio_codec" in raw:
        return {key: value for key, value in raw.items() if key != "video_audio_codec"}
    return None


def _per_mode_defaults(raw: dict[str, Any]) -> dict[str, Any]:
    merged = _merged(raw)
    return {
        "mode": "audio" if str(raw.get("mode") or "").strip().lower() == "audio" else "merged",
        "merged": {key: merged[key] for key in (*_VIDEO_KEYS, *_AUDIO_KEYS) if key in merged},
        "video": {key: raw[key] for key in _VIDEO_KEYS if key in raw},
        "audio": {key: raw[key] for key in _AUDIO_KEYS if key in raw},
    }


def _migrate_saved_defaults(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    if row is None:
        return
    try:
        payload = json.loads(row[0] or "{}")
    except (TypeError, ValueError):
        # Unreadable payload: the app rebuilds it from defaults on the next save.
        return
    if not isinstance(payload, dict):
        return
    defaults = payload.get("default_quality")
    if not isinstance(defaults, dict) or isinstance(defaults.get("merged"), dict):
        return
    payload["default_quality"] = _per_mode_defaults(defaults)
    connection.execute(
        "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'app'",
        (json.dumps(payload, ensure_ascii=False), datetime.now(UTC).isoformat()),
    )


def _migrate_payloads(connection: sqlite3.Connection, table: str, column: str) -> None:
    if not table_exists(connection, table):
        return
    rows = connection.execute(f"SELECT id, {column} FROM {table}").fetchall()  # noqa: S608
    for row_id, blob in rows:
        try:
            payload = json.loads(blob or "{}")
        except (TypeError, ValueError):
            # Unreadable blob: the app rebuilds it from defaults on the next write.
            continue
        if not isinstance(payload, dict):
            continue
        selection = _rewritten(payload.get("quality"))
        if selection is None:
            continue
        payload["quality"] = selection
        connection.execute(
            f"UPDATE {table} SET {column} = ? WHERE id = ?",  # noqa: S608
            (json.dumps(payload, ensure_ascii=False), row_id),
        )


def _migrate_trackers(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "trackers"):
        return
    for row_id, blob in connection.execute("SELECT id, quality FROM trackers").fetchall():
        try:
            selection = _rewritten(json.loads(blob or "{}"))
        except (TypeError, ValueError):
            continue
        if selection is not None:
            connection.execute(
                "UPDATE trackers SET quality = ? WHERE id = ?",
                (json.dumps(selection, ensure_ascii=False), row_id),
            )


def upgrade(connection: sqlite3.Connection) -> None:
    _migrate_saved_defaults(connection)
    for table, column in _PAYLOAD_COLUMNS:
        _migrate_payloads(connection, table, column)
    _migrate_trackers(connection)
