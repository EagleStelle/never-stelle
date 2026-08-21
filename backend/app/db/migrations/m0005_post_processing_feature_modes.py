"""Post-processing gains one mode per feature.

``{"metadata": true, "chapters": false, "save_as": "embed"}`` becomes
``{"metadata": "embed", "chapters": "off"}``, so a run can embed one feature while
writing another beside the media, and ``both`` can do each at once. The new
``subtitle_languages`` list arrives empty, which selects the source language
instead of every translated caption track the extractor offers.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

_FEATURES = ("metadata", "subtitles", "automatic_subtitles", "chapters", "thumbnail")
# Every place a selection was stored: saved defaults, queued tasks, finished rows,
# and the dry payload an enrichment job replays a completion from.
_JSON_COLUMNS = (
    ("download_tasks", "encoding"),
    ("download_history", "encoding"),
    ("download_enrichment_jobs", "payload"),
)


def _rewritten(raw: Any) -> dict[str, Any] | None:
    """One stored selection in per-feature form, or None when it already is."""
    if not isinstance(raw, dict):
        return None
    if "save_as" not in raw and not any(isinstance(raw.get(name), bool) for name in _FEATURES):
        return None
    mode = str(raw.get("save_as") or "").strip().lower()
    mode = mode if mode in {"sidecar", "embed"} else "sidecar"
    selection = {key: value for key, value in raw.items() if key != "save_as"}
    for feature in _FEATURES:
        selection[feature] = mode if bool(raw.get(feature)) else "off"
    selection.setdefault("subtitle_languages", [])
    return selection


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _migrate_rows(connection: sqlite3.Connection, table: str, column: str) -> None:
    if not _table_exists(connection, table):
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
        selection = _rewritten(payload.get("post_processing"))
        if selection is None:
            continue
        payload["post_processing"] = selection
        connection.execute(
            f"UPDATE {table} SET {column} = ? WHERE id = ?",  # noqa: S608
            (json.dumps(payload, ensure_ascii=False), row_id),
        )


def _migrate_saved_defaults(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    if row is None:
        return
    try:
        payload = json.loads(row[0] or "{}")
    except (TypeError, ValueError):
        return
    if not isinstance(payload, dict):
        return
    selection = _rewritten(payload.get("default_post_processing"))
    if selection is None:
        return
    payload["default_post_processing"] = selection
    connection.execute(
        "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'app'",
        (json.dumps(payload, ensure_ascii=False), datetime.now(UTC).isoformat()),
    )


def upgrade(connection: sqlite3.Connection) -> None:
    _migrate_saved_defaults(connection)
    for table, column in _JSON_COLUMNS:
        _migrate_rows(connection, table, column)
