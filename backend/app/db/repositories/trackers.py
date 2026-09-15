from __future__ import annotations

from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import _decode, _encode

_TRACKER_COLUMNS = (
    "id",
    "source_url",
    "source_key",
    "name",
    "enabled",
    "interval_seconds",
    "backfill",
    "quality",
    "post_processing",
    "next_check_at",
    "last_checked_at",
    "last_success_at",
    "last_error",
    "checking_at",
    "created_at",
    "updated_at",
)
_TRACKER_SELECT = ", ".join(_TRACKER_COLUMNS)
_JSON_COLUMNS = {"quality", "post_processing"}
_BOOL_COLUMNS = {"enabled", "backfill"}
_UPDATABLE = set(_TRACKER_COLUMNS) - {"id", "source_url", "source_key", "created_at", "updated_at"}
# SQLite caps bound parameters per statement; stay well under it.
_CHUNK = 500

# History ids linked to one tracker (bind its id twice): each download's own row, plus the
# `{download_id}:{suffix}` child rows of a multi-file post, found as a primary-key range.
TRACKER_HISTORY_IDS_SQL = (
    "SELECT download_id FROM tracker_entries WHERE tracker_id = ? AND download_id != ''"
    " UNION ALL SELECT h.id FROM tracker_entries e JOIN download_history h"
    " ON h.id > e.download_id || ':' AND h.id < e.download_id || ';'"
    " WHERE e.tracker_id = ? AND e.download_id != ''"
)


def _tracker_from_row(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    tracker: dict[str, Any] = {}
    for column in _TRACKER_COLUMNS:
        value = row[column]
        if column in _JSON_COLUMNS:
            decoded = _decode(str(value or ""), {})
            tracker[column] = decoded if isinstance(decoded, dict) else {}
        elif column in _BOOL_COLUMNS:
            tracker[column] = bool(safe_int(value))
        elif column == "interval_seconds":
            tracker[column] = safe_int(value)
        else:
            tracker[column] = str(value or "")
    return tracker


def _column_value(column: str, value: Any) -> Any:
    if column in _JSON_COLUMNS:
        return _encode(value if isinstance(value, dict) else {})
    if column in _BOOL_COLUMNS:
        return int(bool(value))
    if column == "interval_seconds":
        return safe_int(value)
    return str(value or "")


def insert_tracker_row(tracker: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    row = {**tracker, "created_at": now, "updated_at": now}
    values = [_column_value(column, row.get(column)) for column in _TRACKER_COLUMNS]
    placeholders = ", ".join("?" for _ in _TRACKER_COLUMNS)
    with transaction() as connection:
        connection.execute(f"INSERT INTO trackers ({_TRACKER_SELECT}) VALUES ({placeholders})", values)
    return load_tracker_row(str(tracker["id"]))


def load_tracker_row(tracker_id: str) -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute(f"SELECT {_TRACKER_SELECT} FROM trackers WHERE id = ?", (str(tracker_id),)).fetchone()
    return _tracker_from_row(row)


def load_tracker_rows() -> list[dict[str, Any]]:
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT {_TRACKER_SELECT} FROM trackers ORDER BY created_at DESC, id DESC"
        ).fetchall()
    return [_tracker_from_row(row) for row in rows]


def find_tracker_by_url(source_url: str) -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_TRACKER_SELECT} FROM trackers WHERE source_url = ? LIMIT 1", (str(source_url),)
        ).fetchone()
    return _tracker_from_row(row)


def update_tracker_row(tracker_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    fields = {column: value for column, value in updates.items() if column in _UPDATABLE}
    if fields:
        assignments = ", ".join(f"{column} = ?" for column in fields)
        values = [_column_value(column, value) for column, value in fields.items()]
        with transaction() as connection:
            connection.execute(
                f"UPDATE trackers SET {assignments}, updated_at = ? WHERE id = ?",
                (*values, utc_now(), str(tracker_id)),
            )
    return load_tracker_row(tracker_id)


def delete_tracker_rows(tracker_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM tracker_entries WHERE tracker_id = ?", (str(tracker_id),))
        connection.execute("DELETE FROM trackers WHERE id = ?", (str(tracker_id),))


def claim_due_tracker_row(now: str) -> dict[str, Any]:
    """Atomically mark the most overdue enabled tracker as checking and return it."""
    with transaction() as connection:
        row = connection.execute(
            "SELECT id FROM trackers WHERE enabled = 1 AND checking_at = '' AND next_check_at <= ?"
            " ORDER BY next_check_at, id LIMIT 1",
            (now,),
        ).fetchone()
        if not row:
            return {}
        cursor = connection.execute(
            "UPDATE trackers SET checking_at = ? WHERE id = ? AND checking_at = ''",
            (now, str(row["id"])),
        )
        if cursor.rowcount != 1:
            return {}
        claimed = connection.execute(
            f"SELECT {_TRACKER_SELECT} FROM trackers WHERE id = ?", (str(row["id"]),)
        ).fetchone()
    return _tracker_from_row(claimed)


def next_due_tracker_at() -> str | None:
    """Earliest next check among idle enabled trackers; None when none is enabled."""
    with transaction() as connection:
        enabled = connection.execute("SELECT COUNT(*) FROM trackers WHERE enabled = 1").fetchone()
        row = connection.execute(
            "SELECT MIN(next_check_at) FROM trackers WHERE enabled = 1 AND checking_at = ''"
        ).fetchone()
    if not safe_int(enabled[0]):
        return None
    return str(row[0] or "")


def reset_checking_trackers() -> int:
    with transaction() as connection:
        cursor = connection.execute("UPDATE trackers SET checking_at = '' WHERE checking_at != ''")
    return int(cursor.rowcount or 0)


def has_tracker_entry(tracker_id: str, entry_key: str) -> bool:
    with transaction() as connection:
        row = connection.execute(
            "SELECT 1 FROM tracker_entries WHERE tracker_id = ? AND entry_key = ?",
            (str(tracker_id), str(entry_key)),
        ).fetchone()
    return row is not None


def record_tracker_entry_rows(tracker_id: str, rows: list[tuple[str, str, str]]) -> None:
    """Insert ``(entry_key, entry_url, download_id)`` rows; an entry already seen keeps its first record."""
    if not rows:
        return
    now = utc_now()
    with transaction() as connection:
        connection.executemany(
            "INSERT OR IGNORE INTO tracker_entries (tracker_id, entry_key, entry_url, download_id, seen_at)"
            " VALUES (?, ?, ?, ?, ?)",
            [(str(tracker_id), key, url, download_id, now) for key, url, download_id in rows],
        )


def tracker_download_ids(tracker_id: str) -> list[str]:
    with transaction() as connection:
        rows = connection.execute(
            "SELECT DISTINCT download_id FROM tracker_entries WHERE tracker_id = ? AND download_id != ''",
            (str(tracker_id),),
        ).fetchall()
    return [str(row[0]) for row in rows]


def tracker_history_ids(tracker_id: str) -> list[str]:
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT id FROM download_history WHERE id IN ({TRACKER_HISTORY_IDS_SQL})",
            (str(tracker_id), str(tracker_id)),
        ).fetchall()
    return [str(row[0]) for row in rows]


def tracker_ids_for_download_rows(download_ids: list[str]) -> dict[str, str]:
    owners: dict[str, str] = {}
    with transaction() as connection:
        for start in range(0, len(download_ids), _CHUNK):
            chunk = download_ids[start : start + _CHUNK]
            placeholders = ", ".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT download_id, tracker_id FROM tracker_entries WHERE download_id IN ({placeholders})",
                chunk,
            ).fetchall()
            owners.update((str(row[0]), str(row[1])) for row in rows)
    return owners


def count_tracker_items() -> dict[str, dict[str, int]]:
    """Per tracker: entries seen and completed history rows. Active rows are counted by the task feed."""
    counts: dict[str, dict[str, int]] = {}
    with transaction() as connection:
        seen = connection.execute("SELECT tracker_id, COUNT(*) FROM tracker_entries GROUP BY tracker_id").fetchall()
        for tracker_id, count in seen:
            completed = connection.execute(
                f"SELECT COUNT(*) FROM download_history WHERE id IN ({TRACKER_HISTORY_IDS_SQL})",
                (str(tracker_id), str(tracker_id)),
            ).fetchone()
            counts[str(tracker_id)] = {"seen": safe_int(count), "completed": safe_int(completed[0])}
    return counts
