from __future__ import annotations

from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import _decode, _encode

_TRACKER_COLUMNS = (
    "id",
    "source_url",
    "name",
    "enabled",
    "interval_seconds",
    "quality",
    "post_processing",
    "next_check_at",
    "last_checked_at",
    "last_success_at",
    "last_error",
    "checking_at",
    "feeds",
    "created_at",
    "updated_at",
)
_TRACKER_SELECT = ", ".join(_TRACKER_COLUMNS)
_JSON_COLUMNS = {"quality", "post_processing", "feeds"}
_BOOL_COLUMNS = {"enabled"}
_UPDATABLE = set(_TRACKER_COLUMNS) - {"id", "source_url", "created_at", "updated_at"}
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
        connection.execute("DELETE FROM tracker_backlog WHERE tracker_id = ?", (str(tracker_id),))
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


def due_tracker_count(now: str) -> int:
    with transaction() as connection:
        row = connection.execute(
            "SELECT COUNT(*) FROM trackers WHERE enabled = 1 AND checking_at = '' AND next_check_at <= ?", (now,)
        ).fetchone()
    return safe_int(row[0])


def has_tracker_entry(tracker_id: str, entry_key: str, seen_by: str = "") -> bool:
    """Whether the tracker recorded the entry; with ``seen_by``, only at or before that time."""
    sql = "SELECT 1 FROM tracker_entries WHERE tracker_id = ? AND entry_key = ?"
    params: tuple[str, ...] = (str(tracker_id), str(entry_key))
    if seen_by:
        sql += " AND seen_at <= ?"
        params = (*params, seen_by)
    with transaction() as connection:
        row = connection.execute(sql, params).fetchone()
    return row is not None


def record_tracker_entry_rows(tracker_id: str, rows: list[tuple[str, str, str]]) -> None:
    """Insert ``(entry_key, entry_url, download_id)`` rows; an entry already seen keeps its first record.

    Rows for a tracker deleted meanwhile are dropped, so a check still running leaves nothing behind.
    A recorded entry leaves the backlog.
    """
    if not rows:
        return
    now = utc_now()
    with transaction() as connection:
        connection.executemany(
            "INSERT OR IGNORE INTO tracker_entries (tracker_id, entry_key, entry_url, download_id, seen_at)"
            " SELECT ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM trackers WHERE id = ?)",
            [(str(tracker_id), key, url, download_id, now, str(tracker_id)) for key, url, download_id in rows],
        )
        connection.executemany(
            "DELETE FROM tracker_backlog WHERE tracker_id = ? AND entry_key = ?",
            [(str(tracker_id), key) for key, _, _ in rows],
        )


def add_tracker_backlog_rows(tracker_id: str, found_at: str, rows: list[tuple[str, str, int]]) -> None:
    """Insert ``(entry_key, entry_url, position)`` rows a walk found; recorded or backlogged entries are skipped."""
    if not rows:
        return
    with transaction() as connection:
        connection.executemany(
            "INSERT OR IGNORE INTO tracker_backlog (tracker_id, entry_key, entry_url, found_at, position)"
            " SELECT ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM trackers WHERE id = ?)"
            " AND NOT EXISTS (SELECT 1 FROM tracker_entries WHERE tracker_id = ? AND entry_key = ?)",
            [
                (str(tracker_id), key, url, found_at, position, str(tracker_id), str(tracker_id), key)
                for key, url, position in rows
            ],
        )


def has_tracker_backlog_row(tracker_id: str, entry_key: str, found_by: str = "") -> bool:
    """Whether the entry waits in the backlog; with ``found_by``, only when found at or before that time."""
    sql = "SELECT 1 FROM tracker_backlog WHERE tracker_id = ? AND entry_key = ?"
    params: tuple[str, ...] = (str(tracker_id), str(entry_key))
    if found_by:
        sql += " AND found_at <= ?"
        params = (*params, found_by)
    with transaction() as connection:
        row = connection.execute(sql, params).fetchone()
    return row is not None


def tracker_backlog_urls(tracker_id: str) -> list[str]:
    """Backlogged links, the newest walk's first and each walk's in page order."""
    with transaction() as connection:
        rows = connection.execute(
            "SELECT entry_url FROM tracker_backlog WHERE tracker_id = ? ORDER BY found_at DESC, position",
            (str(tracker_id),),
        ).fetchall()
    return [str(row[0]) for row in rows]


def fail_tracker_backlog_row(tracker_id: str, entry_key: str, max_attempts: int) -> None:
    """Count a check that could not read the entry; it leaves the backlog after ``max_attempts``."""
    with transaction() as connection:
        connection.execute(
            "UPDATE tracker_backlog SET attempts = attempts + 1 WHERE tracker_id = ? AND entry_key = ?",
            (str(tracker_id), str(entry_key)),
        )
        connection.execute(
            "DELETE FROM tracker_backlog WHERE tracker_id = ? AND entry_key = ? AND attempts >= ?",
            (str(tracker_id), str(entry_key), int(max_attempts)),
        )


def missing_tracker_download_rows(tracker_id: str) -> list[tuple[str, str, str]]:
    """``(entry_url, download_id, task_status)`` of queued entries whose download is neither active nor complete."""
    with transaction() as connection:
        rows = connection.execute(
            "SELECT e.entry_url, e.download_id, COALESCE(MAX(t.status), '') FROM tracker_entries e"
            " LEFT JOIN download_tasks t ON t.id = e.download_id"
            " WHERE e.tracker_id = ? AND e.download_id != ''"
            " AND COALESCE(t.status, '') NOT IN ('pending', 'running', 'completed')"
            " AND NOT EXISTS (SELECT 1 FROM download_history h WHERE h.id = e.download_id)"
            " AND NOT EXISTS (SELECT 1 FROM download_history h"
            " WHERE h.id > e.download_id || ':' AND h.id < e.download_id || ';')"
            " GROUP BY e.download_id",
            (str(tracker_id),),
        ).fetchall()
    return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]


def relink_tracker_download_rows(tracker_id: str, old_id: str, new_id: str) -> None:
    with transaction() as connection:
        connection.execute(
            "UPDATE tracker_entries SET download_id = ? WHERE tracker_id = ? AND download_id = ?",
            (str(new_id), str(tracker_id), str(old_id)),
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
    """Per tracker: items seen and items downloaded. Active rows are counted by the task feed.

    A post's photos are recorded under its url and download, and a multi-file download keeps a
    history row per file, so both counts are per item and a fully downloaded tracker shows them equal.
    """
    counts: dict[str, dict[str, int]] = {}
    with transaction() as connection:
        seen = connection.execute(
            "SELECT tracker_id, COUNT(DISTINCT entry_url) FROM tracker_entries GROUP BY tracker_id"
        ).fetchall()
        for tracker_id, count in seen:
            completed = connection.execute(
                "SELECT COUNT(DISTINCT e.download_id) FROM tracker_entries e"
                " WHERE e.tracker_id = ? AND e.download_id != ''"
                " AND (EXISTS (SELECT 1 FROM download_history h WHERE h.id = e.download_id)"
                " OR EXISTS (SELECT 1 FROM download_history h"
                " WHERE h.id > e.download_id || ':' AND h.id < e.download_id || ';'))",
                (str(tracker_id),),
            ).fetchone()
            counts[str(tracker_id)] = {"seen": safe_int(count), "completed": safe_int(completed[0])}
    return counts
