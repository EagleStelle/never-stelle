from __future__ import annotations

from collections.abc import Collection
from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import _decode, _encode, chunks, marks

_TRACKER_COLUMNS = (
    "id",
    "source_url",
    "enabled",
    "overrides",
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
_JSON_COLUMNS = {"overrides", "quality", "post_processing", "feeds"}
_BOOL_COLUMNS = {"enabled"}
_UPDATABLE = set(_TRACKER_COLUMNS) - {"id", "source_url", "created_at", "updated_at"}


def _linked_history(select: str, where: str) -> str:
    """SQL selecting ``select`` over tracker entries ``e`` joined to history rows ``h``: the download's own row,
    plus the ``{download_id}:{suffix}`` child rows of a multi-file post, found as a primary-key range."""
    joins = ("h.id = e.download_id", "h.id > e.download_id || ':' AND h.id < e.download_id || ';'")
    return " UNION ALL ".join(
        f"SELECT {select} FROM tracker_entries e JOIN download_history h ON {join} WHERE {where}" for join in joins
    )


# History ids linked to one tracker; bind its id twice.
TRACKER_HISTORY_IDS_SQL = _linked_history("h.id", "e.tracker_id = ? AND e.download_id != ''")


def _in_history(column: str) -> str:
    """SQL true while ``column`` names a history row or the parent of ``{id}:{suffix}`` child rows."""
    return (
        f"(EXISTS (SELECT 1 FROM download_history h WHERE h.id = {column})"
        f" OR EXISTS (SELECT 1 FROM download_history h WHERE h.id > {column} || ':' AND h.id < {column} || ';'))"
    )


def _only_seen(table: str) -> str:
    """SQL true for a shown entry of ``table`` only seen: never queued, or its download left the queue and history."""
    return (
        f"{table}.deleted_at = '' AND ({table}.download_id = '' OR NOT (EXISTS (SELECT 1 FROM download_tasks t"
        f" WHERE t.id = {table}.download_id AND t.status IN ('pending', 'running', 'failed', 'completed'))"
        f" OR {_in_history(f'{table}.download_id')}))"
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
        else:
            tracker[column] = str(value or "")
    return tracker


def _column_value(column: str, value: Any) -> Any:
    if column in _JSON_COLUMNS:
        return _encode(value if isinstance(value, dict) else {})
    if column in _BOOL_COLUMNS:
        return int(bool(value))
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
    update_tracker_rows({str(tracker_id): updates})
    return load_tracker_row(tracker_id)


def update_tracker_rows(changes: dict[str, dict[str, Any]]) -> None:
    """Apply each tracker's own column updates in one transaction."""
    now = utc_now()
    with transaction() as connection:
        for tracker_id, updates in changes.items():
            fields = {column: value for column, value in updates.items() if column in _UPDATABLE}
            if not fields:
                continue
            assignments = ", ".join(f"{column} = ?" for column in fields)
            values = [_column_value(column, value) for column, value in fields.items()]
            connection.execute(
                f"UPDATE trackers SET {assignments}, updated_at = ? WHERE id = ?",
                (*values, now, str(tracker_id)),
            )


def delete_tracker_rows(tracker_ids: list[str]) -> None:
    tables = (("tracker_backlog", "tracker_id"), ("tracker_entries", "tracker_id"), ("trackers", "id"))
    with transaction() as connection:
        for chunk in chunks(tracker_ids):
            for table, column in tables:
                connection.execute(f"DELETE FROM {table} WHERE {column} IN ({marks(chunk)})", chunk)


def claim_due_tracker_row(now: str, first: Collection[str] = ()) -> dict[str, Any]:
    """Atomically mark the most overdue enabled tracker as checking and return it; due ones in ``first`` lead."""
    lead = f"id IN ({marks(list(first))}) DESC, " if first else ""
    with transaction() as connection:
        row = connection.execute(
            "SELECT id FROM trackers WHERE enabled = 1 AND checking_at = '' AND next_check_at <= ?"
            f" ORDER BY {lead}next_check_at, id LIMIT 1",
            (now, *first),
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
            f" AND NOT {_in_history('e.download_id')} GROUP BY e.download_id",
            (str(tracker_id),),
        ).fetchall()
    return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]


def relink_tracker_download(old_id: str, new_id: str) -> None:
    with transaction() as connection:
        connection.execute(
            "UPDATE tracker_entries SET download_id = ? WHERE download_id = ?", (str(new_id), str(old_id))
        )


def linked_download_ids(history_ids: list[str]) -> list[str]:
    """Ids an entry may link these rows under: each row's own, and a ``{download_id}:{suffix}`` child's parent."""
    return [*history_ids, *(value.rsplit(":", 1)[0] for value in history_ids if ":" in value)]


def unlink_tracker_downloads(download_ids: list[str]) -> None:
    """Unlink entries from downloads with no history row left, so no check queues them again.

    A parent stays linked while a child row remains.
    """
    with transaction() as connection:
        for chunk in chunks(linked_download_ids(download_ids)):
            connection.execute(
                f"UPDATE tracker_entries SET download_id = '' WHERE download_id IN ({marks(chunk)})"
                f" AND NOT {_in_history('tracker_entries.download_id')}",
                chunk,
            )


def tracker_active_download_ids(tracker_ids: list[str]) -> list[str]:
    """Queued, running and failed downloads the trackers queued."""
    ids: list[str] = []
    with transaction() as connection:
        for chunk in chunks(tracker_ids):
            rows = connection.execute(
                "SELECT DISTINCT e.download_id FROM tracker_entries e JOIN download_tasks t ON t.id = e.download_id"
                f" WHERE e.tracker_id IN ({marks(chunk)}) AND t.status != 'completed'",
                chunk,
            ).fetchall()
            ids.extend(str(row[0]) for row in rows)
    return ids


def tracker_history_ids(tracker_ids: list[str]) -> list[str]:
    ids: list[str] = []
    with transaction() as connection:
        for tracker_id in dict.fromkeys(map(str, tracker_ids)):
            rows = connection.execute(
                f"SELECT id FROM download_history WHERE id IN ({TRACKER_HISTORY_IDS_SQL})", (tracker_id, tracker_id)
            ).fetchall()
            ids.extend(str(row[0]) for row in rows)
    return ids


def tracker_ids_for_download_rows(download_ids: list[str]) -> dict[str, str]:
    owners: dict[str, str] = {}
    with transaction() as connection:
        for chunk in chunks(download_ids):
            rows = connection.execute(
                f"SELECT download_id, tracker_id FROM tracker_entries WHERE download_id IN ({marks(chunk)})",
                chunk,
            ).fetchall()
            owners.update((str(row[0]), str(row[1])) for row in rows)
    return owners


def count_tracker_items() -> dict[str, dict[str, int]]:
    """Per tracker: items seen and items downloaded. Active rows are counted by the task feed.

    A post's photos are recorded under its url and download, and a multi-file download keeps a
    history row per file, so both counts are per item and a fully downloaded tracker shows them equal.
    Deleted entries count nowhere.
    """
    with transaction() as connection:
        rows = connection.execute(
            "SELECT e.tracker_id, COUNT(DISTINCT CASE WHEN e.deleted_at = '' THEN e.entry_url END),"
            f" COUNT(DISTINCT CASE WHEN e.download_id != '' AND {_in_history('e.download_id')} THEN e.download_id END)"
            " FROM tracker_entries e GROUP BY e.tracker_id"
        ).fetchall()
    return {str(row[0]): {"seen": safe_int(row[1]), "completed": safe_int(row[2])} for row in rows}


def _encoded(path: str) -> str:
    # A blob that is no JSON reads as empty rather than failing the query.
    return f"COALESCE(CASE WHEN json_valid(h.encoding) THEN json_extract(h.encoding, '{path}') END, '')"


def tracker_filing_rows() -> list[tuple[str, dict[str, Any], int, str]]:
    """``(tracker_id, row, downloads, newest)`` per way a tracker's downloads are filed.

    ``row`` holds the templates and creator values their history rows record, ``downloads`` how
    many downloads share it and ``newest`` when the latest of them was saved.
    """
    linked = _linked_history(
        "e.tracker_id, e.download_id, h.folder_template, h.filename_template, h.creator, h.created_at,"
        f" {_encoded('$.subfolder_template')} AS subfolder_template,"
        f" {_encoded('$.resolved_tokens.nickname')} AS nickname",
        "e.download_id != ''",
    )
    with transaction() as connection:
        rows = connection.execute(
            "SELECT tracker_id, folder_template, subfolder_template, filename_template, creator, nickname,"
            f" COUNT(DISTINCT download_id), MAX(created_at) FROM ({linked}) GROUP BY 1, 2, 3, 4, 5, 6"
        ).fetchall()
    return [
        (
            str(row[0]),
            {
                "folder_template": str(row[1] or ""),
                "subfolder_template": str(row[2] or ""),
                "filename_template": str(row[3] or ""),
                "creator": str(row[4] or ""),
                "resolved_tokens": {"nickname": str(row[5] or "")},
            },
            safe_int(row[6]),
            str(row[7] or ""),
        )
        for row in rows
    ]


def _only_seen_where(chunk: list[str]) -> str:
    return f" WHERE tracker_id = ? AND entry_url IN ({marks(chunk)}) AND {_only_seen('tracker_entries')}"


def only_seen_tracker_entry_urls(tracker_id: str, urls: list[str] | None = None) -> list[str]:
    """The tracker's urls whose entries are only seen, the newest first; with ``urls``, those of them in their order."""
    found: set[str] = set()
    with transaction() as connection:
        if urls is None:
            rows = connection.execute(
                f"SELECT entry_url FROM tracker_entries WHERE tracker_id = ? AND {_only_seen('tracker_entries')}"
                " GROUP BY entry_url ORDER BY MAX(seen_at) DESC, entry_url DESC",
                (str(tracker_id),),
            ).fetchall()
            return [str(row[0]) for row in rows]
        for chunk in chunks(urls):
            rows = connection.execute(
                f"SELECT DISTINCT entry_url FROM tracker_entries{_only_seen_where(chunk)}", (str(tracker_id), *chunk)
            ).fetchall()
            found.update(str(row[0]) for row in rows)
    return [url for url in dict.fromkeys(map(str, urls)) if url in found]


def link_tracker_entry_url(tracker_id: str, url: str, download_id: str) -> None:
    with transaction() as connection:
        connection.execute(
            "UPDATE tracker_entries SET download_id = ? WHERE tracker_id = ? AND entry_url = ? AND deleted_at = ''",
            (str(download_id), str(tracker_id), str(url)),
        )


def _change_only_seen(tracker_id: str, urls: list[str], statement: str, values: tuple[Any, ...] = ()) -> int:
    """Run ``statement`` on the entries of ``urls`` only seen; returns how many urls it reached."""
    found = only_seen_tracker_entry_urls(tracker_id, urls)
    with transaction() as connection:
        for chunk in chunks(found):
            connection.execute(statement + _only_seen_where(chunk), (*values, str(tracker_id), *chunk))
    return len(found)


def dismiss_tracker_entry_urls(tracker_id: str, urls: list[str]) -> int:
    """Forget the entries only seen, so a later check that lists them records them again."""
    return _change_only_seen(tracker_id, urls, "DELETE FROM tracker_entries")


def delete_tracker_entry_urls(tracker_id: str, urls: list[str]) -> int:
    """Hide the entries only seen for good; they stay recorded, so no check queues them again."""
    return _change_only_seen(
        tracker_id, urls, "UPDATE tracker_entries SET deleted_at = ?, download_id = ''", (utc_now(),)
    )
