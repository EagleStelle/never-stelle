from __future__ import annotations

from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.paths import path_key
from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import (
    _decode,
    _encode,
    _payload_source_key,
    _safe_float,
)
from backend.app.domains.downloads.constants import IMAGE_EXTENSIONS


def _insert_sql(table: str, columns: tuple[str, ...]) -> str:
    names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT OR REPLACE INTO {table} ({names}) VALUES ({placeholders})"


_TASK_COLUMNS = (
    "id",
    "source_url",
    "source_key",
    "status",
    "progress_pct",
    "engine",
    "creator",
    "title",
    "media_id",
    "resolved_full_path",
    "resolved_folder",
    "resolved_filename",
    "error",
    "output_dir",
    "output_template",
    "folder_template",
    "filename_template",
    "created_at",
    "updated_at",
    "encoding",
    "last_log_lines",
)
_TASK_SELECT = ", ".join(_TASK_COLUMNS)
_TASK_STATUSES = {"pending", "running", "completed", "failed"}
_TASK_STORAGE_KEYS = set(_TASK_COLUMNS)
_TASK_INSERT_SQL = _insert_sql("download_tasks", _TASK_COLUMNS)

_HISTORY_COLUMNS = (
    "id",
    "source_url",
    "source_key",
    "engine",
    "creator",
    "title",
    "media_id",
    "resolved_full_path",
    "resolved_path_key",
    "resolved_folder",
    "resolved_filename",
    "file_size",
    "scan_mtime_ns",
    "scan_revision",
    "folder_template",
    "filename_template",
    "created_at",
    "updated_at",
    "encoding",
)
_HISTORY_SELECT = ", ".join(_HISTORY_COLUMNS)
_HISTORY_STORAGE_KEYS = set(_HISTORY_COLUMNS)
_HISTORY_INSERT_SQL = _insert_sql("download_history", _HISTORY_COLUMNS)

_ENRICHMENT_COLUMNS = (
    "id",
    "kind",
    "status",
    "attempts",
    "error",
    "payload",
    "created_at",
    "updated_at",
)
_ENRICHMENT_SELECT = ", ".join(_ENRICHMENT_COLUMNS)
_ENRICHMENT_STATUSES = {"pending", "running", "failed"}


def _media_kind_sql() -> tuple[str, tuple[str, ...]]:
    image_suffixes = tuple(f"%{suffix}" for suffix in sorted(IMAGE_EXTENSIONS))
    image_checks = " OR ".join("LOWER(resolved_filename) LIKE ?" for _ in image_suffixes)
    return (
        "CASE "
        f"WHEN ({image_checks}) THEN 'image' "
        "WHEN INSTR(resolved_filename, '.') > 0 THEN 'video' "
        "WHEN LOWER(engine) = 'gallerydl' THEN 'image' "
        "ELSE 'video' END",
        image_suffixes,
    )


def _json_dict(value: Any) -> dict[str, Any]:
    payload = _decode(str(value or ""), {})
    return payload if isinstance(payload, dict) else {}


def _json_list(value: Any) -> list[Any]:
    payload = _decode(str(value or ""), [])
    return payload if isinstance(payload, list) else []


def _text(payload: dict[str, Any], key: str) -> str:
    return str(payload.get(key) or "")


def _status(value: Any) -> str:
    status = str(value or "pending")
    if status not in _TASK_STATUSES:
        raise ValueError(f"Invalid download task status: {status}")
    return status


def _compact_encoding(payload: dict[str, Any], storage_keys: set[str]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in storage_keys}


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _task_payload_from_row(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    payload = _json_dict(row["encoding"])
    payload.update(
        {
            "source_url": str(row["source_url"] or ""),
            "source_key": str(row["source_key"] or ""),
            "status": str(row["status"] or "pending"),
            "progress_pct": _safe_float(row["progress_pct"]),
            "engine": str(row["engine"] or ""),
            "creator": str(row["creator"] or ""),
            "title": str(row["title"] or ""),
            "media_id": str(row["media_id"] or ""),
            "resolved_full_path": str(row["resolved_full_path"] or ""),
            "resolved_folder": str(row["resolved_folder"] or ""),
            "resolved_filename": str(row["resolved_filename"] or ""),
            "error": str(row["error"] or ""),
            "output_dir": str(row["output_dir"] or ""),
            "output_template": str(row["output_template"] or ""),
            "folder_template": str(row["folder_template"] or ""),
            "filename_template": str(row["filename_template"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "last_log_lines": _json_list(row["last_log_lines"]),
        }
    )
    return payload


def _history_payload_from_row(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    payload = _json_dict(row["encoding"])
    payload.update(
        {
            "source_url": str(row["source_url"] or ""),
            "source_key": str(row["source_key"] or ""),
            "engine": str(row["engine"] or ""),
            "creator": str(row["creator"] or ""),
            "title": str(row["title"] or ""),
            "media_id": str(row["media_id"] or ""),
            "resolved_full_path": str(row["resolved_full_path"] or ""),
            "resolved_folder": str(row["resolved_folder"] or ""),
            "resolved_filename": str(row["resolved_filename"] or ""),
            "file_size": safe_int(row["file_size"]),
            "scan_mtime_ns": safe_int(row["scan_mtime_ns"]),
            "scan_revision": str(row["scan_revision"] or ""),
            "folder_template": str(row["folder_template"] or ""),
            "filename_template": str(row["filename_template"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
    )
    return payload


def _enrichment_payload_from_row(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    payload = _json_dict(row["payload"])
    return {
        "id": str(row["id"] or ""),
        "kind": str(row["kind"] or "completion"),
        "status": str(row["status"] or "pending"),
        "attempts": safe_int(row["attempts"]),
        "error": str(row["error"] or ""),
        "payload": payload,
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _task_row_values(task_id: str, payload: dict[str, Any], now: str) -> tuple[Any, ...]:
    source_url = _text(payload, "source_url")
    return (
        str(task_id),
        source_url,
        _payload_source_key(payload, source_url),
        _status(payload.get("status")),
        _safe_float(payload.get("progress_pct")),
        _text(payload, "engine"),
        _text(payload, "creator"),
        _text(payload, "title"),
        _text(payload, "media_id"),
        _text(payload, "resolved_full_path"),
        _text(payload, "resolved_folder"),
        _text(payload, "resolved_filename"),
        _text(payload, "error"),
        _text(payload, "output_dir"),
        _text(payload, "output_template"),
        _text(payload, "folder_template"),
        _text(payload, "filename_template"),
        str(payload.get("created_at") or now),
        str(payload.get("updated_at") or now),
        _encode(_compact_encoding(payload, _TASK_STORAGE_KEYS)),
        _encode(list(payload.get("last_log_lines") or [])),
    )


def _history_row_values(task_id: str, payload: dict[str, Any], now: str) -> tuple[Any, ...]:
    source_url = _text(payload, "source_url")
    return (
        str(task_id),
        source_url,
        _payload_source_key(payload, source_url),
        _text(payload, "engine"),
        _text(payload, "creator"),
        _text(payload, "title"),
        _text(payload, "media_id"),
        _text(payload, "resolved_full_path"),
        path_key(_text(payload, "resolved_full_path")) if _text(payload, "resolved_full_path") else "",
        _text(payload, "resolved_folder"),
        _text(payload, "resolved_filename"),
        safe_int(payload.get("file_size")),
        safe_int(payload.get("scan_mtime_ns")),
        _text(payload, "scan_revision"),
        _text(payload, "folder_template"),
        _text(payload, "filename_template"),
        str(payload.get("created_at") or now),
        str(payload.get("updated_at") or now),
        _encode(_compact_encoding(payload, _HISTORY_STORAGE_KEYS)),
    )


def _upsert_task(connection: Any, task_id: str, payload: dict[str, Any], now: str) -> None:
    connection.execute(_TASK_INSERT_SQL, _task_row_values(task_id, payload, now))


def load_task_store_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(f"SELECT {_TASK_SELECT} FROM download_tasks ORDER BY created_at, id").fetchall()
    return {"tasks": {row["id"]: _task_payload_from_row(row) for row in rows}}


def load_active_task_store_payload() -> dict[str, Any]:
    # Only non-completed rows: completed downloads live in history and are paginated there.
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT {_TASK_SELECT} FROM download_tasks WHERE status != 'completed' ORDER BY created_at, id"
        ).fetchall()
    return {"tasks": {row["id"]: _task_payload_from_row(row) for row in rows}}


def count_active_by_source() -> dict[str, dict[str, int]]:
    # Status tallies for queued/running/failed straight from SQL, no JSON decode or disk I/O.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, status, COUNT(*) AS n "
            "FROM download_tasks WHERE status != 'completed' "
            "GROUP BY source_key, status"
        ).fetchall()
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        key = str(row["source_key"] or "")
        result.setdefault(key, {})[str(row["status"] or "pending")] = int(row["n"] or 0)
    return result


def count_active_by_source_and_media() -> dict[str, dict[str, dict[str, int]]]:
    media_sql, params = _media_kind_sql()
    with transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT source_key, status, {media_sql} AS media_kind, COUNT(*) AS n
            FROM download_tasks
            WHERE status != 'completed'
            GROUP BY source_key, status, media_kind
            """,
            params,
        ).fetchall()
    result: dict[str, dict[str, dict[str, int]]] = {}
    for row in rows:
        media = str(row["media_kind"] or "video")
        key = str(row["source_key"] or "")
        status = str(row["status"] or "pending")
        result.setdefault(media, {}).setdefault(key, {})[status] = int(row["n"] or 0)
    return result


def count_history_by_source() -> dict[str, int]:
    # Completed tally per source from SQL COUNT, no disk stat per row.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, COUNT(*) AS n FROM download_history GROUP BY source_key"
        ).fetchall()
    return {str(row["source_key"] or ""): int(row["n"] or 0) for row in rows}


def count_history_by_source_and_media() -> dict[str, dict[str, int]]:
    media_sql, params = _media_kind_sql()
    with transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT source_key, {media_sql} AS media_kind, COUNT(*) AS n
            FROM download_history
            GROUP BY source_key, media_kind
            """,
            params,
        ).fetchall()
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        media = str(row["media_kind"] or "video")
        key = str(row["source_key"] or "")
        result.setdefault(media, {})[key] = int(row["n"] or 0)
    return result


def load_history_page(
    limit: int,
    cursor: tuple[str, ...] | None = None,
    source_key: str = "",
    search: str = "",
) -> list[tuple[str, dict[str, Any], str]]:
    # Keyset page ordered newest-first. The caller asks for limit + 1 to detect another page.
    limit = max(1, int(limit))
    clauses: list[str] = []
    params: list[Any] = []
    if source_key:
        clauses.append("source_key = ?")
        params.append(source_key)
    if cursor:
        created_at, row_id = str(cursor[0] or ""), str(cursor[-1] or "")
        clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
        params.extend([created_at, created_at, row_id])
    term = search.strip().lower()
    if term:
        like = f"%{_like_escape(term)}%"
        clauses.append(
            "("
            "LOWER(source_url) LIKE ? ESCAPE '\\'"
            " OR LOWER(creator) LIKE ? ESCAPE '\\'"
            " OR LOWER(title) LIKE ? ESCAPE '\\'"
            " OR LOWER(resolved_filename) LIKE ? ESCAPE '\\'"
            " OR LOWER(resolved_folder) LIKE ? ESCAPE '\\'"
            " OR LOWER(media_id) LIKE ? ESCAPE '\\'"
            ")"
        )
        params.extend([like, like, like, like, like, like])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT {_HISTORY_SELECT} FROM download_history {where}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    return [
        (
            str(row["id"]),
            _history_payload_from_row(row),
            str(row["created_at"] or ""),
        )
        for row in rows
    ]


def source_activity_revision() -> tuple[int | str, ...]:
    """Fingerprint of the source mix only, blind to progress churn.

    A fingerprint that tracked "anything changed" moved on every progress write,
    which made anything keyed on it recompute twice a second for a whole download. Source
    profiles depend only on which URLs and source keys exist, so this counts rows
    and distinct values and deliberately carries no timestamp: a running download
    rewrites its own row constantly without changing any of these. Every column
    read is indexed and no JSON is decoded, so the probe stays sub-millisecond
    as history grows.
    """
    query = (
        "SELECT COUNT(*), COUNT(DISTINCT source_key),"
        " COALESCE(MIN(source_key), ''), COALESCE(MAX(source_key), '') FROM {table}"
    )
    with transaction() as connection:
        tasks = connection.execute(query.format(table="download_tasks")).fetchone()
        history = connection.execute(query.format(table="download_history")).fetchone()
    return (
        int(tasks[0]),
        int(tasks[1]),
        str(tasks[2]),
        str(tasks[3]),
        int(history[0]),
        int(history[1]),
        str(history[2]),
        str(history[3]),
    )


def load_task_payload(task_id: str) -> dict[str, Any]:
    """One task row by id. Replaces decoding the whole store to read a single task."""
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_TASK_SELECT} FROM download_tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
    return _task_payload_from_row(row)


def next_pending_task_payload() -> tuple[str, dict[str, Any]] | None:
    """The oldest queued task, chosen by SQL rather than by scanning every row."""
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_TASK_SELECT} FROM download_tasks "
            "WHERE status = 'pending' ORDER BY created_at, id LIMIT 1"
        ).fetchone()
    if not row:
        return None
    return str(row["id"]), _task_payload_from_row(row)


def count_pending_tasks() -> int:
    with transaction() as connection:
        row = connection.execute("SELECT COUNT(*) FROM download_tasks WHERE status = 'pending'").fetchone()
    return int(row[0] or 0)


def count_active_download_tasks() -> int:
    with transaction() as connection:
        row = connection.execute(
            "SELECT COUNT(*) FROM download_tasks WHERE status IN ('pending', 'running')"
        ).fetchone()
    return int(row[0] or 0)


def count_pending_enrichment_jobs_payload() -> int:
    with transaction() as connection:
        row = connection.execute(
            "SELECT COUNT(*) FROM download_enrichment_jobs WHERE status = 'pending'"
        ).fetchone()
    return int(row[0] or 0)


def upsert_enrichment_job_payload(job_id: str, kind: str, payload: dict[str, Any]) -> None:
    now = utc_now()
    job_id = str(job_id or "").strip()
    if not job_id:
        raise ValueError("Enrichment job id is required.")
    with transaction() as connection:
        connection.execute(
            """
            INSERT INTO download_enrichment_jobs
                (id, kind, status, attempts, error, payload, created_at, updated_at)
            VALUES (?, ?, 'pending', 0, '', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind = excluded.kind,
                status = 'pending',
                error = '',
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (job_id, str(kind or "completion"), _encode(payload if isinstance(payload, dict) else {}), now, now),
        )


def claim_next_enrichment_job_payload() -> dict[str, Any] | None:
    now = utc_now()
    with transaction() as connection:
        row = connection.execute(
            f"""
            SELECT {_ENRICHMENT_SELECT} FROM download_enrichment_jobs
            WHERE status = 'pending'
            ORDER BY created_at, id
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        job_id = str(row["id"] or "")
        cursor = connection.execute(
            """
            UPDATE download_enrichment_jobs
            SET status = 'running',
                attempts = attempts + 1,
                error = '',
                updated_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (now, job_id),
        )
        if cursor.rowcount != 1:
            return None
        claimed = connection.execute(
            f"SELECT {_ENRICHMENT_SELECT} FROM download_enrichment_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
    return _enrichment_payload_from_row(claimed)


def complete_enrichment_job_payload(job_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_enrichment_jobs WHERE id = ?", (str(job_id),))


def retry_enrichment_job_payload(job_id: str, error: str, *, max_attempts: int = 3) -> None:
    now = utc_now()
    status = "pending"
    with transaction() as connection:
        row = connection.execute(
            "SELECT attempts FROM download_enrichment_jobs WHERE id = ?",
            (str(job_id),),
        ).fetchone()
        if not row:
            return
        if safe_int(row["attempts"]) >= max(1, int(max_attempts)):
            status = "failed"
        connection.execute(
            """
            UPDATE download_enrichment_jobs
            SET status = ?, error = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, str(error or ""), now, str(job_id)),
        )


def load_enrichment_jobs_payload() -> list[dict[str, Any]]:
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT {_ENRICHMENT_SELECT} FROM download_enrichment_jobs ORDER BY created_at, id"
        ).fetchall()
    return [_enrichment_payload_from_row(row) for row in rows]


def merge_task_payload(task_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Atomically merge ``updates`` into a single task row and return it.

    The read and write happen inside one transaction while the process-wide DB
    lock is held, so concurrent writers (the worker streaming progress while an
    API request mutates the same task) can no longer clobber each other. Only
    the one affected row is rewritten, not the whole table.
    """
    task_id = str(task_id)
    now = utc_now()
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_TASK_SELECT} FROM download_tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        payload = _task_payload_from_row(row)
        payload.update(updates if isinstance(updates, dict) else {})
        payload["updated_at"] = now
        _upsert_task(connection, task_id, payload, now)
    return payload


def claim_pending_task_payload(task_id: str) -> dict[str, Any] | None:
    """Atomically flip one pending task to running and return the updated payload."""
    task_id = str(task_id)
    now = utc_now()
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_TASK_SELECT} FROM download_tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        if not row or str(row["status"] or "") != "pending":
            return None
        payload = _task_payload_from_row(row)
        payload.update({"status": "running", "progress_pct": 0, "error": "", "last_log_lines": []})
        payload["updated_at"] = now
        _upsert_task(connection, task_id, payload, now)
    return payload


def delete_task_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_tasks WHERE id = ?", (str(task_id),))


def delete_task_row_if_status(task_id: str, statuses: set[str]) -> bool:
    normalized = {str(status) for status in statuses}
    if not normalized:
        return False
    invalid = normalized - _TASK_STATUSES
    if invalid:
        raise ValueError(f"Invalid download task status: {sorted(invalid)[0]}")
    placeholders = ",".join("?" for _ in normalized)
    with transaction() as connection:
        cursor = connection.execute(
            f"DELETE FROM download_tasks WHERE id = ? AND status IN ({placeholders})",
            (str(task_id), *sorted(normalized)),
        )
    return bool(cursor.rowcount)


def load_history_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT {_HISTORY_SELECT} FROM download_history ORDER BY created_at DESC, id DESC"
        ).fetchall()
    return {"entries": {row["id"]: _history_payload_from_row(row) for row in rows}}


def load_history_entry_payload(task_id: str) -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_HISTORY_SELECT} FROM download_history WHERE id = ?",
            (str(task_id),),
        ).fetchone()
    return _history_payload_from_row(row)


def load_history_entries_by_media_id(media_id: str) -> list[tuple[str, dict[str, Any]]]:
    value = str(media_id or "").strip()
    if not value:
        return []
    with transaction() as connection:
        rows = list(
            connection.execute(
                f"""
                SELECT {_HISTORY_SELECT} FROM download_history
                WHERE media_id = ?
                ORDER BY created_at DESC, id DESC
                """,
                (value,),
            ).fetchall()
        )
        seen_ids = {str(row["id"]) for row in rows}
        filename_rows = connection.execute(
            f"""
            SELECT {_HISTORY_SELECT} FROM download_history
            WHERE media_id = '' AND resolved_filename LIKE ? ESCAPE '\\'
            ORDER BY created_at DESC, id DESC
            """,
            (f"%[{_like_escape(value)}]%",),
        ).fetchall()
        rows.extend(row for row in filename_rows if str(row["id"]) not in seen_ids)
        rows.sort(key=lambda row: (str(row["created_at"] or ""), str(row["id"] or "")), reverse=True)
    return [(str(row["id"]), _history_payload_from_row(row)) for row in rows]


def load_history_entry_by_path(resolved_full_path: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    value = path_key(resolved_full_path) if str(resolved_full_path or "").strip() else ""
    if not value:
        return None, None
    with transaction() as connection:
        row = connection.execute(
            f"SELECT {_HISTORY_SELECT} FROM download_history WHERE resolved_path_key = ? LIMIT 1",
            (value,),
        ).fetchone()
    if not row:
        return None, None
    return str(row["id"]), _history_payload_from_row(row)


def save_history_row(task_id: str, payload: dict[str, Any]) -> None:
    """Upsert one completed-download record without rewriting the whole table."""
    save_history_rows([(task_id, payload)])


def save_history_rows(rows: list[tuple[str, dict[str, Any]]]) -> None:
    """Upsert many completed-download records in one transaction.

    A library scan writes a row per file; one commit per row makes the scan cost
    scale with fsyncs rather than with work, so batches share a transaction.
    """
    if not rows:
        return
    now = utc_now()
    prepared = [
        _history_row_values(
            str(task_id),
            {
                **(raw_payload if isinstance(raw_payload, dict) else {}),
                "updated_at": now,
            },
            now,
        )
        for task_id, raw_payload in rows
    ]
    with transaction() as connection:
        connection.executemany(_HISTORY_INSERT_SQL, prepared)


def fail_running_tasks(error: str) -> int:
    """Mark every ``running`` row failed in one pass, for crash recovery at boot."""
    now = utc_now()
    with transaction() as connection:
        cursor = connection.execute(
            "UPDATE download_tasks SET status = 'failed', error = ?, updated_at = ? WHERE status = 'running'",
            (str(error), now),
        )
    return int(cursor.rowcount or 0)


def delete_history_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_history WHERE id = ?", (str(task_id),))


def open_rename_journal_entries() -> list[dict[str, str]]:
    """Renames that were started but never confirmed, oldest first."""
    with transaction() as connection:
        rows = connection.execute(
            "SELECT task_id, old_path, new_path FROM rename_journal ORDER BY created_at, task_id"
        ).fetchall()
    return [
        {
            "task_id": str(row["task_id"]),
            "old_path": str(row["old_path"] or ""),
            "new_path": str(row["new_path"] or ""),
        }
        for row in rows
    ]


def begin_rename_journal_entry(task_id: str, old_path: str, new_path: str) -> None:
    with transaction() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO rename_journal (task_id, old_path, new_path, created_at) VALUES (?, ?, ?, ?)",
            (str(task_id), str(old_path), str(new_path), utc_now()),
        )


def clear_rename_journal_entries(task_ids: Any) -> None:
    rows = [(str(task_id),) for task_id in task_ids]
    if not rows:
        return
    with transaction() as connection:
        connection.executemany("DELETE FROM rename_journal WHERE task_id = ?", rows)
