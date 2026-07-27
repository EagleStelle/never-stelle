from __future__ import annotations

from typing import Any

from backend.app.db.database import transaction, utc_now
from backend.app.db.repositories.utils import _decode, _encode, _payload_source_key, _safe_float


def load_task_store_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute("SELECT id, payload FROM download_tasks ORDER BY created_at, id").fetchall()
    return {"tasks": {row["id"]: _decode(row["payload"], {}) for row in rows}}


def load_active_task_store_payload() -> dict[str, Any]:
    # Only non-completed rows: completed downloads live in history and are paginated there.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT id, payload FROM download_tasks WHERE status != 'completed' ORDER BY created_at, id"
        ).fetchall()
    return {"tasks": {row["id"]: _decode(row["payload"], {}) for row in rows}}


def count_active_by_source() -> dict[str, dict[str, int]]:
    # Status tallies for queued/running/failed straight from SQL, no payload decode or disk I/O.
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


def count_history_by_source() -> dict[str, int]:
    # Completed tally per source from SQL COUNT, no disk stat per row.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, COUNT(*) AS n FROM download_history GROUP BY source_key"
        ).fetchall()
    return {str(row["source_key"] or ""): int(row["n"] or 0) for row in rows}


def load_history_page(
    limit: int,
    cursor: tuple[str, str, str] | None = None,
    source_key: str = "",
    search: str = "",
) -> list[tuple[str, dict[str, Any], str, str]]:
    # Keyset page ordered newest-first. The caller asks for limit + 1 to detect another page.
    limit = max(1, int(limit))
    clauses: list[str] = []
    params: list[Any] = []
    if source_key:
        clauses.append("source_key = ?")
        params.append(source_key)
    if cursor:
        completed_at, updated_at, task_id = cursor
        clauses.append(
            "("
            "completed_at < ?"
            " OR (completed_at = ? AND updated_at < ?)"
            " OR (completed_at = ? AND updated_at = ? AND task_id < ?)"
            ")"
        )
        params.extend([completed_at, completed_at, updated_at, completed_at, updated_at, task_id])
    term = search.strip().lower()
    if term:
        # Match the user-visible fields (url, creator, filename, folder, media id) via JSON1.
        like = f"%{term}%"
        clauses.append(
            "("
            "LOWER(source_url) LIKE ?"
            " OR LOWER(COALESCE(json_extract(payload, '$.creator'), '')) LIKE ?"
            " OR LOWER(COALESCE(json_extract(payload, '$.resolved_filename'), '')) LIKE ?"
            " OR LOWER(COALESCE(json_extract(payload, '$.resolved_folder'), '')) LIKE ?"
            " OR LOWER(COALESCE(json_extract(payload, '$.media_id'), '')) LIKE ?"
            ")"
        )
        params.extend([like, like, like, like, like])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT task_id, payload, completed_at, updated_at FROM download_history {where}
            ORDER BY completed_at DESC, updated_at DESC, task_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    return [
        (
            str(row["task_id"]),
            _decode(row["payload"], {}),
            str(row["completed_at"] or ""),
            str(row["updated_at"] or ""),
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
    read is indexed and no payload is decoded, so the probe stays sub-millisecond
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
        row = connection.execute("SELECT payload FROM download_tasks WHERE id = ?", (str(task_id),)).fetchone()
    payload = _decode(row["payload"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def next_pending_task_payload() -> tuple[str, dict[str, Any]] | None:
    """The oldest queued task, chosen by SQL rather than by scanning every row."""
    with transaction() as connection:
        row = connection.execute(
            "SELECT id, payload FROM download_tasks WHERE status = 'pending' ORDER BY created_at, id LIMIT 1"
        ).fetchone()
    if not row:
        return None
    payload = _decode(row["payload"], {})
    return str(row["id"]), payload if isinstance(payload, dict) else {}


def count_pending_tasks() -> int:
    with transaction() as connection:
        row = connection.execute("SELECT COUNT(*) FROM download_tasks WHERE status = 'pending'").fetchone()
    return int(row[0] or 0)


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
        row = connection.execute("SELECT payload FROM download_tasks WHERE id = ?", (task_id,)).fetchone()
        payload = _decode(row["payload"] if row else None, {})
        payload = payload if isinstance(payload, dict) else {}
        payload.update(updates)
        source_url = str(payload.get("source_url") or "")
        connection.execute(
            """
            INSERT OR REPLACE INTO download_tasks (
                id, source_url, status, source_key, progress_pct, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                source_url,
                str(payload.get("status") or "pending"),
                _payload_source_key(payload, source_url),
                _safe_float(payload.get("progress_pct")),
                _encode(payload),
                str(payload.get("created_at") or now),
                now,
            ),
        )
    return payload


def claim_pending_task_payload(task_id: str) -> dict[str, Any] | None:
    """Atomically flip one pending task to running and return the updated payload."""
    task_id = str(task_id)
    now = utc_now()
    with transaction() as connection:
        row = connection.execute("SELECT payload FROM download_tasks WHERE id = ?", (task_id,)).fetchone()
        if not row:
            return None
        payload = _decode(row["payload"], {})
        payload = payload if isinstance(payload, dict) else {}
        if payload.get("status") != "pending":
            return None
        payload.update({"status": "running", "progress_pct": 0, "error": "", "last_log_lines": []})
        source_url = str(payload.get("source_url") or "")
        connection.execute(
            """
            INSERT OR REPLACE INTO download_tasks (
                id, source_url, status, source_key, progress_pct, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                source_url,
                "running",
                _payload_source_key(payload, source_url),
                _safe_float(payload.get("progress_pct")),
                _encode(payload),
                str(payload.get("created_at") or now),
                now,
            ),
        )
    return payload


def delete_task_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_tasks WHERE id = ?", (str(task_id),))


def delete_task_row_if_status(task_id: str, statuses: set[str]) -> bool:
    normalized = {str(status) for status in statuses}
    if not normalized:
        return False
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
            "SELECT task_id, payload FROM download_history ORDER BY completed_at DESC, updated_at DESC, task_id DESC"
        ).fetchall()
    return {"entries": {row["task_id"]: _decode(row["payload"], {}) for row in rows}}


def save_history_row(task_id: str, payload: dict[str, Any]) -> None:
    """Upsert one completed-download record without rewriting the whole table."""
    payload = payload if isinstance(payload, dict) else {}
    now = utc_now()
    source_url = str(payload.get("source_url") or "")
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO download_history (
                task_id, source_url, source_key, payload, completed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(task_id),
                source_url,
                _payload_source_key(payload, source_url),
                _encode(payload),
                str(payload.get("completed_at") or now),
                now,
            ),
        )


def save_history_rows(rows: list[tuple[str, dict[str, Any]]]) -> None:
    """Upsert many completed-download records in one transaction.

    A library scan writes a row per file; one commit per row makes the scan cost
    scale with fsyncs rather than with work, so batches share a transaction.
    """
    if not rows:
        return
    now = utc_now()
    prepared = []
    for task_id, raw_payload in rows:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        source_url = str(payload.get("source_url") or "")
        prepared.append(
            (
                str(task_id),
                source_url,
                _payload_source_key(payload, source_url),
                _encode(payload),
                str(payload.get("completed_at") or now),
                now,
            )
        )
    with transaction() as connection:
        connection.executemany(
            """
            INSERT OR REPLACE INTO download_history (
                task_id, source_url, source_key, payload, completed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            prepared,
        )


def fail_running_tasks(error: str) -> int:
    """Mark every ``running`` row failed in one pass, for crash recovery at boot."""
    now = utc_now()
    with transaction() as connection:
        rows = connection.execute("SELECT id, payload FROM download_tasks WHERE status = 'running'").fetchall()
        updates = []
        for row in rows:
            payload = _decode(row["payload"], {})
            payload = payload if isinstance(payload, dict) else {}
            payload.update({"status": "failed", "error": error})
            updates.append((_encode(payload), now, str(row["id"])))
        if updates:
            connection.executemany(
                "UPDATE download_tasks SET status = 'failed', payload = ?, updated_at = ? WHERE id = ?",
                updates,
            )
    return len(updates)


def delete_history_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_history WHERE task_id = ?", (str(task_id),))
