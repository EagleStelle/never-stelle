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


def activity_revision() -> tuple[int, str, int, str]:
    """Cheap fingerprint of tasks + history that changes on every write.

    Lets callers cache activity-derived data without re-decoding whole tables;
    row counts plus latest timestamps flip whenever anything is added, updated,
    or removed.
    """
    with transaction() as connection:
        tasks = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM download_tasks"
        ).fetchone()
        history = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM download_history"
        ).fetchone()
    return (int(tasks[0]), str(tasks[1]), int(history[0]), str(history[1]))


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


def delete_history_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM download_history WHERE task_id = ?", (str(task_id),))
