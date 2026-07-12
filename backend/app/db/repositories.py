from __future__ import annotations

import json
from typing import Any

from backend.app.core.sources import normalize_source_key, source_key_from_url
from backend.app.db.database import transaction, utc_now


def _encode(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _decode(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _source_key(source_url: str) -> str:
    return source_key_from_url(source_url)


def load_settings_payload() -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute("SELECT value FROM settings WHERE key = ?", ("app",)).fetchone()
    payload = _decode(row["value"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def save_settings_payload(payload: dict[str, Any]) -> None:
    now = utc_now()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("app", _encode(payload if isinstance(payload, dict) else {}), now),
        )


def _row_templates(row: Any, template: str) -> list[str]:
    # Decode the persisted route list, falling back to the single template column.
    try:
        raw = row["templates"]
    except (IndexError, KeyError):
        raw = ""
    templates = [item for item in _decode(raw, []) if isinstance(item, str) and item.strip()]
    if not templates and template:
        templates = [template]
    seen: list[str] = []
    for item in templates:
        if item not in seen:
            seen.append(item)
    return seen


def _format_entry_from_row(row: Any) -> dict[str, Any]:
    template = str(row["template"] or "")
    templates = _row_templates(row, template)
    entry: dict[str, Any] = {
        "template": template or (templates[0] if templates else ""),
        "templates": templates,
        "host": str(row["host"] or ""),
        "id_part": str(row["id_part"] or ""),
        "creator_part": str(row["creator_part"] or ""),
        "samples": int(row["samples"] or 0),
    }
    id_min = int(row["id_min"] or 0)
    id_max = int(row["id_max"] or 0)
    if id_min:
        entry["id_min"] = id_min
    if id_max:
        entry["id_max"] = id_max
    id_classes = [item for item in str(row["id_classes"] or "").split(",") if item]
    if id_classes:
        entry["id_classes"] = id_classes
    return {key: value for key, value in entry.items() if value not in ("", [], None)}


def _normalize_format_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_key, raw_entry in payload.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        if not str(entry.get("template") or "").strip():
            continue
        normalized[key] = entry
    return normalized


def _save_format_rows(connection: Any, payload: dict[str, Any]) -> None:
    now = utc_now()
    normalized = _normalize_format_payload(payload)
    existing = {
        str(row["source_key"]): str(row["created_at"] or now)
        for row in connection.execute("SELECT source_key, created_at FROM formats").fetchall()
    }
    if normalized:
        placeholders = ",".join("?" for _ in normalized)
        connection.execute(
            f"DELETE FROM formats WHERE source_key NOT IN ({placeholders})",
            tuple(sorted(normalized)),
        )
    else:
        connection.execute("DELETE FROM formats")

    for key, entry in normalized.items():
        id_classes = ",".join(sorted(str(item) for item in (entry.get("id_classes") or []) if str(item)))
        template = str(entry.get("template") or "")
        templates = [str(item).strip() for item in (entry.get("templates") or []) if str(item or "").strip()]
        if template and template not in templates:
            templates = [template, *templates]
        connection.execute(
            """
            INSERT OR REPLACE INTO formats (
                source_key, host, template, templates, id_min, id_max, id_classes,
                id_part, creator_part, samples, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                str(entry.get("host") or ""),
                template,
                _encode(templates),
                int(entry.get("id_min") or 0),
                int(entry.get("id_max") or 0),
                id_classes,
                str(entry.get("id_part") or ""),
                str(entry.get("creator_part") or ""),
                int(entry.get("samples") or 0),
                existing.get(key, now),
                now,
            ),
        )


def load_learned_formats_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(
            """
            SELECT source_key, host, template, templates, id_min, id_max, id_classes,
                   id_part, creator_part, samples
            FROM formats
            ORDER BY source_key
            """
        ).fetchall()
        if rows:
            return {normalize_source_key(row["source_key"]): _format_entry_from_row(row) for row in rows}

        row = connection.execute("SELECT value FROM settings WHERE key = ?", ("learned_formats",)).fetchone()
        payload = _decode(row["value"] if row else None, {})
        payload = _normalize_format_payload(payload if isinstance(payload, dict) else {})
        if payload:
            _save_format_rows(connection, payload)
            connection.execute("DELETE FROM settings WHERE key = ?", ("learned_formats",))
        return payload


def save_learned_formats_payload(payload: dict[str, Any]) -> None:
    with transaction() as connection:
        _save_format_rows(connection, payload)
        connection.execute("DELETE FROM settings WHERE key = ?", ("learned_formats",))


def get_file_blob(key: str) -> dict[str, Any] | None:
    with transaction() as connection:
        row = connection.execute(
            """
            SELECT key, filename, content_type, content, created_at, updated_at
            FROM cookies
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
    if not row:
        return None
    return {
        "key": row["key"],
        "filename": row["filename"],
        "content_type": row["content_type"],
        "content": bytes(row["content"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_file_blob_metadata(key: str) -> dict[str, Any] | None:
    with transaction() as connection:
        row = connection.execute(
            """
            SELECT key, filename, content_type, length(content) AS size, created_at, updated_at
            FROM cookies
            WHERE key = ?
            """,
            (key,),
        ).fetchone()
    if not row:
        return None
    return {
        "key": row["key"],
        "filename": row["filename"],
        "content_type": row["content_type"],
        "size": int(row["size"] or 0),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def save_file_blob(key: str, filename: str, content: bytes, content_type: str = "application/octet-stream") -> None:
    now = utc_now()
    existing = get_file_blob_metadata(key)
    created_at = str(existing.get("created_at") or now) if existing else now
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO cookies (
                key, filename, content_type, content, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key, filename, content_type, content, created_at, now),
        )


def delete_file_blob(key: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM cookies WHERE key = ?", (key,))


def load_task_store_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute("SELECT id, payload FROM queue ORDER BY created_at, id").fetchall()
    return {"tasks": {row["id"]: _decode(row["payload"], {}) for row in rows}}


def load_active_task_store_payload() -> dict[str, Any]:
    # Only non-completed rows: completed downloads live in history and are paginated there.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT id, payload FROM queue WHERE status != 'completed' ORDER BY created_at, id"
        ).fetchall()
    return {"tasks": {row["id"]: _decode(row["payload"], {}) for row in rows}}


def count_active_by_source() -> dict[str, dict[str, int]]:
    # Status tallies for queued/running/failed straight from SQL, no payload decode or disk I/O.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, status, COUNT(*) AS n FROM queue WHERE status != 'completed' GROUP BY source_key, status"
        ).fetchall()
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        key = str(row["source_key"] or "others")
        result.setdefault(key, {})[str(row["status"] or "pending")] = int(row["n"] or 0)
    return result


def count_history_by_source() -> dict[str, int]:
    # Completed tally per source from SQL COUNT, no disk stat per row.
    with transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, COUNT(*) AS n FROM history GROUP BY source_key"
        ).fetchall()
    return {str(row["source_key"] or "others"): int(row["n"] or 0) for row in rows}


def load_history_page(limit: int, offset: int, source_key: str = "") -> tuple[list[tuple[str, dict[str, Any]]], int]:
    # One ordered page plus the filtered total, so infinite scroll knows when to stop.
    limit = max(1, int(limit))
    offset = max(0, int(offset))
    where = "WHERE source_key = ?" if source_key else ""
    params: tuple[Any, ...] = (source_key,) if source_key else ()
    with transaction() as connection:
        total = int(
            connection.execute(f"SELECT COUNT(*) FROM history {where}", params).fetchone()[0] or 0
        )
        rows = connection.execute(
            f"""
            SELECT task_id, payload FROM history {where}
            ORDER BY completed_at DESC, updated_at DESC, task_id
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
    return [(str(row["task_id"]), _decode(row["payload"], {})) for row in rows], total


def activity_revision() -> tuple[int, str, int, str]:
    """Cheap fingerprint of tasks + history that changes on every write.

    Lets callers cache activity-derived data without re-decoding whole tables;
    row counts plus latest timestamps flip whenever anything is added, updated,
    or removed.
    """
    with transaction() as connection:
        tasks = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM queue"
        ).fetchone()
        history = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM history"
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
        row = connection.execute("SELECT payload FROM queue WHERE id = ?", (task_id,)).fetchone()
        payload = _decode(row["payload"] if row else None, {})
        payload = payload if isinstance(payload, dict) else {}
        payload.update(updates)
        source_url = str(payload.get("source_url") or "")
        connection.execute(
            """
            INSERT OR REPLACE INTO queue (
                id, source_url, status, source_key, progress_pct, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                source_url,
                str(payload.get("status") or "pending"),
                str(payload.get("source_key") or _source_key(source_url)),
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
        row = connection.execute("SELECT payload FROM queue WHERE id = ?", (task_id,)).fetchone()
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
            INSERT OR REPLACE INTO queue (
                id, source_url, status, source_key, progress_pct, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                source_url,
                "running",
                str(payload.get("source_key") or _source_key(source_url)),
                _safe_float(payload.get("progress_pct")),
                _encode(payload),
                str(payload.get("created_at") or now),
                now,
            ),
        )
    return payload


def delete_task_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM queue WHERE id = ?", (str(task_id),))


def delete_task_row_if_status(task_id: str, statuses: set[str]) -> bool:
    normalized = {str(status) for status in statuses}
    if not normalized:
        return False
    placeholders = ",".join("?" for _ in normalized)
    with transaction() as connection:
        cursor = connection.execute(
            f"DELETE FROM queue WHERE id = ? AND status IN ({placeholders})",
            (str(task_id), *sorted(normalized)),
        )
    return bool(cursor.rowcount)


def load_history_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(
            "SELECT task_id, payload FROM history ORDER BY completed_at DESC, updated_at DESC, task_id"
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
            INSERT OR REPLACE INTO history (
                task_id, source_url, source_key, payload, completed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(task_id),
                source_url,
                str(payload.get("source_key") or _source_key(source_url)),
                _encode(payload),
                str(payload.get("completed_at") or now),
                now,
            ),
        )


def delete_history_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM history WHERE task_id = ?", (str(task_id),))
