from __future__ import annotations

import json
from typing import Any

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


def _site_category(source_url: str) -> str:
    source_url = str(source_url or "").lower()
    if "youtube.com" in source_url or "youtu.be" in source_url:
        return "youtube"
    if "facebook.com" in source_url or "fb.com" in source_url or "fb.watch" in source_url:
        return "facebook"
    if "instagram.com" in source_url:
        return "instagram"
    if "tiktok.com" in source_url:
        return "tiktok"
    return "others"


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


def get_file_blob(key: str) -> dict[str, Any] | None:
    with transaction() as connection:
        row = connection.execute(
            """
            SELECT key, filename, content_type, content, created_at, updated_at
            FROM file_blobs
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
            FROM file_blobs
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
            INSERT OR REPLACE INTO file_blobs (
                key, filename, content_type, content, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key, filename, content_type, content, created_at, now),
        )


def delete_file_blob(key: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM file_blobs WHERE key = ?", (key,))


def load_task_store_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute("SELECT id, payload FROM tasks ORDER BY created_at, id").fetchall()
    return {"tasks": {row["id"]: _decode(row["payload"], {}) for row in rows}}


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
        row = connection.execute("SELECT payload FROM tasks WHERE id = ?", (task_id,)).fetchone()
        payload = _decode(row["payload"] if row else None, {})
        payload = payload if isinstance(payload, dict) else {}
        payload.update(updates)
        source_url = str(payload.get("source_url") or "")
        connection.execute(
            """
            INSERT OR REPLACE INTO tasks (
                id, source_url, status, site_category, progress_pct, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                source_url,
                str(payload.get("status") or "pending"),
                str(payload.get("site_category") or _site_category(source_url)),
                _safe_float(payload.get("progress_pct")),
                _encode(payload),
                str(payload.get("created_at") or now),
                now,
            ),
        )
    return payload


def delete_task_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM tasks WHERE id = ?", (str(task_id),))


def load_task_meta_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute("SELECT task_id, payload FROM task_meta ORDER BY task_id").fetchall()
    return {"tasks": {row["task_id"]: _decode(row["payload"], {}) for row in rows}}


def load_task_meta_row(task_id: str) -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute(
            "SELECT payload FROM task_meta WHERE task_id = ?", (str(task_id),)
        ).fetchone()
    payload = _decode(row["payload"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def merge_task_meta_payload(task_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Atomically merge ``updates`` into one task_meta row and return it."""
    task_id = str(task_id)
    now = utc_now()
    with transaction() as connection:
        row = connection.execute("SELECT payload FROM task_meta WHERE task_id = ?", (task_id,)).fetchone()
        payload = _decode(row["payload"] if row else None, {})
        payload = payload if isinstance(payload, dict) else {}
        payload.update(updates)
        connection.execute(
            """
            INSERT OR REPLACE INTO task_meta (task_id, payload, updated_at)
            VALUES (?, ?, ?)
            """,
            (task_id, _encode(payload), now),
        )
    return payload


def delete_task_meta_row(task_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM task_meta WHERE task_id = ?", (str(task_id),))


def load_history_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(
            "SELECT task_id, payload FROM download_history ORDER BY completed_at DESC, updated_at DESC, task_id"
        ).fetchall()
    return {"entries": {row["task_id"]: _decode(row["payload"], {}) for row in rows}}


def save_history_payload(data: dict[str, Any]) -> None:
    entries = data.get("entries") if isinstance(data, dict) else {}
    entries = entries if isinstance(entries, dict) else {}
    now = utc_now()
    with transaction() as connection:
        connection.execute("DELETE FROM download_history")
        for task_id, payload in entries.items():
            payload = payload if isinstance(payload, dict) else {}
            source_url = str(payload.get("source_url") or "")
            connection.execute(
                """
                INSERT OR REPLACE INTO download_history (
                    task_id, source_url, site_category, payload, completed_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(task_id),
                    source_url,
                    str(payload.get("site_category") or _site_category(source_url)),
                    _encode(payload),
                    str(payload.get("completed_at") or now),
                    now,
                ),
            )
