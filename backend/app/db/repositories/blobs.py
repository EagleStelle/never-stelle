from __future__ import annotations

from typing import Any

from backend.app.db.database import transaction, utc_now


def get_file_blob(key: str) -> dict[str, Any] | None:
    with transaction() as connection:
        row = connection.execute(
            """
            SELECT key, filename, content_type, content, created_at, updated_at
            FROM cookie_blobs
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
            FROM cookie_blobs
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
            INSERT OR REPLACE INTO cookie_blobs (
                key, filename, content_type, content, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key, filename, content_type, content, created_at, now),
        )


def delete_file_blob(key: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM cookie_blobs WHERE key = ?", (key,))
