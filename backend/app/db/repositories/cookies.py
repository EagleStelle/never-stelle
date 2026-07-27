from __future__ import annotations

from typing import Any

from backend.app.db.database import transaction, utc_now

_METADATA_COLUMNS = "id, source_key, filename, position, length(content) AS size, created_at, updated_at"
# List order is the user's preference order: the pool breaks ties top-first.
_LIST_ORDER = "ORDER BY position ASC, created_at ASC, rowid ASC"


def _metadata_row(row: Any) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "source_key": str(row["source_key"] or ""),
        "filename": str(row["filename"] or ""),
        "position": int(row["position"] or 0),
        "size": int(row["size"] or 0),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def list_source_cookies(source_key: str) -> list[dict[str, Any]]:
    """Cookie jars stored for one source, in the user's chosen order."""
    with transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT {_METADATA_COLUMNS}
            FROM source_cookies
            WHERE source_key = ?
            {_LIST_ORDER}
            """,
            (source_key,),
        ).fetchall()
    return [_metadata_row(row) for row in rows]


def get_source_cookie(cookie_id: str) -> dict[str, Any] | None:
    with transaction() as connection:
        row = connection.execute(
            """
            SELECT id, source_key, filename, position, content, created_at, updated_at
            FROM source_cookies
            WHERE id = ?
            """,
            (cookie_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "id": str(row["id"]),
        "source_key": str(row["source_key"] or ""),
        "filename": str(row["filename"] or ""),
        "position": int(row["position"] or 0),
        "content": bytes(row["content"]),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def add_source_cookie(cookie_id: str, source_key: str, filename: str, content: bytes) -> dict[str, Any]:
    """Append a jar to the end of a source's rotation list."""
    now = utc_now()
    with transaction() as connection:
        row = connection.execute(
            "SELECT MAX(position) AS last FROM source_cookies WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        position = int(row["last"] or 0) + 1 if row and row["last"] is not None else 0
        connection.execute(
            """
            INSERT OR REPLACE INTO source_cookies (
                id, source_key, filename, position, content, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (cookie_id, source_key, filename, position, content, now, now),
        )
    return {
        "id": cookie_id,
        "source_key": source_key,
        "filename": filename,
        "position": position,
        "size": len(content),
        "created_at": now,
        "updated_at": now,
    }


def reorder_source_cookies(source_key: str, cookie_ids: list[str]) -> None:
    """Renumber a source's jars to match ``cookie_ids``; unlisted jars keep the tail."""
    now = utc_now()
    with transaction() as connection:
        rows = connection.execute(
            f"SELECT id FROM source_cookies WHERE source_key = ? {_LIST_ORDER}",
            (source_key,),
        ).fetchall()
        known = [str(row["id"]) for row in rows]
        ordered = [cookie_id for cookie_id in cookie_ids if cookie_id in known]
        ordered.extend(cookie_id for cookie_id in known if cookie_id not in ordered)
        connection.executemany(
            "UPDATE source_cookies SET position = ?, updated_at = ? WHERE id = ?",
            [(index, now, cookie_id) for index, cookie_id in enumerate(ordered)],
        )


def delete_source_cookie(cookie_id: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM source_cookies WHERE id = ?", (cookie_id,))


def delete_source_cookies(source_key: str) -> None:
    with transaction() as connection:
        connection.execute("DELETE FROM source_cookies WHERE source_key = ?", (source_key,))
