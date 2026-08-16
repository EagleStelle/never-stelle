from __future__ import annotations

from typing import Any

from backend.app.db.database import database_path, transaction
from backend.app.db.repositories.utils import _decode

PUBLIC_TABLES: tuple[str, ...] = (
    "download_history",
    "download_tasks",
    "learned_formats",
    "seeded_downloads",
    "download_enrichment_jobs",
)

_ORDER_BY: dict[str, str] = {
    "download_history": "created_at DESC, id DESC",
    "download_tasks": "created_at DESC, id DESC",
    "learned_formats": "source_key ASC",
    "seeded_downloads": "seeded_at DESC, task_id DESC",
    "download_enrichment_jobs": "created_at ASC, id ASC",
}

_JSON_COLUMNS: dict[str, set[str]] = {
    "download_history": {"encoding"},
    "download_tasks": {"encoding", "last_log_lines"},
    "learned_formats": {"templates", "id_classes"},
    "download_enrichment_jobs": {"payload"},
}


def _validate_table(table: str) -> str:
    value = str(table or "").strip()
    if value not in PUBLIC_TABLES:
        raise ValueError(f"Unsupported integration table: {value}")
    return value


def _row_payload(table: str, row: Any, *, decode_json: bool) -> dict[str, Any]:
    payload = dict(row)
    if not decode_json:
        return payload
    for column in _JSON_COLUMNS.get(table, set()):
        if column in payload:
            payload[column] = _decode(str(payload.get(column) or ""), [] if column == "last_log_lines" else {})
    return payload


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def integration_schema() -> dict[str, Any]:
    with transaction() as connection:
        tables = []
        for table in PUBLIC_TABLES:
            columns = [
                {
                    "name": str(row["name"]),
                    "type": str(row["type"] or ""),
                    "not_null": bool(row["notnull"]),
                    "primary_key": bool(row["pk"]),
                    "default": row["dflt_value"],
                }
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            count_row = connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            tables.append({"name": table, "rows": int(count_row["n"] or 0), "columns": columns})
    return {
        "database": {
            "engine": "sqlite",
            "path": str(database_path()),
        },
        "tables": tables,
    }


def list_integration_rows(table: str, limit: int, offset: int, *, decode_json: bool = True) -> dict[str, Any]:
    table = _validate_table(table)
    limit = max(1, min(1000, int(limit)))
    offset = max(0, int(offset))
    order_by = _ORDER_BY[table]
    with transaction() as connection:
        total_row = connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        rows = connection.execute(
            f"SELECT * FROM {table} ORDER BY {order_by} LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    total = int(total_row["n"] or 0)
    next_offset = offset + limit if offset + limit < total else None
    payload: dict[str, Any] = {
        "table": table,
        "limit": limit,
        "offset": offset,
        "total": total,
        "rows": [_row_payload(table, row, decode_json=decode_json) for row in rows],
    }
    if next_offset is not None:
        payload["next_offset"] = next_offset
    return payload


def list_download_records(
    state: str,
    limit: int,
    offset: int,
    source_key: str = "",
    search: str = "",
) -> dict[str, Any]:
    normalized_state = str(state or "history").strip().lower()
    if normalized_state in {"completed", "history"}:
        table = "download_history"
        status = "completed"
    elif normalized_state in {"active", "tasks", "queue"}:
        table = "download_tasks"
        status = ""
    else:
        raise ValueError("State must be history or active.")

    limit = max(1, min(1000, int(limit)))
    offset = max(0, int(offset))
    clauses: list[str] = []
    params: list[Any] = []
    if table == "download_tasks":
        clauses.append("status != 'completed'")
    if source_key:
        clauses.append("source_key = ?")
        params.append(source_key)
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
    order_by = "created_at DESC, id DESC"
    with transaction() as connection:
        total_row = connection.execute(f"SELECT COUNT(*) AS n FROM {table} {where}", params).fetchone()
        rows = connection.execute(
            f"SELECT * FROM {table} {where} ORDER BY {order_by} LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    total = int(total_row["n"] or 0)
    next_offset = offset + limit if offset + limit < total else None
    records = [_row_payload(table, row, decode_json=True) for row in rows]
    if status:
        for record in records:
            record["status"] = status
    payload: dict[str, Any] = {
        "state": "history" if table == "download_history" else "active",
        "limit": limit,
        "offset": offset,
        "total": total,
        "records": records,
    }
    if next_offset is not None:
        payload["next_offset"] = next_offset
    return payload
