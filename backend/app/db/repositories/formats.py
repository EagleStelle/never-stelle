from __future__ import annotations

from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.db.database import transaction, utc_now
from backend.app.db.repositories.utils import _decode, _encode


def _dedupe_templates(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: list[str] = []
    for item in values:
        template = str(item or "").strip()
        if template and template not in seen:
            seen.append(template)
    return seen


def _normalize_field_roles(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        fields: list[str] = []
        raw_fields = value.get(role)
        if not isinstance(raw_fields, list):
            continue
        for raw_field in raw_fields:
            field = str(raw_field or "").strip()
            if field and field not in fields:
                fields.append(field)
        if fields:
            out[role] = fields
    return out


def _format_entry_from_row(row: Any) -> dict[str, Any]:
    templates = _dedupe_templates(_decode(str(row["templates"] or ""), []))
    url_field_roles = _normalize_field_roles(_decode(str(row["url_field_roles"] or ""), {}))
    entry: dict[str, Any] = {
        "templates": templates,
        "url_field_roles": url_field_roles,
        "host": str(row["host"] or ""),
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
    return {key: value for key, value in entry.items() if value not in ("", [], {}, None)}


def _normalize_format_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_key, raw_entry in payload.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        templates = _dedupe_templates(entry.get("templates"))
        if not templates:
            continue
        entry["templates"] = templates
        entry["url_field_roles"] = _normalize_field_roles(entry.get("url_field_roles"))
        normalized[key] = entry
    return normalized


def _save_format_rows(connection: Any, payload: dict[str, Any]) -> None:
    now = utc_now()
    normalized = _normalize_format_payload(payload)
    existing = {
        str(row["source_key"]): str(row["created_at"] or now)
        for row in connection.execute("SELECT source_key, created_at FROM learned_formats").fetchall()
    }
    if normalized:
        placeholders = ",".join("?" for _ in normalized)
        connection.execute(
            f"DELETE FROM learned_formats WHERE source_key NOT IN ({placeholders})",
            tuple(sorted(normalized)),
        )
    else:
        connection.execute("DELETE FROM learned_formats")

    for key, entry in normalized.items():
        id_classes = ",".join(sorted(str(item) for item in (entry.get("id_classes") or []) if str(item)))
        templates = _dedupe_templates(entry.get("templates"))
        url_field_roles = _normalize_field_roles(entry.get("url_field_roles"))
        connection.execute(
            """
            INSERT OR REPLACE INTO learned_formats (
                source_key, host, templates, url_field_roles, id_min, id_max, id_classes,
                samples, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                str(entry.get("host") or ""),
                _encode(templates),
                _encode(url_field_roles),
                int(entry.get("id_min") or 0),
                int(entry.get("id_max") or 0),
                id_classes,
                int(entry.get("samples") or 0),
                existing.get(key, now),
                now,
            ),
        )


def load_learned_formats_payload() -> dict[str, Any]:
    with transaction() as connection:
        rows = connection.execute(
            """
            SELECT source_key, host, templates, url_field_roles, id_min, id_max, id_classes,
                   samples
            FROM learned_formats
            ORDER BY source_key
            """
        ).fetchall()
        if rows:
            loaded: dict[str, Any] = {}
            for row in rows:
                key = normalize_source_key(row["source_key"])
                entry = _format_entry_from_row(row)
                if key and entry.get("templates"):
                    loaded[key] = entry
            return loaded
        return {}


def save_learned_formats_payload(payload: dict[str, Any]) -> None:
    with transaction() as connection:
        _save_format_rows(connection, payload)
