"""Import a database written before the 1.1 layout, then drop what it was stored in.

1.0 held five tables (``settings``, ``queue``, ``history``, ``cookies``, ``formats``)
whose downloads kept most of their fields inside a JSON payload column. 1.1 renamed
every one of them and promoted those fields to real columns, so none of this is a
column-by-column copy: rows are expanded into the new shape, settings keys that were
renamed are carried over, and the 1.0 tables are dropped once emptied. Nothing of the
old layout survives the upgrade.

Part of the baseline rather than a later version because the migrations after it read
the tables written here.
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from typing import Any

from backend.app.core.paths import path_key

_TABLES = ("settings", "queue", "history", "cookies", "formats")
_SOURCE_KEY_RE = re.compile(r"[^a-z0-9]+")
# "others" was 1.0's placeholder for an unrecognized source; 1.1 spells that "".
_DROPPED_SOURCE_KEYS = {"others"}
# A row id starts with the engine that produced it. The payload's task_type does not:
# history defaulted it to gallerydl for every row created before that field existed.
_ENGINES = {"gallerydl", "ytdlp", "disk", "swaratelle"}
_STATUSES = {"pending", "running", "completed", "failed"}
_COOKIE_KEY_PREFIX = "ytdlp_cookies::"

# Payload keys the new tables store in columns of their own. Whatever is left rides in
# ``encoding``, the same split the repositories make when they write a row today.
_TASK_CONSUMED = frozenset(
    {
        "id",
        "task_id",
        "task_type",
        "engine",
        "source_url",
        "source_key",
        "status",
        "progress_pct",
        "creator",
        "title",
        "media_id",
        "resolved_full_path",
        "resolved_folder",
        "resolved_filename",
        "error",
        "output_dir",
        "output_template",
        "template_settings",
        "folder_template",
        "filename_template",
        "created_at",
        "updated_at",
        "last_log_lines",
    }
)
_HISTORY_CONSUMED = frozenset(
    {
        "id",
        "task_id",
        "task_type",
        "engine",
        "source_url",
        "source_key",
        "creator",
        "artist",
        "title",
        "media_id",
        "resolved_full_path",
        "resolved_path_key",
        "resolved_folder",
        "resolved_filename",
        "file_size",
        "scan_mtime_ns",
        "scan_revision",
        "template_settings",
        "folder_template",
        "filename_template",
        "needs_resolve",
        "completed_at",
        "created_at",
        "updated_at",
    }
)

_TASK_INSERT = """
INSERT OR REPLACE INTO download_tasks (
    id, source_url, source_key, status, progress_pct, engine, creator, title, media_id,
    resolved_full_path, resolved_folder, resolved_filename, error, output_dir,
    output_template, folder_template, filename_template, created_at, updated_at,
    encoding, last_log_lines
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_HISTORY_INSERT = """
INSERT OR REPLACE INTO download_history (
    id, source_url, source_key, engine, creator, title, media_id, resolved_full_path,
    resolved_path_key, resolved_folder, resolved_filename, file_size, scan_mtime_ns,
    scan_revision, folder_template, filename_template, needs_resolve, created_at,
    updated_at, encoding
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_COOKIE_INSERT = """
INSERT OR REPLACE INTO source_cookies (
    id, source_key, filename, position, content, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
"""
_FORMAT_INSERT = """
INSERT OR REPLACE INTO learned_formats (
    source_key, host, templates, id_min, id_max, id_classes, samples, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def present(connection: sqlite3.Connection) -> bool:
    return bool(_present_tables(connection))


def import_v1(connection: sqlite3.Connection) -> None:
    tables = _present_tables(connection)
    # Formats first: the location rewrite below is keyed by learned template.
    if "formats" in tables:
        _import_formats(connection)
    if "settings" in tables:
        _import_settings(connection)
    if "queue" in tables:
        _import_queue(connection)
    if "history" in tables:
        _import_history(connection)
    if "cookies" in tables:
        _import_cookies(connection)
    for table in _TABLES:
        connection.execute(f'DROP TABLE IF EXISTS "{table}"')


def _present_tables(connection: sqlite3.Connection) -> set[str]:
    placeholders = ", ".join("?" for _ in _TABLES)
    rows = connection.execute(
        f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ({placeholders})",
        _TABLES,
    ).fetchall()
    return {str(row[0]) for row in rows}


def _rows(connection: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    """Rows as dicts, narrowed to the columns this particular 1.0 build actually has."""
    have = {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()}
    wanted = [name for name in columns if name in have]
    if not wanted:
        return []
    selected = ", ".join(f'"{name}"' for name in wanted)
    cursor = connection.execute(f"SELECT {selected} FROM \"{table}\"")
    return [dict(zip(wanted, tuple(row), strict=True)) for row in cursor.fetchall()]


def _source_key(value: Any) -> str:
    key = _SOURCE_KEY_RE.sub("-", str(value or "").strip().lower()).strip("-")
    return "" if key in _DROPPED_SOURCE_KEYS else key


def _payload(raw: Any) -> dict[str, Any]:
    try:
        decoded = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _leftovers(payload: dict[str, Any], consumed: frozenset[str]) -> str:
    return _encode({key: value for key, value in payload.items() if key not in consumed})


def _engine(row_id: str, payload: dict[str, Any]) -> str:
    prefix = str(row_id).split(":", 1)[0].strip().lower()
    if prefix in _ENGINES:
        return prefix
    return _text(payload.get("engine"), payload.get("task_type")) or "gallerydl"


def _templates(payload: dict[str, Any]) -> tuple[str, str]:
    saved = payload.get("template_settings")
    saved = saved if isinstance(saved, dict) else {}
    return (
        _text(payload.get("folder_template"), saved.get("folder_template")),
        _text(payload.get("filename_template"), saved.get("filename_template")),
    )


def _import_formats(connection: sqlite3.Connection) -> None:
    columns = (
        "source_key",
        "host",
        "templates",
        "id_min",
        "id_max",
        "id_classes",
        "samples",
        "created_at",
        "updated_at",
    )
    for row in _rows(connection, "formats", columns):
        key = _source_key(row.get("source_key"))
        if not key:
            continue
        connection.execute(
            _FORMAT_INSERT,
            (
                key,
                str(row.get("host") or ""),
                str(row.get("templates") or ""),
                _integer(row.get("id_min")),
                _integer(row.get("id_max")),
                str(row.get("id_classes") or ""),
                _integer(row.get("samples")),
                str(row.get("created_at") or ""),
                str(row.get("updated_at") or ""),
            ),
        )


def _import_settings(connection: sqlite3.Connection) -> None:
    learned = _learned_templates(connection)
    for row in _rows(connection, "settings", ("key", "value", "updated_at")):
        value = str(row.get("value") or "")
        payload = _payload(value)
        if payload:
            value = _encode(_rewrite_settings(payload, learned))
        connection.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            (str(row.get("key") or ""), value, str(row.get("updated_at") or "")),
        )


def _learned_templates(connection: sqlite3.Connection) -> dict[str, list[str]]:
    learned: dict[str, list[str]] = {}
    for key, raw in connection.execute("SELECT source_key, templates FROM learned_formats").fetchall():
        try:
            templates = json.loads(raw or "[]")
        except (TypeError, ValueError):
            continue
        if isinstance(templates, list):
            learned[_source_key(key)] = [str(template) for template in templates if str(template or "")]
    return learned


def _rewrite_settings(payload: dict[str, Any], learned: dict[str, list[str]]) -> dict[str, Any]:
    """Rename the settings keys 1.1 reads under different names; leave the rest alone."""
    fields = payload.pop("source_creator_fields", None)
    if isinstance(fields, dict) and not payload.get("source_fields"):
        payload["source_fields"] = fields
    locations = payload.get("site_locations")
    if isinstance(locations, dict):
        # 1.0 stored one folder per source; 1.1 stores one per learned URL format, and
        # the next migration turns each of those into a subpath under the source root.
        payload["site_locations"] = {
            key: value
            if isinstance(value, dict)
            else {template: value for template in learned.get(_source_key(key), [])}
            for key, value in locations.items()
        }
    templates = payload.get("source_templates")
    if isinstance(templates, dict):
        # Same fan-out for the folder/filename pair, which 1.0 also held one of per
        # source. A pair left flat is dropped on read, and the rename pass would then
        # walk the library onto the base template.
        payload["source_templates"] = {
            key: value
            if all(isinstance(inner, dict) for inner in value.values())
            else {template: dict(value) for template in learned.get(_source_key(key), [])}
            for key, value in templates.items()
            if isinstance(value, dict)
        }
    return payload


def _import_queue(connection: sqlite3.Connection) -> None:
    columns = ("id", "source_url", "source_key", "status", "progress_pct", "payload", "created_at", "updated_at")
    for row in _rows(connection, "queue", columns):
        row_id = str(row.get("id") or "")
        if not row_id:
            continue
        payload = _payload(row.get("payload"))
        source_url = _text(row.get("source_url"), payload.get("source_url"))
        status = _text(payload.get("status"), row.get("status"))
        folder_template, filename_template = _templates(payload)
        created_at = _text(payload.get("created_at"), row.get("created_at"))
        connection.execute(
            _TASK_INSERT,
            (
                row_id,
                source_url,
                _source_key(_text(row.get("source_key"), payload.get("source_key"))),
                status if status in _STATUSES else "failed",
                _number(_text(payload.get("progress_pct"), row.get("progress_pct"))),
                _engine(row_id, payload),
                _text(payload.get("creator")),
                _text(payload.get("title")),
                _text(payload.get("media_id")),
                _text(payload.get("resolved_full_path")),
                _text(payload.get("resolved_folder")),
                _text(payload.get("resolved_filename")),
                _text(payload.get("error")),
                _text(payload.get("output_dir")),
                _text(payload.get("output_template")),
                folder_template,
                filename_template,
                created_at,
                _text(row.get("updated_at"), payload.get("updated_at"), created_at),
                _leftovers(payload, _TASK_CONSUMED),
                _encode(list(payload.get("last_log_lines") or [])),
            ),
        )


def _import_history(connection: sqlite3.Connection) -> None:
    columns = ("task_id", "source_url", "source_key", "payload", "completed_at", "updated_at")
    for row in _rows(connection, "history", columns):
        row_id = str(row.get("task_id") or "")
        if not row_id:
            continue
        payload = _payload(row.get("payload"))
        resolved_path = _text(payload.get("resolved_full_path"))
        folder_template, filename_template = _templates(payload)
        # 1.0 stamped one completed_at; 1.1 tracks when the row was made and last touched.
        created_at = _text(payload.get("completed_at"), row.get("completed_at"))
        connection.execute(
            _HISTORY_INSERT,
            (
                row_id,
                _text(row.get("source_url"), payload.get("source_url")),
                _source_key(_text(row.get("source_key"), payload.get("source_key"))),
                _engine(row_id, payload),
                # Disk-scanned rows named the creator "artist" before the field settled.
                _text(payload.get("creator"), payload.get("artist")),
                _text(payload.get("title")),
                _text(payload.get("media_id")),
                resolved_path,
                path_key(resolved_path) if resolved_path else "",
                _text(payload.get("resolved_folder")),
                _text(payload.get("resolved_filename")),
                _integer(payload.get("file_size")),
                # Left unstamped so the next library scan probes the file and fills them.
                0,
                "",
                folder_template,
                filename_template,
                0,
                created_at,
                _text(row.get("updated_at"), created_at),
                _leftovers(payload, _HISTORY_CONSUMED),
            ),
        )


def _import_cookies(connection: sqlite3.Connection) -> None:
    rows = _rows(connection, "cookies", ("key", "filename", "content", "created_at", "updated_at"))
    positions: dict[str, int] = {}
    for row in sorted(rows, key=lambda entry: str(entry.get("created_at") or "")):
        # 1.0 held one jar per source under "ytdlp_cookies::<source>"; 1.1 holds a list.
        key = str(row.get("key") or "")
        source_key = _source_key(key[len(_COOKIE_KEY_PREFIX) :] if key.startswith(_COOKIE_KEY_PREFIX) else key)
        content = row.get("content")
        if not source_key or content is None:
            continue
        position = positions.get(source_key, 0)
        positions[source_key] = position + 1
        connection.execute(
            _COOKIE_INSERT,
            (
                uuid.uuid4().hex,
                source_key,
                str(row.get("filename") or ""),
                position,
                bytes(content),
                str(row.get("created_at") or ""),
                str(row.get("updated_at") or ""),
            ),
        )
