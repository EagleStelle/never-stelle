from __future__ import annotations

import json
import re
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import DATABASE_PATH
from backend.app.core.sources import normalize_source_key

_TOKEN_NAME_RE = re.compile(r"[^a-zA-Z0-9_]+")
_FORMAT_TOKEN_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

_DB_LOCK = threading.RLock()
_INITIALIZED = False
# One long-lived connection reused for every transaction, guarded by _DB_LOCK.
# Opening/closing per call re-ran the WAL pragma and checkpointed the journal to
# disk each time; on a NAS spinning disk that dominated latency under load.
_CONNECTION: sqlite3.Connection | None = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    source_key TEXT NOT NULL DEFAULT '',
    progress_pct REAL NOT NULL DEFAULT 0,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_source_url ON queue(source_url);

CREATE TABLE IF NOT EXISTS history (
    task_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    source_key TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_source_url ON history(source_url);
CREATE INDEX IF NOT EXISTS idx_history_source_key ON history(source_key);
CREATE INDEX IF NOT EXISTS idx_history_completed_at ON history(completed_at);
CREATE INDEX IF NOT EXISTS idx_history_order ON history(completed_at DESC, updated_at DESC, task_id DESC);

CREATE TABLE IF NOT EXISTS formats (
    source_key TEXT PRIMARY KEY,
    host TEXT NOT NULL DEFAULT '',
    templates TEXT NOT NULL DEFAULT '',
    url_creator_fields TEXT NOT NULL DEFAULT '',
    id_min INTEGER NOT NULL DEFAULT 0,
    id_max INTEGER NOT NULL DEFAULT 0,
    id_classes TEXT NOT NULL DEFAULT '',
    samples INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_formats_host ON formats(host);

CREATE TABLE IF NOT EXISTS cookies (
    key TEXT PRIMARY KEY,
    filename TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def database_path() -> Path:
    return DATABASE_PATH


def _connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(DATABASE_PATH), timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    # NORMAL is durable-enough under WAL (no corruption risk) and drops one fsync
    # per commit; batched checkpoints keep the WAL from growing unbounded.
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA wal_autocheckpoint = 1000")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA cache_size = -8000")
    return connection


def _shared_connection() -> sqlite3.Connection:
    # Lazily open and keep one connection for the process; _DB_LOCK serializes use.
    global _CONNECTION
    if _CONNECTION is None:
        _CONNECTION = _connect()
    return _CONNECTION


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    initialize_database()
    with _DB_LOCK:
        connection = _shared_connection()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def initialize_database() -> None:
    global _INITIALIZED, _CONNECTION
    with _DB_LOCK:
        if _INITIALIZED:
            return
        # A reset (tests swap DATABASE_PATH and clear _INITIALIZED) must rebind the
        # shared connection to the current path, so drop any stale one first.
        if _CONNECTION is not None:
            try:
                _CONNECTION.close()
            except Exception:
                pass
            _CONNECTION = None
        connection = _shared_connection()
        try:
            connection.executescript(SCHEMA)
            _migrate_schema(connection)
            connection.execute(
                "DELETE FROM queue WHERE status = 'completed' AND id IN (SELECT task_id FROM history)"
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        _INITIALIZED = True


def _migrate_legacy_settings_json(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM settings WHERE key = ?", ("app",)).fetchone()
    if not row or not row["value"]:
        return
    try:
        payload = json.loads(row["value"])
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    
    modified = False

    # Migrate legacy JSON root keys and remove backwards compatibility definitions.
    for old_key, new_key in [
        ("site_profiles", "source_profiles"),
        ("source_locations", "site_locations"),
        ("source_template_settings", "source_templates"),
        ("scrape_rules", "source_scrape_rules"),
        ("token_roles", "source_token_roles"),
    ]:
        if old_key in payload:
            if new_key not in payload:
                payload[new_key] = payload[old_key]
            del payload[old_key]
            modified = True

    format_templates = _format_templates_by_source(connection)
    first_templates = {key: templates[0] for key, templates in format_templates.items() if templates}

    normalized_token_roles = _normalize_settings_token_roles(payload.get("source_token_roles"))
    if payload.get("source_token_roles") != normalized_token_roles:
        payload["source_token_roles"] = normalized_token_roles
        modified = True

    if "source_templates" in payload:
        source_templates = _migrate_settings_source_templates(
            payload.get("source_templates"),
            first_templates,
            normalized_token_roles,
        )
        if payload.get("source_templates") != source_templates:
            payload["source_templates"] = source_templates
            modified = True

    if "source_scrape_rules" in payload:
        source_scrape_rules = _migrate_settings_scrape_rules(
            payload.get("source_scrape_rules"),
            format_templates,
        )
        if payload.get("source_scrape_rules") != source_scrape_rules:
            payload["source_scrape_rules"] = source_scrape_rules
            modified = True

    if modified:
        now = datetime.now(UTC).isoformat()
        connection.execute(
            "UPDATE settings SET value = ?, updated_at = ? WHERE key = ?",
            (json.dumps(payload, ensure_ascii=False), now, "app"),
        )


def _normalize_token_name(value: Any) -> str:
    token = _TOKEN_NAME_RE.sub("_", str(value or "").strip()).strip("_")
    if not token or not re.match(r"[a-zA-Z_]", token):
        return ""
    return token.lower()


def _normalize_settings_token_roles(raw: Any) -> dict[str, dict[str, str]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, str]] = {}
    for raw_key, raw_roles in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_roles, dict):
            continue
        roles: dict[str, str] = {}
        title_claimed = False
        for raw_token, raw_role in raw_roles.items():
            token = _normalize_token_name(raw_token)
            role = str(raw_role or "").strip().lower()
            if role in {"username", "nickname"}:
                role = "creator"
            if not token or role not in {"creator", "title", "ignore"}:
                continue
            if role == "title":
                if title_claimed:
                    continue
                title_claimed = True
            if role != "ignore":
                roles[token] = role
        if roles:
            out[key] = roles
    return out


def _normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder = str(source.get("folder_template") or "").strip() or "{{username}}"
    filename = str(source.get("filename_template") or "").strip() or "{{username}} - {{title}} [{{id}}]"
    return {"folder_template": folder, "filename_template": filename}


def _migrate_template_tokens(template: str, token_roles: dict[str, str] | None = None) -> str:
    roles = token_roles if isinstance(token_roles, dict) else {}
    replacements = {
        token: role
        for token, role in roles.items()
        if token and role == "title" and token != role
    }
    if not replacements:
        return str(template or "").strip()

    def replace(match: re.Match[str]) -> str:
        token = _normalize_token_name(match.group(1))
        return f"{{{{{replacements[token]}}}}}" if token in replacements else match.group(0)

    return re.sub(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}", replace, str(template or "").strip())


def _migrate_template_settings_tokens(
    template_settings: Any,
    token_roles: dict[str, str] | None = None,
) -> dict[str, str]:
    normalized = _normalize_template_settings(template_settings)
    return {
        "folder_template": _migrate_template_tokens(normalized["folder_template"], token_roles),
        "filename_template": _migrate_template_tokens(normalized["filename_template"], token_roles),
    }


def _format_templates_by_source(connection: sqlite3.Connection) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    rows = connection.execute("SELECT source_key, templates FROM formats ORDER BY source_key").fetchall()
    for row in rows:
        key = normalize_source_key(row["source_key"])
        templates = _decode_templates(row["templates"])
        if key and templates:
            out[key] = templates
    return out


def _migrate_settings_source_templates(
    raw: Any,
    first_templates: dict[str, str],
    token_roles: dict[str, dict[str, str]],
) -> dict[str, dict[str, dict[str, str]]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, dict[str, str]]] = {}
    for raw_key, raw_value in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_value, dict):
            continue
        roles = token_roles.get(key)
        if "folder_template" in raw_value or "filename_template" in raw_value:
            target_format = first_templates.get(key, "")
            if target_format:
                out[key] = {
                    target_format: _migrate_template_settings_tokens(raw_value, roles),
                }
            continue
        formats: dict[str, dict[str, str]] = {}
        for raw_format, raw_settings in raw_value.items():
            if isinstance(raw_settings, dict):
                fmt = str(raw_format or "").strip()
                if fmt:
                    formats[fmt] = _migrate_template_settings_tokens(raw_settings, roles)
        if formats:
            out[key] = formats
    return out


def _migrate_settings_scrape_rules(
    raw: Any,
    format_templates: dict[str, list[str]],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for raw_key, raw_platform in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_platform, dict):
            continue
        rules: list[dict[str, Any]] = []
        seen_tokens: set[str] = set()
        valid_count = 0
        for raw_rule in raw_platform.get("rules") or []:
            if not isinstance(raw_rule, dict):
                continue
            xpath = str(raw_rule.get("xpath") or "").strip()
            selector = str(raw_rule.get("selector") or "").strip()
            match_label = str(raw_rule.get("match_label") or "").strip()
            if not xpath and not selector and not match_label:
                continue
            token = _normalize_token_name(raw_rule.get("token")) or f"var{valid_count}"
            if token in seen_tokens:
                suffix = 0
                while f"{token}_{suffix}" in seen_tokens:
                    suffix += 1
                token = f"{token}_{suffix}"
            seen_tokens.add(token)
            rules.append(
                {
                    "token": token,
                    "match_label": match_label,
                    "selector": selector,
                    "attr": str(raw_rule.get("attr") or "").strip() or "text",
                    "multi": bool(raw_rule.get("multi")),
                    "xpath": xpath,
                    "format": _migrate_rule_format_scope(
                        str(raw_rule.get("format") or "").strip(),
                        format_templates.get(key) or [],
                    ),
                }
            )
            valid_count += 1
        if rules:
            out[key] = {"rules": rules}
    return out


def _format_scope_key(template: str) -> str:
    def replace(match: re.Match[str]) -> str:
        token = _normalize_token_name(match.group(1))
        if token == "id":
            return "{id}"
        if token in {"creator", "username", "nickname"}:
            return "{creator}"
        return "{var}"

    return _FORMAT_TOKEN_RE.sub(replace, str(template or "").strip())


def _migrate_rule_format_scope(rule_format: str, templates: list[str]) -> str:
    value = str(rule_format or "").strip()
    if not templates:
        return value
    if value in templates:
        return value
    if not value:
        return templates[0]

    scope = _format_scope_key(value)
    matches = [template for template in templates if _format_scope_key(template) == scope]
    if len(matches) == 1:
        return matches[0]
    if len(templates) == 1:
        return templates[0]
    return value


def _migrate_schema(connection: sqlite3.Connection) -> None:
    # Add columns to older databases that predate them; CREATE IF NOT EXISTS won't.
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(formats)")}
    if "host" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN host TEXT NOT NULL DEFAULT ''")
        columns.add("host")
    if "templates" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN templates TEXT NOT NULL DEFAULT ''")
        columns.add("templates")
    if "url_creator_fields" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN url_creator_fields TEXT NOT NULL DEFAULT ''")
        columns.add("url_creator_fields")
    if "id_min" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN id_min INTEGER NOT NULL DEFAULT 0")
        columns.add("id_min")
    if "id_max" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN id_max INTEGER NOT NULL DEFAULT 0")
        columns.add("id_max")
    if "id_classes" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN id_classes TEXT NOT NULL DEFAULT ''")
        columns.add("id_classes")
    if "samples" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN samples INTEGER NOT NULL DEFAULT 0")
        columns.add("samples")
    if "created_at" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
        columns.add("created_at")
    if "updated_at" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
        columns.add("updated_at")
    if "template" in columns:
        _migrate_format_templates(connection)
    if {"template", "creator_part", "id_part"} & columns:
        _drop_format_legacy_columns(connection)
    connection.execute("UPDATE queue SET source_key = '' WHERE source_key = 'others'")
    connection.execute("UPDATE history SET source_key = '' WHERE source_key = 'others'")
    connection.execute("DELETE FROM cookies WHERE key = 'ytdlp_cookies::others'")
    _migrate_engine_tags(connection)
    _migrate_legacy_learned_formats_row(connection)
    _migrate_legacy_settings_json(connection)
    _migrate_stem_titles(connection)


def _migrate_stem_titles(connection: sqlite3.Connection) -> None:
    from backend.app.services.tasks.naming import filename_template_fields

    for table, key_column in (("queue", "id"), ("history", "task_id")):
        updates: list[tuple[str, str]] = []
        for row in connection.execute(f"SELECT {key_column}, payload FROM {table}"):
            try:
                payload = json.loads(row["payload"])
            except Exception:
                continue
            if not isinstance(payload, dict) or not str(payload.get("title") or "").strip():
                continue
            settings = payload.get("template_settings")
            template = str((settings or {}).get("filename_template") or "").strip()
            if not isinstance(settings, dict) or not template:
                continue
            title = filename_template_fields(payload.get("resolved_filename") or "", template).get("title", "")
            if title != payload["title"]:
                payload["title"] = title
                updates.append((json.dumps(payload), row[key_column]))
        connection.executemany(f"UPDATE {table} SET payload = ? WHERE {key_column} = ?", updates)


def _migrate_engine_tags(connection: sqlite3.Connection) -> None:
    # gallery-dl is now the universal broker engine; yt-dlp survives only as a
    # fallback and as gallery-dl's ytdl backend. Rewrite persisted rows so no
    # queue task routes to yt-dlp as its primary engine and no history row
    # reports a stale engine. `disk` (reconstructed) tags are left intact.
    # Drop the stale output_template too: it was built for yt-dlp's `%(ext)s`
    # syntax and is meaningless to gallery-dl. The worker rebuilds it on run.
    connection.execute(
        "UPDATE queue SET payload = json_remove("
        "json_set(payload, '$.engine', 'gallerydl'), '$.engine_policy', '$.output_template') "
        "WHERE json_valid(payload) AND ("
        "json_extract(payload, '$.engine') IS NOT 'gallerydl' "
        "OR json_extract(payload, '$.engine_policy') IS NOT NULL)"
    )
    for key in ("$.task_type", "$.engine"):
        connection.execute(
            f"UPDATE history SET payload = json_set(payload, '{key}', 'gallerydl') "
            f"WHERE json_valid(payload) AND json_extract(payload, '{key}') = 'ytdlp'"
        )


def _decode_json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _dedupe_text_list(values: Any) -> list[str]:
    if isinstance(values, str):
        raw = values.strip()
        if not raw:
            values = []
        elif raw.startswith("["):
            decoded = _decode_json(raw, [])
            values = decoded if isinstance(decoded, list) else [raw]
        else:
            values = [raw]
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for item in values:
        value = str(item or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _normalize_legacy_creator_fields(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname"):
        fields = _dedupe_text_list(value.get(role))
        if fields:
            out[role] = fields
    return out


def _merge_creator_fields(
    first: dict[str, list[str]],
    second: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for role in ("username", "nickname"):
        values: list[str] = []
        for source in (first, second):
            for field in source.get(role, []):
                if field not in values:
                    values.append(field)
        if values:
            merged[role] = values
    return merged


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _id_classes(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(sorted(str(item).strip() for item in value if str(item).strip()))
    return ",".join(item for item in str(value or "").split(",") if item)


def _normalize_legacy_format_payload(payload: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_key, raw_entry in payload.items():
        key = normalize_source_key(raw_key)
        if not key:
            continue
        entry = raw_entry if isinstance(raw_entry, dict) else {"template": raw_entry}
        templates = _dedupe_text_list(entry.get("templates"))
        template = str(entry.get("template") or "").strip()
        if template and template not in templates:
            templates = [template, *templates]
        if not templates:
            continue
        normalized[key] = {
            "templates": templates,
            "url_creator_fields": _normalize_legacy_creator_fields(entry.get("url_creator_fields")),
            "host": str(entry.get("host") or ""),
            "id_min": _int_or_zero(entry.get("id_min")),
            "id_max": _int_or_zero(entry.get("id_max")),
            "id_classes": _id_classes(entry.get("id_classes")),
            "samples": _int_or_zero(entry.get("samples")),
        }
    return normalized


def _migrate_legacy_learned_formats_row(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM settings WHERE key = ?", ("learned_formats",)).fetchone()
    if not row:
        return

    payload = _decode_json(row["value"], {})
    normalized = _normalize_legacy_format_payload(payload)
    now = datetime.now(UTC).isoformat()

    for key, entry in normalized.items():
        existing = connection.execute("SELECT * FROM formats WHERE source_key = ?", (key,)).fetchone()
        existing_templates = _decode_templates(existing["templates"] if existing else "")
        templates = [*existing_templates]
        for template in entry["templates"]:
            if template not in templates:
                templates.append(template)
        existing_creator_fields = _normalize_legacy_creator_fields(
            _decode_json(existing["url_creator_fields"] if existing else "", {})
        )
        creator_fields = _merge_creator_fields(existing_creator_fields, entry["url_creator_fields"])
        connection.execute(
            """
            INSERT OR REPLACE INTO formats (
                source_key, host, templates, url_creator_fields, id_min, id_max, id_classes,
                samples, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                str((existing["host"] if existing else "") or entry["host"] or ""),
                json.dumps(templates, ensure_ascii=False),
                json.dumps(creator_fields, ensure_ascii=False),
                _int_or_zero((existing["id_min"] if existing else 0) or entry["id_min"]),
                _int_or_zero((existing["id_max"] if existing else 0) or entry["id_max"]),
                str((existing["id_classes"] if existing else "") or entry["id_classes"] or ""),
                _int_or_zero((existing["samples"] if existing else 0) or entry["samples"]),
                str((existing["created_at"] if existing else "") or now),
                now,
            ),
        )

    connection.execute("DELETE FROM settings WHERE key = ?", ("learned_formats",))


def _decode_templates(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    out: list[str] = []
    for item in payload:
        template = str(item or "").strip()
        if template and template not in out:
            out.append(template)
    return out


def _migrate_format_templates(connection: sqlite3.Connection) -> None:
    rows = connection.execute("SELECT source_key, template, templates FROM formats").fetchall()
    for row in rows:
        legacy = str(row["template"] or "").strip()
        templates = _decode_templates(row["templates"])
        if legacy and legacy not in templates:
            templates = [legacy, *templates]
        connection.execute(
            "UPDATE formats SET templates = ? WHERE source_key = ?",
            (json.dumps(templates, ensure_ascii=False), row["source_key"]),
        )


def _drop_format_legacy_columns(connection: sqlite3.Connection) -> None:
    connection.execute("DROP INDEX IF EXISTS idx_formats_host")
    connection.execute("DROP TABLE IF EXISTS formats_next")
    connection.execute(
        """
        CREATE TABLE formats_next (
            source_key TEXT PRIMARY KEY,
            host TEXT NOT NULL DEFAULT '',
            templates TEXT NOT NULL DEFAULT '',
            url_creator_fields TEXT NOT NULL DEFAULT '',
            id_min INTEGER NOT NULL DEFAULT 0,
            id_max INTEGER NOT NULL DEFAULT 0,
            id_classes TEXT NOT NULL DEFAULT '',
            samples INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO formats_next (
            source_key, host, templates, url_creator_fields, id_min, id_max, id_classes,
            samples, created_at, updated_at
        )
        SELECT source_key, COALESCE(host, ''), COALESCE(templates, ''),
               COALESCE(url_creator_fields, ''),
               COALESCE(id_min, 0), COALESCE(id_max, 0), COALESCE(id_classes, ''),
               COALESCE(samples, 0),
               created_at, updated_at
        FROM formats
        """
    )
    connection.execute("DROP TABLE formats")
    connection.execute("ALTER TABLE formats_next RENAME TO formats")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_formats_host ON formats(host)")
