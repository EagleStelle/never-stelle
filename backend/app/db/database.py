from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from backend.app.core.config import DATABASE_PATH

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
    id_part TEXT NOT NULL DEFAULT '',
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


def _migrate_schema(connection: sqlite3.Connection) -> None:
    # Add columns to older databases that predate them; CREATE IF NOT EXISTS won't.
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(formats)")}
    if "templates" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN templates TEXT NOT NULL DEFAULT ''")
        columns.add("templates")
    if "url_creator_fields" not in columns:
        connection.execute("ALTER TABLE formats ADD COLUMN url_creator_fields TEXT NOT NULL DEFAULT ''")
        columns.add("url_creator_fields")
    if "template" in columns:
        _migrate_format_templates(connection)
    if "template" in columns or "creator_part" in columns:
        _drop_format_legacy_columns(connection)
    connection.execute("UPDATE queue SET source_key = '' WHERE source_key = 'others'")
    connection.execute("UPDATE history SET source_key = '' WHERE source_key = 'others'")
    connection.execute("DELETE FROM cookies WHERE key = 'ytdlp_cookies::others'")


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
            id_part TEXT NOT NULL DEFAULT '',
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
            id_part, samples, created_at, updated_at
        )
        SELECT source_key, COALESCE(host, ''), COALESCE(templates, ''),
               COALESCE(url_creator_fields, ''),
               COALESCE(id_min, 0), COALESCE(id_max, 0), COALESCE(id_classes, ''),
               COALESCE(id_part, ''), COALESCE(samples, 0),
               created_at, updated_at
        FROM formats
        """
    )
    connection.execute("DROP TABLE formats")
    connection.execute("ALTER TABLE formats_next RENAME TO formats")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_formats_host ON formats(host)")
