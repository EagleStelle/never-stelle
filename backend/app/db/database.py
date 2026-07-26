from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from backend.app.core.config import DATABASE_PATH

_DB_LOCK = threading.RLock()
_INITIALIZED = False
_CONNECTION: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS download_tasks (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    source_key TEXT NOT NULL DEFAULT '',
    progress_pct REAL NOT NULL DEFAULT 0,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_download_tasks_status ON download_tasks(status);
CREATE INDEX IF NOT EXISTS idx_download_tasks_source_url ON download_tasks(source_url);

CREATE TABLE IF NOT EXISTS download_history (
    task_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    source_key TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_download_history_source_url ON download_history(source_url);
CREATE INDEX IF NOT EXISTS idx_download_history_source_key ON download_history(source_key);
CREATE INDEX IF NOT EXISTS idx_download_history_completed_at ON download_history(completed_at);
CREATE INDEX IF NOT EXISTS idx_download_history_order
    ON download_history(completed_at DESC, updated_at DESC, task_id DESC);

CREATE TABLE IF NOT EXISTS learned_formats (
    source_key TEXT PRIMARY KEY,
    host TEXT NOT NULL DEFAULT '',
    templates TEXT NOT NULL DEFAULT '',
    url_field_roles TEXT NOT NULL DEFAULT '',
    id_min INTEGER NOT NULL DEFAULT 0,
    id_max INTEGER NOT NULL DEFAULT 0,
    id_classes TEXT NOT NULL DEFAULT '',
    samples INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_learned_formats_host ON learned_formats(host);

CREATE TABLE IF NOT EXISTS cookie_blobs (
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
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA wal_autocheckpoint = 1000")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA cache_size = -8000")
    return connection


def _shared_connection() -> sqlite3.Connection:
    global _CONNECTION
    if _CONNECTION is None:
        _CONNECTION = _connect()
    return _CONNECTION


def close_database() -> None:
    global _CONNECTION, _INITIALIZED
    with _DB_LOCK:
        if _CONNECTION is not None:
            _CONNECTION.close()
        _CONNECTION = None
        _INITIALIZED = False


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_current_schema(connection: sqlite3.Connection) -> None:
    learned_columns = _columns(connection, "learned_formats")
    if "url_field_roles" not in learned_columns:
        connection.execute(
            "ALTER TABLE learned_formats ADD COLUMN url_field_roles TEXT NOT NULL DEFAULT ''"
        )


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
    global _CONNECTION, _INITIALIZED
    with _DB_LOCK:
        if _INITIALIZED:
            return
        if _CONNECTION is not None:
            _CONNECTION.close()
            _CONNECTION = None
        connection = _shared_connection()
        try:
            connection.executescript(SCHEMA)
            _ensure_current_schema(connection)
            connection.execute(
                """
                DELETE FROM download_tasks
                WHERE status = 'completed'
                  AND id IN (SELECT task_id FROM download_history)
                """
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        _INITIALIZED = True
