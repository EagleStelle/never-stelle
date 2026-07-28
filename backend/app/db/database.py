from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
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
    id                  TEXT PRIMARY KEY,
    source_url          TEXT NOT NULL DEFAULT '',
    source_key          TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','running','completed','failed')),
    progress_pct        REAL NOT NULL DEFAULT 0,
    engine              TEXT NOT NULL DEFAULT '',
    creator             TEXT NOT NULL DEFAULT '',
    title               TEXT NOT NULL DEFAULT '',
    media_id            TEXT NOT NULL DEFAULT '',
    resolved_full_path  TEXT NOT NULL DEFAULT '',
    resolved_folder     TEXT NOT NULL DEFAULT '',
    resolved_filename   TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    output_dir          TEXT NOT NULL DEFAULT '',
    output_template     TEXT NOT NULL DEFAULT '',
    folder_template     TEXT NOT NULL DEFAULT '',
    filename_template   TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    encoding            TEXT NOT NULL DEFAULT '{}',
    last_log_lines      TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created
    ON download_tasks(status, created_at, id);

CREATE TABLE IF NOT EXISTS download_history (
    id                  TEXT PRIMARY KEY,
    source_url          TEXT    NOT NULL DEFAULT '',
    source_key          TEXT    NOT NULL DEFAULT '',
    engine              TEXT    NOT NULL DEFAULT '',
    creator             TEXT    NOT NULL DEFAULT '',
    title               TEXT    NOT NULL DEFAULT '',
    media_id            TEXT    NOT NULL DEFAULT '',
    resolved_full_path  TEXT    NOT NULL DEFAULT '',
    resolved_path_key   TEXT    NOT NULL DEFAULT '',
    resolved_folder     TEXT    NOT NULL DEFAULT '',
    resolved_filename   TEXT    NOT NULL DEFAULT '',
    file_size           INTEGER NOT NULL DEFAULT 0,
    scan_mtime_ns       INTEGER NOT NULL DEFAULT 0,
    scan_revision       TEXT    NOT NULL DEFAULT '',
    folder_template     TEXT    NOT NULL DEFAULT '',
    filename_template   TEXT    NOT NULL DEFAULT '',
    created_at          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL,
    encoding            TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_history_order
    ON download_history(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_history_source_order
    ON download_history(source_key, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_history_path
    ON download_history(resolved_path_key);
CREATE INDEX IF NOT EXISTS idx_history_media_id
    ON download_history(media_id);

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

-- Which past downloads have already been folded into learned_formats. Learning is
-- cumulative and already persisted, so re-teaching the same download on every scan
-- is pure duplicated work; this records what is done so a scan only reads what is new.
CREATE TABLE IF NOT EXISTS seeded_downloads (
    task_id TEXT PRIMARY KEY,
    seeded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cookies (
    id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL DEFAULT '',
    filename TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL DEFAULT 0,
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_cookies_source_key ON source_cookies(source_key);
"""

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
            connection.execute(
                """
                DELETE FROM download_tasks
                WHERE status = 'completed'
                  AND id IN (SELECT id FROM download_history)
                """
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        _INITIALIZED = True
