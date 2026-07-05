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
    source_key TEXT NOT NULL DEFAULT 'others',
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
    source_key TEXT NOT NULL DEFAULT 'others',
    payload TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_source_url ON history(source_url);

CREATE TABLE IF NOT EXISTS formats (
    source_key TEXT PRIMARY KEY,
    host TEXT NOT NULL DEFAULT '',
    template TEXT NOT NULL DEFAULT '',
    id_min INTEGER NOT NULL DEFAULT 0,
    id_max INTEGER NOT NULL DEFAULT 0,
    id_classes TEXT NOT NULL DEFAULT '',
    id_part TEXT NOT NULL DEFAULT '',
    creator_part TEXT NOT NULL DEFAULT '',
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
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    initialize_database()
    with _DB_LOCK:
        connection = _connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def initialize_database() -> None:
    global _INITIALIZED
    with _DB_LOCK:
        if _INITIALIZED:
            return
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = _connect()
        try:
            connection.executescript(SCHEMA)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        _INITIALIZED = True
