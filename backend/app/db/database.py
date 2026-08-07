from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from backend.app.core.config import DATABASE_PATH
from backend.app.db.migrations import apply_pending

_DB_LOCK = threading.RLock()
_INITIALIZED = False
_CONNECTION: sqlite3.Connection | None = None


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
        # Schema and stored payload shapes both come from the migration chain, so the
        # first connection of a process is also the upgrade point.
        apply_pending(_shared_connection(), DATABASE_PATH)
        _INITIALIZED = True
