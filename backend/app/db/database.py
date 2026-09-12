from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from backend.app.core.config import DATABASE_PATH
from backend.app.db.migrations import apply_pending
from backend.app.db.volume import (
    free_bytes,
    megabytes,
    network_mount,
    open_connection,
    release_stale_lock,
)

logger = logging.getLogger(__name__)

_DB_LOCK = threading.RLock()
_INITIALIZED = False
_CONNECTION: sqlite3.Connection | None = None


def database_path() -> Path:
    return DATABASE_PATH


# Room for a checkpoint and a rollback journal. Below this SQLite starts failing
# writes, and a write it cannot finish is how a database file gets truncated.
_MIN_FREE_BYTES = 32 * 1024 * 1024


class DatabaseVolumeError(RuntimeError):
    """The volume cannot host the database, with the reason already established."""


def verify_database_volume(path: Path = DATABASE_PATH) -> None:
    """Refuse a volume that is missing, read-only or full, each by name.

    Runs before the first connection, so an operator reads which condition failed
    instead of a disk I/O error raised from whichever PRAGMA touched the disk first.
    """
    directory = path.parent
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DatabaseVolumeError(
            f"The database directory {directory} is missing and cannot be created: {exc}"
        ) from exc

    probe = directory / ".never-stelle-write-probe"
    try:
        probe.write_bytes(b"")
        probe.unlink()
    except OSError as exc:
        raise DatabaseVolumeError(
            f"{directory} is not writable: {exc}. Check that the volume is mounted read-write and "
            f"that its owner matches the user the container runs as."
        ) from exc

    free = free_bytes(directory)
    if 0 <= free < _MIN_FREE_BYTES:
        raise DatabaseVolumeError(
            f"{directory} has {megabytes(free)} free, below the {megabytes(_MIN_FREE_BYTES)} SQLite "
            f"needs to journal a write. Free space on this volume before starting."
        )


def _open_failure_hint(exc: sqlite3.Error) -> str:
    """What to do about a failed open, told apart by what SQLite reported.

    A locked file is whole and someone else has it; only the rest is worth sending
    an operator to a snapshot for.
    """
    if "locked" in str(exc) or "busy" in str(exc):
        return (
            "The file is intact and another process still has it open. A second copy of this "
            f"container, or anything else holding {DATABASE_PATH}, has to stop before this one "
            "starts. A lock left by a killed process is cleared at startup on its own."
        )
    return (
        "Its volume passed the permission and free-space checks, so the file itself is likely "
        f"damaged; restore the {DATABASE_PATH.name}.v*.bak snapshot beside it."
    )


def _connect() -> sqlite3.Connection:
    verify_database_volume()
    kind = network_mount(DATABASE_PATH)
    if kind:
        logger.warning(
            "%s is a %s mount: the database is held under an exclusive lock, so only this "
            "container can open it while it runs.",
            DATABASE_PATH.parent,
            kind,
        )
        stale = release_stale_lock(DATABASE_PATH, kind)
        if stale is not None:
            logger.warning(
                "Removed %s, a lock left behind by a process that was killed while holding it.",
                stale,
            )
    try:
        connection = open_connection(DATABASE_PATH, kind)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        if kind:
            # Exclusive locking runs WAL off heap memory instead of the -shm file,
            # which these mounts cannot back.
            connection.execute("PRAGMA locking_mode = EXCLUSIVE")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA wal_autocheckpoint = 1000")
        connection.execute("PRAGMA cache_size = -8000")
    except sqlite3.Error as exc:
        raise DatabaseVolumeError(
            f"Could not open the database at {DATABASE_PATH}: {exc}. {_open_failure_hint(exc)}"
        ) from exc
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
