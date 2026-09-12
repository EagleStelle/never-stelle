from __future__ import annotations

import importlib
import logging
import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from backend.app.db.volume import (
    dot_lock,
    free_bytes,
    megabytes,
    network_mount,
    open_connection,
    release_stale_lock,
)

logger = logging.getLogger(__name__)

_MIGRATION_RE = re.compile(r"^m(\d{4})_[a-z0-9_]+$")
_MIGRATIONS_DIR = Path(__file__).resolve().parent


def _discover() -> list[tuple[int, str]]:
    """Every migration as ``(version, module name)``, lowest first.

    The version is read off the filename, so a startup with nothing pending compares
    integers and imports no migration module at all.
    """
    found: dict[int, str] = {}
    for path in sorted(_MIGRATIONS_DIR.glob("m[0-9][0-9][0-9][0-9]_*.py")):
        match = _MIGRATION_RE.match(path.stem)
        if not match:
            raise RuntimeError(f"malformed migration filename: {path.name}")
        version = int(match.group(1))
        if version < 1:
            raise RuntimeError(f"migration versions start at 1: {path.name}")
        if version in found:
            raise RuntimeError(f"duplicate migration version {version}: {found[version]}, {path.stem}")
        found[version] = path.stem
    ordered = sorted(found.items())
    versions = [version for version, _ in ordered]
    if versions != list(range(1, len(versions) + 1)):
        raise RuntimeError(f"migration versions must run 1..N without gaps, got {versions}")
    return ordered


def latest_version() -> int:
    ordered = _discover()
    return ordered[-1][0] if ordered else 0


def current_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row else 0


def _is_populated(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' LIMIT 1"
    ).fetchone()
    return row is not None


def table_exists(connection: sqlite3.Connection, table: str) -> bool:
    """Only the baseline creates tables, so a migration reaching a database that
    joined the chain later has to check before touching one."""
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _snapshots(database_path: Path) -> list[Path]:
    """The snapshots sitting beside the database, oldest schema version first."""
    pattern = re.compile(rf"{re.escape(database_path.name)}\.v(\d+)\.bak\Z")
    found: list[tuple[int, Path]] = []
    for path in database_path.parent.glob(f"{database_path.name}.v*.bak"):
        match = pattern.match(path.name)
        if match:
            found.append((int(match.group(1)), path))
    return [path for _, path in sorted(found)]


def _delete_snapshots(paths: Iterable[Path]) -> None:
    for path in paths:
        try:
            path.unlink()
        except OSError as exc:
            logger.warning("Could not delete the superseded snapshot %s: %s", path, exc)
            continue
        logger.info("Deleted the superseded snapshot %s", path)
        # The copy was written through the dot-file VFS, which may have left its lock.
        lock = dot_lock(path)
        try:
            if lock.is_dir():
                lock.rmdir()
            else:
                lock.unlink(missing_ok=True)
        except OSError:
            pass


def prune_snapshots(database_path: Path, keep: Path | None = None) -> None:
    """Delete every snapshot but ``keep``, or but the newest when none is named.

    One snapshot is the restore point. The rest only hold space on a volume the
    downloads land in too.
    """
    found = _snapshots(database_path)
    survivor = keep if keep is not None else (found[-1] if found else None)
    _delete_snapshots(path for path in found if path != survivor)


def _require_room_for_backup(database_path: Path) -> None:
    """Refuse the upgrade before it starts rather than fill the volume halfway through.

    The snapshot is a second copy of the database and VACUUM builds a third, so an
    upgrade that runs out of room mid-write leaves a file nothing can open. Earlier
    snapshots go first: the one about to be written replaces them.
    """
    try:
        needed = database_path.stat().st_size * 2
    except OSError:
        return
    free = free_bytes(database_path.parent)
    if free < 0 or free >= needed:
        return
    _delete_snapshots(_snapshots(database_path))
    free = free_bytes(database_path.parent)
    if free < 0 or free >= needed:
        return
    raise RuntimeError(
        f"Not enough room on {database_path.parent} to upgrade the database: "
        f"{megabytes(needed)} needed, {megabytes(free)} free."
    )


def _backup(connection: sqlite3.Connection, target: Path) -> None:
    # Snapshot before an upgrade; there are no downgrades.
    target.parent.mkdir(parents=True, exist_ok=True)
    kind = network_mount(target)
    release_stale_lock(target, kind)
    # A snapshot left at this name by an earlier run may be a partial write that no
    # longer opens as a database, and this copy replaces it either way.
    target.unlink(missing_ok=True)
    destination = open_connection(target, kind)
    try:
        connection.backup(destination)
    finally:
        destination.close()


def apply_pending(connection: sqlite3.Connection, database_path: Path | None = None) -> list[int]:
    """Bring the database to ``latest_version()``; returns the versions applied.

    A database newer than this build is refused rather than opened: an app downgrade
    that kept writing would leave data the older code cannot read back.
    """
    ordered = _discover()
    latest = ordered[-1][0] if ordered else 0
    current = current_version(connection)
    if database_path is not None:
        prune_snapshots(database_path)
    if current > latest:
        raise RuntimeError(
            f"database schema version {current} is newer than this build's {latest}; "
            "run the newer app version or restore a backup"
        )
    pending = [entry for entry in ordered if entry[0] > current]
    if not pending:
        return []
    # A fresh file holds nothing worth copying. Anything else does: even at version 0
    # the baseline rebuilds tables an older install left on a different shape.
    populated = database_path is not None and _is_populated(connection)
    if populated:
        _require_room_for_backup(database_path)
        snapshot = database_path.with_name(f"{database_path.name}.v{current}.bak")
        _backup(connection, snapshot)
        prune_snapshots(database_path, keep=snapshot)

    applied: list[int] = []
    for version, module_name in pending:
        module = importlib.import_module(f"{__name__}.{module_name}")
        try:
            module.upgrade(connection)
            connection.execute(f"PRAGMA user_version = {int(version)}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        applied.append(version)
    if populated:
        # An upgrade that rebuilt or dropped tables leaves the file holding pages
        # nothing references; the copy above is what those pages are still needed for.
        # Reclaiming them is housekeeping, and the schema is already committed, so a
        # volume with no room for the rewrite must not take the app down with it.
        try:
            connection.execute("VACUUM")
        except sqlite3.Error as exc:
            logger.warning("Skipped reclaiming free pages in %s: %s", database_path, exc)
    return applied
