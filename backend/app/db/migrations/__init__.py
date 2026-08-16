from __future__ import annotations

import importlib
import re
import sqlite3
from pathlib import Path

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


def _backup(connection: sqlite3.Connection, target: Path) -> None:
    # Snapshot before an upgrade; there are no downgrades.
    target.parent.mkdir(parents=True, exist_ok=True)
    destination = sqlite3.connect(str(target))
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
    if database_path is not None and _is_populated(connection):
        _backup(connection, database_path.with_name(f"{database_path.name}.v{current}.bak"))

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
    return applied
