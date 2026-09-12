from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.db.database as database_module
from backend.app.domains.downloads.engine import Engine, all_engines


def engine_by_name(name: str) -> Engine:
    """Pick one backend to exercise. Production runs the default or the whole run order."""
    return next(engine for engine in all_engines() if engine.name == name)


def use_temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Closed first: the shared connection would otherwise keep serving the old file.
    database_module.close_database()
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
