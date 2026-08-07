from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.db.database as database_module


def use_temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Closed first: the shared connection would otherwise keep serving the old file.
    database_module.close_database()
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
