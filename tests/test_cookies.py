from __future__ import annotations

from pathlib import Path

import backend.app.db.database as database_module
import backend.app.runtime.scratch as scratch_module
from backend.app.db import repositories
from backend.app.domains.settings import cookies as cookies_module


def test_materialized_cookie_is_one_use_temp_file(tmp_path, monkeypatch):
    database_module.close_database()
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch)

    try:
        repositories.add_source_cookie("jar1", "instagram", "jar.txt", b"cookies")

        path = Path(cookies_module.materialize_cookie("jar1"))

        assert path.parent == scratch
        assert path.name.startswith("nvs-cookie-")
        assert path.name.endswith(".txt")
        assert path.read_bytes() == b"cookies"

        cookies_module.drop_materialized_cookie(str(path))

        assert not path.exists()
    finally:
        database_module.close_database()
