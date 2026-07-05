from __future__ import annotations

import json

import backend.app.db.database as database_module
from backend.app.db import repositories
from backend.app.services.tasks.formats import learn_download


def test_learned_formats_persist_to_formats_table(tmp_path, monkeypatch):
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)

    payload = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
        "7493558766131039489",
    )
    repositories.save_learned_formats_payload(payload)

    with database_module.transaction() as connection:
        rows = connection.execute("SELECT source_key, template, creator_part FROM formats").fetchall()
        legacy = connection.execute("SELECT value FROM settings WHERE key = ?", ("learned_formats",)).fetchone()

    assert legacy is None
    assert len(rows) == 1
    assert rows[0]["source_key"] == "tiktok"
    assert rows[0]["template"] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert rows[0]["creator_part"] == "path:0"

    loaded = repositories.load_learned_formats_payload()
    assert loaded["tiktok"]["template"] == "https://www.tiktok.com/@{creator}/video/{id}"


def test_learned_formats_migrate_legacy_settings_row(tmp_path, monkeypatch):
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)

    legacy = learn_download({}, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ")
    with database_module.transaction() as connection:
        connection.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            ("learned_formats", json.dumps(legacy), "2026-07-05T00:00:00+00:00"),
        )

    loaded = repositories.load_learned_formats_payload()

    with database_module.transaction() as connection:
        format_row = connection.execute("SELECT template FROM formats WHERE source_key = ?", ("youtube",)).fetchone()
        legacy_row = connection.execute("SELECT value FROM settings WHERE key = ?", ("learned_formats",)).fetchone()

    assert loaded["youtube"]["template"] == "https://www.youtube.com/watch?v={id}"
    assert format_row["template"] == "https://www.youtube.com/watch?v={id}"
    assert legacy_row is None
