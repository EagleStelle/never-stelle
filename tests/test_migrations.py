from __future__ import annotations

import json
import sqlite3

import pytest

import backend.app.db.database as database_module
from backend.app.db import migrations
from backend.app.db.migrations.m0001_baseline import SCHEMA
from tests.support import use_temp_db

_STATUS_FORMAT = "https://twitter.com/{creator}/status/{id}"
_PHOTO_FORMAT = "https://twitter.com/{creator}/status/{id}/photo/{var}"


def _legacy_payload() -> dict:
    return {
        "site_locations": {
            "Twitter": {
                _STATUS_FORMAT: "/srv/library/twitter/clips",
                _PHOTO_FORMAT: "D:\\library\\instagram\\saved",
            }
        },
        "source_profiles": [
            {"key": "twitter", "label": "Twitter", "default_download_location": "/srv/library/twitter"}
        ],
        "template_settings": {"folder_template": "{{creator}}"},
    }


def _seed_pre_migration_db(path, payload: dict | None, version: int = 0) -> None:
    """A database as it looked before the migration chain existed."""
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(SCHEMA)
        if payload is not None:
            connection.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ('app', ?, '')",
                (json.dumps(payload),),
            )
        connection.execute(f"PRAGMA user_version = {int(version)}")
        connection.commit()
    finally:
        connection.close()


def _stored_payload() -> dict:
    with database_module.transaction() as connection:
        row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    return json.loads(row["value"])


def test_migration_versions_run_from_one_without_gaps():
    versions = [version for version, _ in migrations._discover()]

    assert versions == list(range(1, len(versions) + 1))
    assert migrations.latest_version() == versions[-1]


def test_fresh_database_lands_on_the_latest_version(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()

    with database_module.transaction() as connection:
        assert migrations.current_version(connection) == migrations.latest_version()
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }

    assert {"app_settings", "download_tasks", "download_history", "learned_formats"} <= tables


def test_applying_again_is_a_no_op(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()

    with database_module.transaction() as connection:
        assert migrations.apply_pending(connection) == []


def test_pre_migration_database_upgrades_to_the_latest_version(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, _legacy_payload())
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    payload = _stored_payload()
    assert "site_locations" not in payload
    assert payload["source_locations"] == {
        "twitter": {_STATUS_FORMAT: "clips", _PHOTO_FORMAT: ""},
    }
    assert payload["source_profiles"] == [{"key": "twitter", "label": "Twitter"}]
    # Untouched sections survive the rewrite.
    assert payload["template_settings"] == {"folder_template": "{{creator}}"}
    with database_module.transaction() as connection:
        assert migrations.current_version(connection) == migrations.latest_version()


def test_upgrade_from_a_stamped_version_backs_the_database_up_first(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, _legacy_payload(), version=1)
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    backup = tmp_path / "never-stelle.sqlite3.v1.bak"
    assert backup.exists()
    backup_connection = sqlite3.connect(str(backup))
    try:
        row = backup_connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    finally:
        backup_connection.close()
    # The copy predates the upgrade, so it still holds the old shape.
    assert "site_locations" in json.loads(row[0])


def test_fresh_database_is_not_backed_up(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()

    assert list(tmp_path.glob("*.bak")) == []


def test_a_newer_database_is_refused(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, None, version=migrations.latest_version() + 1)
    use_temp_db(tmp_path, monkeypatch)

    with pytest.raises(RuntimeError, match="newer than this build"):
        database_module.initialize_database()


def test_source_location_migration_is_idempotent(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, _legacy_payload())
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()
    first = _stored_payload()

    from backend.app.db.migrations import m0002_source_locations_relative as migration

    with database_module.transaction() as connection:
        migration.upgrade(connection)

    assert _stored_payload() == first


def _seed_learning(path, rows: list[tuple[str, str, str]], seeded: list[str], history: list[str]) -> None:
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(SCHEMA)
        connection.executemany(
            "INSERT INTO learned_formats (source_key, host, templates, created_at, updated_at)"
            " VALUES (?, ?, ?, '', '')",
            rows,
        )
        connection.executemany(
            "INSERT INTO seeded_downloads (task_id, seeded_at) VALUES (?, '')",
            [(task_id,) for task_id in seeded],
        )
        connection.executemany(
            "INSERT INTO download_history (id, created_at, updated_at) VALUES (?, '', '')",
            [(task_id,) for task_id in history],
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    finally:
        connection.close()


def test_test_fixture_learning_is_purged(tmp_path, monkeypatch):
    _seed_learning(
        tmp_path / "never-stelle.sqlite3",
        [
            ("example", "example.test", '["https://example.test/@other/video/{id}"]'),
            ("tiktok", "tiktok.com", '["https://www.tiktok.com/@{creator}/video/{id}"]'),
        ],
        seeded=["gallerydl:1", "task-1"],
        history=["gallerydl:1"],
    )
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        sources = [row["source_key"] for row in connection.execute("SELECT source_key FROM learned_formats")]
        seeded = [row["task_id"] for row in connection.execute("SELECT task_id FROM seeded_downloads")]

    # The real source and the seeded id that still has its download both survive.
    assert sources == ["tiktok"]
    assert seeded == ["gallerydl:1"]


_OLD_HISTORY = """
CREATE TABLE download_history (
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
    completed_at        TEXT    NOT NULL,
    encoding            TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_history_order ON download_history(completed_at DESC, id DESC);
CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);

CREATE TABLE learned_formats (
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
"""


def _seed_old_shape_db(path) -> None:
    """A pre-chain install whose tables never gained the columns added after them."""
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(_OLD_HISTORY)
        connection.execute(
            "INSERT INTO download_history (id, title, media_id, file_size, completed_at)"
            " VALUES ('gallerydl:1', 'clip', 'm-1', 42, '2026-01-01T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO learned_formats (source_key, host, templates, url_field_roles,"
            " samples, created_at, updated_at)"
            " VALUES ('tiktok', 'tiktok.com', '[]', '{}', 3, '', '')"
        )
        connection.commit()
    finally:
        connection.close()


def test_old_shape_tables_are_rebuilt_onto_the_declared_schema(tmp_path, monkeypatch):
    _seed_old_shape_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info('download_history')")}
        row = connection.execute("SELECT * FROM download_history").fetchone()
        formats = {row["name"] for row in connection.execute("PRAGMA table_info('learned_formats')")}
        indexes = {
            index["name"]
            for index in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL"
            )
        }
        assert migrations.current_version(connection) == migrations.latest_version()

    # The added column arrives with its default, the dropped one is gone.
    assert "needs_resolve" in columns
    assert "completed_at" not in columns
    assert "url_field_roles" not in formats
    assert row["needs_resolve"] == 0
    # Existing data survives, and the renamed timestamp carries into both replacements.
    assert (row["id"], row["title"], row["media_id"], row["file_size"]) == ("gallerydl:1", "clip", "m-1", 42)
    assert row["created_at"] == row["updated_at"] == "2026-01-01T00:00:00+00:00"
    # The partial index that the old table could not support is in place.
    assert "idx_history_needs_resolve" in indexes


def test_old_shape_database_is_backed_up_before_the_rebuild(tmp_path, monkeypatch):
    _seed_old_shape_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    backup = tmp_path / "never-stelle.sqlite3.v0.bak"
    assert backup.exists()
    connection = sqlite3.connect(str(backup))
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info('download_history')")}
    finally:
        connection.close()
    assert "completed_at" in columns


def test_source_location_migration_without_a_settings_row(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, None)
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        assert connection.execute("SELECT COUNT(*) AS n FROM app_settings").fetchone()["n"] == 0
        assert migrations.current_version(connection) == migrations.latest_version()
