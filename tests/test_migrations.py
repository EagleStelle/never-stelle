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

    assert {
        "app_settings",
        "download_tasks",
        "download_history",
        "learned_formats",
        "learned_redirects",
    } <= tables


def test_pre_migration_database_gains_the_redirect_table(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, _legacy_payload())
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'learned_redirects'"
        ).fetchone()
    assert row is not None


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


_V1_SCHEMA = """
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);

CREATE TABLE queue (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    source_key TEXT NOT NULL DEFAULT 'others',
    progress_pct REAL NOT NULL DEFAULT 0,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX idx_queue_status ON queue(status);

CREATE TABLE history (
    task_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    source_key TEXT NOT NULL DEFAULT 'others',
    payload TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX idx_history_order ON history(completed_at DESC, updated_at DESC, task_id DESC);

CREATE TABLE cookies (
    key TEXT PRIMARY KEY,
    filename TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE formats (
    source_key TEXT PRIMARY KEY,
    host TEXT NOT NULL DEFAULT '',
    templates TEXT NOT NULL DEFAULT '',
    url_creator_fields TEXT NOT NULL DEFAULT '',
    id_min INTEGER NOT NULL DEFAULT 0,
    id_max INTEGER NOT NULL DEFAULT 0,
    id_classes TEXT NOT NULL DEFAULT '',
    samples INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX idx_formats_host ON formats(host);
"""

_V1_TEMPLATE = "https://www.youtube.com/watch?v={id}"
_V1_TABLES = ("settings", "queue", "history", "cookies", "formats")


def _v1_settings() -> dict:
    return {
        "auth": {"username": "root", "password_hash": "pbkdf2$1", "session_version": 1},
        "source_profiles": [{"key": "youtube", "label": "Youtube", "hosts": ["www.youtube.com"]}],
        # One folder per source, and an absolute one: both are shapes 1.1 no longer stores.
        "site_locations": {"youtube": "/media/youtube/music"},
        # One template pair per source, likewise keyed per learned format in 1.1.
        "source_templates": {
            "youtube": {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}
        },
        "source_creator_fields": {"youtube": {"username": ["uploader_id"], "nickname": ["uploader"]}},
        "template_settings": {"folder_template": "{{username}}"},
    }


def _seed_v1_db(path, settings: dict | None = None) -> None:
    """A 1.0 database: different table names, most fields inside a JSON payload."""
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(_V1_SCHEMA)
        connection.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES ('app', ?, '2026-07-21T22:56:46+00:00')",
            (json.dumps(_v1_settings() if settings is None else settings),),
        )
        connection.execute(
            "INSERT INTO formats (source_key, host, templates, url_creator_fields, id_min, id_max,"
            " id_classes, samples, created_at, updated_at)"
            " VALUES ('youtube', 'www.youtube.com', ?, '{}', 11, 11, 'd,l,u', 4, '', '')",
            (json.dumps([_V1_TEMPLATE]),),
        )
        connection.execute(
            "INSERT INTO history (task_id, source_url, source_key, payload, completed_at, updated_at)"
            " VALUES ('ytdlp:abc', 'https://www.youtube.com/watch?v=abc', 'youtube', ?,"
            " '2026-07-12T20:33:07+00:00', '2026-07-13T01:00:00+00:00')",
            (
                json.dumps(
                    {
                        "task_id": "ytdlp:abc",
                        # Defaulted by 1.0 for every row older than the field, so the id decides.
                        "task_type": "gallerydl",
                        "creator": "richamu",
                        "media_id": "abc",
                        "resolved_folder": "/media/youtube/richamu",
                        "resolved_filename": "richamu - clip [abc].mp4",
                        "resolved_full_path": "/media/youtube/richamu/richamu - clip [abc].mp4",
                        "file_size": 42,
                        "quality": {"mode": "audio"},
                        "completed_at": "2026-07-12T20:33:07+00:00",
                    }
                ),
            ),
        )
        connection.execute(
            "INSERT INTO history (task_id, source_url, source_key, payload, completed_at, updated_at)"
            " VALUES ('disk:xyz', '', 'others', ?, '2026-07-12T20:02:56+00:00', '2026-07-12T20:02:56+00:00')",
            (
                json.dumps(
                    {
                        "task_type": "disk",
                        "media_id": "xyz",
                        "title": "walk",
                        "artist": "amuchan",
                        "source_pending": True,
                        "source_candidates": ["youtube"],
                        "resolved_full_path": "/media/youtube/amuchan/walk [xyz].mp3",
                    }
                ),
            ),
        )
        connection.execute(
            "INSERT INTO queue (id, source_url, status, source_key, progress_pct, payload, created_at, updated_at)"
            " VALUES ('gallerydl:q1', 'https://www.youtube.com/watch?v=q1', 'failed', 'youtube', 0, ?,"
            " '2026-07-21T22:59:37+00:00', '2026-07-21T22:59:46+00:00')",
            (
                json.dumps(
                    {
                        "engine": "gallerydl",
                        "status": "failed",
                        "error": "boom",
                        "output_dir": "/media/youtube",
                        "output_template": "{title}.{extension}",
                        "template_settings": {
                            "folder_template": "{{username}}",
                            "filename_template": "{{title}} [{{id}}]",
                        },
                        "preview_warning": "",
                        "last_log_lines": ["one", "two"],
                    }
                ),
            ),
        )
        connection.execute(
            "INSERT INTO cookies (key, filename, content_type, content, created_at, updated_at)"
            " VALUES ('ytdlp_cookies::youtube', 'cookies.txt', 'text/plain', ?, '2026-07-13T09:38:21+00:00',"
            " '2026-07-13T09:38:21+00:00')",
            (b"# Netscape HTTP Cookie File",),
        )
        connection.commit()
    finally:
        connection.close()


def test_v1_tables_are_imported_and_dropped(tmp_path, monkeypatch):
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        indexes = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
        }
        assert migrations.current_version(connection) == migrations.latest_version()

    # Nothing of the 1.0 layout is left, not the tables and not the indexes over them.
    assert tables.isdisjoint(_V1_TABLES)
    assert {"app_settings", "download_tasks", "download_history", "learned_formats", "source_cookies"} <= tables
    assert {"idx_queue_status", "idx_formats_host"}.isdisjoint(indexes)
    assert "idx_history_order" in indexes


def test_v1_history_payloads_become_columns(tmp_path, monkeypatch):
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        downloaded = connection.execute("SELECT * FROM download_history WHERE id = 'ytdlp:abc'").fetchone()
        scanned = connection.execute("SELECT * FROM download_history WHERE id = 'disk:xyz'").fetchone()

    # The id names the engine that produced the row; the payload's task_type does not.
    assert downloaded["engine"] == "ytdlp"
    assert (downloaded["creator"], downloaded["media_id"], downloaded["file_size"]) == ("richamu", "abc", 42)
    assert downloaded["resolved_path_key"]
    # One 1.0 timestamp feeds both, and the row's own updated_at wins where it has one.
    assert downloaded["created_at"] == "2026-07-12T20:33:07+00:00"
    assert downloaded["updated_at"] == "2026-07-13T01:00:00+00:00"
    # Only what 1.1 has no column for rides along; the dead keys are dropped.
    assert json.loads(downloaded["encoding"]) == {"quality": {"mode": "audio"}}

    assert scanned["engine"] == "disk"
    assert (scanned["creator"], scanned["title"]) == ("amuchan", "walk")
    assert scanned["source_key"] == ""
    assert json.loads(scanned["encoding"]) == {"source_pending": True, "source_candidates": ["youtube"]}


def test_v1_queue_cookies_and_formats_are_imported(tmp_path, monkeypatch):
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        task = connection.execute("SELECT * FROM download_tasks").fetchone()
        cookie = connection.execute("SELECT * FROM source_cookies").fetchone()
        learned = connection.execute("SELECT * FROM learned_formats").fetchone()
        format_columns = {row["name"] for row in connection.execute("PRAGMA table_info('learned_formats')")}

    assert (task["id"], task["status"], task["engine"]) == ("gallerydl:q1", "failed", "gallerydl")
    assert task["error"] == "boom"
    assert (task["folder_template"], task["filename_template"]) == ("{{username}}", "{{title}} [{{id}}]")
    assert json.loads(task["last_log_lines"]) == ["one", "two"]
    assert json.loads(task["encoding"]) == {"preview_warning": ""}

    # One jar per source becomes the first entry of that source's rotation.
    assert (cookie["source_key"], cookie["filename"], cookie["position"]) == ("youtube", "cookies.txt", 0)
    assert bytes(cookie["content"]) == b"# Netscape HTTP Cookie File"

    assert (learned["source_key"], learned["samples"], learned["id_classes"]) == ("youtube", 4, "d,l,u")
    assert json.loads(learned["templates"]) == [_V1_TEMPLATE]
    assert "url_creator_fields" not in format_columns


def test_v1_settings_keys_are_carried_onto_their_new_names(tmp_path, monkeypatch):
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()
    payload = _stored_payload()

    assert "source_creator_fields" not in payload
    assert payload["source_fields"] == {"youtube": {"username": ["uploader_id"], "nickname": ["uploader"]}}
    # The single 1.0 folder is keyed per learned format and rewritten to a subpath.
    assert "site_locations" not in payload
    assert payload["source_locations"] == {"youtube": {_V1_TEMPLATE: "music"}}
    # The single 1.0 template pair is keyed the same way, so the rename pass sees the
    # names the library already carries instead of walking it onto the base template.
    assert payload["source_templates"] == {
        "youtube": {_V1_TEMPLATE: {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}}
    }
    # Everything the rename did not touch is left exactly as it was.
    assert payload["auth"] == _v1_settings()["auth"]
    assert payload["template_settings"] == {"folder_template": "{{username}}"}


def test_v1_templates_survive_onto_the_effective_settings(tmp_path, monkeypatch):
    """What the importer writes is what the app reads back, not a shape read drops."""
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    from backend.app.domains.settings import get_effective_saved_settings

    assert get_effective_saved_settings()["source_templates"]["youtube"][_V1_TEMPLATE] == {
        "folder_template": "{{username}}",
        "filename_template": "{{title}} [{{id}}]",
    }


def test_v1_templates_already_keyed_by_format_are_left_alone(tmp_path, monkeypatch):
    settings = _v1_settings()
    nested = {"folder_template": "{{nickname}}", "filename_template": "{{title}}"}
    settings["source_templates"] = {"youtube": {_V1_TEMPLATE: nested}}
    _seed_v1_db(tmp_path / "never-stelle.sqlite3", settings)
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    assert _stored_payload()["source_templates"] == {"youtube": {_V1_TEMPLATE: nested}}


def test_v1_templates_without_a_learned_format_are_dropped(tmp_path, monkeypatch):
    settings = _v1_settings()
    # Nothing learned for the source, so there is no format key to hang the pair on.
    settings["source_templates"]["twitter"] = {"folder_template": "{{username}}", "filename_template": "{{id}}"}
    _seed_v1_db(tmp_path / "never-stelle.sqlite3", settings)
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    assert _stored_payload()["source_templates"]["twitter"] == {}


def test_v1_database_is_backed_up_before_the_import(tmp_path, monkeypatch):
    _seed_v1_db(tmp_path / "never-stelle.sqlite3")
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    connection = sqlite3.connect(str(tmp_path / "never-stelle.sqlite3.v0.bak"))
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        rows = connection.execute("SELECT COUNT(*) FROM history").fetchone()[0]
    finally:
        connection.close()
    assert set(_V1_TABLES) <= tables
    assert rows == 2


def test_source_location_migration_without_a_settings_row(tmp_path, monkeypatch):
    database_path = tmp_path / "never-stelle.sqlite3"
    _seed_pre_migration_db(database_path, None)
    use_temp_db(tmp_path, monkeypatch)

    database_module.initialize_database()

    with database_module.transaction() as connection:
        assert connection.execute("SELECT COUNT(*) AS n FROM app_settings").fetchone()["n"] == 0
        assert migrations.current_version(connection) == migrations.latest_version()
