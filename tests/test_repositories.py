from __future__ import annotations

import json

import backend.app.db.database as database_module
from backend.app.db import repositories
from backend.app.domains.downloads import serializers
from backend.app.domains.downloads.formats import learn_download


def use_temp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)


def test_fresh_schema_uses_current_table_names(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()

    with database_module.transaction() as connection:
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }

    assert {"app_settings", "download_tasks", "download_history", "learned_formats", "cookie_blobs"} <= tables
    assert {"settings", "queue", "history", "formats", "cookies"}.isdisjoint(tables)


def test_settings_payload_roundtrips_through_app_settings(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_settings_payload({"template_settings": {"filename_template": "{{title}}"}})

    assert repositories.load_settings_payload() == {
        "template_settings": {"filename_template": "{{title}}"}
    }


def test_learned_formats_persist_to_learned_formats_table(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    payload = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
        "7493558766131039489",
        {"uploader": "fzyahoo.com"},
    )
    repositories.save_learned_formats_payload(payload)

    with database_module.transaction() as connection:
        rows = connection.execute(
            "SELECT source_key, templates, url_field_roles FROM learned_formats"
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_key"] == "tiktok"
    assert json.loads(rows[0]["templates"]) == ["https://www.tiktok.com/@{creator}/video/{id}"]
    assert json.loads(rows[0]["url_field_roles"]) == {}
    assert repositories.load_learned_formats_payload()["tiktok"]["templates"] == [
        "https://www.tiktok.com/@{creator}/video/{id}"
    ]


def test_learned_formats_persist_multiple_templates(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    payload = learn_download({}, "https://www.tiktok.com/@a/video/7493558766131039489", "7493558766131039489")
    payload = learn_download(payload, "https://www.tiktok.com/@a/photo/7420705673542978833", "7420705673542978833")
    repositories.save_learned_formats_payload(payload)

    loaded = repositories.load_learned_formats_payload()
    assert set(loaded["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@a/video/{id}",
        "https://www.tiktok.com/@a/photo/{id}",
    }


def _seed_history(monkeypatch, tmp_path):
    use_temp_db(tmp_path, monkeypatch)
    rows = [
        (
            "t1",
            {
                "source_url": "https://youtube.com/watch?v=1",
                "source_key": "youtube",
                "creator": "Hoshimachi Suisei",
                "resolved_filename": "Comet [1].mp4",
                "resolved_folder": "Hoshimachi",
                "media_id": "1",
                "completed_at": "2026-07-10T00:00:00+00:00",
            },
        ),
        (
            "t2",
            {
                "source_url": "https://tiktok.com/@a/video/2",
                "source_key": "tiktok",
                "creator": "Gawr Gura",
                "resolved_filename": "Shark Dance [2].mp4",
                "resolved_folder": "Gura",
                "media_id": "2",
                "completed_at": "2026-07-09T00:00:00+00:00",
            },
        ),
    ]
    for task_id, payload in rows:
        repositories.save_history_row(task_id, payload)


def test_load_history_page_search_matches_creator(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)
    rows = repositories.load_history_page(30, search="hoshi")
    assert len(rows) == 1
    assert [task_id for task_id, *_ in rows] == ["t1"]


def test_load_history_page_search_matches_filename_and_url(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)
    assert len(repositories.load_history_page(30, search="shark")) == 1
    assert len(repositories.load_history_page(30, search="tiktok.com")) == 1


def test_load_history_page_search_combines_with_source_key(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)
    assert len(repositories.load_history_page(30, source_key="youtube", search="gura")) == 0
    assert len(repositories.load_history_page(30, source_key="youtube", search="comet")) == 1


def test_load_history_page_empty_search_returns_all(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)
    assert len(repositories.load_history_page(30, search="")) == 2


def test_load_history_page_cursor_moves_after_last_row(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)

    first_page = repositories.load_history_page(1)
    cursor = (first_page[0][2], first_page[0][3], first_page[0][0])
    next_page = repositories.load_history_page(1, cursor)

    assert [task_id for task_id, *_ in first_page] == ["t1"]
    assert [task_id for task_id, *_ in next_page] == ["t2"]


def test_fetch_history_page_returns_opaque_next_cursor(tmp_path, monkeypatch):
    _seed_history(monkeypatch, tmp_path)
    monkeypatch.setattr(serializers.swaratelle, "is_configured", lambda: False)

    first_page = serializers.fetch_history_page("", 1, "", "")
    next_page = serializers.fetch_history_page(first_page["next_cursor"], 1, "", "")

    assert "total" not in first_page
    assert [task["vid"] for task in first_page["entries"]] == ["t1"]
    assert [task["vid"] for task in next_page["entries"]] == ["t2"]

