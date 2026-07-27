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

    assert {"app_settings", "download_tasks", "download_history", "learned_formats", "source_cookies"} <= tables
    assert {"settings", "queue", "history", "formats", "cookies", "cookie_blobs"}.isdisjoint(tables)


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



def test_source_activity_revision_ignores_progress_writes(tmp_path, monkeypatch):
    # Anything cached against this revision must survive a running download, which
    # rewrites its own row several times a minute for the whole transfer.
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload(
        "t1", {"source_url": "https://example.test/a/1", "source_key": "example", "status": "running"}
    )
    before = repositories.source_activity_revision()

    repositories.merge_task_payload("t1", {"progress_pct": 41, "last_log_lines": ["a", "b"]})
    repositories.merge_task_payload("t1", {"progress_pct": 87})

    assert repositories.source_activity_revision() == before


def test_source_activity_revision_moves_when_a_new_source_appears(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("t1", {"source_url": "https://example.test/a/1", "source_key": "example"})
    before = repositories.source_activity_revision()

    repositories.merge_task_payload("t2", {"source_url": "https://other.test/b/2", "source_key": "other"})

    assert repositories.source_activity_revision() != before


def test_load_task_payload_returns_one_row(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("t1", {"source_url": "https://example.test/a/1", "status": "running"})
    repositories.merge_task_payload("t2", {"source_url": "https://example.test/a/2", "status": "pending"})

    assert repositories.load_task_payload("t1")["source_url"] == "https://example.test/a/1"
    assert repositories.load_task_payload("missing") == {}


def test_next_pending_task_payload_picks_the_oldest_queued_row(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("running", {"status": "running", "created_at": "2026-01-01T00:00:00"})
    repositories.merge_task_payload("first", {"status": "pending", "created_at": "2026-01-02T00:00:00"})
    repositories.merge_task_payload("second", {"status": "pending", "created_at": "2026-01-03T00:00:00"})

    task_id, payload = repositories.next_pending_task_payload()

    assert task_id == "first"
    assert payload["status"] == "pending"
    assert repositories.count_pending_tasks() == 2


def test_next_pending_task_payload_is_none_when_queue_is_empty(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("running", {"status": "running"})

    assert repositories.next_pending_task_payload() is None
    assert repositories.count_pending_tasks() == 0


def test_fail_running_tasks_marks_every_running_row(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("a", {"status": "running", "source_url": "https://example.test/a"})
    repositories.merge_task_payload("b", {"status": "running", "source_url": "https://example.test/b"})
    repositories.merge_task_payload("c", {"status": "pending"})

    assert repositories.fail_running_tasks("interrupted") == 2

    assert repositories.load_task_payload("a")["status"] == "failed"
    assert repositories.load_task_payload("a")["error"] == "interrupted"
    assert repositories.load_task_payload("b")["status"] == "failed"
    assert repositories.load_task_payload("c")["status"] == "pending"


def test_save_history_rows_writes_the_whole_batch(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_history_rows(
        [
            (f"disk:{index}", {"source_url": f"https://example.test/p/{index}", "source_key": "example"})
            for index in range(5)
        ]
    )

    entries = repositories.load_history_payload()["entries"]
    assert len(entries) == 5
    assert entries["disk:3"]["source_url"] == "https://example.test/p/3"
    assert repositories.count_history_by_source() == {"example": 5}


def test_save_history_rows_accepts_an_empty_batch(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_history_rows([])

    assert repositories.load_history_payload()["entries"] == {}
