from __future__ import annotations

import json

import pytest

import backend.app.db.database as database_module
import backend.app.db.repositories.downloads as downloads_repository_module
from backend.app.core.paths import path_key
from backend.app.db import repositories
from backend.app.domains.downloads import history as history_module
from backend.app.domains.downloads import serializers
from backend.app.domains.downloads.formats import learn_download
from backend.app.domains.downloads.templates import template_columns, template_settings_from_columns


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


def test_download_tables_use_real_task_and_history_columns(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    database_module.initialize_database()

    with database_module.transaction() as connection:
        task_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(download_tasks)").fetchall()
        }
        history_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(download_history)").fetchall()
        }

    assert {
        "creator",
        "title",
        "media_id",
        "resolved_full_path",
        "folder_template",
        "filename_template",
        "encoding",
        "last_log_lines",
    } <= task_columns
    assert {"payload", "updated_at"}.isdisjoint(task_columns)
    assert {
        "id",
        "creator",
        "title",
        "media_id",
        "resolved_full_path",
        "resolved_path_key",
        "file_size",
        "scan_mtime_ns",
        "scan_revision",
        "encoding",
    } <= history_columns
    assert {"task_id", "payload", "updated_at"}.isdisjoint(history_columns)


def test_template_column_helpers_trim_symmetrically():
    columns = template_columns(
        {
            "folder_template": "  {{username}}  ",
            "filename_template": "  {{title}} [{{id}}]  ",
        }
    )

    assert columns == {
        "folder_template": "{{username}}",
        "filename_template": "{{title}} [{{id}}]",
    }
    assert template_settings_from_columns(columns) == columns


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


def test_load_history_page_search_treats_like_wildcards_literally(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.save_history_row(
        "literal",
        {
            "source_key": "example",
            "title": "Progress 50% done",
            "resolved_filename": "clip_a [1].mp4",
            "completed_at": "2026-07-10T00:00:00+00:00",
        },
    )
    repositories.save_history_row(
        "wild",
        {
            "source_key": "example",
            "title": "Progress 500 done",
            "resolved_filename": "clipxa [2].mp4",
            "completed_at": "2026-07-09T00:00:00+00:00",
        },
    )

    assert [task_id for task_id, *_ in repositories.load_history_page(30, search="50%")] == ["literal"]
    assert [task_id for task_id, *_ in repositories.load_history_page(30, search="clip_a")] == ["literal"]


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
    cursor = (first_page[0][2], first_page[0][0])
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


def test_task_payload_roundtrips_through_real_columns(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.merge_task_payload(
        "t1",
        {
            "source_url": "https://example.test/a/1",
            "source_key": "example",
            "status": "running",
            "progress_pct": 42.5,
            "engine": "gallerydl",
            "creator": "Creator",
            "title": "Clip",
            "media_id": "abc123",
            "resolved_full_path": str(tmp_path / "Creator" / "Clip [abc123].mp4"),
            "resolved_folder": str(tmp_path / "Creator"),
            "resolved_filename": "Clip [abc123].mp4",
            "error": "still fine",
            "output_dir": str(tmp_path),
            "output_template": str(tmp_path / "{{title}}.%(ext)s"),
            "folder_template": "{{username}}",
            "filename_template": "{{title}} [{{id}}]",
            "quality": {"mode": "audio", "audio_format": "opus"},
            "last_log_lines": ["line one", "line two"],
            "created_at": "2026-07-10T00:00:00+00:00",
        },
    )

    with database_module.transaction() as connection:
        row = connection.execute(
            """
            SELECT creator, title, media_id, resolved_full_path, folder_template,
                   filename_template, encoding, last_log_lines
            FROM download_tasks WHERE id = ?
            """,
            ("t1",),
        ).fetchone()

    assert row["creator"] == "Creator"
    assert row["title"] == "Clip"
    assert row["media_id"] == "abc123"
    assert row["resolved_full_path"].endswith("Clip [abc123].mp4")
    assert row["folder_template"] == "{{username}}"
    assert row["filename_template"] == "{{title}} [{{id}}]"
    assert json.loads(row["encoding"])["quality"]["audio_format"] == "opus"
    assert json.loads(row["last_log_lines"]) == ["line one", "line two"]
    loaded = repositories.load_task_payload("t1")
    assert loaded["folder_template"] == "{{username}}"
    assert loaded["filename_template"] == "{{title}} [{{id}}]"
    assert loaded["quality"]["audio_format"] == "opus"


def test_task_payload_rejects_invalid_status_before_sqlite_check(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="Invalid download task status"):
        repositories.merge_task_payload("t1", {"status": "typo"})


def test_delete_task_row_if_status_rejects_invalid_status_filter(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("t1", {"status": "pending"})

    with pytest.raises(ValueError, match="Invalid download task status"):
        repositories.delete_task_row_if_status("t1", {"pendnig"})


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


def test_fail_running_tasks_uses_one_sql_update_without_json_decode(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("a", {"status": "running", "source_url": "https://example.test/a"})

    def fail_decode(*args, **kwargs):
        raise AssertionError("fail_running_tasks should not decode task JSON")

    monkeypatch.setattr(downloads_repository_module, "_decode", fail_decode)

    assert repositories.fail_running_tasks("interrupted") == 1
    with database_module.transaction() as connection:
        row = connection.execute("SELECT status, error FROM download_tasks WHERE id = ?", ("a",)).fetchone()
    assert row["status"] == "failed"
    assert row["error"] == "interrupted"


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


def test_history_payload_roundtrips_through_real_columns(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_history_row(
        "disk:abc123",
        {
            "source_url": "https://example.test/p/abc123",
            "source_key": "example",
            "engine": "disk",
            "creator": "Disk Artist",
            "title": "Disk Clip",
            "media_id": "abc123",
            "resolved_full_path": str(tmp_path / "Disk Artist" / "Disk Clip [abc123].mp4"),
            "resolved_folder": str(tmp_path / "Disk Artist"),
            "resolved_filename": "Disk Clip [abc123].mp4",
            "folder_template": "{{username}}",
            "filename_template": "{{title}} [{{id}}]",
            "source_pending": True,
            "source_candidates": ["example"],
            "completed_at": "2026-07-10T00:00:00+00:00",
            "file_size": 4096,
            "scan_mtime_ns": 123456,
            "scan_revision": "rules-rev",
        },
    )

    with database_module.transaction() as connection:
        row = connection.execute(
            """
            SELECT id, engine, creator, title, media_id, resolved_full_path, file_size,
                   scan_mtime_ns, scan_revision, folder_template, filename_template, encoding
            FROM download_history WHERE id = ?
            """,
            ("disk:abc123",),
        ).fetchone()

    assert row["id"] == "disk:abc123"
    assert row["engine"] == "disk"
    assert row["creator"] == "Disk Artist"
    assert row["file_size"] == 4096
    assert row["scan_mtime_ns"] == 123456
    assert row["scan_revision"] == "rules-rev"
    assert row["folder_template"] == "{{username}}"
    assert row["filename_template"] == "{{title}} [{{id}}]"
    assert json.loads(row["encoding"]) == {"source_pending": True, "source_candidates": ["example"]}

    entry = repositories.load_history_payload()["entries"]["disk:abc123"]
    assert entry["creator"] == "Disk Artist"
    assert entry["scan_mtime_ns"] == 123456
    assert entry["scan_revision"] == "rules-rev"
    assert entry["filename_template"] == "{{title}} [{{id}}]"
    assert repositories.load_history_page(30, search="disk artist")[0][0] == "disk:abc123"


def test_history_encoding_preserves_explicit_empty_extra_values(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_history_row(
        "t1",
        {
            "source_url": "https://example.test/p/1",
            "source_key": "example",
            "quality": {},
            "source_candidates": [],
            "preview_warning": "",
            "nullable_extra": None,
        },
    )

    entry = repositories.load_history_payload()["entries"]["t1"]
    assert entry["quality"] == {}
    assert entry["source_candidates"] == []
    assert entry["preview_warning"] == ""
    assert entry["nullable_extra"] is None


def test_load_history_entry_payload_reads_one_primary_key_row(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.save_history_row("t1", {"source_url": "https://example.test/p/1", "source_key": "example"})
    repositories.save_history_row("t2", {"source_url": "https://example.test/p/2", "source_key": "example"})

    entry = repositories.load_history_entry_payload("t2")

    assert entry["source_url"] == "https://example.test/p/2"
    assert repositories.load_history_entry_payload("missing") == {}


def test_history_source_page_uses_source_order_index(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.save_history_rows(
        [
            (
                f"task:{index}",
                {
                    "source_url": f"https://example.test/p/{index}",
                    "source_key": "example" if index % 2 else "other",
                    "completed_at": f"2026-07-{(index % 28) + 1:02d}T00:00:00+00:00",
                },
            )
            for index in range(40)
        ]
    )

    with database_module.transaction() as connection:
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT id FROM download_history
            WHERE source_key = ?
            ORDER BY completed_at DESC, id DESC
            LIMIT 10
            """,
            ("example",),
        ).fetchall()

    details = " ".join(str(row["detail"]) for row in plan)
    assert "idx_history_source_order" in details


def test_history_media_and_path_lookups_use_real_column_indexes(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    media_path = str(tmp_path / "Clip [abc123].mp4")
    repositories.save_history_row(
        "t1",
        {
            "source_url": "https://example.test/p/abc123",
            "source_key": "example",
            "media_id": "abc123",
            "resolved_full_path": media_path,
        },
    )

    media_rows = repositories.load_history_entries_by_media_id("abc123")
    path_task_id, path_entry = repositories.load_history_entry_by_path(media_path)

    assert media_rows[0][0] == "t1"
    assert path_task_id == "t1"
    assert path_entry["media_id"] == "abc123"
    with database_module.transaction() as connection:
        media_plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM download_history WHERE media_id = ?",
            ("abc123",),
        ).fetchall()
        path_plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM download_history WHERE resolved_path_key = ?",
            (path_key(media_path),),
        ).fetchall()

    assert "idx_history_media_id" in " ".join(str(row["detail"]) for row in media_plan)
    assert "idx_history_path" in " ".join(str(row["detail"]) for row in path_plan)


def test_history_path_lookup_uses_normalized_path_key(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    stored_path = str(tmp_path / "nested" / ".." / "Clip [abc123].mp4")
    lookup_path = str(tmp_path / "Clip [abc123].mp4")
    repositories.save_history_row(
        "t1",
        {
            "source_key": "example",
            "media_id": "abc123",
            "resolved_full_path": stored_path,
        },
    )

    task_id, entry = repositories.load_history_entry_by_path(lookup_path)

    assert task_id == "t1"
    assert entry["resolved_full_path"] == stored_path


def test_history_media_lookup_falls_back_to_filename_id_column(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.save_history_row(
        "disk:K1-BVtsHrOY",
        {
            "source_url": "",
            "source_key": "example",
            "engine": "disk",
            "media_id": "",
            "resolved_filename": "Creator - Clip [K1-BVtsHrOY].mp4",
            "completed_at": "2026-07-10T00:00:00+00:00",
        },
    )

    rows = repositories.load_history_entries_by_media_id("K1-BVtsHrOY")

    assert [(task_id, entry["resolved_filename"]) for task_id, entry in rows] == [
        ("disk:K1-BVtsHrOY", "Creator - Clip [K1-BVtsHrOY].mp4")
    ]


def test_history_source_lookup_matches_filename_media_id_through_real_sql(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.save_history_row(
        "disk:3238394",
        {
            "source_url": "",
            "source_key": "rule34video",
            "engine": "disk",
            "media_id": "",
            "resolved_filename": "wsds-minus8_source [3238394].mp4",
            "completed_at": "2026-07-10T00:00:00+00:00",
        },
    )

    task_id, found = history_module.find_history_by_source(
        "https://rule34video.com/video/3238394/wsds-minus8/"
    )

    assert task_id == "disk:3238394"
    assert found["resolved_filename"] == "wsds-minus8_source [3238394].mp4"


def test_initialize_database_prunes_completed_task_already_saved_to_history(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    repositories.merge_task_payload("t1", {"status": "completed", "source_url": "https://example.test/p/1"})
    repositories.save_history_row("t1", {"source_url": "https://example.test/p/1", "source_key": "example"})

    database_module.close_database()
    database_module.initialize_database()

    assert repositories.load_task_payload("t1") == {}
    assert repositories.load_history_entry_payload("t1")["source_url"] == "https://example.test/p/1"


def test_save_history_rows_accepts_an_empty_batch(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    repositories.save_history_rows([])

    assert repositories.load_history_payload()["entries"] == {}
