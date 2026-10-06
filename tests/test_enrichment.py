from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.db.repositories as repositories
import backend.app.domains.downloads.workers.enrichment as enrichment_module
from backend.app.domains.downloads import store as store_module
from tests.support import use_temp_db


def test_enqueue_completion_enrichment_persists_minimal_dry_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setattr(enrichment_module, "ensure_enrichment_worker", lambda: None)

    enrichment_module.enqueue_completion_enrichment(
        "gallerydl:abc123",
        metadata={"id": "abc123", "username": "creator"},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        quality={"mode": "merged", "video_quality": "720p"},
        output_root=str(tmp_path),
        extra_tokens={"artist": "creator"},
        post_processing={"metadata": "sidecar"},
        needs_metadata_probe=True,
        needs_field_probe=True,
    )

    jobs = repositories.load_enrichment_jobs_payload()
    assert len(jobs) == 1
    payload = jobs[0]["payload"]
    assert payload == {
        "task_id": "gallerydl:abc123",
        "template_settings": {
            "folder_template": "{{username}}",
            "filename_template": "{{title}} [{{id}}]",
        },
        "quality": {
            "mode": "merged",
            "video_quality": "720p",
            "video_container": "auto",
            "video_codec": "auto",
            "audio_format": "auto",
            "audio_bitrate": "best",
        },
        "output_root": str(tmp_path),
        "metadata": {"id": "abc123", "username": "creator"},
        "extra_tokens": {"artist": "creator"},
        "post_processing": {
            "metadata": "sidecar",
            "subtitles": "off",
            "automatic_subtitles": "off",
            "chapters": "off",
            "thumbnail": "off",
            "subtitle_languages": [],
            "split_chapters": False,
            "mtime": False,
        },
        "needs_metadata_probe": True,
        "needs_field_probe": True,
    }
    assert {
        "source_url",
        "source_key",
        "engine",
        "creator",
        "media_id",
        "resolved_full_path",
        "resolved_folder",
        "resolved_filename",
    }.isdisjoint(payload)


def test_enrichment_repairs_sparse_creator_title_and_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    media_id = "DZwrrifkye4"
    raw_video = tmp_path / f"None - [{media_id}] [{media_id}].mp4"
    raw_video.write_bytes(b"video")
    source_url = f"https://www.instagram.com/reel/{media_id}/"
    task_id = "gallerydl:instagram-cookie-metadata"
    template_settings = {
        "folder_template": "",
        "filename_template": "{{username}} - {{title}} [{{id}}]",
    }
    store_module.save_history_entry_row(
        task_id,
        {
            "engine": "gallerydl",
            "source_url": source_url,
            "source_key": "instagram",
            "creator": "None",
            "media_id": media_id,
            "resolved_full_path": str(raw_video),
            "resolved_folder": str(tmp_path),
            "resolved_filename": raw_video.name,
            "title": "",
            **template_settings,
        },
    )
    monkeypatch.setattr(
        enrichment_module,
        "probe_link_metadata",
        lambda url, source_key="", **kwargs: {
            "id": media_id,
            "webpage_url": source_url,
            "username": "real.creator",
            "title": f"None - [{media_id}]",
        },
    )
    monkeypatch.setattr(enrichment_module, "drop_file_cache", lambda paths: None)

    enrichment_module._run_enrichment_job(
        {
            "id": f"completion:{task_id}",
            "payload": {
                "task_id": task_id,
                "template_settings": template_settings,
                "output_root": str(tmp_path),
                "needs_metadata_probe": True,
                "needs_field_probe": False,
            },
        }
    )

    updated = store_module.load_history_entry(task_id)
    clean_video = tmp_path / f"real.creator - Unknown [{media_id}].mp4"
    assert clean_video.is_file()
    assert not raw_video.exists()
    assert updated["resolved_full_path"] == str(clean_video)
    assert updated["resolved_filename"] == clean_video.name
    assert updated["creator"] == "real.creator"


def test_enrichment_worker_deletes_stale_job_when_history_row_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    store_module.enqueue_enrichment_job(
        "completion:missing",
        "completion",
        {"task_id": "missing", "needs_metadata_probe": True, "needs_field_probe": True},
    )

    job = store_module.claim_next_enrichment_job()
    assert job is not None
    enrichment_module._process_enrichment_job(job)

    assert repositories.load_enrichment_jobs_payload() == []


def test_enrichment_worker_does_not_start_when_queue_is_empty(monkeypatch: pytest.MonkeyPatch):
    started: list[object] = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            started.append((args, kwargs))

        def start(self):
            raise AssertionError("empty queue should not start a worker thread")

    monkeypatch.setattr(enrichment_module, "_worker_running", False)
    monkeypatch.setattr(enrichment_module, "pending_enrichment_job_count", lambda: 0)
    monkeypatch.setattr(enrichment_module.threading, "Thread", FakeThread)

    enrichment_module.ensure_enrichment_worker()

    assert started == []
    assert enrichment_module._worker_running is False


def test_enrichment_worker_marks_itself_stopped_when_queue_drains(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(enrichment_module, "_worker_running", True)
    monkeypatch.setattr(enrichment_module, "pending_enrichment_job_count", lambda: 0)

    assert enrichment_module._stop_worker_if_drained() is True
    assert enrichment_module._worker_running is False


def test_enrichment_worker_keeps_running_when_job_arrives_during_drain(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(enrichment_module, "_worker_running", True)
    monkeypatch.setattr(enrichment_module, "pending_enrichment_job_count", lambda: 1)

    assert enrichment_module._stop_worker_if_drained() is False
    assert enrichment_module._worker_running is True
