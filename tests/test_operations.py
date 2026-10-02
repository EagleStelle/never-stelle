from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

import backend.app.db.repositories as repositories
import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.slideshow as slideshow_module
import backend.app.runtime.scratch as scratch_module
from tests.support import use_temp_db


def _slideshow_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, Path, Path]:
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    monkeypatch.setattr(slideshow_module, "_archives", {})

    first = tmp_path / "Creator - Cap [abc123]_1.jpg"
    second = tmp_path / "Creator - Cap [abc123]_2.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    task_id = "gallerydl:abc123"

    monkeypatch.setattr(
        operations_module,
        "load_task_store",
        lambda: {
            "tasks": {
                task_id: {
                    "status": "completed",
                    "resolved_full_path": str(first),
                    "resolved_filename": "Creator - Cap [abc123].jpg",
                }
            }
        },
    )
    monkeypatch.setattr(operations_module, "find_history_by_id", lambda task_id: None)
    return task_id, first, second


def test_queue_task_stores_quality_and_falls_back_to_saved_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from backend.app.domains.downloads.planning import ResolvedTaskSettings

    resolved = ResolvedTaskSettings(
        source_key="example",
        source_profile={"key": "example", "label": "Example"},
        source_profiles=[{"key": "example", "label": "Example", "hosts": []}],
        source_locations={"example": {"https://example.com/{id}": ""}},
        output_dir=str(tmp_path),
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}}"},
    )
    captured: dict[str, dict] = {}

    monkeypatch.setattr(operations_module, "ensure_worker", lambda: None)
    monkeypatch.setattr(operations_module, "resolve_redirect_url", lambda url: url)
    monkeypatch.setattr(operations_module, "find_active_by_source", lambda url: (None, None))
    monkeypatch.setattr(operations_module, "find_history_by_source", lambda url: (None, None))
    monkeypatch.setattr(operations_module, "load_app_config", lambda: {})
    monkeypatch.setattr(operations_module, "resolve_task_settings", lambda *a, **k: resolved)
    monkeypatch.setattr(operations_module, "is_allowed_location", lambda location: True)
    saved_default = {
        "mode": "merged",
        "video_quality": "1080p",
        "video_container": "mp4",
        "video_codec": "auto",
        "audio_format": "mp3",
        "audio_bitrate": "best",
    }
    # A request without quality takes the default mode's remembered selection.
    saved_defaults = {"mode": "merged", "merged": saved_default, "audio": {"mode": "audio", "audio_format": "flac"}}
    monkeypatch.setattr(
        operations_module, "get_effective_saved_settings", lambda cfg: {"default_quality": saved_defaults}
    )
    monkeypatch.setattr(operations_module, "task_to_api", lambda task_id, task: task)

    def fake_update_task(task_id, **kwargs):
        captured["task"] = kwargs
        return kwargs

    monkeypatch.setattr(operations_module, "update_task", fake_update_task)

    operations_module.queue_task(
        "https://example.test/watch?v=1",
        quality={"mode": "audio", "audio_format": "opus", "audio_bitrate": "320"},
    )
    assert captured["task"]["engine"] == "gallerydl"
    assert "engine_policy" not in captured["task"]
    assert captured["task"]["quality"] == {
        "mode": "audio",
        "video_quality": "best",
        "video_container": "auto",
        "video_codec": "auto",
        "audio_format": "opus",
        "audio_bitrate": "320",
    }
    assert captured["task"]["post_processing"] == {
        "metadata": "off",
        "subtitles": "off",
        "automatic_subtitles": "off",
        "chapters": "off",
        "thumbnail": "off",
        "subtitle_languages": [],
        "split_chapters": False,
        "mtime": False,
    }

    operations_module.queue_task(
        "https://example.test/watch?v=metadata",
        quality={
            "mode": "merged",
            "_post_processing": {"metadata": "both", "subtitle_languages": ["en"]},
        },
    )
    assert captured["task"]["post_processing"] == {
        "metadata": "both",
        "subtitles": "off",
        "automatic_subtitles": "off",
        "chapters": "off",
        "thumbnail": "off",
        "subtitle_languages": ["en"],
        "split_chapters": False,
        "mtime": False,
    }

    operations_module.queue_task("https://example.test/watch?v=2", quality=None)
    assert captured["task"]["quality"] == saved_default

    operations_module.queue_task(
        "https://example.test/watch?v=3",
        quality={"mode": "merged", "video_container": "webm", "video_codec": "vp9"},
    )
    assert captured["task"]["quality"]["video_container"] == "webm"
    assert captured["task"]["quality"]["video_codec"] == "vp9"


def test_retry_downloads_rebuilds_failed_tasks_with_selected_engine(monkeypatch: pytest.MonkeyPatch):
    repositories.merge_task_payload(
        "ytdlp:failed",
        {
            "status": "failed",
            "engine": "ytdlp",
            "source_url": "https://example.test/watch?v=1",
            "output_dir": "/media/example",
            "folder_template": "",
            "filename_template": "{{title}} [{{id}}]",
        },
    )
    repositories.merge_task_payload("ytdlp:queued", {"status": "pending"})
    started: list[bool] = []
    monkeypatch.setattr(operations_module, "ensure_worker", lambda: started.append(True))

    result = operations_module.retry_downloads(["ytdlp:failed", "ytdlp:queued"])

    retried = repositories.load_task_payload("ytdlp:failed")
    assert result == {"count": 1, "errors": ["Only failed downloads can be retried."]}
    assert started == [True]
    assert retried["status"] == "pending"
    assert retried["engine"] == "gallerydl"
    assert "{extension}" in retried["output_template"]


def test_queue_task_reuses_history_regardless_of_stored_engine(
    monkeypatch: pytest.MonkeyPatch,
):
    # Dedup is engine-agnostic: a prior download (even one tagged ytdlp) is
    # reused rather than re-queued, so queue_task never reaches task creation.
    source_url = "https://example.test/post/abc123"
    entry = {
        "engine": "ytdlp",
        "source_url": source_url,
        "resolved_filename": "Clip [abc123].mp4",
    }

    monkeypatch.setattr(operations_module, "resolve_redirect_url", lambda url: url)
    monkeypatch.setattr(operations_module, "find_active_by_source", lambda url: (None, None))
    monkeypatch.setattr(operations_module, "find_history_by_source", lambda url: ("ytdlp:old", entry))
    monkeypatch.setattr(operations_module, "history_to_api", lambda task_id, e: {"vid": task_id, **e})

    def _fail(*args, **kwargs):
        raise AssertionError("reuse must short-circuit before task creation")

    monkeypatch.setattr(operations_module, "resolve_task_settings", _fail)
    monkeypatch.setattr(operations_module, "update_task", _fail)

    tasks, reused = operations_module.queue_task(source_url)

    assert reused is True
    assert tasks == [{"vid": "ytdlp:old", **entry}]


def test_resolve_task_file_prefers_saved_template_for_gallerydl_download_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_file = tmp_path / "[abc123].mp4"
    media_file.write_bytes(b"video")
    task = {
        "engine": "gallerydl",
        "status": "completed",
        "source_key": "example",
        "source_url": "https://www.example.test/watch/abc123",
        "creator": "@ChannelHandle",
        "media_id": "abc123",
        "title": "Nice clip",
        "resolved_full_path": str(media_file),
        "resolved_filename": media_file.name,
        "folder_template": "{{username}}",
        "filename_template": "{{username}} - {{title}} [{{id}}]",
    }

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": {"gallerydl:test": task}})
    monkeypatch.setattr(operations_module, "find_history_by_id", lambda task_id: None)
    monkeypatch.setattr(
        operations_module,
        "recover_task_path",
        lambda task_id, task, persist=True: (str(media_file), str(tmp_path), media_file.name),
    )
    monkeypatch.setattr(operations_module, "find_numbered_media_siblings", lambda path: [path])
    monkeypatch.setattr(
        operations_module,
        "get_effective_title_cleaning",
        lambda url: {"strip_handle_at": False},
    )

    path, filename = operations_module.resolve_task_file("gallerydl:test")

    assert path == media_file
    assert filename == "@ChannelHandle - Nice clip [abc123].mp4"


def test_correct_reconstructed_url_adopts_pasted_link_for_disk_entry(monkeypatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(operations_module, "save_history_entry_row", lambda tid, p: saved.update({tid: p}))
    entry = {"engine": "disk", "source_url": "https://www.tiktok.com/@a/photo/7100000000000000004"}
    real_url = "https://www.tiktok.com/@a/video/7100000000000000004"

    out = operations_module._correct_reconstructed_url("disk:7100000000000000004", entry, real_url)

    assert out["source_url"] == real_url
    assert saved["disk:7100000000000000004"]["source_url"] == real_url


def test_correct_reconstructed_url_leaves_real_download_untouched(monkeypatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(operations_module, "save_history_entry_row", lambda tid, p: saved.update({tid: p}))
    real_url = "https://www.tiktok.com/@a/video/7100000000000000004"
    entry = {"engine": "ytdlp", "source_url": real_url}

    out = operations_module._correct_reconstructed_url("ytdlp:abc", entry, "https://www.tiktok.com/@a/photo/7100000000000000004")

    assert out["source_url"] == real_url  # a real download's link is authoritative; never overwritten
    assert saved == {}


def test_resolve_task_file_for_history_entry_validates_on_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_file = tmp_path / "clip [abc123].jpg"
    media_file.write_bytes(b"image")

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(
        operations_module,
        "find_history_by_id",
        lambda task_id: {
            "engine": "gallerydl",
            "creator": "creator",
            "source_url": "https://imgur.com/a/abc123",
            "resolved_full_path": str(media_file),
            "resolved_filename": media_file.name,
            "resolved_folder": str(tmp_path),
        },
    )

    path, filename = operations_module.resolve_task_file("gallerydl:abc123")

    assert path == media_file
    assert filename == "clip [abc123].jpg"


def test_resolve_task_file_zips_numbered_gallerydl_siblings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    task_id, first, second = _slideshow_task(tmp_path, monkeypatch)

    archive_path, filename = operations_module.resolve_task_file(task_id)

    assert filename == "Creator - Cap [abc123].zip"
    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == [first.name, second.name]


def test_resolve_task_file_reuses_one_slideshow_archive_across_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    task_id, _, _ = _slideshow_task(tmp_path, monkeypatch)

    first_path, _ = operations_module.resolve_task_file(task_id)
    second_path, _ = operations_module.resolve_task_file(task_id)

    assert second_path == first_path
    assert first_path.is_file()
    assert len(list(scratch_module.SCRATCH_DIR.glob("nvs-slideshow-*.zip"))) == 1


def test_resolve_task_file_rebuilds_slideshow_archive_when_siblings_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    task_id, _, second = _slideshow_task(tmp_path, monkeypatch)

    first_path, _ = operations_module.resolve_task_file(task_id)
    second.write_bytes(b"second edited")
    rebuilt_path, _ = operations_module.resolve_task_file(task_id)

    assert rebuilt_path != first_path
    with zipfile.ZipFile(rebuilt_path) as archive:
        assert archive.read(second.name) == b"second edited"


def test_clear_slideshow_archives_removes_cached_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    task_id, _, _ = _slideshow_task(tmp_path, monkeypatch)

    archive_path, _ = operations_module.resolve_task_file(task_id)
    slideshow_module.clear_slideshow_archives()

    assert not archive_path.exists()


def test_add_source_and_learn_format_returns_matched_template(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setattr(
        operations_module,
        "load_app_config",
        lambda: {
            "sourceProfiles": [
                {"key": "facebook", "label": "Facebook", "hosts": ["facebook.com"]}
            ]
        },
    )
    monkeypatch.setattr(operations_module, "probe_link_fields", lambda *args, **kwargs: {})

    result = operations_module.add_source_and_learn_format(
        "https://www.facebook.com/reel/800000000000002"
    )

    assert result["source_key"] == "facebook"
    assert result["media_id"] == "800000000000002"
    assert result["format_template"] == "https://www.facebook.com/reel/{id}"


def test_adding_a_link_learns_its_format_from_one_probe(tmp_path, monkeypatch):
    from backend.app.db import repositories

    use_temp_db(tmp_path, monkeypatch)
    probes: list[str] = []

    def probe(url, key=""):
        probes.append(url)
        return {
            "source_key": "example",
            "field_roles": {"username": ["author[uniqueId]"]},
            "metadata": {"author[uniqueId]": "alice"},
        }

    monkeypatch.setattr(operations_module, "probe_link_fields", probe)

    result = operations_module.add_source_and_learn_format("https://example.test/alice/post/22222222")

    assert probes == ["https://example.test/alice/post/22222222"]
    assert result["format_template"] == "https://example.test/{username}/post/{id}"
    assert repositories.load_learned_formats_payload()["example"]["samples"] == 1
