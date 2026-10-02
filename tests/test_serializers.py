from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.domains.downloads.library.history as history_module
import backend.app.domains.downloads.serializers as serializers_module
from backend.app.domains.downloads.serializers import history_to_api, task_to_api


def test_task_to_api_leaves_raw_gallerydl_filename_without_template(tmp_path: Path):
    media_file = tmp_path / "fakeacc.com - TikTok photo #7100000000000000002 [7100000000000000002]_1.jpg"
    media_file.write_bytes(b"image")

    api_task = task_to_api(
        "gallerydl:test",
        {
            "engine": "gallerydl",
            "status": "completed",
            "source_url": "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002",
            "resolved_full_path": str(media_file),
            "resolved_filename": media_file.name,
        },
    )

    assert api_task["resolved_filename"] == media_file.name


def test_task_to_api_prefers_saved_template_over_gallerydl_id_display(tmp_path: Path):
    media_file = tmp_path / "[abc123].mp4"
    media_file.write_bytes(b"video")

    api_task = task_to_api(
        "gallerydl:test",
        {
            "engine": "gallerydl",
            "status": "completed",
            "source_key": "example",
            "source_url": "https://www.example.test/watch/abc123",
            "creator": "ChannelHandle",
            "media_id": "abc123",
            "title": "Nice clip",
            "resolved_full_path": str(media_file),
            "resolved_filename": media_file.name,
            "folder_template": "{{username}}",
            "filename_template": "{{username}} - {{title}} [{{id}}]",
        },
    )

    assert api_task["resolved_filename"] == "ChannelHandle - Nice clip [abc123].mp4"


def test_task_to_api_honors_effective_naming_cleaning_for_gallerydl_display(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    media_file = tmp_path / "[abc123].mp4"
    media_file.write_bytes(b"video")
    monkeypatch.setattr(
        serializers_module,
        "get_effective_title_cleaning",
        lambda url: {"strip_handle_at": False},
    )

    api_task = task_to_api(
        "gallerydl:test",
        {
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
        },
    )

    assert api_task["resolved_filename"] == "@ChannelHandle - Nice clip [abc123].mp4"


def test_task_to_api_keeps_stored_creator_over_url_creator(tmp_path: Path):
    media_file = tmp_path / "fakeacc.com - Clip [7100000000000000002].mp4"
    media_file.write_bytes(b"video")

    api_task = task_to_api(
        "ytdlp:test",
        {
            "engine": "ytdlp",
            "status": "completed",
            "creator": "Some Display Name",
            "source_url": "https://www.tiktok.com/@fakeacc.com/video/7100000000000000002",
            "resolved_full_path": str(media_file),
            "resolved_filename": media_file.name,
        },
    )

    assert api_task["creator"] == "Some Display Name"


def test_history_source_lookup_matches_filename_media_id_without_stored_url(monkeypatch):
    entry = {
        "engine": "disk",
        "source_url": "",
        "source_key": "rule34video",
        "media_id": "3238394",
        "resolved_filename": "wsds-minus8_source [3238394].mp4",
    }
    monkeypatch.setattr(history_module, "load_history_entries_for_media_id", lambda media_id: [("disk:3238394", entry)])
    monkeypatch.setattr(
        history_module,
        "load_history",
        lambda: (_ for _ in ()).throw(AssertionError("media-id lookup should avoid full history decode")),
    )

    task_id, found = history_module.find_history_by_source("https://rule34video.com/video/3238394/wsds-minus8/")

    assert task_id == "disk:3238394"
    assert found is entry


def test_history_preserves_completed_engine(monkeypatch: pytest.MonkeyPatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(
        history_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )

    history_module.save_history_entry(
        "gallerydl:abc123",
        {
            "engine": "gallerydl",
            "source_url": "https://imgur.com/a/abc123",
            "source_key": "imgur",
            "resolved_folder": "/media/imgur",
            "resolved_filename": "clip [abc123].jpg",
            "resolved_full_path": "/media/imgur/clip [abc123].jpg",
            "quality": {"mode": "audio", "audio_format": "opus", "audio_bitrate": "192"},
        },
    )

    assert saved["gallerydl:abc123"]["engine"] == "gallerydl"
    assert saved["gallerydl:abc123"]["quality"]["mode"] == "audio"
    api_task = history_to_api("gallerydl:abc123", saved["gallerydl:abc123"])
    assert api_task["task_type"] == "gallerydl"
    assert api_task["quality"]["audio_format"] == "opus"


def test_history_to_api_does_not_touch_filesystem(monkeypatch: pytest.MonkeyPatch):
    def fail_recovery(*args, **kwargs):
        raise AssertionError("History list serialization should not stat or recover files.")

    monkeypatch.setattr(serializers_module, "recover_task_path", fail_recovery)

    api_task = history_to_api(
        "gallerydl:abc123",
        {
            "engine": "gallerydl",
            "source_url": "https://imgur.com/a/abc123",
            "source_key": "imgur",
            "resolved_folder": "/media/imgur",
            "resolved_filename": "clip [abc123].jpg",
            "resolved_full_path": "/media/imgur/clip [abc123].jpg",
            "file_size": 1234,
        },
    )

    assert api_task["can_download"] is True
    assert api_task["file_size"] == 1234
