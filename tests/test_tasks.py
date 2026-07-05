from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.services.tasks.scan as scan_module
from backend.app.services.tasks import (
    canonicalize_source_url,
    convert_template_to_ytdlp,
    count_tasks,
    counts_by_menu,
    detect_source_key,
    extract_downloaded_path,
    is_media_file,
    parse_filename_media_id,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("   ", ""),
        ("instagram.com/reel/abc", "https://instagram.com/reel/abc"),
        ("https://www.instagram.com/reel/abc", "https://www.instagram.com/reel/abc"),
        ("https://tiktok.com/@x/video/1", "https://tiktok.com/@x/video/1"),
        ("https://youtube.com/watch?v=1", "https://youtube.com/watch?v=1"),
    ],
)
def test_canonicalize_source_url(raw, expected):
    assert canonicalize_source_url(raw) == expected


def test_canonicalize_trailing_slash_idempotent():
    once = canonicalize_source_url("instagram.com/reel/abc")
    assert canonicalize_source_url(once) == once


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://www.youtube.com/watch?v=1", "youtube"),
        ("https://youtu.be/abc", "youtu"),
        ("https://facebook.com/x", "facebook"),
        ("https://fb.watch/x", "fb"),
        ("https://www.instagram.com/reel/x", "instagram"),
        ("https://tiktok.com/@x/video/1", "tiktok"),
        ("https://example.com/video", "example"),
        ("https://www.pornhub.com/view_video.php?viewkey=1", "pornhub"),
        ("https://rule34video.com/video/1", "rule34video"),
        ("not a url", "others"),
    ],
)
def test_detect_source_key(url, expected):
    assert detect_source_key(url) == expected


def test_convert_template_to_ytdlp_maps_placeholders():
    result = convert_template_to_ytdlp("{{creator}} - {{title}} [{{id}}]")
    assert "%(title|Unknown)s" in result
    assert "%(id|NA)s" in result
    assert "{{" not in result


def test_convert_template_unknown_placeholder_falls_back():
    assert convert_template_to_ytdlp("{{weird}}") == "%(weird|Unknown)s"


def test_convert_template_empty():
    assert convert_template_to_ytdlp("") == ""
    assert convert_template_to_ytdlp("   ") == ""


@pytest.mark.parametrize(
    "line,expected",
    [
        ("[download] Destination: /media/a.mp4", "/media/a.mp4"),
        ('[Merger] Merging formats into "/media/b.mkv"', "/media/b.mkv"),
        ("[download] /media/c.mp4 has already been downloaded", "/media/c.mp4"),
        ("[download]  50.0% of 10MiB", ""),
        ("", ""),
    ],
)
def test_extract_downloaded_path(line, expected):
    assert extract_downloaded_path(line) == expected


def test_is_media_file(tmp_path: Path):
    good = tmp_path / "clip.mp4"
    good.write_bytes(b"x")
    bad = tmp_path / "notes.txt"
    bad.write_bytes(b"x")
    assert is_media_file(good) is True
    assert is_media_file(bad) is False
    assert is_media_file(tmp_path / "missing.mp4") is False


def test_parse_filename_media_id_uses_last_bracketed_id():
    assert parse_filename_media_id("Creator - Soft Light [Abc_123-xy].mp4") == (
        "Abc_123-xy",
        "Creator - Soft Light",
    )


def test_parse_filename_media_id_rejects_unrecoverable_names():
    assert parse_filename_media_id("Creator - Soft Light.mp4") == ("", "Creator - Soft Light")
    assert parse_filename_media_id("Creator - Soft Light [NA].mp4")[0] == ""


def test_scan_media_library_imports_history_from_filename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    artist_dir = media_root / "Trace Artist"
    artist_dir.mkdir(parents=True)
    media_file = artist_dir / "Trace Artist - Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    result = scan_module.scan_media_library([media_root])

    assert result == {"checked": 0, "missing": 0, "added": 1}
    assert saved["disk:abc123"]["resolved_full_path"] == str(media_file)
    assert saved["disk:abc123"]["resolved_filename"] == media_file.name
    assert saved["disk:abc123"]["source_key"] == "others"


def test_scan_media_library_removes_missing_completed_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    missing_file = tmp_path / "missing [gone123].mp4"
    removed_tasks: list[str] = []
    removed_history: list[str] = []
    monkeypatch.setattr(
        scan_module,
        "load_task_store",
        lambda: {"tasks": {"task-1": {"status": "completed", "resolved_full_path": str(missing_file)}}},
    )
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "save_history_entry_row", lambda task_id, payload: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed_tasks.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed_history.append(task_id))

    result = scan_module.scan_media_library([tmp_path])

    assert result == {"checked": 1, "missing": 1, "added": 0}
    assert removed_tasks == ["task-1"]
    assert removed_history == ["task-1"]


def test_count_tasks_and_by_menu():
    tasks = [
        {"status": "pending", "source_key": "youtube"},
        {"status": "running", "source_key": "youtube"},
        {"status": "completed", "source_key": "tiktok"},
        {"status": "failed", "source_key": "others"},
    ]
    counts = count_tasks(tasks)
    assert counts == {"queued": 1, "running": 1, "completed": 1, "failed": 1}
    by_menu = counts_by_menu(tasks)
    assert by_menu["all"]["queued"] == 1
    assert by_menu["youtube"]["running"] == 1
    assert by_menu["tiktok"]["completed"] == 1
