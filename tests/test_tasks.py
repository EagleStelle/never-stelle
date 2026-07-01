from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.services.tasks import (
    can_delete_done_task,
    canonicalize_source_url,
    convert_template_to_ytdlp,
    count_tasks,
    counts_by_menu,
    detect_site_category,
    extract_downloaded_path,
    is_media_file,
    normalize_tabs,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("   ", ""),
        ("instagram.com/reel/abc", "https://instagram.com/reel/abc/"),
        ("https://www.instagram.com/reel/abc", "https://www.instagram.com/reel/abc/"),
        ("https://tiktok.com/@x/video/1", "https://tiktok.com/@x/video/1/"),
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
        ("https://youtu.be/abc", "youtube"),
        ("https://facebook.com/x", "facebook"),
        ("https://fb.watch/x", "facebook"),
        ("https://www.instagram.com/reel/x", "instagram"),
        ("https://tiktok.com/@x/video/1", "tiktok"),
        ("https://example.com/video", "others"),
        ("not a url", "others"),
    ],
)
def test_detect_site_category(url, expected):
    assert detect_site_category(url) == expected


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


def test_normalize_tabs_dedups_and_strips():
    assert normalize_tabs([" a ", "a", "b", "", None]) == ["a", "b"]
    assert normalize_tabs("not a list") == []


def test_is_media_file(tmp_path: Path):
    good = tmp_path / "clip.mp4"
    good.write_bytes(b"x")
    bad = tmp_path / "notes.txt"
    bad.write_bytes(b"x")
    assert is_media_file(good) is True
    assert is_media_file(bad) is False
    assert is_media_file(tmp_path / "missing.mp4") is False


def test_can_delete_failed_task():
    assert can_delete_done_task("t", {"status": "failed"}, {"tasks": {}}) is True


def test_can_delete_pending_task_is_false():
    assert can_delete_done_task("t", {"status": "pending"}, {"tasks": {}}) is False


def test_can_delete_completed_nas_task():
    assert can_delete_done_task("t", {"status": "completed", "save_mode": "nas"}, {"tasks": {}}) is True


def test_can_delete_device_task_waits_for_delivery():
    meta = {"tasks": {"t": {"device_request_tabs": ["tab1"], "delivered_device_tabs": []}}}
    task = {"status": "completed", "save_mode": "device"}
    assert can_delete_done_task("t", task, meta) is False
    meta["tasks"]["t"]["delivered_device_tabs"] = ["tab1"]
    assert can_delete_done_task("t", task, meta) is True


def test_count_tasks_and_by_menu():
    tasks = [
        {"status": "pending", "site_category": "youtube"},
        {"status": "running", "site_category": "youtube"},
        {"status": "completed", "site_category": "tiktok"},
        {"status": "failed", "site_category": "others"},
    ]
    counts = count_tasks(tasks)
    assert counts == {"queued": 1, "running": 1, "completed": 1, "failed": 1}
    by_menu = counts_by_menu(tasks)
    assert by_menu["all"]["queued"] == 1
    assert by_menu["youtube"]["running"] == 1
    assert by_menu["tiktok"]["completed"] == 1
