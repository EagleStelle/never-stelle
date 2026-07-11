from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.services.tasks.history as history_module
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
from backend.app.services.tasks.formats import (
    conflicts_with_source,
    creator_from_url,
    guess_sources,
    learn_download,
    reconstruct_url,
)
from backend.app.services.tasks.serializers import history_to_api
from backend.app.services.tasks.ytdlp import clean_filename_title, clean_social_title


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("   ", ""),
        ("instagram.com/reel/abc", "https://instagram.com/reel/abc"),
        ("https://www.instagram.com/reel/abc", "https://www.instagram.com/reel/abc"),
        ("https://tiktok.com/@x/video/1", "https://tiktok.com/@x/video/1"),
        ("https://youtube.com/watch?v=1", "https://youtube.com/watch?v=1"),
        (
            "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
            "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
        ),
        ("https://youtube.com/watch?v=1&feature=share", "https://youtube.com/watch?v=1"),
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


def test_convert_template_prefers_creator_from_url():
    result = convert_template_to_ytdlp(
        "{{creator}} - {{title}} [{{id}}]",
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
    )

    assert result.startswith("fzyahoo.com - ")
    assert "%(creator" not in result


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


def test_clean_social_title_removes_engagement_and_attribution_junk():
    assert clean_social_title("Soft Light 1.5M views · 62K reactions") == "Soft Light"
    assert clean_social_title("Soft Light ｜ NJ Tony on Reels") == "Soft Light"
    assert clean_social_title("NJ Tony - Video by NJ Tony", "NJ Tony") == "NJ Tony"


def test_clean_filename_title_removes_duplicate_social_display_name():
    title = (
        "ININIinNINI - "
        "\u6c99\u96e8 \u30a4\u30cb \u2726\u2726 - "
        "Photoshop\u3067\u3064\u304f\u308b\u3001 \u590f\u30b5\u30e0\u30cd\u30a4\u30eb"
        "\u306e\u30e1\u30a4\u30ad\u30f3\u30b0\u898b\u3066\u2026\uff01\uff01"
    )

    assert clean_filename_title(title, "ININIinNINI") == (
        "ININIinNINI - "
        "Photoshop\u3067\u3064\u304f\u308b\u3001 \u590f\u30b5\u30e0\u30cd\u30a4\u30eb"
        "\u306e\u30e1\u30a4\u30ad\u30f3\u30b0\u898b\u3066\u2026\uff01\uff01"
    )


def test_clean_filename_title_keeps_content_like_leading_segment():
    title = "ININIinNINI - Part 1 - Photoshop summer thumbnail process"

    assert clean_filename_title(title, "ININIinNINI") == title


def test_scan_media_library_imports_history_from_filename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    artist_dir = media_root / "Trace Artist"
    artist_dir.mkdir(parents=True)
    media_file = artist_dir / "Trace Artist - Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
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


def _learned_youtube_twitter() -> dict:
    learned = learn_download({}, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ")
    learned = learn_download(learned, "https://www.youtube.com/watch?v=Tz-E6i7Mylc", "Tz-E6i7Mylc")
    return learn_download(learned, "https://twitter.com/DohaVT/status/2073635724684054528", "2073635724684054528")


def test_learn_download_derives_url_template():
    assert _learned_youtube_twitter()["youtube"]["template"] == "https://www.youtube.com/watch?v={id}"


def test_learn_download_marks_creator_segment():
    learned = learn_download({}, "https://twitter.com/DohaVT/status/2073635724684054528", "2073635724684054528")
    learned = learn_download(learned, "https://twitter.com/Other/status/1111111111111111111", "1111111111111111111")
    assert learned["twitter"]["template"] == "https://twitter.com/{creator}/status/{id}"
    assert learned["twitter"]["creator_part"] == "path:0"


def test_learn_download_trims_seo_query_and_keeps_creator_token():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
        "7493558766131039489",
    )

    assert learned["tiktok"]["template"] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert reconstruct_url(learned, "tiktok", "7493558766131039489") == ""


def test_creator_from_url_uses_handle_segment_without_at_sign():
    assert creator_from_url("https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489") == "fzyahoo.com"
    assert creator_from_url("https://x.com/ININIinNINI/status/2073390288501166083") == "ININIinNINI"


def test_learn_download_ignores_unknown_host():
    assert learn_download({}, "not a url", "abc123") == {}


def test_guess_sources_uses_learned_signatures():
    learned = _learned_youtube_twitter()
    assert guess_sources(learned, "kZ0vN9pLm-Q") == ["youtube"]
    assert guess_sources(learned, "2073635724684054528") == ["twitter"]


@pytest.mark.parametrize(
    "media_id,source_key,expected",
    [
        ("2073635724684054528", "youtube", True),
        ("2073635724684054528", "twitter", False),
        ("kZ0vN9pLm-Q", "youtube", False),
        ("kZ0vN9pLm-Q", "twitter", True),
        ("anything", "unlearnedsite", False),
    ],
)
def test_conflicts_with_source(media_id, source_key, expected):
    assert conflicts_with_source(_learned_youtube_twitter(), source_key, media_id) is expected


@pytest.mark.parametrize(
    "source_key,media_id,expected",
    [
        ("youtube", "newid1234567", "https://www.youtube.com/watch?v=newid1234567"),
        ("tiktok", "123", ""),
        ("youtube", "", ""),
    ],
)
def test_reconstruct_url_from_learned(source_key, media_id, expected):
    assert reconstruct_url(_learned_youtube_twitter(), source_key, media_id) == expected


def test_infer_disk_source_vetoes_folder_then_uses_learned_guess(tmp_path: Path):
    folder = tmp_path / "yt"
    folder.mkdir()
    media_file = folder / "DOHA - DOHA - Squishy cheeks [2073635724684054528].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index({"youtube": str(folder)})

    source_key, pending, _ = scan_module.infer_disk_source(
        media_file, "2073635724684054528", index, _learned_youtube_twitter()
    )

    assert source_key == "twitter"
    assert pending is False


def test_infer_disk_source_ambiguous_when_multiple_learned_match(tmp_path: Path):
    media_file = tmp_path / "Clip [1111111111111111111].mp4"
    media_file.write_bytes(b"video")
    learned = learn_download({}, "https://twitter.com/A/status/2073635724684054528", "2073635724684054528")
    learned = learn_download(learned, "https://www.tiktok.com/@a/video/7123456789012345678", "7123456789012345678")

    source_key, pending, candidates = scan_module.infer_disk_source(
        media_file, "1111111111111111111", [], learned
    )

    assert source_key == "others"
    assert pending is True
    assert set(candidates) == {"twitter", "tiktok"}


def test_infer_disk_source_prefers_configured_folder(tmp_path: Path):
    folder = tmp_path / "yt"
    folder.mkdir()
    media_file = folder / "Clip [dQw4w9WgXcQ].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index({"youtube": str(folder)})

    source_key, pending, candidates = scan_module.infer_disk_source(
        media_file, "dQw4w9WgXcQ", index, _learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert pending is False
    assert candidates == []


def test_scan_media_library_flags_ambiguous_source_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "Clip [7123456789012345678].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7123456789012345678"]
    assert entry["source_key"] == "others"
    assert entry["source_pending"] is True


def test_scan_media_library_reconstructs_link_from_learned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    learned = learn_download({}, "https://www.bilibili.com/video/BV1xx411c7mD", "BV1xx411c7mD")
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "Clip [BV1xx411c7mD].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:BV1xx411c7mD"]
    assert entry["source_key"] == "bilibili"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://www.bilibili.com/video/BV1xx411c7mD"


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
        },
    )

    assert saved["gallerydl:abc123"]["task_type"] == "gallerydl"
    assert history_to_api("gallerydl:abc123", saved["gallerydl:abc123"])["task_type"] == "gallerydl"
