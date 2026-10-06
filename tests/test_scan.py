from __future__ import annotations

import time
from pathlib import Path

import pytest

import backend.app.db.repositories as repositories
import backend.app.domains.downloads.engines.probe as probe_module
import backend.app.domains.downloads.files as files_module
import backend.app.domains.downloads.library.scan as scan_module
from backend.app.domains.formats.learning import (
    learn_download,
)
from tests.support import learned_youtube_twitter

_VIDEO_FORMAT = "https://example.test/video/{id}"


_PHOTO_FORMAT = "https://example.test/photo/{id}"


def _patch_two_format_scan(
    monkeypatch: pytest.MonkeyPatch, saved: dict[str, dict], entries: dict[str, dict] | None = None
) -> None:
    """A source whose video and photo files are named apart."""
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(entries or {})})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(
        scan_module, "load_learned_formats", lambda: {"example": {"templates": [_VIDEO_FORMAT, _PHOTO_FORMAT]}}
    )
    base = {"folder_template": "", "filename_template": "{{title}} [{{id}}]"}
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: (
            base,
            {
                "example": {
                    _VIDEO_FORMAT: {**base, "filename_template": "video {{title}} [{{id}}]"},
                    _PHOTO_FORMAT: {
                        **base,
                        "subfolder_template": "post {{id}}",
                        "filename_template": "photo {{title}} [{{id}}]",
                    },
                }
            },
        ),
    )
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: saved.update(dict(rows)))
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)


def _scan_locations(source_key: str, folder: Path, format_template: str = "https://www.youtube.com/watch?v={id}"):
    return [(source_key, format_template, str(folder))]


def _incremental_scan_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A scan wired to a real history store, so a rescan sees the first pass's rows."""
    media_root = tmp_path / "media"
    media_root.mkdir()
    rows: dict[str, dict] = {}
    resolved: list[str] = []
    written: list[str] = []
    learned: dict[str, dict] = {"example": {"templates": ["https://example.test/v/{id}"]}}

    def save(batch):
        written.extend(task_id for task_id, _ in batch)
        rows.update(dict(batch))

    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(rows)})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(scan_module, "save_history_entry_rows", save)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(
        scan_module, "remove_history_records", lambda task_ids: [rows.pop(task_id, None) for task_id in task_ids]
    )

    real_parse = scan_module._parse_media_fields

    def counting_parse(path, pattern):
        resolved.append(str(path))
        return real_parse(path, pattern)

    monkeypatch.setattr(scan_module, "_parse_media_fields", counting_parse)
    return media_root, rows, resolved, written, learned


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
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 0,
        "missing": 0,
        "added": 1,
        "unchanged": 0,
        "needs_resolve": 0,
    }
    assert saved["disk:abc123"]["resolved_full_path"] == str(media_file)
    assert saved["disk:abc123"]["resolved_filename"] == media_file.name
    assert saved["disk:abc123"]["source_key"] == ""


def test_scan_media_library_infers_source_from_named_source_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    tiktok_dir = media_root / "tiktok" / "fakeacc.com"
    tiktok_dir.mkdir(parents=True)
    media_file = tiktok_dir / "fakeacc.com - [7100000000000000002]_1.jpg"
    media_file.write_bytes(b"image")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: {"tiktok"})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7100000000000000002"]
    assert entry["source_key"] == "tiktok"
    assert entry["source_pending"] is False
    assert entry["resolved_filename"] == "fakeacc.com - Unknown [7100000000000000002].jpg"


def test_scan_media_library_uses_learned_tiktok_photo_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002",
        "7100000000000000002",
    )
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "fakeacc.com - [7100000000000000002]_1.jpg"
    media_file.write_bytes(b"image")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: set())
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7100000000000000002"]
    assert entry["source_key"] == "tiktok"
    assert entry["source_pending"] is False
    assert entry["resolved_filename"] == "fakeacc.com - Unknown [7100000000000000002].jpg"
    assert entry["resolved_full_path"] == str(media_file)
    assert entry["source_url"] == "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002"


def test_prune_disk_shadows_drops_disk_duplicate_of_real_download(monkeypatch):
    removed: list[str] = []
    monkeypatch.setattr(scan_module, "remove_history_records", removed.extend)
    records = {
        "ytdlp:abc": {"source_url": "https://www.tiktok.com/@a/video/7100000000000000004", "media_id": ""},
        "disk:7100000000000000004": {
            "engine": "disk",
            "media_id": "7100000000000000004",
            "source_url": "https://www.tiktok.com/@a/photo/7100000000000000004",
        },
    }
    _paths, real = scan_module._owned_media(records, disk=False)

    scan_module._prune_disk_shadows(records, real)

    assert removed == ["disk:7100000000000000004"]
    assert "disk:7100000000000000004" not in records


def test_infer_disk_source_vetoes_folder_then_uses_learned_guess(tmp_path: Path):
    folder = tmp_path / "yt"
    folder.mkdir()
    media_file = folder / "DEMO - DEMO - Sample clip [2000000000000000001].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index(
        [("youtube", "https://www.youtube.com/watch?v={id}", str(folder))]
    )

    source_key, pending, _, format_template = scan_module.infer_disk_source(
        media_file, "2000000000000000001", index, learned_youtube_twitter()
    )

    assert source_key == "twitter"
    assert pending is False
    # The folder's format is only a hint for its own source; a vetoed folder drops it.
    assert format_template == ""


def test_infer_disk_source_ambiguous_when_multiple_learned_match(tmp_path: Path):
    media_file = tmp_path / "Clip [1111111111111111111].mp4"
    media_file.write_bytes(b"video")
    learned = learn_download({}, "https://twitter.com/A/status/2000000000000000001", "2000000000000000001")
    learned = learn_download(learned, "https://www.tiktok.com/@a/video/7123456789012345678", "7123456789012345678")

    source_key, pending, candidates, _ = scan_module.infer_disk_source(
        media_file, "1111111111111111111", [], learned
    )

    assert source_key == ""
    assert pending is True
    assert set(candidates) == {"twitter", "tiktok"}


def test_infer_disk_source_prefers_configured_folder(tmp_path: Path):
    folder = tmp_path / "yt"
    folder.mkdir()
    media_file = folder / "Clip [YtDemoVid04].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index(
        [("youtube", "https://www.youtube.com/watch?v={id}", str(folder))]
    )

    source_key, pending, candidates, _ = scan_module.infer_disk_source(
        media_file, "YtDemoVid04", index, learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert pending is False
    assert candidates == []


def test_infer_disk_source_reports_the_folder_format(tmp_path: Path):
    shorts = tmp_path / "yt-shorts"
    shorts.mkdir()
    media_file = shorts / "Clip [YtDemoVid04].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index(
        [
            ("youtube", "https://www.youtube.com/watch?v={id}", str(tmp_path / "yt")),
            ("youtube", "https://www.youtube.com/shorts/{id}", str(shorts)),
        ]
    )

    source_key, _, _, format_template = scan_module.infer_disk_source(
        media_file, "YtDemoVid04", index, learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert format_template == "https://www.youtube.com/shorts/{id}"


def test_source_location_index_keeps_one_source_sharing_a_folder(tmp_path: Path):
    shared = str(tmp_path / "yt")
    index = scan_module._source_location_index(
        [
            ("youtube", "https://www.youtube.com/watch?v={id}", shared),
            ("youtube", "https://www.youtube.com/shorts/{id}", shared),
        ]
    )

    # One source, two formats: the source is still unambiguous, the format is not.
    assert [(key, fmt) for _, key, fmt in index] == [("youtube", "")]


def test_source_location_index_drops_a_folder_two_sources_share(tmp_path: Path):
    shared = str(tmp_path / "shared")
    index = scan_module._source_location_index(
        [
            ("youtube", "https://www.youtube.com/watch?v={id}", shared),
            ("twitter", "https://twitter.com/{creator}/status/{id}", shared),
        ]
    )

    assert index == []


def test_source_folder_keys_covers_every_format_folder(tmp_path: Path):
    watch = tmp_path / "yt"
    shorts = tmp_path / "yt-shorts"
    keys = scan_module._source_folder_keys(
        [
            ("youtube", "https://www.youtube.com/watch?v={id}", str(watch)),
            ("youtube", "https://www.youtube.com/shorts/{id}", str(shorts)),
        ]
    )

    assert keys == {scan_module._path_key(watch), scan_module._path_key(shorts)}


def test_scan_media_library_creator_from_filename_in_platform_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    platform_dir.mkdir(parents=True)
    media_file = platform_dir / "Cool Channel - Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: _scan_locations("youtube", platform_dir))
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Cool Channel"


def test_scan_media_library_prefers_the_format_owning_the_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    watch_dir = media_root / "youtube"
    shorts_dir = media_root / "youtube-shorts"
    watch_dir.mkdir(parents=True)
    shorts_dir.mkdir(parents=True)
    media_file = shorts_dir / "Cool Channel - Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    watch_format = "https://www.youtube.com/watch?v={id}"
    shorts_format = "https://www.youtube.com/shorts/{id}"
    # Both filename templates match this name; only the shorts one splits off the creator.
    per_source = {
        "youtube": {
            watch_format: {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
            shorts_format: {
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            },
        }
    }

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(
        scan_module,
        "_scan_location_rows",
        lambda: [
            ("youtube", watch_format, str(watch_dir)),
            ("youtube", shorts_format, str(shorts_dir)),
        ],
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, per_source),
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:abc123"]
    assert entry["source_key"] == "youtube"
    assert {
        "folder_template": entry["folder_template"],
        "filename_template": entry["filename_template"],
    } == per_source["youtube"][shorts_format]
    assert entry["title"] == "Soft Light"
    assert entry["creator"] == "Cool Channel"


def test_scan_media_library_creator_from_folder_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Cool Channel"
    creator_dir.mkdir(parents=True)
    media_file = creator_dir / "Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: _scan_locations("youtube", platform_dir))
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Cool Channel"
    assert saved["disk:abc123"]["title"] == "Soft Light"


def test_scan_media_library_creator_from_role_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    platform_dir = media_root / "rule34video"
    platform_dir.mkdir(parents=True)
    media_file = platform_dir / "Trace Artist - Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(
        scan_module,
        "_scan_location_rows",
        lambda: _scan_locations("rule34video", platform_dir, "https://rule34video.com/video/{id}"),
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: (
            {"folder_template": "", "filename_template": "{{artist}} - {{title}} [{{id}}]"},
            {
                "rule34video": {
                    "https://rule34video.com/video/{id}": {
                        "folder_template": "",
                        "filename_template": "{{artist}} - {{title}} [{{id}}]",
                    }
                }
            },
        ),
    )
    monkeypatch.setattr(scan_module, "_scan_token_role_map", lambda: {"rule34video": {"artist": "username"}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Trace Artist"
    assert saved["disk:abc123"]["title"] == "Soft Light"


def test_scan_media_library_creator_empty_when_no_creator_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    quality_dir = media_root / "1080p"
    quality_dir.mkdir(parents=True)
    media_file = quality_dir / "Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{quality}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == ""


def test_scan_media_library_flags_ambiguous_source_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "Clip [7123456789012345678].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7123456789012345678"]
    assert entry["source_key"] == ""
    assert entry["source_pending"] is True


def test_scan_media_library_reconstructs_link_from_learned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    learned = learn_download({}, "https://www.bilibili.com/video/BV1Ab4y1C7De", "BV1Ab4y1C7De")
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "Clip [BV1Ab4y1C7De].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:BV1Ab4y1C7De"]
    assert entry["source_key"] == "bilibili"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://www.bilibili.com/video/BV1Ab4y1C7De"


def test_scan_media_library_reconstructs_url_part_from_filename_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    learned = {
        "rule34video": {
            "templates": ["https://rule34video.com/video/{id}/cocolia-rand-sutekimeppou"],
        }
    }
    media_root = tmp_path / "media"
    platform_dir = media_root / "rule34video"
    platform_dir.mkdir(parents=True)
    media_file = platform_dir / "wsds - minus8_source [3238394].mp4"
    media_file.write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: {"rule34video"})
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: (
            {"folder_template": "", "filename_template": "{{slug}}_{{quality}} [{{id}}]"},
            {
                "rule34video": {
                    "https://rule34video.com/video/{id}/cocolia-rand-sutekimeppou": {
                        "folder_template": "",
                        "filename_template": "{{slug}}_{{quality}} [{{id}}]",
                    }
                }
            },
        ),
    )
    # The user mapped path segment 2 to a custom URL-part token named "slug"; capture + reconstruct.
    monkeypatch.setattr(
        scan_module,
        "_scan_slug_tokens_map",
        lambda: {"rule34video": [{"part": "path:2", "token": "slug"}]},
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:3238394"]
    assert entry["source_key"] == "rule34video"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://rule34video.com/video/3238394/wsds-minus8"


def test_scan_links_a_file_in_the_format_its_name_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    (media_root / "photo Pic [7123456789].jpg").write_bytes(b"image")
    saved: dict[str, dict] = {}
    _patch_two_format_scan(monkeypatch, saved)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7123456789"]
    assert entry["source_url"] == "https://example.test/photo/7123456789"
    assert entry["subfolder_template"] == "post {{id}}"


def test_scan_replaces_a_link_in_another_format_than_the_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    path = media_root / "photo Pic [7123456789].jpg"
    path.write_bytes(b"image")
    prior = {
        "engine": "disk",
        "source_key": "example",
        "source_url": "https://example.test/video/7123456789",
        "creator": "Creator",
        "title": "Pic",
        "media_id": "7123456789",
        "resolved_full_path": str(path),
        "resolved_folder": str(path.parent),
        "resolved_filename": path.name,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    saved: dict[str, dict] = {}
    _patch_two_format_scan(monkeypatch, saved, {"disk:7123456789": prior})

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7123456789"]
    assert entry["creator"] == "Creator"
    assert entry["source_url"] == "https://example.test/photo/7123456789"


def test_scan_builds_a_creator_link_from_the_folder_without_a_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_id = "7100000000000000005"
    learned = learn_download(
        {},
        f"https://www.tiktok.com/@demo0n/video/{media_id}",
        media_id,
        {"uploader": "demo0n"},
    )
    media_root = tmp_path / "media"
    platform_dir = media_root / "tiktok"
    creator_dir = platform_dir / "demo0n"
    creator_dir.mkdir(parents=True)
    (creator_dir / f"Soft Light [{media_id}].mp4").write_bytes(b"video")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(
        scan_module,
        "_scan_location_rows",
        lambda: _scan_locations("tiktok", platform_dir, "https://www.tiktok.com/@{creator}/video/{id}"),
    )
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: {"tiktok"})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    def no_lookup(*args, **kwargs):
        raise AssertionError("a refresh must not look anything up")

    monkeypatch.setattr(probe_module, "probe_metadata", no_lookup)

    scan_module.scan_media_library([media_root])

    assert saved[f"disk:{media_id}"]["creator"] == "demo0n"
    assert saved[f"disk:{media_id}"]["source_url"] == f"https://www.tiktok.com/@demo0n/video/{media_id}"


def test_scan_keeps_a_resolved_creator_and_link(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Some Channel"
    creator_dir.mkdir(parents=True)
    media_file = creator_dir / "Soft Light [abc123].mp4"
    media_file.write_bytes(b"video")

    prior = {
        "media_id": "abc123",
        "engine": "disk",
        "creator": "AlreadyResolved",
        "source_url": "https://www.youtube.com/watch?v=abc123",
        "resolved_full_path": str(media_file),
    }
    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {"disk:abc123": prior}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: _scan_locations("youtube", platform_dir))
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "AlreadyResolved"
    assert saved["disk:abc123"]["source_url"] == "https://www.youtube.com/watch?v=abc123"


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
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed_tasks.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_records", removed_history.extend)

    result = scan_module.scan_media_library([tmp_path])

    assert result == {
        "checked": 1,
        "missing": 1,
        "added": 0,
        "unchanged": 0,
        "needs_resolve": 0,
    }
    assert removed_tasks == ["task-1"]
    assert removed_history == ["task-1"]


def test_scan_media_library_unreadable_subtree_keeps_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    locked = media_root / "locked"
    locked.mkdir(parents=True)
    media_file = locked / "Creator - Clip [abc123].mp4"
    media_file.write_bytes(b"video")
    removed: list[str] = []
    real_scandir = scan_module.os.scandir

    def flaky_scandir(folder):
        if scan_module._path_key(folder) == scan_module._path_key(locked):
            raise OSError("blocked")
        return real_scandir(folder)

    monkeypatch.setattr(scan_module.os, "scandir", flaky_scandir)
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(
        scan_module,
        "load_history",
        lambda: {
            "entries": {
                "disk:abc123": {
                    "engine": "disk",
                    "media_id": "abc123",
                    "resolved_full_path": str(media_file),
                }
            }
        },
    )
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_records", removed.extend)

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "needs_resolve": 1,
    }
    assert removed == []


def test_scan_media_library_keeps_non_media_history_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    document = tmp_path / "notes.txt"
    document.write_text("keep me", encoding="utf-8")
    removed: list[str] = []
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(
        scan_module,
        "load_history",
        lambda: {
            "entries": {
                "disk:notes": {
                    "engine": "disk",
                    "media_id": "notes",
                    "resolved_full_path": str(document),
                }
            }
        },
    )
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_records", removed.extend)

    result = scan_module.scan_media_library([tmp_path])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "needs_resolve": 0,
    }
    assert removed == []


def test_scan_media_library_keeps_history_file_outside_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    outside = tmp_path / "outside [abc123].mp4"
    outside.write_bytes(b"video")
    removed: list[str] = []
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(
        scan_module,
        "load_history",
        lambda: {
            "entries": {
                "disk:abc123": {
                    "engine": "disk",
                    "media_id": "abc123",
                    "resolved_full_path": str(outside),
                }
            }
        },
    )
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_records", removed.extend)

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "needs_resolve": 1,
    }
    assert removed == []


def test_scan_media_library_healthy_scan_skips_fallback_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_file = tmp_path / "Creator - Clip [abc123].mp4"
    media_file.write_bytes(b"video")
    fallback_checks = 0

    def counting_exists(path):
        nonlocal fallback_checks
        fallback_checks += 1
        return True

    monkeypatch.setattr(scan_module, "_path_exists", counting_exists)
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(
        scan_module,
        "load_history",
        lambda: {
            "entries": {
                "disk:abc123": {
                    "engine": "disk",
                    "media_id": "abc123",
                    "resolved_full_path": str(media_file),
                }
            }
        },
    )
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    result = scan_module.scan_media_library([tmp_path])

    assert result["checked"] == 1
    assert result["missing"] == 0
    assert fallback_checks == 0


def test_scan_media_library_recovers_stale_completed_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "Creator - Clip [abc123].mp4"
    media_file.write_bytes(b"video")
    stale_file = tmp_path / "missing [abc123].mp4"
    persisted: dict[str, str] = {}
    removed: list[str] = []
    task = {
        "status": "completed",
        "media_id": "abc123",
        "resolved_full_path": str(stale_file),
        "last_log_lines": [f'[download] Destination: "{media_file}"'],
    }
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {"task-1": dict(task)}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_records", removed.extend)
    monkeypatch.setattr(files_module, "update_task", lambda task_id, **updates: persisted.update(updates))

    result = scan_module.scan_media_library([media_root])

    assert result["checked"] == 1
    assert result["missing"] == 0
    assert persisted == {
        "resolved_full_path": str(media_file),
        "resolved_folder": str(media_root),
        "resolved_filename": media_file.name,
    }
    assert removed == []


def test_scan_batches_history_writes_instead_of_one_commit_per_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    media_root.mkdir()
    for index in range(5):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")

    batches: list[int] = []
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: batches.append(len(rows)))
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    result = scan_module.scan_media_library([media_root])

    assert result["added"] == 5
    # Every row lands, but as one batch rather than five separate commits.
    assert sum(batches) == 5
    assert len([size for size in batches if size]) == 1


def test_scan_skips_non_media_files_without_touching_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    media_root.mkdir()
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")
    (media_root / "notes.txt").write_text("ignore me", encoding="utf-8")
    (media_root / "Creator - Clip [vid1].mp4.json").write_text("{}", encoding="utf-8")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: saved.update(dict(rows)))
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_records", lambda task_ids: None)

    result = scan_module.scan_media_library([media_root])

    assert result["added"] == 1
    assert list(saved) == ["disk:vid1"]


def test_scan_runs_one_at_a_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Two callers hitting /library/scan must not walk the same tree concurrently.
    import threading

    media_root = tmp_path / "media"
    media_root.mkdir()
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")

    overlap = {"max": 0, "current": 0}
    guard = threading.Lock()

    def counting_walk(roots):
        with guard:
            overlap["current"] += 1
            overlap["max"] = max(overlap["max"], overlap["current"])
        time.sleep(0.05)
        try:
            yield from ()
        finally:
            with guard:
                overlap["current"] -= 1

    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "_iter_media_files", counting_walk)

    threads = [threading.Thread(target=scan_module.scan_media_library, args=([media_root],)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert overlap["max"] == 1


def test_rescan_skips_files_that_did_not_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, resolved, _written, _learned = _incremental_scan_env(tmp_path, monkeypatch)
    for index in range(4):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")

    first = scan_module.scan_media_library([media_root])
    resolved.clear()
    second = scan_module.scan_media_library([media_root])

    assert first["added"] == 4
    assert first["unchanged"] == 0
    assert second == {
        "checked": 4,
        "missing": 0,
        "added": 0,
        "unchanged": 4,
        "needs_resolve": 0,
    }
    assert resolved == []
    assert len(rows) == 4


def test_rescan_reresolves_a_file_whose_contents_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, _rows, resolved, _written, _learned = _incremental_scan_env(tmp_path, monkeypatch)
    stable = media_root / "Creator - Clip [vid1].mp4"
    edited = media_root / "Creator - Clip [vid2].mp4"
    stable.write_bytes(b"video")
    edited.write_bytes(b"video")

    scan_media = scan_module.scan_media_library
    scan_media([media_root])
    resolved.clear()
    edited.write_bytes(b"a longer video file")
    result = scan_media([media_root])

    assert resolved == [str(edited)]
    assert result["unchanged"] == 1


def test_learning_a_format_leaves_settled_rows_alone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A download teaching a new format used to rebuild every disk row on the next refresh.
    media_root, _rows, resolved, written, learned = _incremental_scan_env(tmp_path, monkeypatch)
    for index in range(3):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")

    scan_module.scan_media_library([media_root])
    resolved.clear()
    written.clear()
    learned["other"] = {"templates": ["https://other.test/p/{id}"]}
    result = scan_module.scan_media_library([media_root])

    assert resolved == []
    assert written == []
    assert result["unchanged"] == 3


def test_a_row_without_a_link_gets_one_once_its_format_is_learned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root, rows, _resolved, written, learned = _incremental_scan_env(tmp_path, monkeypatch)
    learned.clear()
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")

    scan_module.scan_media_library([media_root])
    assert rows["disk:vid1"]["source_url"] == ""
    written.clear()
    # Derived again to the same answer, so nothing is written.
    assert scan_module.scan_media_library([media_root])["unchanged"] == 1
    assert written == []

    learned["example"] = {"templates": ["https://example.test/v/{id}"]}
    scan_module.scan_media_library([media_root])

    assert rows["disk:vid1"]["source_url"] == "https://example.test/v/vid1"


def test_a_scan_learns_no_format_from_past_downloads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A download teaches its format when it succeeds; a scan only reads formats, so one the
    # user deleted stays deleted.
    media_root, _rows, _resolved, _written, _learned = _incremental_scan_env(tmp_path, monkeypatch)
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")
    history = {
        "gallerydl:1": {"source_url": "https://example.test/@creator/video/1", "media_id": "1"},
    }
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(history)})
    monkeypatch.setattr(scan_module, "_drop_missing_records", lambda records, seen_paths, pacer=None: (0, 0))

    scan_module.scan_media_library([media_root])

    assert repositories.load_learned_formats_payload() == {}


def test_a_stopped_scan_keeps_the_rows_it_derived(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, _resolved, _written, _learned = _incremental_scan_env(tmp_path, monkeypatch)
    for index in range(3):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")
    flagged: list[list[str]] = []
    monkeypatch.setattr(scan_module, "sync_history_resolve_flags", flagged.append)
    parse = scan_module._parse_media_fields

    def stop_on_first_file(path, pattern):
        scan_module.stop_scan()
        return parse(path, pattern)

    assert scan_module.stop_scan() is False
    monkeypatch.setattr(scan_module, "_parse_media_fields", stop_on_first_file)
    result = scan_module.scan_media_library([media_root])

    # Asked mid-file, so that file is saved before the scan stops at the next one.
    assert result["stopped"] == 1
    assert result["added"] == 1
    assert len(rows) == 1
    assert flagged == []
    assert scan_module.scan_in_progress() is False

    monkeypatch.setattr(scan_module, "_parse_media_fields", parse)
    result = scan_module.scan_media_library([media_root])

    # The stop was for that scan alone.
    assert "stopped" not in result
    assert len(rows) == 3


def test_a_scan_reads_the_creator_above_a_subfolder():
    root = Path("/media/instagram")
    path = root / "NASA" / "ABC123" / "Cool Rocket [ABC123]_1.jpg"
    compiled = scan_module._CompiledTemplates(
        scan_module.compile_template("{{username}}"),
        scan_module.compile_template("{{id}}"),
        scan_module.compile_template("{{title}} [{{id}}]"),
    )

    assert scan_module._creator_for_file(root, path, set(), compiled) == "NASA"
    # Without a subfolder template the extra segment is not a subfolder, so it is not trimmed.
    flat = scan_module._CompiledTemplates(compiled.folder, None, compiled.filename)
    assert scan_module._creator_for_file(root, path, set(), flat) == ""
