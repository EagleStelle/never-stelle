from __future__ import annotations

import json
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from yt_dlp import YoutubeDL

import backend.app.domains.downloads.files as files_module
import backend.app.domains.downloads.gallerydl as gallerydl_module
import backend.app.domains.downloads.history as history_module
import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.postprocessing as postprocessing_module
import backend.app.domains.downloads.scan as scan_module
import backend.app.domains.downloads.serializers as serializers_module
import backend.app.domains.downloads.urls as urls_module
import backend.app.domains.downloads.workers.completion as completion_module
import backend.app.domains.downloads.workers.completion_finalization as completion_finalization_module
import backend.app.domains.downloads.workers.completion_learning as completion_learning_module
import backend.app.domains.downloads.workers.completion_metadata as completion_metadata_module
import backend.app.domains.downloads.workers.completion_outputs as completion_outputs_module
import backend.app.domains.downloads.workers.enrichment as enrichment_module
import backend.app.domains.downloads.workers.execution as worker_module
import backend.app.domains.downloads.workers.runner as runner_module
from backend.app.core.paths import path_key
from backend.app.core.sources import source_label_from_key
from backend.app.domains.downloads import (
    canonicalize_source_url,
    convert_template_to_ytdlp,
    count_tasks,
    counts_by_menu,
    detect_source_key,
    engine_by_name,
    extract_downloaded_path,
    is_media_file,
    parse_filename_media_id,
)
from backend.app.domains.downloads import store as store_module
from backend.app.domains.downloads.formats import (
    conflicts_with_source,
    creator_from_url,
    describe_learned_segments,
    extract_url_part,
    guess_sources,
    learn_download,
    learn_media_id,
    media_id_from_url,
    reconstruct_url,
    reconstruct_url_candidates,
    url_dedup_key,
)
from backend.app.domains.downloads.naming import (
    clean_template_filename,
    filename_template_title,
    sanitize_filename_component,
    sanitize_path_literal,
    strip_numbered_suffix,
    strip_placeholder_title,
)
from backend.app.domains.downloads.serializers import history_to_api, task_to_api
from backend.app.domains.downloads.workers.completion_finalization import FinalizedCompletionOutput
from backend.app.domains.downloads.ytdlp import (
    YTDLP_NICKNAME_FIELD,
    YTDLP_USERNAME_FIELD,
    clean_filename_title,
    clean_social_title,
)
from tests.support import use_temp_db
from yt_dlp_plugins.postprocessor.never_stelle_capture import NeverStelleCapturePP


def _patch_worker_task_store(monkeypatch: pytest.MonkeyPatch, store: dict, update_task):
    def load_task(task_id: str):
        return (store.get("tasks") or {}).get(task_id, {})

    monkeypatch.setattr(worker_module, "load_task", load_task)
    monkeypatch.setattr(worker_module, "update_task", update_task)
    monkeypatch.setattr(runner_module, "load_task", load_task)
    monkeypatch.setattr(runner_module, "update_task", update_task)


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


def _patch_head(monkeypatch, final_url=None, exc=None):
    def fake_head(url, **kwargs):
        if exc is not None:
            raise exc
        return type("Resp", (), {"url": final_url if final_url is not None else url})()

    monkeypatch.setattr(urls_module.httpx, "head", fake_head)


def test_resolve_redirect_expands_share_link(monkeypatch):
    _patch_head(monkeypatch, "https://www.facebook.com/charechii/posts/pfbid02xDjH4VegXXU7epxAJJVz6vJnaRnWScxmggX1iSB4GhoBexQB926QQg9NQdAvByPEl")
    resolved = urls_module.resolve_redirect_url("https://www.facebook.com/share/p/17tZcAG16f/")
    assert resolved.endswith("pfbid02xDjH4VegXXU7epxAJJVz6vJnaRnWScxmggX1iSB4GhoBexQB926QQg9NQdAvByPEl")


def test_resolve_redirect_keeps_original_when_target_loses_id(monkeypatch):
    _patch_head(monkeypatch, "https://www.instagram.com/accounts/login/")
    original = "https://www.instagram.com/reel/DWyrvI9Ef3z/"
    assert urls_module.resolve_redirect_url(original) == original


def test_resolve_redirect_survives_network_error(monkeypatch):
    _patch_head(monkeypatch, exc=RuntimeError("boom"))
    original = "https://www.facebook.com/share/p/17tZcAG16f/"
    assert urls_module.resolve_redirect_url(original) == original


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
        ("not a url", ""),
    ],
)
def test_detect_source_key(url, expected):
    assert detect_source_key(url) == expected


def test_unresolved_source_label_is_unresolved():
    assert source_label_from_key("") == "Unresolved"
    assert source_label_from_key("others") == "Unresolved"


def test_convert_template_to_ytdlp_maps_placeholders():
    result = convert_template_to_ytdlp("{{username}} - {{title}} [{{id}}]")
    assert "%(title|Unknown)s" in result
    assert "%(id|NA)s" in result
    assert "{{" not in result


def test_convert_template_unknown_placeholder_falls_back():
    assert convert_template_to_ytdlp("{{weird}}") == "%(weird|Unknown)s"


def test_convert_template_username_uses_metadata_field_even_when_url_has_handle():
    result = convert_template_to_ytdlp(
        "{{username}} - {{title}} [{{id}}]",
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
    )

    assert result.startswith(f"{YTDLP_USERNAME_FIELD} - ")
    assert "fzyahoo.com" not in result


def test_convert_template_can_keep_explicit_creator_at_sign():
    url = "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489"

    ytdlp_result = convert_template_to_ytdlp(
        "{{username}}",
        url,
        extra_tokens={"username": "@fzyahoo.com"},
        cleaning={"strip_handle_at": False},
    )
    gallerydl_result = gallerydl_module.convert_template_to_gallerydl(
        "{{username}}",
        url,
        extra_tokens={"username": "@fzyahoo.com"},
        cleaning={"strip_handle_at": False},
    )

    assert ytdlp_result == "@fzyahoo.com"
    assert gallerydl_result == "@fzyahoo.com"


def test_convert_template_username_without_url_handle_uses_handle_field():
    # With no handle in the URL, fall back to the handle-first metadata field.
    result = convert_template_to_ytdlp(
        "{{username}} - {{title}} [{{id}}]",
        "https://video.example/channel/UC-wNqHVYS82PF4mkaQb0Alg",
    )

    assert result.startswith(YTDLP_USERNAME_FIELD)
    assert "UC-wNqHVYS82PF4mkaQb0Alg" not in result


def test_convert_template_nickname_uses_display_name_field():
    result = convert_template_to_ytdlp(
        "{{nickname}}",
        "https://video.example/channel/UC-wNqHVYS82PF4mkaQb0Alg",
    )

    assert result == YTDLP_NICKNAME_FIELD


def test_convert_template_empty():
    assert convert_template_to_ytdlp("") == ""
    assert convert_template_to_ytdlp("   ") == ""


@pytest.mark.parametrize(
    "line,expected",
    [
        ("[download] Destination: /media/a.mp4", "/media/a.mp4"),
        ("[ExtractAudio] Destination: /media/a.opus", "/media/a.opus"),
        ("[ExtractAudio] Destination: /media/a.wav", "/media/a.wav"),
        ("[VideoConvertor] Converting video from webm to mkv; Destination: /media/a.mkv", "/media/a.mkv"),
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
    audio = tmp_path / "clip.mp3"
    audio.write_bytes(b"x")
    bad = tmp_path / "notes.txt"
    bad.write_bytes(b"x")
    assert is_media_file(good) is True
    assert is_media_file(audio) is True
    assert is_media_file(bad) is False
    assert is_media_file(tmp_path / "missing.mp4") is False


def test_is_media_file_handles_oserror(monkeypatch: pytest.MonkeyPatch):
    def fail_is_file(_path):
        raise OSError("blocked")

    monkeypatch.setattr(Path, "is_file", fail_is_file)

    assert is_media_file(Path("clip.mp4")) is False


def test_parse_filename_media_id_uses_last_bracketed_id():
    assert parse_filename_media_id("Creator - Soft Light [Abc_123-xy].mp4") == (
        "Abc_123-xy",
        "Creator - Soft Light",
    )


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
        "mode": "video",
        "video_quality": "1080p",
        "video_container": "mp4",
        "video_codec": "auto",
        "video_audio_codec": "auto",
        "audio_format": "mp3",
        "audio_bitrate": "best",
    }
    monkeypatch.setattr(
        operations_module, "get_effective_saved_settings", lambda cfg: {"default_quality": saved_default}
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
        "video_audio_codec": "auto",
        "audio_format": "opus",
        "audio_bitrate": "320",
    }
    assert captured["task"]["post_processing"] == {
        "metadata": False,
        "subtitles": False,
        "automatic_subtitles": False,
        "chapters": False,
        "thumbnail": False,
        "save_as": "sidecar",
    }

    operations_module.queue_task(
        "https://example.test/watch?v=metadata",
        quality={
            "mode": "video",
            "_post_processing": {"metadata": True, "save_as": "embed"},
        },
    )
    assert captured["task"]["post_processing"] == {
        "metadata": True,
        "subtitles": False,
        "automatic_subtitles": False,
        "chapters": False,
        "thumbnail": False,
        "save_as": "embed",
    }

    operations_module.queue_task("https://example.test/watch?v=2", quality=None)
    assert captured["task"]["quality"] == saved_default

    operations_module.queue_task(
        "https://example.test/watch?v=3",
        quality={"mode": "video", "video_container": "webm", "video_codec": "vp9"},
    )
    assert captured["task"]["quality"]["video_container"] == "webm"
    assert captured["task"]["quality"]["video_codec"] == "vp9"


def test_extractor_metadata_sidecars_are_discovered_in_task_scratch(tmp_path: Path):
    output_root = tmp_path / "media"
    raw = output_root / "creator" / "raw.mp4"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"media")
    extractor_root = tmp_path / "scratch" / "task" / "extractor"
    nested = extractor_root / "creator"
    nested.mkdir(parents=True)
    sidecar = nested / "raw.info.json"
    sidecar.write_text('{"title":"Raw title"}', encoding="utf-8")

    assert worker_module._metadata_sidecars_for(
        raw,
        scratch_root=extractor_root,
        output_root=output_root,
    ) == [sidecar]


def test_delegated_ytdlp_payload_capture_preserves_subtitles_for_gallerydl(tmp_path: Path):
    media = tmp_path / "media" / "clip.mkv"
    media.parent.mkdir()
    media.write_bytes(b"media")
    extractor_root = tmp_path / "extractor"

    processor = NeverStelleCapturePP(YoutubeDL({"quiet": True}), str(extractor_root))
    _, captured = processor.run(
        {
            "filepath": str(media),
            "subtitles": {"en": [{"ext": "vtt", "url": "https://example.test/en.vtt"}]},
            "automatic_captions": {
                "ja-orig": [{"ext": "vtt", "url": "https://example.test/ja.vtt"}]
            },
        }
    )

    sidecars = postprocessing_module.metadata_sidecars_for(media, scratch_root=extractor_root)
    assert len(sidecars) == 1
    assert postprocessing_module.extractor_payload_from_sidecars(sidecars) == captured


def test_user_metadata_sidecar_uses_the_final_settings_pipeline_values(tmp_path: Path):
    raw_folder = tmp_path / "@raw.creator"
    raw_folder.mkdir()
    raw = raw_folder / "raw.mp4"
    gallery_sidecar = Path(f"{raw}.json")
    ytdlp_sidecar = raw.with_suffix(".info.json")
    gallery_sidecar.write_text(
        '{"private":"kept","title":"Raw title","upload_date":"20260801"}',
        encoding="utf-8",
    )
    ytdlp_sidecar.write_text('{"comments":[{"text":"kept too"}]}', encoding="utf-8")
    sidecars = worker_module._metadata_sidecars_for(raw)

    final = tmp_path / "Creator" / "Creator - Title [abc].mp4"
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=final,
        display_filename=final.name,
        title="Title",
        keep_paths=[final],
    )
    postprocessing_module.apply_metadata_post_processing(
        [final],
        {"description": "Description"},
        finalized,
        save_as="sidecar",
        sidecars=sidecars,
        output_root=tmp_path,
    )

    output = final.with_name(f"{final.name}.json")
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload == {
        "title": "Title",
        "artist": "Creator",
        "album_artist": "Creator",
        "date": "2026-08-01",
        "description": "Description",
        "comment": "https://example.test/post/abc",
        "source": "https://example.test/post/abc",
        "identifier": "abc",
        "publisher": "example",
    }
    assert not gallery_sidecar.exists()
    assert not ytdlp_sidecar.exists()
    assert not raw_folder.exists()


def test_embedded_metadata_uses_the_final_settings_pipeline_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"original")
    raw_sidecar = Path(f"{media}.json")
    raw_sidecar.write_text(
        '{"description":"Full extractor value","timestamp":1785542400}',
        encoding="utf-8",
    )
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[-1]).write_bytes(b"embedded")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module.subprocess, "run", fake_run)

    postprocessing_module.apply_metadata_post_processing(
        [media],
        {},
        finalized,
        save_as="embed",
        sidecars=[raw_sidecar],
    )

    assert media.read_bytes() == b"embedded"
    assert captured["cmd"].count("-i") == 1
    assert "title=Title" in captured["cmd"]
    assert "artist=Creator" in captured["cmd"]
    assert "album_artist=Creator" in captured["cmd"]
    assert "date=2026-08-01" in captured["cmd"]
    assert "description=Full extractor value" in captured["cmd"]
    assert "comment=https://example.test/post/abc" in captured["cmd"]
    assert "source=https://example.test/post/abc" in captured["cmd"]
    assert "identifier=abc" in captured["cmd"]
    assert "publisher=example" in captured["cmd"]
    assert not raw_sidecar.exists()


def test_metadata_embed_discards_source_metadata_and_omits_empty_title(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.webm"
    media.write_bytes(b"media")
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[-1]).write_bytes(b"remuxed")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", fake_run)

    assert postprocessing_module._embed_metadata(media, {"artist": "Creator"})
    metadata_index = captured["cmd"].index("-map_metadata")
    assert captured["cmd"][metadata_index + 1] == "-1"
    assert not any(value.startswith("title=") for value in captured["cmd"])
    assert "artist=Creator" in captured["cmd"]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"release_date": "2026"}, "2026"),
        ({"release_date": "2026-08"}, "2026-08"),
        ({"release_date": "2026-08-01"}, "2026-08-01"),
        ({"release_date": "20260801"}, "2026-08-01"),
        ({"release_date": "2026-08-01T19:42:31+08:00"}, "2026-08-01"),
        ({"upload_date": "2026-08-01 19:42:31"}, "2026-08-01"),
        ({"timestamp": 1785542400}, "2026-08-01"),
        ({"timestamp": 1785542400000}, "2026-08-01"),
    ],
)
def test_metadata_date_preserves_calendar_precision_without_time(
    payload: dict[str, object], expected: str
):
    assert postprocessing_module._metadata_date(payload) == expected


def test_song_metadata_uses_real_track_numbers_and_portable_music_fields(tmp_path: Path):
    media = tmp_path / "song.mp3"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=Yb9FzUPpk0Y",
        source_key="youtube",
        creator="Uploader channel",
        media_id="Yb9FzUPpk0Y",
        final_path=media,
        display_filename=media.name,
        title="Video title",
        keep_paths=[media],
    )
    payload = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={
            "track": "Actual song title",
            "track_number": 3,
            "track_count": 12,
            "playlist_index": 99,
            "disc_number": "2",
            "disc_count": 2,
            "artists": ["Track Artist", "Guest Artist"],
            "album_artists": ["Album Artist"],
            "composers": ["Composer One", "Composer Two"],
            "performers": ["Orchestra"],
            "genres": ["Rock", "Pop"],
            "album": "Album",
        },
    )

    assert payload["title"] == "Actual song title"
    assert payload["artist"] == "Track Artist, Guest Artist"
    assert payload["album_artist"] == "Album Artist"
    assert payload["composer"] == "Composer One, Composer Two"
    assert payload["performer"] == "Orchestra"
    assert payload["genre"] == "Rock, Pop"
    assert payload["track"] == "3/12"
    assert payload["disc"] == "2/2"


def test_song_title_and_playlist_position_are_never_used_as_track_number(tmp_path: Path):
    media = tmp_path / "song.m4a"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=x",
        source_key="youtube",
        creator="Artist",
        media_id="x",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )

    payload = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={"track": "Song title", "playlist_index": 7},
    )

    assert "track" not in payload


def test_metadata_title_rejects_carousel_position_and_synthetic_delegation_url(
    tmp_path: Path,
):
    media = tmp_path / "post.webm"
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="",
        keep_paths=[media],
    )

    positional = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={"title": "20", "num": 20, "count": 21},
    )
    delegated = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={
            "title": "20",
            "original_url": "https://example.test/post/abc/20.mp4",
        },
    )
    legitimate = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={"title": "20", "original_url": "https://example.test/post/abc"},
    )
    empty = postprocessing_module.finalized_metadata_payload(
        {},
        finalized,
        extractor_payload={"title": "None"},
    )

    assert "title" not in positional
    assert "title" not in delegated
    assert legitimate["title"] == "20"
    assert "title" not in empty


def test_youtube_generated_song_description_supplies_portable_credits(tmp_path: Path):
    media = tmp_path / "song.flac"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=x",
        source_key="youtube",
        creator="Artist",
        media_id="x",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )
    description = (
        "Provided to YouTube by Distributor\n\n"
        "Title · Artist\n\nAlbum\n\n℗ 2020 Label\n\n"
        "Composer: Composer Name\nWriter: Writer Name\n"
        "Associated Performer: Orchestra\n\nAuto-generated by YouTube."
    )

    payload = postprocessing_module.finalized_metadata_payload(
        {}, finalized, extractor_payload={"description": description}
    )

    assert payload["composer"] == "Composer Name, Writer Name"
    assert payload["performer"] == "Orchestra"
    assert payload["copyright"] == "℗ 2020 Label"


def test_music_cover_art_precedes_scraped_thumbnail_and_has_a_fallback():
    cover = "https://yt3.googleusercontent.com/music-cover=w544-h544-l90-rj"
    scraped = "https://i.ytimg.com/vi/Yb9FzUPpk0Y/maxresdefault.jpg"
    payload = {
        "track": "Song",
        "album": "Album",
        "thumbnail": scraped,
        "thumbnails": [
            {"url": cover, "width": 544, "height": 544, "preference": -39},
            {"url": scraped, "width": 1920, "height": 1080, "preference": -1},
        ],
    }

    assert postprocessing_module._thumbnail_url(payload, prefer_cover_art=True)[0] == cover
    assert postprocessing_module._thumbnail_url(payload)[0] == scraped


def test_thumbnail_sidecar_uses_the_final_media_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"media")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text('{"thumbnail":"https://cdn.example.test/cover.webp"}', encoding="utf-8")

    monkeypatch.setattr(
        postprocessing_module,
        "_download_thumbnail",
        lambda payload, **_: (b"thumbnail-bytes", ".webp"),
    )
    postprocessing_module.apply_thumbnail_post_processing(
        [media],
        {},
        save_as="sidecar",
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert media.with_suffix(".webp").read_bytes() == b"thumbnail-bytes"
    assert not extractor_sidecar.exists()


def test_thumbnail_sidecar_never_overwrites_an_image_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media = tmp_path / "Creator - Image [abc].webp"
    media.write_bytes(b"original-image")
    monkeypatch.setattr(
        postprocessing_module,
        "_download_thumbnail",
        lambda payload, **_: (b"thumbnail-bytes", ".webp"),
    )

    postprocessing_module.apply_thumbnail_post_processing([media], {}, save_as="sidecar")

    assert media.read_bytes() == b"original-image"
    assert media.with_name(f"{media.stem}.thumbnail.webp").read_bytes() == b"thumbnail-bytes"


def test_manual_and_auto_subtitle_sidecars_are_separate_and_use_final_name(tmp_path: Path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "subtitles": {"en": [{"ext": "vtt", "data": "WEBVTT\n\nmanual"}]},
                "automatic_captions": {
                    "en": [{"ext": "vtt", "data": "WEBVTT\n\nautomatic"}]
                },
            }
        ),
        encoding="utf-8",
    )

    postprocessing_module.apply_subtitle_post_processing(
        [media],
        {},
        manual=True,
        automatic=True,
        save_as="sidecar",
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert media.with_name(f"{media.stem}.en.vtt").read_text(encoding="utf-8").endswith("manual")
    assert media.with_name(f"{media.stem}.en.auto.vtt").read_text(encoding="utf-8").endswith("automatic")
    assert not extractor_sidecar.exists()


def test_subtitles_collect_every_manual_and_auto_caption_language():
    payload = {
        "language": "ja",
        "subtitles": {
            "en": [{"ext": "vtt", "data": "WEBVTT\n\nEnglish"}],
            "ja": [{"ext": "vtt", "data": "WEBVTT\n\nJapanese"}],
            "fr": [{"ext": "vtt", "data": "WEBVTT\n\nFrench"}],
        },
        "automatic_captions": {
            "en": [{"ext": "vtt", "data": "WEBVTT\n\nTranslated"}],
            "ja-orig": [{"ext": "vtt", "data": "WEBVTT\n\nOriginal ASR"}],
            "fr": [{"ext": "vtt", "data": "WEBVTT\n\nFrench auto"}],
            "de": [{"ext": "vtt", "data": "WEBVTT\n\nGerman auto"}],
            "es": [{"ext": "vtt", "data": "WEBVTT\n\nSpanish auto"}],
            "zh-Hans": [{"ext": "vtt", "data": "WEBVTT\n\nChinese auto"}],
        },
    }

    tracks = postprocessing_module.prepare_subtitle_post_processing(
        {},
        manual=True,
        automatic=True,
        extractor_payload=payload,
    )

    assert [(track["language"], track["automatic"]) for track in tracks] == [
        ("ja", False),
        ("en", False),
        ("fr", False),
        ("ja-orig", True),
        ("en", True),
        ("fr", True),
        ("de", True),
        ("es", True),
        ("zh-Hans", True),
    ]
    assert tracks[0]["data"].endswith(b"Japanese")
    assert tracks[3]["data"].endswith(b"Original ASR")


def test_chapter_sidecar_uses_final_name_and_normalizes_boundaries(tmp_path: Path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "duration": 30,
                "chapters": [
                    {"start_time": 0, "end_time": 5.5, "title": "Intro"},
                    {"start_time": 5.5, "title": "Main"},
                    {"start_time": 20, "title": ""},
                ],
            }
        ),
        encoding="utf-8",
    )

    postprocessing_module.apply_chapter_post_processing(
        [media],
        {},
        save_as="sidecar",
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    sidecar = media.with_name(f"{media.stem}.chapters.json")
    assert json.loads(sidecar.read_text(encoding="utf-8")) == {
        "chapters": [
            {"start_time": 0.0, "end_time": 5.5, "title": "Intro"},
            {"start_time": 5.5, "end_time": 20.0, "title": "Main"},
            {"start_time": 20.0, "end_time": 30.0, "title": "Chapter 3"},
        ]
    }
    assert not extractor_sidecar.exists()


def test_chapters_embed_from_the_same_normalized_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    captured: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        chapter_input = Path(cmd[cmd.index("ffmetadata") + 2])
        captured["chapters"] = chapter_input.read_text(encoding="utf-8")
        Path(cmd[-1]).write_bytes(b"embedded")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module.subprocess, "run", fake_run)

    postprocessing_module.apply_chapter_post_processing(
        [media],
        {},
        save_as="embed",
        prepared_chapters=[
            {"start_time": 0.0, "end_time": 12.345, "title": "Intro; #1 = ready"}
        ],
    )

    assert media.read_bytes() == b"embedded"
    cmd = captured["cmd"]
    assert cmd[cmd.index("-map_chapters") + 1] == "1"
    assert cmd.count("-i") == 2
    assert "START=0" in captured["chapters"]
    assert "END=12345" in captured["chapters"]
    assert r"title=Intro\; \#1 \= ready" in captured["chapters"]


def test_finalized_post_processing_reads_extractor_payload_once_for_all_sidecars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "title": "Extractor title",
                "thumbnail": "https://cdn.example.test/cover.jpg",
                "subtitles": {"en": [{"ext": "vtt", "data": "WEBVTT\n\nmanual"}]},
                "automatic_captions": {
                    "en": [{"ext": "vtt", "data": "WEBVTT\n\nautomatic"}]
                },
                "chapters": [{"start_time": 0, "end_time": 10, "title": "Intro"}],
            }
        ),
        encoding="utf-8",
    )
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )
    original_extract = postprocessing_module._extractor_payload
    calls = 0

    def count_extract(sidecars, metadata):
        nonlocal calls
        calls += 1
        return original_extract(sidecars, metadata)

    monkeypatch.setattr(postprocessing_module, "_extractor_payload", count_extract)
    monkeypatch.setattr(
        postprocessing_module,
        "_download_thumbnail",
        lambda payload, **_: (b"cover", ".jpg"),
    )

    assert postprocessing_module.apply_finalized_post_processing(
        [media],
        {},
        finalized,
        post_processing={
            "metadata": True,
            "thumbnail": True,
            "subtitles": True,
            "automatic_subtitles": True,
            "chapters": True,
            "save_as": "sidecar",
        },
        quality={"mode": "video", "video_container": "mp4"},
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert calls == 1
    assert Path(f"{media}.json").is_file()
    assert media.with_suffix(".jpg").read_bytes() == b"cover"
    assert media.with_name(f"{media.stem}.en.vtt").is_file()
    assert media.with_name(f"{media.stem}.en.auto.vtt").is_file()
    assert media.with_name(f"{media.stem}.chapters.json").is_file()
    assert not extractor_sidecar.exists()


def test_finalized_embed_attaches_thumbnail_after_subtitle_remux(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )
    calls: list[str] = []

    monkeypatch.setattr(
        postprocessing_module,
        "prepare_thumbnail_post_processing",
        lambda *args, **kwargs: (b"cover", ".jpg"),
    )
    monkeypatch.setattr(
        postprocessing_module,
        "prepare_subtitle_post_processing",
        lambda *args, **kwargs: [
            {"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}
        ],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "prepare_chapter_post_processing",
        lambda *args, **kwargs: [{"start_time": 0, "end_time": 1, "title": "Intro"}],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "apply_metadata_post_processing",
        lambda *args, **kwargs: calls.append("metadata") or set(),
    )
    monkeypatch.setattr(
        postprocessing_module,
        "apply_subtitle_post_processing",
        lambda *args, **kwargs: calls.append("subtitles"),
    )
    monkeypatch.setattr(
        postprocessing_module,
        "apply_chapter_post_processing",
        lambda *args, **kwargs: calls.append("chapters"),
    )
    monkeypatch.setattr(
        postprocessing_module,
        "apply_thumbnail_post_processing",
        lambda *args, **kwargs: calls.append("thumbnail"),
    )

    assert postprocessing_module.apply_finalized_post_processing(
        [media],
        {},
        finalized,
        post_processing={
            "metadata": True,
            "thumbnail": True,
            "subtitles": True,
            "automatic_subtitles": False,
            "chapters": True,
            "save_as": "embed",
        },
        quality={"mode": "video", "video_container": "mkv"},
        output_root=tmp_path,
        extractor_payload={},
    )

    assert calls == ["metadata", "subtitles", "chapters", "thumbnail"]


def test_manual_and_auto_subtitles_embed_as_distinct_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    tracks = [
        {"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"},
        {"language": "en", "automatic": True, "extension": "vtt", "data": b"WEBVTT\n"},
    ]
    captured: list[list[str]] = []
    stream_counts: dict[Path, int] = {media: 0}

    def fake_run(cmd, **kwargs):
        captured.append(cmd)
        output = Path(cmd[-1])
        output.write_bytes(b"embedded")
        stream_counts[output] = 2
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        postprocessing_module,
        "_subtitle_stream_count",
        lambda ffmpeg, path: stream_counts.get(path, 0),
    )
    monkeypatch.setattr(postprocessing_module.subprocess, "run", fake_run)

    postprocessing_module.apply_subtitle_post_processing(
        [media],
        {},
        manual=True,
        automatic=True,
        save_as="embed",
        prepared_subtitles=tracks,
    )

    assert media.read_bytes() == b"embedded"
    assert len(captured) == 1
    embed_cmd = captured[0]
    assert embed_cmd.count("-i") == 3
    assert embed_cmd[embed_cmd.index("-c:s") + 1] == "copy"
    assert "title=en" in embed_cmd
    assert "title=en (auto-generated)" in embed_cmd


def test_many_subtitles_embed_through_bounded_bundle_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].webm"
    media.write_bytes(b"video")
    tracks = [
        {
            "language": f"translated-{index:03d}",
            "automatic": True,
            "extension": "vtt",
            "data": f"WEBVTT\n\ncaption {index}".encode(),
        }
        for index in range(100)
    ]
    calls: list[list[str]] = []
    stream_counts: dict[Path, int] = {media: 0}

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        output = Path(cmd[-1])
        output.write_bytes(b"muxed")
        if "nvs-subtitle-bundle-" in output.name:
            prior = next(
                (
                    stream_counts[Path(cmd[index + 1])]
                    for index, value in enumerate(cmd)
                    if value == "-i" and Path(cmd[index + 1]) in stream_counts
                ),
                0,
            )
            new_tracks = sum(1 for value in cmd if "nvs-subtitle-input-" in value)
            stream_counts[output] = prior + new_tracks
        else:
            bundle = next(
                Path(cmd[index + 1])
                for index, value in enumerate(cmd)
                if value == "-i" and "nvs-subtitle-bundle-" in cmd[index + 1]
            )
            stream_counts[output] = stream_counts[bundle]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        postprocessing_module,
        "_subtitle_stream_count",
        lambda ffmpeg, path: stream_counts.get(path, 0),
    )
    monkeypatch.setattr(postprocessing_module.subprocess, "run", fake_run)

    assert postprocessing_module._embed_subtitles(media, tracks)

    bundle_calls = [cmd for cmd in calls if "nvs-subtitle-bundle-" in cmd[-1]]
    assert len(bundle_calls) == 5
    assert all(cmd.count("-i") <= postprocessing_module._SUBTITLE_BUNDLE_BATCH_SIZE + 1 for cmd in bundle_calls)
    assert len(calls[-1]) < 40
    assert media.read_bytes() == b"muxed"


def test_thumbnail_embed_uses_container_aware_tags_after_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mp3"
    media.write_bytes(b"original")
    captured: dict[str, Path] = {}

    def fake_embed(path: Path, thumbnail: Path) -> bool:
        captured["path"] = path
        captured["thumbnail"] = thumbnail
        assert thumbnail.read_bytes() == b"thumbnail-bytes"
        path.write_bytes(b"embedded")
        return True

    monkeypatch.setattr(postprocessing_module, "_download_thumbnail", lambda payload, **_: (b"thumbnail-bytes", ".jpg"))
    monkeypatch.setattr(postprocessing_module, "_embed_thumbnail_with_mutagen", fake_embed)

    postprocessing_module.apply_thumbnail_post_processing([media], {}, save_as="embed")

    assert media.read_bytes() == b"embedded"
    assert captured["path"] == media
    assert captured["thumbnail"].suffix == ".jpg"
    assert not media.with_suffix(".jpg").exists()


def test_webp_thumbnail_is_converted_before_mp4_embedding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media = tmp_path / "Title [abc].mp4"
    media.write_bytes(b"video")
    cover = tmp_path / "cover.webp"
    cover.write_bytes(b"webp")
    converted = tmp_path / "converted.png"
    converted.write_bytes(b"png")
    captured: dict[str, Path] = {}

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        postprocessing_module,
        "_convert_thumbnail_for_embedding",
        lambda ffmpeg, thumbnail: converted,
    )
    monkeypatch.setattr(
        postprocessing_module,
        "_embed_thumbnail_with_mutagen",
        lambda path, thumbnail: captured.update(path=path, thumbnail=thumbnail) is None,
    )

    assert postprocessing_module._embed_thumbnail(media, cover)
    assert captured == {"path": media, "thumbnail": converted}
    assert not converted.exists()


def test_mp3_thumbnail_is_written_as_a_real_front_cover_tag(tmp_path: Path):
    from mutagen.id3 import ID3, PictureType

    media = tmp_path / "Title [abc].mp3"
    media.write_bytes(b"audio-payload")
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"jpeg-payload")

    assert postprocessing_module._embed_thumbnail_with_mutagen(media, cover)

    pictures = ID3(media).getall("APIC")
    assert len(pictures) == 1
    assert pictures[0].type == PictureType.COVER_FRONT
    assert pictures[0].mime == "image/jpeg"
    assert pictures[0].data == b"jpeg-payload"
    assert media.read_bytes().endswith(b"audio-payload")


def test_unsupported_metadata_embed_does_not_fail_the_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].mkv"
    media.write_bytes(b"original")
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        postprocessing_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="unsupported metadata", stdout=""),
    )

    postprocessing_module.apply_metadata_post_processing([media], {}, finalized, save_as="embed")

    assert media.read_bytes() == b"original"
    assert "unsupported metadata" in caplog.text


def test_auto_output_silently_skips_unsupported_embed_targets(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].aac"
    media.write_bytes(b"audio")
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    tracks = [
        {"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}
    ]

    assert not postprocessing_module._embed_metadata(
        media,
        {"title": "Title"},
        silent_unsupported=True,
    )
    assert not postprocessing_module._embed_thumbnail(
        media,
        cover,
        silent_unsupported=True,
    )
    assert not postprocessing_module._embed_subtitles(
        media,
        tracks,
        silent_unsupported=True,
    )

    assert not caplog.text


def test_explicit_unsupported_embed_targets_remain_diagnostic(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].aac"
    media.write_bytes(b"audio")
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")

    assert not postprocessing_module._embed_metadata(media, {"title": "Title"})
    assert not postprocessing_module._embed_thumbnail(media, cover)
    assert not postprocessing_module._embed_subtitles(media, [])

    assert "Metadata embed skipped" in caplog.text
    assert "Thumbnail embed skipped" in caplog.text
    assert "Subtitle embed skipped" in caplog.text


def test_image_metadata_embed_writes_lossless_xmp_without_sidecar(tmp_path: Path):
    media = tmp_path / "Creator - Photo [abc].jpg"
    original_scan = b"compressed-image-data"
    media.write_bytes(b"\xff\xd8\xff\xe0\x00\x04JF\xff\xda" + original_scan)
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Photo",
        keep_paths=[media],
    )

    sidecars = postprocessing_module.apply_metadata_post_processing(
        [media],
        {"description": "Gallery description", "upload_date": "20260801"},
        finalized,
        save_as="embed",
    )

    embedded = media.read_bytes()
    assert not sidecars
    assert embedded.endswith(original_scan)
    assert b"http://ns.adobe.com/xap/1.0/" in embedded
    assert b"Photo" in embedded
    assert b"Creator" in embedded
    assert b"Gallery description" in embedded
    assert b"https://example.test/post/abc" in embedded
    assert b"<dc:identifier>abc</dc:identifier>" in embedded
    assert b"<rdf:li>example</rdf:li>" in embedded
    assert b"2026-08-01" in embedded
    assert b"xmlns:nvs" not in embedded
    assert not Path(f"{media}.json").exists()


def test_image_metadata_embed_falls_back_to_sidecar_when_lossless_writer_is_unavailable(
    tmp_path: Path,
):
    media = tmp_path / "Creator - Photo [abc].webp"
    media.write_bytes(b"RIFFnot-a-real-webp")
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Photo",
        keep_paths=[media],
    )

    sidecars = postprocessing_module.apply_metadata_post_processing(
        [media], {}, finalized, save_as="embed"
    )

    sidecar = Path(f"{media}.json")
    assert sidecars == {sidecar}
    assert json.loads(sidecar.read_text(encoding="utf-8"))["title"] == "Photo"


def test_webp_metadata_embed_adds_standard_xmp_without_reencoding_pixels():
    # A minimal lossless WebP bitstream with a 2x3 canvas. The writer must add
    # VP8X/XMP chunks while preserving the original compressed VP8L payload.
    dimensions = 1 | (2 << 14)
    vp8l = b"\x2f" + dimensions.to_bytes(4, "little")
    chunk = postprocessing_module._webp_chunk(b"VP8L", vp8l)
    body = b"WEBP" + chunk
    original = b"RIFF" + len(body).to_bytes(4, "little") + body
    xmp = postprocessing_module._image_xmp_packet(
        {
            "title": "Photo",
            "artist": "Creator",
            "source": "https://example.test/post/abc",
            "identifier": "abc",
        }
    )

    embedded = postprocessing_module._webp_with_xmp(original, xmp)

    assert embedded is not None
    assert b"VP8X" in embedded
    assert b"XMP " in embedded
    assert vp8l in embedded
    assert b"<dc:source>https://example.test/post/abc</dc:source>" in embedded
    assert int.from_bytes(embedded[4:8], "little") == len(embedded) - 8


def test_finished_video_repairs_codec_mismatches_from_any_extractor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisomvp9-input")
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, path):
        if Path(path) == media:
            return [
                {"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"},
                {"codec_type": "audio", "codec_name": "opus", "codec_tag_string": "Opus"},
            ]
        return [
            {"codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1"},
            {"codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a"},
        ]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"portable-output")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", fake_run)

    assert postprocessing_module.ensure_container_codec_compatibility(
        [media], {"mode": "video", "video_container": "mp4"}
    )
    assert media.read_bytes() == b"portable-output"
    assert "libx264" in commands[0]
    assert "aac" in commands[0]


def test_finished_video_auto_mode_never_runs_compatibility_transcode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisomvp9-input")

    def compatible_streams(_ffmpeg, _path):
        return [
            {"codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1"},
            {"codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a"},
        ]

    def unexpected_run(*_args, **_kwargs):
        raise AssertionError("Compatible Auto output must not be remuxed or transcoded")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", compatible_streams)
    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", unexpected_run)

    assert not postprocessing_module.ensure_container_codec_compatibility(
        [media],
        {
            "mode": "video",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
            "video_audio_codec": "auto",
        },
    )


def test_finished_video_losslessly_repairs_empty_vpcc_in_auto_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def box(kind: bytes, payload: bytes) -> bytes:
        return (len(payload) + 8).to_bytes(4, "big") + kind + payload

    empty_vpcc = box(b"vpcC", b"")
    sample_entry = box(b"vp09", (b"\0" * 78) + empty_vpcc)
    stsd = box(b"stsd", (b"\0" * 4) + (1).to_bytes(4, "big") + sample_entry)
    media = tmp_path / "clip.mp4"
    stbl = box(b"stbl", stsd)
    media.write_bytes(
        box(b"ftyp", b"isom")
        + box(b"moov", box(b"trak", box(b"mdia", box(b"minf", stbl))))
    )
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, path):
        if b"\x00\x00\x00\x08vpcC" in Path(path).read_bytes():
            return []
        return [{"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"}]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(Path(cmd[cmd.index("-i") + 1]).read_bytes())
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", fake_run)

    paths = [media]
    updates: dict[Path, Path] = {}
    assert postprocessing_module.ensure_container_codec_compatibility(
        paths,
        {
            "mode": "video",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
            "video_audio_codec": "auto",
        },
        path_updates=updates,
    )
    assert commands and commands[0][commands[0].index("-c") + 1] == "copy"
    assert "libx264" not in commands[0]
    assert paths[0].suffix == ".webm"
    assert paths[0].is_file()
    assert not media.exists()
    assert updates == {media: paths[0]}


def test_finished_video_auto_remuxes_vp9_mp4_to_webm_without_encoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisom-valid-vp9")
    paths = [media]
    updates: dict[Path, Path] = {}
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, _path):
        return [
            {"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"},
            {"codec_type": "audio", "codec_name": "opus", "codec_tag_string": "Opus"},
        ]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"webm-stream-copy")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", fake_run)

    assert postprocessing_module.ensure_container_codec_compatibility(
        paths,
        {
            "mode": "video",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
            "video_audio_codec": "auto",
        },
        path_updates=updates,
    )
    assert paths[0] == tmp_path / "clip.webm"
    assert paths[0].read_bytes() == b"webm-stream-copy"
    assert not media.exists()
    assert commands[0][commands[0].index("-c") + 1] == "copy"
    assert all("libvpx" not in argument and "libx" not in argument for argument in commands[0])


def test_retry_task_rebuilds_with_selected_engine(monkeypatch: pytest.MonkeyPatch):
    store = {
        "tasks": {
            "ytdlp:failed": {
                "status": "failed",
                "engine": "ytdlp",
                "source_url": "https://example.test/watch?v=1",
                "output_dir": "/media/example",
                "folder_template": "",
                "filename_template": "{{title}} [{{id}}]",
            }
        }
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(operations_module, "load_task_store", lambda: store)
    monkeypatch.setattr(operations_module, "ensure_worker", lambda: None)
    monkeypatch.setattr(
        operations_module,
        "update_task",
        lambda task_id, **kwargs: captured.update(kwargs) or {**store["tasks"][task_id], **kwargs},
    )

    operations_module.retry_task("ytdlp:failed")

    assert captured["status"] == "pending"
    assert captured["engine"] == "gallerydl"
    assert "engine_policy" not in captured
    assert "{extension}" in str(captured["output_template"])


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


def test_parse_filename_media_id_accepts_numbered_gallerydl_suffix():
    assert parse_filename_media_id("Creator - Cap [id]_8.jpg") == ("id", "Creator - Cap")


@pytest.mark.parametrize(
    "title,media_id,source_key,expected",
    [
        ("MadeUpHub photo #123", "", "madeuphub", ""),
        ("Another Site video #abc_123", "abc_123", "", ""),
        ("Actual caption #123", "123", "madeuphub", "Actual caption #123"),
    ],
)
def test_strip_placeholder_title_drops_dynamic_source_placeholders(title, media_id, source_key, expected):
    assert strip_placeholder_title(title, media_id, source_key) == expected


def test_strip_placeholder_title_drops_when_real_media_id_matches():
    assert strip_placeholder_title("Any Platform video #abc_123", "abc_123") == ""


def test_strip_numbered_suffix_removes_gallerydl_num():
    assert strip_numbered_suffix("Creator - Cap [id]_8") == "Creator - Cap [id]"


def test_find_numbered_media_siblings_skips_plain_filename_directory_scan(monkeypatch):
    def fail_iterdir(self):
        raise AssertionError("plain files should not scan their folder")

    monkeypatch.setattr(Path, "iterdir", fail_iterdir)

    assert files_module.find_numbered_media_siblings(Path("/media/Creator - Cap [id].jpg")) == []


def test_parse_filename_media_id_rejects_unrecoverable_names():
    assert parse_filename_media_id("Creator - Soft Light.mp4") == ("", "Creator - Soft Light")
    assert parse_filename_media_id("Creator - Soft Light [NA].mp4")[0] == ""


def test_clean_social_title_removes_engagement_and_attribution_junk():
    assert clean_social_title("Soft Light 1.5M views · 62K reactions") == "Soft Light"
    assert clean_social_title("Soft Light ｜ NJ Tony on Reels") == "Soft Light"
    assert clean_social_title("NJ Tony - Video by NJ Tony", "NJ Tony") == "NJ Tony"
    assert clean_social_title("Video by NJ Tony", "NJ Tony") == ""
    assert clean_social_title("Photo by NJ Tony - 12K likes", "NJ Tony") == ""


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


def test_clean_social_title_strips_trailing_creator_byline():
    raw = "6.9M views · 66K reactions | Bakit kadiri pag ako? | Charess"
    assert clean_social_title(raw, "charechii", ("Charess",)) == "Bakit kadiri pag ako?"
    # A plain-space trailing name is left intact; only strong separators mark a byline.
    assert clean_social_title("A letter to Charess", "charechii", ("Charess",)) == "A letter to Charess"


def test_clean_social_title_drops_generic_post_caption():
    assert clean_social_title("Photos from Charess's post", "charechii") == ""
    assert clean_social_title("Video from Charess’s timeline", "charechii") == ""
    # Real captions that merely start with a media word survive.
    assert clean_social_title("Photos from my trip to Japan", "charechii") == "Photos from my trip to Japan"


def test_clean_template_filename_redacts_duplicate_display_name():
    name = "Charess - Bakit kadiri pag ako？ ｜ Charess [891576008993182].mp4"
    template = "{{username}} - {{title}} [{{id}}]"
    result = clean_template_filename(
        name,
        template,
        creator="charechii",
        title=filename_template_title(name, template),
        media_id="891576008993182",
    )
    assert result == "charechii - Bakit kadiri pag ako？ [891576008993182].mp4"


def test_clean_template_filename_keeps_username_and_nickname_distinct():
    # {{username}} renders the handle, {{nickname}} the display name; no collapse.
    result = clean_template_filename(
        "nasa - NASA - Cool Rocket [ABC123].jpg",
        "{{username}} - {{nickname}} - {{title}} [{{id}}]",
        creator="nasa",
        nickname="NASA",
        title="Cool Rocket",
        media_id="ABC123",
    )
    assert result == "nasa - NASA - Cool Rocket [ABC123].jpg"


def test_clean_template_filename_nickname_token_not_overwritten_by_handle():
    # A {{nickname}} filename keeps the display name even when a handle is supplied.
    result = clean_template_filename(
        "NASA - Cool Rocket [ABC123].jpg",
        "{{nickname}} - {{title}} [{{id}}]",
        creator="nasa",
        nickname="NASA",
        title="Cool Rocket",
        media_id="ABC123",
    )
    assert result == "NASA - Cool Rocket [ABC123].jpg"


NAMING_TEMPLATE = "{{username}} - {{title}} [{{id}}]"
NAMING_SAMPLE = "nasa - Café Rocket Launch [aB3dK9x].jpg"


def _named(**cleaning: object) -> str:
    return clean_template_filename(
        NAMING_SAMPLE,
        NAMING_TEMPLATE,
        creator="nasa",
        title=filename_template_title(NAMING_SAMPLE, NAMING_TEMPLATE),
        media_id="aB3dK9x",
        cleaning=cleaning,
    )


def test_naming_case_never_folds_the_media_id():
    # Folding an id breaks every later match of the file against it, so case is per token.
    assert _named(case="lowercase") == "nasa - café rocket launch [aB3dK9x].jpg"
    assert _named(case="uppercase") == "NASA - CAFÉ ROCKET LAUNCH [aB3dK9x].jpg"
    assert _named(case="capitalized") == "Nasa - Café Rocket Launch [aB3dK9x].jpg"


def test_naming_styles_token_values_and_never_template_literals():
    # The template's own " - " and brackets are the layout the user wrote; only spaces
    # inside a token's value are separators.
    assert _named(separator="underscore") == "nasa - Café_Rocket_Launch [aB3dK9x].jpg"
    assert _named(separator="dash") == "nasa - Café-Rocket-Launch [aB3dK9x].jpg"
    assert _named(charset="remove") == "nasa - Cafe Rocket Launch [aB3dK9x].jpg"
    assert _named(case="lowercase") == "nasa - café rocket launch [aB3dK9x].jpg"


def test_naming_ascii_folds_accents_rather_than_dropping_them():
    assert _named(charset="remove") == "nasa - Cafe Rocket Launch [aB3dK9x].jpg"


def test_naming_stem_cap_bounds_the_whole_name():
    result = _named(stem_max_chars=20)
    assert result == "nasa - Café Rocket.jpg"
    assert len(Path(result).stem) <= 20


def test_naming_blocked_character_replacement_is_configurable():
    # `:` and `/` are illegal in a filename and were always forced to `_`.
    def named(**cleaning: object) -> str:
        return clean_template_filename(
            "sample.jpg",
            NAMING_TEMPLATE,
            creator="nasa",
            title="Ep 1: A/B what?",
            media_id="aB3dK9x",
            cleaning=cleaning,
        )

    assert named() == "nasa - Ep 1_ A_B what_ [aB3dK9x].jpg"
    assert named(invalid_chars="dash") == "nasa - Ep 1- A-B what- [aB3dK9x].jpg"
    assert named(invalid_chars="space") == "nasa - Ep 1 A B what [aB3dK9x].jpg"
    # An unknown value falls back to the default rather than dropping the character.
    assert named(invalid_chars="bogus") == "nasa - Ep 1_ A_B what_ [aB3dK9x].jpg"


def test_naming_defaults_leave_the_name_untouched():
    assert _named() == NAMING_SAMPLE


def test_naming_title_case_handles_unicode_letters_and_apostrophes():
    name = "nasa - naïve don't stop [aB3dK9x].jpg"
    result = clean_template_filename(
        name,
        NAMING_TEMPLATE,
        creator="nasa",
        title=filename_template_title(name, NAMING_TEMPLATE),
        media_id="aB3dK9x",
        cleaning={"case": "capitalized"},
    )

    assert result == "Nasa - Naïve Don't Stop [aB3dK9x].jpg"


def test_clean_template_filename_handle_at_cleanup_can_be_disabled():
    name = "@alice - Nice clip [abc123].mp4"
    template = "{{username}} - {{title}} [{{id}}]"

    assert clean_template_filename(name, template, title="Nice clip") == "alice - Nice clip [abc123].mp4"
    assert clean_template_filename(name, template, title="Nice clip", cleaning={"strip_handle_at": False}) == name


def test_filename_creator_ignores_nickname_token_for_handle():
    # A {{nickname}} filename must NOT feed the {{username}} handle with the display name.
    path = Path("/media/instagram/nasa/NASA - Cool Rocket [ABC123].jpg")
    creator = completion_module._filename_creator(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        {},
        "https://www.instagram.com/p/Cxyz/",
        "ABC123",
    )
    assert creator == ""


def test_username_folder_not_clobbered_by_nickname_filename():
    # No handle known -> {{username}} folder unresolved -> no move, keeping the engine's handle folder.
    folder = completion_module._render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{username}}"},
        creator="",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder is None


def test_render_template_folder_renders_nickname_distinct_from_username():
    folder = completion_module._render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{nickname}}"},
        creator="nasa",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder == Path("/media/instagram/NASA")


def test_render_template_folder_renders_selected_quality():
    folder = completion_module._render_template_folder(
        Path("/media/rule34video"),
        {"folder_template": "{{quality}}/{{username}}"},
        creator="artist",
        media_id="4483553",
        quality={"mode": "video", "video_quality": "1080p"},
    )

    assert folder == Path("/media/rule34video/1080p/artist")


def test_render_template_folder_handle_at_cleanup_can_be_disabled():
    root = Path("/media/tiktok")
    template = {"folder_template": "{{username}}"}

    assert completion_module._render_template_folder(root, template, "@alice", "abc123") == root / "alice"
    assert (
        completion_module._render_template_folder(
            root,
            template,
            "@alice",
            "abc123",
            cleaning={"strip_handle_at": False},
        )
        == root / "@alice"
    )


def test_filename_nickname_recovers_display_name_from_gallerydl_folder():
    # gallery-dl ships no metadata; the display name only survives in the folder it wrote.
    root = Path("/media/instagram")
    path = root / "NASA" / "nasa - Cool Rocket [ABC123].jpg"
    nickname = completion_module._filename_nickname(
        path,
        "{{username}} - {{title}} [{{id}}]",
        "{{nickname}}",
        completion_module._template_folder_text(root, path),
        {},
    )
    assert nickname == "NASA"


def test_filename_nickname_skips_username_value_and_uses_display_metadata():
    root = Path("/media/tiktok")
    path = root / "fzyahoo.com" / "fzyahoo.com - Clip [7493558766131039489].mp4"
    metadata = {
        "webpage_url": "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
        "channel": "FZ Yahoo",
        "uploader": "fzyahoo.com",
        "uploader_url": "https://www.tiktok.com/@fzyahoo.com",
    }

    nickname = completion_module._filename_nickname(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        "{{username}}",
        completion_module._template_folder_text(root, path),
        metadata,
        "fzyahoo.com",
    )

    assert nickname == "FZ Yahoo"


def test_username_folder_and_nickname_filename_stay_distinct_for_handle_metadata(tmp_path: Path):
    media_id = "7493558766131039489"
    source_url = f"https://www.tiktok.com/@fzyahoo.com/video/{media_id}"
    template_settings = {
        "folder_template": "{{username}}",
        "filename_template": "{{nickname}} - {{title}} [{{id}}]",
    }
    raw_path = tmp_path / "fzyahoo.com" / f"fzyahoo.com - Clip [{media_id}].mp4"
    raw_path.parent.mkdir()
    raw_path.write_bytes(b"video")
    metadata = {
        "webpage_url": source_url,
        "channel": "FZ Yahoo",
        "uploader": "fzyahoo.com",
        "uploader_url": "https://www.tiktok.com/@fzyahoo.com",
    }

    creator = completion_module._filename_creator(
        raw_path,
        template_settings["filename_template"],
        metadata,
        source_url,
        media_id,
    )
    nickname = completion_module._filename_nickname(
        raw_path,
        template_settings["filename_template"],
        template_settings["folder_template"],
        completion_module._template_folder_text(tmp_path, raw_path),
        metadata,
        creator,
    )
    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        raw_path,
        template_settings,
        "tiktok",
        creator_hint=creator,
        media_id_hint=media_id,
        nickname_hint=nickname,
        title_hint="Clip",
    )
    final_path = completion_module._move_group_to_template_folder(
        final_path,
        tmp_path,
        template_settings,
        creator,
        media_id,
        nickname,
    )

    expected = tmp_path / "fzyahoo.com" / f"FZ Yahoo - Clip [{media_id}].mp4"
    assert creator == "fzyahoo.com"
    assert nickname == "FZ Yahoo"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not raw_path.exists()


def test_clean_template_filename_drops_generic_post_caption():
    result = clean_template_filename(
        "charechii - Photos from Charess's post [pfbid02xDjH4VegXX]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="charechii",
        media_id="pfbid02xDjH4VegXX",
    )
    assert result == "charechii - [pfbid02xDjH4VegXX]_1.jpg"


def test_resolve_creator_handle_extracts_vanity_without_network(monkeypatch):
    _patch_head(monkeypatch, exc=RuntimeError("should not be called"))
    assert urls_module.resolve_creator_handle("https://www.facebook.com/charechii") == "charechii"
    assert urls_module.resolve_creator_handle("https://www.tiktok.com/@fzyahoo.com") == "fzyahoo.com"


def test_resolve_creator_handle_follows_numeric_id_redirect(monkeypatch):
    _patch_head(monkeypatch, "https://www.facebook.com/charechii")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100044174692204") == "charechii"


def test_resolve_creator_handle_rejects_media_and_walls(monkeypatch):
    # Media URLs are rejected pre-network (multi-segment); auth walls redirect with a query string.
    _patch_head(monkeypatch, "https://www.facebook.com/login/?next=https%3A%2F%2Fwww.facebook.com%2F100044174692204")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/reel/891576008993182") == ""
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100044174692204") == ""


def test_resolve_creator_handle_rejects_cross_host_redirect(monkeypatch):
    # An off-site consent/login host must never supply a handle for the source's creator.
    _patch_head(monkeypatch, "https://login.example.com/charechii")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100044174692204") == ""


def test_metadata_creator_prefers_resolved_handle_over_display_name(monkeypatch):
    _patch_head(monkeypatch, "https://www.facebook.com/charechii")
    metadata = {
        "uploader": "Charess",
        "channel": "Charess",
        "uploader_id": "100044174692204",
        "original_url": "https://www.facebook.com/reel/891576008993182",
    }
    assert completion_module._metadata_creator(metadata, "891576008993182") == "charechii"


def test_metadata_creator_skips_mobile_host_wall(monkeypatch):
    # yt-dlp's webpage_url is often a mobile host that walls a bare-id fetch; the apex/www host must win.
    def fake_head(url, **kwargs):
        target = "https://m.facebook.com/login/?next=x" if "m.facebook.com" in url else "https://www.facebook.com/charechii"
        return type("Resp", (), {"url": target})()

    monkeypatch.setattr(urls_module.httpx, "head", fake_head)
    metadata = {
        "uploader": "Charess",
        "uploader_id": "100044174692204",
        "webpage_url": "https://m.facebook.com/watch/?v=1727302008412891",
        "original_url": "https://www.facebook.com/reel/1727302008412891",
    }
    assert completion_module._metadata_creator(metadata, "1727302008412891") == "charechii"


def test_metadata_creator_prefers_at_handle_metadata():
    metadata = {
        "channel": "Mili",
        "uploader": "Mili",
        "creator": "Mili",
        "uploader_id": "@mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    assert completion_module._metadata_creator(metadata, "In5Du5x6MZM") == "mili"


def test_metadata_creator_rejects_opaque_id_metadata():
    metadata = {
        "channel": "UC-wNqHVYS82PF4mkaQb0Alg",
        "uploader": "",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    assert completion_module._metadata_creator(metadata, "In5Du5x6MZM") == ""


def test_filename_creator_uses_handle_metadata_without_at():
    metadata = {
        "channel": "Mili",
        "uploader": "Mili",
        "creator": "Mili",
        "uploader_id": "@mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    creator = completion_module._filename_creator(
        Path("@mili - Iron Lotus [In5Du5x6MZM].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://video.example/watch?v=In5Du5x6MZM",
        "In5Du5x6MZM",
    )

    assert creator == "mili"


def test_filename_creator_strips_at_from_filename_username():
    creator = completion_module._filename_creator(
        Path("@mili - Iron Lotus [In5Du5x6MZM].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        {},
        "https://video.example/watch?v=In5Du5x6MZM",
        "In5Du5x6MZM",
    )

    assert creator == "mili"


def test_role_creator_uses_scraped_token_role():
    creator = completion_module._role_creator(
        {"username": "Trace Artist"},
        {"rule34video": {"artist": "username"}},
        "rule34video",
    )

    assert creator == "Trace Artist"


def test_clean_filename_title_drops_empty_title_sentinels():
    assert clean_filename_title("None") == ""
    assert clean_filename_title(" untitled ") == ""


def test_clean_template_filename_drops_none_title_segment():
    result = clean_template_filename(
        "Poster - None [abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        media_id="abc123",
    )

    assert result == "Poster - [abc123]_1.jpg"


def test_clean_template_filename_authoritative_empty_title_clears_extractor_value():
    result = clean_template_filename(
        "Poster - Extractor title [abc123]_1.mp4",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        title="",
        media_id="abc123",
    )

    assert result == "Poster - [abc123]_1.mp4"


def test_clean_template_filename_drops_matching_gallery_position_title():
    name = "Poster - 20 [abc123]_20.mp4"
    template = "{{username}} - {{title}} [{{id}}]"
    result = clean_template_filename(
        name,
        template,
        creator="Poster",
        title=filename_template_title(name, template),
        media_id="abc123",
    )

    assert result == "Poster - [abc123]_20.mp4"


@pytest.mark.parametrize(
    ("template", "empty_title_name", "titled_name"),
    [
        ("{{username}} - {{title}} [{{id}}]", "poster - [abc123].jpg", "poster - Nice clip [abc123].jpg"),
        ("{{nickname}} | {{title}} ({{id}})", "poster | (abc123).jpg", "poster | Nice clip (abc123).jpg"),
        ("{{title}} :: {{username}} [{{id}}]", "poster [abc123].jpg", "Nice clip :: poster [abc123].jpg"),
        ("[{{id}}] {{username}}_{{title}}", "[abc123] poster.jpg", "[abc123] poster_Nice clip.jpg"),
    ],
)
def test_clean_template_filename_keeps_empty_title_empty_across_renders(template, empty_title_name, titled_name):
    def render(name: str, title: str) -> str:
        return clean_template_filename(name, template, creator="poster", title=title, media_id="abc123")

    assert render("gallerydl-raw.jpg", "Nice clip") == titled_name
    assert render("gallerydl-raw.jpg", "") == empty_title_name
    assert empty_title_name.count("poster") == 1
    assert render(empty_title_name, "") == empty_title_name
    assert render(titled_name, "Nice clip") == titled_name


def test_clean_template_filename_truncates_long_title_when_enabled():
    title = "A" * 140
    result = clean_template_filename(
        f"Poster - {title} [abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        title=title,
        media_id="abc123",
        cleaning={"shorten": True},
    )

    assert result == f"Poster - {'A' * 100} [abc123]_1.jpg"


def test_clean_template_filename_preserves_custom_quality_tokens_without_title():
    result = clean_template_filename(
        "daiwa-scarlet-suokanawer_source - [4483553].mp4",
        "{{slug}}_{{quality}} - [{{id}}]",
        media_id="4483553",
    )

    assert result == "daiwa-scarlet-suokanawer_source - [4483553].mp4"


def test_clean_template_filename_renders_selected_quality_when_rebuilding():
    result = clean_template_filename(
        "Clip [4483553].mp4",
        "{{quality}} - {{title}} [{{id}}]",
        media_id="4483553",
        quality={"mode": "video", "video_quality": "1080p"},
    )

    assert result == "1080p - [4483553].mp4"


def test_clean_template_filename_rebuilds_sparse_gallerydl_name_from_title_hint():
    result = clean_template_filename(
        "[abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="alice",
        title="Nice clip",
        media_id="abc123",
    )

    assert result == "alice - Nice clip [abc123]_1.jpg"


def test_clean_template_filename_repairs_none_creator_and_duplicate_id_title():
    result = clean_template_filename(
        "None - [DZwrrifkye4] [DZwrrifkye4].mp4",
        "{{username}} - {{title}} [{{id}}]",
        creator="real.creator",
        media_id="DZwrrifkye4",
    )

    assert result == "real.creator - [DZwrrifkye4].mp4"


def test_clean_resolved_filename_renames_real_file_using_settings_template(tmp_path: Path):
    source_url = "https://twitter.com/DohaVT/status/2073635724684054528"
    media_file = tmp_path / "DohaVT - 2073635724684054528 - Video by DohaVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{username}} - {{id}} - {{title}}"},
        "twitter",
    )

    expected = tmp_path / "DohaVT - 2073635724684054528.mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_rerenders_selected_quality(tmp_path: Path):
    source_url = "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/"
    media_file = tmp_path / "source - Video by Artist [4483553].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{quality}} - {{title}} [{{id}}]"},
        "rule34video",
        creator_hint="Artist",
        media_id_hint="4483553",
        quality={"mode": "video", "video_quality": "1080p"},
    )

    expected = tmp_path / "1080p - [4483553].mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_finalization_runs_scraper_fields_templates_and_naming_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    raw = tmp_path / "Extractor title [abc123].mp4"
    raw.write_bytes(b"video")
    monkeypatch.setattr(
        completion_finalization_module,
        "get_effective_title_cleaning",
        lambda source_url: {"strip_hashtags": True},
    )

    finalized = completion_module._finalize_completed_output(
        source_url="https://example.test/watch/abc123",
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"id": "abc123", "title": "Extractor title"},
        template_settings={
            "folder_template": "{{title}}",
            "filename_template": "{{title}} [{{id}}]",
        },
        extra_tokens={"page_title": "Scraped title #ignored"},
        token_roles={"example": {"page_title": "title"}},
        cache_dropper=None,
    )

    expected = tmp_path / "Scraped title" / "Scraped title [abc123].mp4"
    assert finalized.final_path == expected
    assert finalized.display_filename == expected.name
    assert finalized.title == "Scraped title"
    assert expected.is_file()
    assert not raw.exists()


def test_configured_title_fields_are_authoritative():
    assert completion_module._metadata_title(
        completion_module._extractor_metadata_fields(
            {"headline": "Configured headline", "title": "Extractor title"}
        ),
        ["headline", "title"],
    ) == "Configured headline"


def test_configured_title_fields_do_not_fall_through_to_extractor_title():
    assert completion_module._metadata_title(
        {"description": "Configured caption", "title": "20"},
        ["description"],
    ) == "Configured caption"
    assert completion_module._metadata_title(
        {"description": "", "title": "20"},
        ["description"],
    ) == ""


def test_coerce_audio_output_extension_prefers_postprocessed_target(tmp_path: Path):
    raw = tmp_path / "clip.webm"
    final = tmp_path / "clip.opus"
    final.write_bytes(b"opus")
    group_paths = [raw]

    result = completion_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "opus"},
    )

    assert result == final
    assert group_paths == [final]


def test_coerce_audio_output_extension_renames_ytdlp_aac_m4a(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def fail(*args):
        raise AssertionError("already the target container")

    monkeypatch.setattr(completion_outputs_module, "convert_audio_output", fail)
    # yt-dlp writes ADTS AAC under a `.m4a` name.
    raw = tmp_path / "clip.m4a"
    raw.write_bytes(b"\xff\xf1" + bytes(16))
    group_paths = [raw]

    result = completion_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "aac"},
    )

    expected = tmp_path / "clip.aac"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_coerce_audio_output_extension_keeps_m4a_when_remux_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(completion_outputs_module, "convert_audio_output", lambda *args: False)
    raw = tmp_path / "clip.m4a"
    raw.write_bytes(bytes(4) + b"ftypM4A " + bytes(8))
    group_paths = [raw]

    result = completion_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "aac"},
    )

    assert result == raw
    assert group_paths == [raw]
    assert raw.is_file()


def test_coerce_audio_output_extension_renames_relabeled_gallerydl_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def fail(*args):
        raise AssertionError("already the target container")

    monkeypatch.setattr(completion_outputs_module, "convert_audio_output", fail)
    # gallery-dl finalizes yt-dlp's converted Ogg Opus under the source extension.
    raw = tmp_path / "clip.webm"
    raw.write_bytes(b"OggS" + bytes(16))
    group_paths = [raw]

    result = completion_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "opus"},
    )

    expected = tmp_path / "clip.opus"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_coerce_audio_output_extension_converts_unconverted_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def convert(source: Path, target: Path, quality: dict[str, str]) -> bool:
        assert quality["audio_format"] == "wav"
        target.write_bytes(b"RIFF____WAVE")
        return True

    monkeypatch.setattr(completion_outputs_module, "convert_audio_output", convert)
    raw = tmp_path / "clip.webm"
    raw.write_bytes(b"\x1a\x45\xdf\xa3" + bytes(16))
    group_paths = [raw]

    result = completion_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "wav"},
    )

    expected = tmp_path / "clip.wav"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_clean_resolved_filename_rebuilds_sparse_gallerydl_name_from_title_hint(tmp_path: Path):
    source_url = "https://example.com/alice/post/abc123"
    media_file = tmp_path / "[abc123]_1.jpg"
    media_file.write_bytes(b"image")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "example",
        creator_hint="alice",
        media_id_hint="abc123",
        title_hint="Nice clip",
    )

    expected = tmp_path / "alice - Nice clip [abc123]_1.jpg"
    assert final_path == expected
    assert display_filename == "alice - Nice clip [abc123].jpg"
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_title_only_template_falls_back_to_media_id(tmp_path: Path):
    source_url = "https://twitter.com/DohaVT/status/2073635724684054528"
    media_file = tmp_path / "Video by DohaVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{title}}"},
        "twitter",
    )

    expected = tmp_path / "2073635724684054528.mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_strips_at_from_username(tmp_path: Path):
    source_url = "https://video.example/watch?v=In5Du5x6MZM"
    media_file = tmp_path / "@mili - Iron Lotus [In5Du5x6MZM].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "",
        creator_hint="mili",
        media_id_hint="In5Du5x6MZM",
        nickname_hint="Mili",
        title_hint="Iron Lotus",
    )

    expected = tmp_path / "mili - Iron Lotus [In5Du5x6MZM].mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_configured_field_value_honors_opaque_id_in_priority_order():
    # channel_id first in the configured order must win, even though the handle
    # heuristics reject it as an opaque identifier.
    metadata = {
        "uploader": "Mili",
        "channel": "Mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
    }

    assert (
        completion_module._configured_field_value(metadata, ["channel_id", "uploader"])
        == "UC-wNqHVYS82PF4mkaQb0Alg"
    )


def test_configured_field_value_empty_order_defers_to_heuristics():
    metadata = {"channel_id": "UC-wNqHVYS82PF4mkaQb0Alg", "uploader": "Mili"}

    assert completion_module._configured_field_value(metadata, []) == ""


def test_clean_resolved_filename_keeps_authoritative_creator_over_url_handle(tmp_path: Path):
    # An authoritative (configured) creator must not be clobbered by a URL-derived handle.
    source_url = "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489"
    media_file = tmp_path / "UC1234567890 - Clip [7493558766131039489].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = completion_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "tiktok",
        creator_hint="UC1234567890",
        media_id_hint="7493558766131039489",
        title_hint="Clip",
        creator_authoritative=True,
    )

    expected = tmp_path / "UC1234567890 - Clip [7493558766131039489].mp4"
    assert final_path == expected
    assert display_filename == expected.name


def test_gallerydl_multifile_run_uses_first_image_and_clean_display_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first = tmp_path / "Creator - TikTok photo #1234567890 [1234567890]_1.jpg"
    second = tmp_path / "Creator - TikTok photo #1234567890 [1234567890]_2.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    source_url = "https://www.tiktok.com/@Creator/photo/1234567890"
    task_id = "gallerydl:test"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "tiktok",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        stdout = iter([f"{second}\n", f"{first}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(gallerydl_module, "count_gallerydl_items", lambda *args, **kwargs: 2)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    first_clean = tmp_path / "Creator - [1234567890]_1.jpg"
    second_clean = tmp_path / "Creator - [1234567890]_2.jpg"
    assert completed["status"] == "completed"
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert not first.exists()
    assert not second.exists()
    assert completed["resolved_full_path"] == str(first_clean)
    assert completed["resolved_filename"] == "Creator - [1234567890].jpg"
    assert completed["title"] == ""
    assert saved[task_id]["resolved_full_path"] == str(first_clean)
    assert saved[task_id]["resolved_filename"] == "Creator - [1234567890].jpg"


def test_gallerydl_sparse_single_output_enqueues_metadata_repair_without_inline_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    raw_video = tmp_path / "[abc123].mp4"
    raw_video.write_bytes(b"video")
    source_url = "https://www.example.test/watch/abc123"
    task_id = "gallerydl:sparse-template"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    queued: list[dict[str, object]] = []

    class FakeProcess:
        stdout = iter([f"{raw_video}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "get_effective_fields", lambda url: {"username": ["channel"]})
    monkeypatch.setattr(
        completion_metadata_module,
        "_probe_output_metadata",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("probe must not run inline")),
    )
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "_learn_field_roles_from_download", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))
    monkeypatch.setattr(
        worker_module,
        "enqueue_completion_enrichment",
        lambda *args, **kwargs: queued.append({"args": args, "kwargs": kwargs}),
    )

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert completed["status"] == "completed"
    assert raw_video.is_file()
    assert saved[task_id]["folder_template"] == store["tasks"][task_id]["folder_template"]
    assert saved[task_id]["filename_template"] == store["tasks"][task_id]["filename_template"]
    assert len(queued) == 1
    assert queued[0]["args"] == (task_id,)
    assert queued[0]["kwargs"]["needs_metadata_probe"] is True
    assert queued[0]["kwargs"]["needs_field_probe"] is False


def test_enqueue_completion_enrichment_persists_minimal_dry_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setattr(enrichment_module, "ensure_enrichment_worker", lambda: None)

    enrichment_module.enqueue_completion_enrichment(
        "gallerydl:abc123",
        metadata={"id": "abc123", "username": "creator"},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        quality={"mode": "video", "video_quality": "720p"},
        output_root=str(tmp_path),
        extra_tokens={"artist": "creator"},
        token_roles={"example": {"artist": "username"}},
        post_processing={"metadata": True, "save_as": "sidecar"},
        needs_metadata_probe=True,
        needs_field_probe=True,
    )

    jobs = store_module.load_enrichment_jobs()
    assert len(jobs) == 1
    payload = jobs[0]["payload"]
    assert payload == {
        "task_id": "gallerydl:abc123",
        "template_settings": {
            "folder_template": "{{username}}",
            "filename_template": "{{title}} [{{id}}]",
        },
        "quality": {
            "mode": "video",
            "video_quality": "720p",
            "video_container": "auto",
            "video_codec": "auto",
            "video_audio_codec": "auto",
            "audio_format": "auto",
            "audio_bitrate": "best",
        },
        "output_root": str(tmp_path),
        "metadata": {"id": "abc123", "username": "creator"},
        "extra_tokens": {"artist": "creator"},
        "token_roles": {"example": {"artist": "username"}},
        "post_processing": {
            "metadata": True,
            "subtitles": False,
            "automatic_subtitles": False,
            "chapters": False,
            "thumbnail": False,
            "save_as": "sidecar",
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


def test_complete_sidecar_metadata_skips_completion_enrichment(tmp_path: Path):
    path = tmp_path / "ChannelHandle - Nice clip [abc123].mp4"
    path.write_bytes(b"video")

    class GalleryDlEngine:
        name = "gallerydl"

    needed = completion_metadata_module._single_output_metadata_enrichment_needed(
        [{"path": path, "engine": GalleryDlEngine()}],
        {
            path_key(path): {
                "id": "abc123",
                "channel": "ChannelHandle",
                "title": "Nice clip",
            }
        },
        {"filename_template": "{{username}} - {{title}} [{{id}}]"},
    )

    assert needed is False


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
        "_probe_output_metadata",
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
    clean_video = tmp_path / f"real.creator - [{media_id}].mp4"
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

    assert store_module.load_enrichment_jobs() == []


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


def test_gallerydl_same_source_assets_share_one_row_and_source_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    first = tmp_path / "Poster - Image [childA]_1.jpg"
    second = tmp_path / "Poster - Image [childB]_2.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    source_url = "https://www.example.test/post/DWyrvI9Ef3z"
    task_id = "ytdlp:gallery-post"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        if cmd[0] == "yt-dlp":
            return FakeProcess(["ERROR: [Example] DWyrvI9Ef3z: No video formats found!\n"], 1)
        return FakeProcess([f"{first}\n", f"{second}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(gallerydl_module, "count_gallerydl_items", lambda *args, **kwargs: 2)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    first_clean = tmp_path / "Poster - Image [DWyrvI9Ef3z]_1.jpg"
    second_clean = tmp_path / "Poster - Image [DWyrvI9Ef3z]_2.jpg"
    completed = store["tasks"][task_id]
    assert set(saved) == {task_id}
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert not first.exists()
    assert not second.exists()
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert completed["media_id"] == "DWyrvI9Ef3z"
    assert completed["source_url"] == source_url
    assert completed["resolved_full_path"] == str(first_clean)
    assert completed["resolved_filename"] == "Poster - Image [DWyrvI9Ef3z].jpg"
    assert saved[task_id]["media_id"] == "DWyrvI9Ef3z"
    assert saved[task_id]["resolved_filename"] == "Poster - Image [DWyrvI9Ef3z].jpg"


def test_gallerydl_distinct_metadata_urls_split_rows_dynamically(tmp_path: Path):
    first = tmp_path / "Poster - Image [asset-a]_1.jpg"
    second = tmp_path / "Poster - Image [asset-b]_2.jpg"
    for path in (first, second):
        path.write_bytes(b"image")
    metadata = {
        path_key(first): {"webpage_url": "https://www.example.test/item/asset-a"},
        path_key(second): {"webpage_url": "https://www.example.test/item/asset-b"},
    }

    groups = completion_module._download_groups(
        [first, second],
        engine_by_name("gallerydl"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://www.example.test/",
    )

    assert len(groups) == 2
    assert {group["media_id"] for group in groups} == {"asset-a", "asset-b"}


def test_gallerydl_source_url_id_groups_distinct_child_metadata_urls(tmp_path: Path):
    first = tmp_path / "Poster - Image [child-a]_1.jpg"
    second = tmp_path / "Poster - Image [child-b]_2.mp4"
    for path in (first, second):
        path.write_bytes(b"media")
    metadata = {
        path_key(first): {"webpage_url": "https://www.example.test/item/child-a"},
        path_key(second): {"webpage_url": "https://www.example.test/item/child-b"},
    }

    groups = completion_module._download_groups(
        [first, second],
        engine_by_name("gallerydl"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://www.example.test/post/root123",
    )

    assert len(groups) == 1
    assert groups[0]["media_id"] == "root123"


def test_gallerydl_parent_group_keeps_pasted_source_url_for_child_metadata():
    source_url = "https://www.example.test/post/root123"
    metadata = {
        "webpage_url": "https://www.example.test/item/child-a",
    }

    assert (
        completion_module._item_source_url(source_url, "example", "root123", "poster", metadata)
        == source_url
    )


def test_duplicate_library_cleanup_removes_history_row_for_duplicate_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    keep = tmp_path / "Creator - Clip [abc123].mp4"
    duplicate = tmp_path / "Old Creator - Clip [abc123].mp4"
    keep.write_bytes(b"current")
    duplicate.write_bytes(b"stale")
    removed: list[str] = []
    queried: list[str] = []

    def fake_load_history_entry_for_path(path: str):
        queried.append(path)
        return ("disk:old-abc123", {"resolved_full_path": path}) if path == str(duplicate) else (None, None)

    monkeypatch.setattr(
        completion_outputs_module,
        "load_history_entry_for_path",
        fake_load_history_entry_for_path,
    )
    monkeypatch.setattr(completion_outputs_module, "load_history_entries_for_media_id", lambda media_id: [])
    monkeypatch.setattr(completion_outputs_module, "remove_history_record", removed.append)

    completion_outputs_module._cleanup_duplicate_library_media(tmp_path, "abc123", [keep])

    assert queried == [str(duplicate)]
    assert removed == ["disk:old-abc123"]
    assert not duplicate.exists()
    assert keep.exists()


def test_duplicate_library_cleanup_uses_history_index_for_different_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    keep = tmp_path / "new" / "Creator - Clip [abc123].mp4"
    duplicate = tmp_path / "old" / "Old Creator - Clip [abc123].mp4"
    keep.parent.mkdir()
    duplicate.parent.mkdir()
    keep.write_bytes(b"current")
    duplicate.write_bytes(b"stale")
    removed: list[str] = []

    monkeypatch.setattr(
        completion_outputs_module,
        "load_history_entries_for_media_id",
        lambda media_id: [
            (
                "disk:old-abc123",
                {
                    "media_id": media_id,
                    "resolved_full_path": str(duplicate),
                },
            )
        ],
    )
    monkeypatch.setattr(
        completion_outputs_module,
        "load_history_entry_for_path",
        lambda path: (_ for _ in ()).throw(AssertionError("sibling fallback should not be used")),
    )
    monkeypatch.setattr(completion_outputs_module, "remove_history_record", removed.append)

    completion_outputs_module._cleanup_duplicate_library_media(tmp_path, "abc123", [keep])

    assert removed == ["disk:old-abc123"]
    assert not duplicate.exists()
    assert keep.exists()


def test_read_metadata_sidecar_accepts_gallerydl_jsonl(tmp_path: Path):
    media_file = tmp_path / "clip.mp4"
    sidecar = tmp_path / "metadata.jsonl"
    sidecar.write_text(
        json.dumps(
            {
                "filepath": str(media_file),
                "id": "child-a",
                "user": {"name": "poster"},
                "tags": ["one", "two"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    metadata = completion_metadata_module._read_metadata_sidecar(str(sidecar))

    row = metadata[path_key(media_file)]
    assert row["id"] == "child-a"
    assert row["user[name]"] == "poster"
    assert row["tags"] == "one, two"


def test_after_move_metadata_path_wins_over_scratch_progress_paths(tmp_path: Path):
    final = tmp_path / "Templated Creator" / "Templated title [abc123].webm"
    final.parent.mkdir()
    final.write_bytes(b"video")
    missing_scratch_intermediate = tmp_path / "scratch" / "raw.f399.webm"

    paths = completion_metadata_module._metadata_output_paths(
        {
            path_key(final): {
                "filepath": str(final),
                "id": "abc123",
                "title": "Templated title",
            },
            path_key(missing_scratch_intermediate): {
                "filepath": str(missing_scratch_intermediate),
            },
        }
    )

    assert paths == [final]


def test_worker_falls_back_to_gallerydl_after_empty_ytdlp_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "ytdlp:fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        if cmd[0] == "yt-dlp":
            return FakeProcess(["ERROR: [Example] abc123: No video formats found!\n"], 1)
        return FakeProcess([f"{image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(gallerydl_module, "count_gallerydl_items", lambda *args, **kwargs: 1)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert saved[task_id]["engine"] == "gallerydl"


def test_worker_does_not_run_fallback_after_media_and_unsupported_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "@Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:no-duplicate-fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        stdout = iter(
            [
                f"{image}\n",
                "ERROR: [Example] child-video: No video formats found!\n",
            ]
        )

        def wait(self):
            return 1

        def poll(self):
            return 1

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        return FakeProcess()

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_image = tmp_path / "Creator - Image [abc123]_1.jpg"
    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert clean_image.is_file()
    assert not image.exists()
    assert saved[task_id]["resolved_full_path"] == str(clean_image)


def test_worker_runs_ytdlp_fallback_after_empty_gallerydl_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    video = tmp_path / "Creator - Clip [abc123].mp4"
    video.write_bytes(b"video")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:ytdlp-fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        if cmd[0] == "gallery-dl":
            return FakeProcess(["ERROR: Unsupported URL: https://www.example.test/post/abc123\n"], 1)
        return FakeProcess([f"[download] Destination: {video}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl", "yt-dlp"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "ytdlp"
    assert saved[task_id]["resolved_full_path"] == str(video)


def test_worker_runs_gallerydl_without_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:no-preflight"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[list[str]] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd)
        return FakeProcess([f"{image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(
        gallerydl_module,
        "count_gallerydl_items",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("worker must not count before download")),
    )
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert [cmd[0] for cmd in commands] == ["gallery-dl"]
    # No stored template: the worker rebuilds a gallery-dl filename, not a yt-dlp one.
    assert commands[0][commands[0].index("--filename") + 1].endswith(".{extension}")
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert saved[task_id]["engine"] == "gallerydl"


def test_worker_merges_fallback_assets_without_duplicate_videos(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    ytdlp_video = tmp_path / "love.rizzzz - Video by Riz [DOS-dVRkUK3].mp4"
    gallery_video = tmp_path / "love.rizzzz - Video by Riz [DOS-dVRkUK3]_1.mp4"
    gallery_image = tmp_path / "love.rizzzz - None [DOS-dVRkUK3]_2.jpg"
    stale_wrong_video = tmp_path / "Riz" / "Riz - [DOS-dVRkUK3].mp4"
    stale_wrong_video.parent.mkdir()
    for path in (ytdlp_video, gallery_video, gallery_image):
        path.write_bytes(b"media")
    stale_wrong_video.write_bytes(b"duplicate")
    source_url = "https://www.example.test/post/DOS-dVRkUK3"
    task_id = "ytdlp:mixed-post"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[list[str]] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd)
        if cmd[0] == "yt-dlp":
            return FakeProcess(
                [
                    f"[download] Destination: {ytdlp_video}\n",
                    "ERROR: [Example] child-image: No video formats found!\n",
                ],
                1,
            )
        return FakeProcess([f"{gallery_video}\n", f"{gallery_image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)

    monkeypatch.setattr(
        gallerydl_module,
        "count_gallerydl_items",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("worker must not count before download")),
    )
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_video = tmp_path / "love.rizzzz - [DOS-dVRkUK3]_1.mp4"
    clean_image = tmp_path / "love.rizzzz - [DOS-dVRkUK3]_2.jpg"
    completed = store["tasks"][task_id]
    assert [cmd[0] for cmd in commands] == ["gallery-dl"]
    assert "--filter" not in commands[0]
    assert set(saved) == {task_id}
    assert clean_video.is_file()
    assert clean_image.is_file()
    assert not ytdlp_video.exists()
    assert not gallery_video.exists()
    assert not gallery_image.exists()
    assert stale_wrong_video.exists()
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert completed["creator"] == "love.rizzzz"
    assert completed["source_url"] == source_url
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_filename"] == "love.rizzzz - [DOS-dVRkUK3].mp4"


def test_worker_renames_display_creator_to_handle_and_template_folder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    raw_video = tmp_path / "Riz" / "Riz - [DOS-dVRkUK3].mp4"
    raw_video.parent.mkdir()
    raw_video.write_bytes(b"video")
    source_url = "https://www.example.test/post/DOS-dVRkUK3"
    task_id = "ytdlp:display-name"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "instagram",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        stdout = iter([f"[download] Destination: {raw_video}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        for index, arg in enumerate(cmd):
            if arg != "--print-to-file":
                continue
            template = cmd[index + 1]
            sidecar = Path(cmd[index + 2])
            if "filepath" in template:
                sidecar.write_text(
                    "\t".join(
                        [
                            str(raw_video),
                            "DOS-dVRkUK3",
                            "",
                            "",
                            "love.rizzzz",
                            "Riz",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
            else:
                sidecar.write_text("Riz\n", encoding="utf-8")
        return FakeProcess()

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "_engine_run_order", lambda task: [engine_by_name("ytdlp")])
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_video = tmp_path / "love.rizzzz" / "love.rizzzz - [DOS-dVRkUK3].mp4"
    completed = store["tasks"][task_id]
    assert clean_video.is_file()
    assert not raw_video.exists()
    assert completed["status"] == "completed"
    assert completed["creator"] == "love.rizzzz"
    assert completed["resolved_folder"] == str(clean_video.parent)
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_filename"] == "love.rizzzz - [DOS-dVRkUK3].mp4"
    assert saved[task_id]["resolved_full_path"] == str(clean_video)


def test_worker_splits_distinct_media_outputs_and_cleans_each_real_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    first = tmp_path / "love.rizzzz - Video by love.rizzzz [DanBhNzkY9_].mp4"
    second = tmp_path / "love.rizzzz - [DapLPfHEQz5].mp4"
    third = tmp_path / "love.rizzzz - Video by love.rizzzz [DapIP3mDqE2].mp4"
    for path in (first, second, third):
        path.write_bytes(b"video")
    source_url = "https://www.instagram.com/stories/love.rizzzz/3938715623970742582/"
    task_id = "ytdlp:story"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "instagram",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    dropped_cache_paths: list[Path] = []

    class FakeProcess:
        stdout = iter(
            [
                f"[download] Destination: {first}\n",
                f"[download] Destination: {second}\n",
                f"[download] Destination: {third}\n",
            ]
        )

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "_engine_run_order", lambda task: [engine_by_name("ytdlp")])
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "drop_file_cache", lambda paths: dropped_cache_paths.extend(paths))
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    first_clean = tmp_path / "love.rizzzz - [DanBhNzkY9_].mp4"
    second_clean = tmp_path / "love.rizzzz - [DapLPfHEQz5].mp4"
    third_clean = tmp_path / "love.rizzzz - [DapIP3mDqE2].mp4"
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert third_clean.is_file()
    assert not first.exists()
    assert not third.exists()
    assert set(saved) == {task_id, f"{task_id}:DapLPfHEQz5", f"{task_id}:DapIP3mDqE2"}
    assert saved[task_id]["resolved_filename"] == first_clean.name
    assert saved[f"{task_id}:DapLPfHEQz5"]["resolved_filename"] == second_clean.name
    assert saved[f"{task_id}:DapIP3mDqE2"]["resolved_filename"] == third_clean.name
    assert {Path(path).name for path in dropped_cache_paths} == {
        first_clean.name,
        second_clean.name,
        third_clean.name,
    }
    assert saved[task_id]["source_url"] == "https://www.instagram.com/stories/love.rizzzz/DanBhNzkY9_"
    assert saved[f"{task_id}:DapLPfHEQz5"]["source_url"] == (
        "https://www.instagram.com/stories/love.rizzzz/DapLPfHEQz5"
    )


def test_task_to_api_leaves_raw_gallerydl_filename_without_template(tmp_path: Path):
    media_file = tmp_path / "fzyahoo.com - TikTok photo #7420705673542978833 [7420705673542978833]_1.jpg"
    media_file.write_bytes(b"image")

    api_task = task_to_api(
        "gallerydl:test",
        {
            "engine": "gallerydl",
            "status": "completed",
            "source_url": "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833",
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

    path, filename, archive = operations_module.resolve_task_file("gallerydl:test")

    assert path == media_file
    assert filename == "@ChannelHandle - Nice clip [abc123].mp4"
    assert archive is None


def test_task_to_api_keeps_stored_creator_over_url_creator(tmp_path: Path):
    media_file = tmp_path / "fzyahoo.com - Clip [7420705673542978833].mp4"
    media_file.write_bytes(b"video")

    api_task = task_to_api(
        "ytdlp:test",
        {
            "engine": "ytdlp",
            "status": "completed",
            "creator": "Some Display Name",
            "source_url": "https://www.tiktok.com/@fzyahoo.com/video/7420705673542978833",
            "resolved_full_path": str(media_file),
            "resolved_filename": media_file.name,
        },
    )

    assert api_task["creator"] == "Some Display Name"


def test_worker_resolved_task_creator_uses_engine_sidecar_not_url_creator(tmp_path: Path):
    sidecar = tmp_path / "creator.txt"
    sidecar.write_text("Some Display Name\n", encoding="utf-8")

    class FakeEngine:
        def read_creator(self, sidecar_path: str, source_url: str) -> str:
            return "Some Display Name"

    creator = completion_module._resolved_task_creator(
        FakeEngine(),
        str(sidecar),
        "https://www.tiktok.com/@fzyahoo.com/video/7420705673542978833",
        "fzyahoo.com - Clip [7420705673542978833].mp4",
    )

    assert creator == "Some Display Name"


@pytest.fixture(autouse=True)
def _scan_stays_offline(monkeypatch: pytest.MonkeyPatch):
    # Library scan probes manually-placed files over the network; keep tests offline by
    # default. Probe-behavior tests opt back in by re-stubbing with canned metadata.
    monkeypatch.setattr(scan_module, "_scan_probe_metadata", lambda url, *, with_cookies=False: {})
    monkeypatch.setattr(scan_module, "_scan_scrape_rule_tokens", lambda: {})


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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 0,
        "missing": 0,
        "added": 1,
        "unchanged": 0,
        "renamed": 0,
        "rename_failed": 0,
        "needs_resolve": 0,
    }
    assert saved["disk:abc123"]["resolved_full_path"] == str(media_file)
    assert saved["disk:abc123"]["resolved_filename"] == media_file.name
    assert saved["disk:abc123"]["source_key"] == ""


def test_scan_media_library_infers_source_from_named_source_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    tiktok_dir = media_root / "tiktok" / "fzyahoo.com"
    tiktok_dir.mkdir(parents=True)
    media_file = tiktok_dir / "fzyahoo.com - [7420705673542978833]_1.jpg"
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7420705673542978833"]
    assert entry["source_key"] == "tiktok"
    assert entry["source_pending"] is False
    assert entry["resolved_filename"] == "fzyahoo.com - [7420705673542978833].jpg"


def test_scan_media_library_uses_learned_tiktok_photo_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833",
        "7420705673542978833",
    )
    media_root = tmp_path / "media"
    media_root.mkdir()
    media_file = media_root / "fzyahoo.com - [7420705673542978833]_1.jpg"
    media_file.write_bytes(b"image")

    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: set())
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: {"uploader": "fzyahoo.com"},
    )
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7420705673542978833"]
    assert entry["source_key"] == "tiktok"
    assert entry["source_pending"] is False
    assert entry["resolved_filename"] == "fzyahoo.com - [7420705673542978833].jpg"
    assert entry["resolved_full_path"] == str(media_file)
    assert entry["source_url"] == "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833"


def _learned_youtube_twitter() -> dict:
    learned = learn_download({}, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ")
    learned = learn_download(learned, "https://www.youtube.com/watch?v=Tz-E6i7Mylc", "Tz-E6i7Mylc")
    return learn_download(learned, "https://twitter.com/DohaVT/status/2073635724684054528", "2073635724684054528")


def test_learn_download_derives_url_template():
    assert _learned_youtube_twitter()["youtube"]["templates"][0] == "https://www.youtube.com/watch?v={id}"


def test_learn_download_generalizes_repeated_format_handle_to_var():
    learned = learn_download({}, "https://twitter.com/DohaVT/status/2073635724684054528", "2073635724684054528")
    learned = learn_download(learned, "https://twitter.com/Other/status/1111111111111111111", "1111111111111111111")
    assert learned["twitter"]["templates"][0] == "https://twitter.com/{var}/status/{id}"


def test_learn_download_marks_metadata_proven_creator_segment():
    learned = learn_download(
        {},
        "https://twitter.com/DohaVT/status/2073635724684054528",
        "2073635724684054528",
        {"uploader": "DohaVT"},
    )

    assert learned["twitter"]["templates"][0] == "https://twitter.com/{creator}/status/{id}"


def test_learn_download_marks_exact_username_or_nickname_segment():
    username = learn_download(
        {},
        "https://www.facebook.com/IvanaAlawi/posts/pfbid02QfbMYiPzVyCsQNawcfTYAc3C5vjA54whJwt4kfBSRxNuVZX7QV6e5rS2m7qokJy1l",
        "pfbid02QfbMYiPzVyCsQNawcfTYAc3C5vjA54whJwt4kfBSRxNuVZX7QV6e5rS2m7qokJy1l",
        {"uploader_id": "IvanaAlawi"},
    )
    nickname = learn_download(
        {},
        "https://www.facebook.com/IvanaAlawi/posts/pfbid02QfbMYiPzVyCsQNawcfTYAc3C5vjA54whJwt4kfBSRxNuVZX7QV6e5rS2m7qokJy1l",
        "pfbid02QfbMYiPzVyCsQNawcfTYAc3C5vjA54whJwt4kfBSRxNuVZX7QV6e5rS2m7qokJy1l",
        {"display_name": "IvanaAlawi"},
    )

    assert username["facebook"]["templates"][0] == "https://www.facebook.com/{username}/posts/{id}"
    assert nickname["facebook"]["templates"][0] == "https://www.facebook.com/{nickname}/posts/{id}"


def test_learn_download_trims_seo_query_and_keeps_handle_literal_without_metadata():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
        "7493558766131039489",
    )

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@fzyahoo.com/video/{id}"
    assert (
        reconstruct_url(learned, "tiktok", "7493558766131039489")
        == "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489"
    )


def test_learn_download_keeps_multiple_templates_per_source():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
        "7493558766131039489",
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833",
        "7420705673542978833",
    )

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@fzyahoo.com/video/{id}"
    assert set(learned["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@fzyahoo.com/video/{id}",
        "https://www.tiktok.com/@fzyahoo.com/photo/{id}",
    }


def test_learn_download_does_not_store_url_creator_role_hints():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@moli0n/video/7645876413593128210",
        "7645876413593128210",
        {"uploader": "moli0n", "channel": "Moli Display"},
    )

    assert learned["tiktok"].get("url_field_roles", {}) == {}


def test_describe_learned_segments_keeps_shared_url_creator_generic():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@moli0n/video/7645876413593128210",
        "7645876413593128210",
        {"uploader": "moli0n"},
    )

    described = describe_learned_segments(learned["tiktok"])

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert described["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert described["segments"][0]["label"] == "{creator}"


def test_describe_learned_segments_ignores_url_field_roles_for_display():
    described = describe_learned_segments(
        {
            "templates": ["https://www.tiktok.com/@{creator}/photo/{id}"],
            "url_field_roles": {"username": ["author[uniqueId]"]},
        }
    )

    assert described["templates"][0] == "https://www.tiktok.com/@{creator}/photo/{id}"
    assert described["segments"][0]["label"] == "{creator}"


def test_learn_download_url_creator_role_does_not_create_field_priority():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@moli0n/video/7645876413593128210",
        "7645876413593128210",
        {"uploader": "moli0n"},
    )

    assert learned["tiktok"].get("url_field_roles", {}) == {}


def test_reconstruct_url_candidates_returns_every_learned_route():
    # Both routes come out as concrete candidates; a probe (not a heuristic) picks the real one.
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
        "7493558766131039489",
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833",
        "7420705673542978833",
    )

    candidates = reconstruct_url_candidates(learned, "tiktok", "7420705673542978833", creator="fzyahoo.com")
    assert set(candidates) == {
        "https://www.tiktok.com/@fzyahoo.com/video/7420705673542978833",
        "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833",
    }


def test_worker_marks_new_format_for_deferred_field_learning(monkeypatch):
    url = "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833"

    monkeypatch.setattr(completion_learning_module, "persist_source_format", lambda *args, **kwargs: True)

    assert (
        completion_learning_module._learn_source_format(
            url,
            "fzyahoo.com - [7420705673542978833].jpg",
            source_key="tiktok",
        )
        is True
    )

    monkeypatch.setattr(completion_learning_module, "persist_source_format", lambda *args, **kwargs: False)
    assert (
        completion_learning_module._learn_source_format(
            url,
            "fzyahoo.com - [7420705673542978833].jpg",
            source_key="tiktok",
        )
        is False
    )


def test_learn_download_keeps_descriptive_segment_literal_for_single_sample():
    # URL parts are not promoted to a built-in {{slug}} token: a lone descriptive
    # segment stays literal until a configured URL-part value overrides it.
    learned = learn_download(
        {},
        "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/",
        "4483553",
    )

    assert learned["rule34video"]["templates"][0] == "https://rule34video.com/video/{id}/daiwa-scarlet-suokanawer"
    assert (
        reconstruct_url(learned, "rule34video", "3238394")
        == "https://rule34video.com/video/3238394/daiwa-scarlet-suokanawer"
    )
    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds-minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_reconstruct_url_replaces_literal_segment_with_configured_url_part_value():
    learned = {
        "rule34video": {
            "templates": ["https://rule34video.com/video/{id}/cocolia-rand-sutekimeppou"],
        }
    }

    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds - minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_reconstruct_url_candidates_needs_url_part_value_for_generalized_var():
    learned = learn_download({}, "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489", "7493558766131039489")
    learned = learn_download(learned, "https://www.tiktok.com/@other/video/7420705673542978833", "7420705673542978833")
    assert reconstruct_url_candidates(learned, "tiktok", "123") == []
    assert reconstruct_url_candidates(learned, "tiktok", "") == []
    assert reconstruct_url_candidates(
        learned,
        "tiktok",
        "123",
        slug_values={"path:0": "fzyahoo.com"},
    ) == ["https://www.tiktok.com/@fzyahoo.com/video/123"]


def test_creator_from_url_uses_handle_segment_without_at_sign():
    assert creator_from_url("https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489") == "fzyahoo.com"
    assert (
        creator_from_url("https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489", strip_at=False)
        == "@fzyahoo.com"
    )
    assert creator_from_url("https://x.com/ININIinNINI/status/2073390288501166083") == "ININIinNINI"
    assert creator_from_url("https://www.facebook.com/share/p/1cvLxqzgHA/") == ""


def test_extract_url_part_reads_configured_path_segment():
    # A configured URL part reads its value straight from the canonical URL.
    url = "https://rule34video.com/video/3056158/84-minus8/"
    assert media_id_from_url(url) == "3056158"
    assert extract_url_part(url, "path:2") == "84-minus8"
    assert extract_url_part(url, "path:0") == "video"
    assert extract_url_part(url, "path:9") == ""


def test_extract_url_part_reads_query_value():
    url = "https://example.com/watch?v=dQw4w9WgXcQ&list=PL123"
    assert extract_url_part(url, "query:list") == "PL123"
    assert extract_url_part(url, "query:missing") == ""


def test_describe_learned_segments_marks_id_reserved_and_url_part_selectable():
    learned = learn_download(
        {},
        "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/",
        "4483553",
    )
    described = describe_learned_segments(learned["rule34video"])
    parts = {seg["part"]: seg for seg in described["segments"]}
    assert parts["path:1"]["kind"] == "id" and parts["path:1"]["reserved"] is True
    # The descriptive segment is selectable so the user can name a URL-part token for it.
    assert parts["path:2"]["reserved"] is False
    assert parts["path:2"]["label"] == "daiwa-scarlet-suokanawer"
    # A constant route word is not a useful token, so it stays reserved.
    assert parts["path:0"]["label"] == "video" and parts["path:0"]["reserved"] is True


def test_learn_download_merges_same_pattern_url_part_to_var():
    # Two downloads of the same route differing only in the descriptive URL part generalize
    # to a single {var} template instead of piling up near-duplicate literals.
    learned = learn_download(
        {},
        "https://rule34video.com/video/4497669/cleaning-the-base/",
        "4497669",
    )
    learned = learn_download(
        learned,
        "https://rule34video.com/video/4499077/charlie-s-late-christmas-special-chaosarts/",
        "4499077",
    )
    assert learned["rule34video"]["templates"] == ["https://rule34video.com/video/{id}/{var}"]


def test_learn_download_third_same_pattern_is_idempotent():
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    learned = learn_download(learned, "https://rule34video.com/video/4500123/another-title-here/", "4500123")
    assert learned["rule34video"]["templates"] == ["https://rule34video.com/video/{id}/{var}"]
    assert learned["rule34video"]["samples"] == 3


def test_describe_marks_var_selectable_after_merge():
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    parts = {seg["part"]: seg for seg in describe_learned_segments(learned["rule34video"])["segments"]}
    assert parts["path:0"]["reserved"] is True
    assert parts["path:1"]["kind"] == "id" and parts["path:1"]["reserved"] is True
    assert parts["path:2"]["kind"] == "var" and parts["path:2"]["reserved"] is False


def test_reconstruct_after_merge_needs_url_part_value():
    # Once a URL part generalizes to {var} the link can't be rebuilt from the id alone;
    # a configured URL-part value fills the position, otherwise there is no candidate.
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    assert reconstruct_url(learned, "rule34video", "3238394") == ""
    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds-minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_learn_download_keeps_distinct_route_words_unmerged():
    # Differing route words (video vs photo) are different routes, not a slug, so both
    # templates survive for reconstruction instead of collapsing to {var}.
    learned = learn_download(
        {},
        "https://www.tiktok.com/@a/video/7493558766131039489",
        "7493558766131039489",
        {"uploader": "a"},
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@a/photo/7420705673542978833",
        "7420705673542978833",
        {"uploader": "a"},
    )
    assert set(learned["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@{creator}/video/{id}",
        "https://www.tiktok.com/@{creator}/photo/{id}",
    }


def test_convert_template_quality_uses_selected_label_best_reads_source():
    tmpl = "{{id}}_{{quality}}"
    url = "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/"
    assert convert_template_to_ytdlp(tmpl, url, {"mode": "video", "video_quality": "best"}).endswith("_source")
    assert convert_template_to_ytdlp(tmpl, url, {"mode": "video", "video_quality": "1080p"}).endswith("_1080p")


def test_convert_template_quality_without_selection_keeps_metadata_specifier():
    # Direct callers with no quality threaded through fall back to the delivered format.
    assert "%(format_id" in convert_template_to_ytdlp("{{quality}}", "https://example.com/x")


def test_learn_download_ignores_unknown_host():
    assert learn_download({}, "not a url", "abc123") == {}


def test_url_dedup_key_ignores_route_so_reposts_dedup():
    # The reported bug: a video reconsolidated to /photo must still dedup its /video link.
    photo = url_dedup_key("https://www.tiktok.com/@fzyahoo.com/photo/7615077542189337873")
    video = url_dedup_key("https://www.tiktok.com/@fzyahoo.com/video/7615077542189337873")
    assert photo == video == "tiktok#7615077542189337873"


def test_url_dedup_key_separates_different_posts():
    a = url_dedup_key("https://www.tiktok.com/@fzyahoo.com/photo/7615077542189337873")
    b = url_dedup_key("https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833")
    assert a != b


def test_media_id_from_url_reads_id_without_prior_knowledge():
    assert media_id_from_url("https://www.tiktok.com/@a/video/7615077542189337873") == "7615077542189337873"
    assert media_id_from_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_correct_reconstructed_url_adopts_pasted_link_for_disk_entry(monkeypatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(operations_module, "save_history_entry_row", lambda tid, p: saved.update({tid: p}))
    entry = {"engine": "disk", "source_url": "https://www.tiktok.com/@a/photo/7615077542189337873"}
    real_url = "https://www.tiktok.com/@a/video/7615077542189337873"

    out = operations_module._correct_reconstructed_url("disk:7615077542189337873", entry, real_url)

    assert out["source_url"] == real_url
    assert saved["disk:7615077542189337873"]["source_url"] == real_url


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


def test_correct_reconstructed_url_leaves_real_download_untouched(monkeypatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(operations_module, "save_history_entry_row", lambda tid, p: saved.update({tid: p}))
    real_url = "https://www.tiktok.com/@a/video/7615077542189337873"
    entry = {"engine": "ytdlp", "source_url": real_url}

    out = operations_module._correct_reconstructed_url("ytdlp:abc", entry, "https://www.tiktok.com/@a/photo/7615077542189337873")

    assert out["source_url"] == real_url  # a real download's link is authoritative; never overwritten
    assert saved == {}


def test_prune_disk_shadows_drops_disk_duplicate_of_real_download(monkeypatch):
    removed: list[str] = []
    monkeypatch.setattr(scan_module, "remove_history_record", lambda tid: removed.append(tid))
    records = {
        "ytdlp:abc": {"source_url": "https://www.tiktok.com/@a/video/7615077542189337873", "media_id": ""},
        "disk:7615077542189337873": {
            "engine": "disk",
            "media_id": "7615077542189337873",
            "source_url": "https://www.tiktok.com/@a/photo/7615077542189337873",
        },
    }
    real = scan_module._real_download_media_ids(records)

    scan_module._prune_disk_shadows(records, real)

    assert removed == ["disk:7615077542189337873"]
    assert "disk:7615077542189337873" not in records


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
    index = scan_module._source_location_index(
        [("youtube", "https://www.youtube.com/watch?v={id}", str(folder))]
    )

    source_key, pending, _, format_template = scan_module.infer_disk_source(
        media_file, "2073635724684054528", index, _learned_youtube_twitter()
    )

    assert source_key == "twitter"
    assert pending is False
    # The folder's format is only a hint for its own source; a vetoed folder drops it.
    assert format_template == ""


def test_guess_sources_tolerates_base64url_separators():
    # Learned only from ids without "-"/"_"; new youtube ids carrying them must still match.
    learned = learn_download({}, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ")
    assert guess_sources(learned, "K1-BVtsHrOY") == ["youtube"]
    assert guess_sources(learned, "_F5vcIlr9bs") == ["youtube"]


def test_learn_media_id_seeds_shape_for_confirmed_source():
    learned = learn_media_id({}, "youtube", "dQw4w9WgXcQ")
    assert learned["youtube"]["id_min"] == 11
    assert learned["youtube"]["id_max"] == 11
    assert guess_sources(learned, "_F5vcIlr9bs") == ["youtube"]


def test_learn_media_id_ignores_removed_and_empty_source():
    assert learn_media_id({}, "others", "dQw4w9WgXcQ") == {}
    assert learn_media_id({}, "youtube", "") == {}


def test_infer_disk_source_ambiguous_when_multiple_learned_match(tmp_path: Path):
    media_file = tmp_path / "Clip [1111111111111111111].mp4"
    media_file.write_bytes(b"video")
    learned = learn_download({}, "https://twitter.com/A/status/2073635724684054528", "2073635724684054528")
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
    media_file = folder / "Clip [dQw4w9WgXcQ].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index(
        [("youtube", "https://www.youtube.com/watch?v={id}", str(folder))]
    )

    source_key, pending, candidates, _ = scan_module.infer_disk_source(
        media_file, "dQw4w9WgXcQ", index, _learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert pending is False
    assert candidates == []


def test_infer_disk_source_reports_the_folder_format(tmp_path: Path):
    shorts = tmp_path / "yt-shorts"
    shorts.mkdir()
    media_file = shorts / "Clip [dQw4w9WgXcQ].mp4"
    media_file.write_bytes(b"video")
    index = scan_module._source_location_index(
        [
            ("youtube", "https://www.youtube.com/watch?v={id}", str(tmp_path / "yt")),
            ("youtube", "https://www.youtube.com/shorts/{id}", str(shorts)),
        ]
    )

    source_key, _, _, format_template = scan_module.infer_disk_source(
        media_file, "dQw4w9WgXcQ", index, _learned_youtube_twitter()
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "_scan_probe_metadata", lambda url, with_cookies=False: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:7123456789012345678"]
    assert entry["source_key"] == ""
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
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: [])
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:BV1xx411c7mD"]
    assert entry["source_key"] == "bilibili"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://www.bilibili.com/video/BV1xx411c7mD"


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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:3238394"]
    assert entry["source_key"] == "rule34video"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://rule34video.com/video/3238394/wsds-minus8"


def _scan_locations(source_key: str, folder: Path, format_template: str = "https://www.youtube.com/watch?v={id}"):
    return [(source_key, format_template, str(folder))]


def _patch_scan_common(monkeypatch: pytest.MonkeyPatch, saved: dict[str, dict], platform_dir: Path) -> None:
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_rows", lambda: _scan_locations("youtube", platform_dir))
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)


def test_scan_probes_manual_file_in_configured_username_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Manual file, no record yet: probe and honor the settings order (channel_id first),
    # even though channel_id is an opaque id the handle heuristics would reject.
    learned = learn_download({}, "https://www.youtube.com/watch?v=abc123", "abc123")
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Some Channel"
    creator_dir.mkdir(parents=True)
    (creator_dir / "Soft Light [abc123].mp4").write_bytes(b"video")

    saved: dict[str, dict] = {}
    _patch_scan_common(monkeypatch, saved, platform_dir)
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_field_roles_map",
        lambda: {"youtube": {"username": ["channel_id", "uploader"]}},
    )
    probed: list[str] = []
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: probed.append(url) or {"uploader": "Mili", "channel_id": "UCopaque123"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "UCopaque123"
    assert saved["disk:abc123"]["source_url"] == "https://www.youtube.com/watch?v=abc123"
    assert probed == ["https://www.youtube.com/watch?v=abc123"]


def test_scan_reconstructs_creator_route_when_probe_matches_url_creator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_id = "7645876413593128210"
    learned = learn_download(
        {},
        f"https://www.tiktok.com/@moli0n/video/{media_id}",
        media_id,
        {"uploader": "moli0n"},
    )
    media_root = tmp_path / "media"
    platform_dir = media_root / "tiktok"
    creator_dir = platform_dir / "moli0n"
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
    monkeypatch.setattr(scan_module, "_scan_field_roles_map", lambda: {"tiktok": {"username": ["uploader"]}})
    monkeypatch.setattr(scan_module, "_scan_probe_metadata", lambda url, *, with_cookies=False: {"uploader": "moli0n"})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved[f"disk:{media_id}"]["creator"] == "moli0n"
    assert saved[f"disk:{media_id}"]["source_url"] == f"https://www.tiktok.com/@moli0n/video/{media_id}"


def test_scan_skips_creator_route_when_probe_field_mismatches_url_creator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_id = "7645876413593128210"
    learned = learn_download(
        {},
        f"https://www.tiktok.com/@moli0n/video/{media_id}",
        media_id,
        {"uploader": "moli0n"},
    )
    media_root = tmp_path / "media"
    platform_dir = media_root / "tiktok"
    creator_dir = platform_dir / "wrongname"
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
    monkeypatch.setattr(scan_module, "_scan_field_roles_map", lambda: {"tiktok": {"username": ["uploader"]}})
    monkeypatch.setattr(scan_module, "_scan_probe_metadata", lambda url, *, with_cookies=False: {"uploader": "moli0n"})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved[f"disk:{media_id}"]["creator"] == "wrongname"
    assert saved[f"disk:{media_id}"]["source_url"] == ""


def test_scan_probe_uses_nickname_order_when_folder_token_is_nickname(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Folder token decides the role: {{nickname}} -> walk the nickname field order.
    learned = learn_download({}, "https://www.youtube.com/watch?v=abc123", "abc123")
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Some Channel"
    creator_dir.mkdir(parents=True)
    (creator_dir / "Soft Light [abc123].mp4").write_bytes(b"video")

    saved: dict[str, dict] = {}
    _patch_scan_common(monkeypatch, saved, platform_dir)
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{nickname}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_field_roles_map",
        lambda: {"youtube": {"username": ["channel_id"], "nickname": ["uploader", "channel"]}},
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: {"channel_id": "UCopaque123", "uploader": "Mili Display", "channel": "Mili"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Mili Display"


def test_scan_skips_creator_probe_for_scraper_backed_template_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    learned = learn_download({}, "https://www.youtube.com/watch?v=abc123", "abc123")
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Scraped Artist"
    creator_dir.mkdir(parents=True)
    (creator_dir / "Soft Light [abc123].mp4").write_bytes(b"video")

    saved: dict[str, dict] = {}
    _patch_scan_common(monkeypatch, saved, platform_dir)
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "_scan_token_role_map", lambda: {"youtube": {"artist": "username"}})
    monkeypatch.setattr(scan_module, "_scan_scrape_rule_tokens", lambda: {"youtube": {"artist"}})
    monkeypatch.setattr(
        scan_module,
        "_scan_field_roles_map",
        lambda: {"youtube": {"username": ["scraper[artist]", "channel"]}},
    )

    def fail_probe(url, *, with_cookies=False):
        raise AssertionError("scraper-backed username must not probe engine metadata")

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", fail_probe)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Scraped Artist"
    assert saved["disk:abc123"]["source_url"] == "https://www.youtube.com/watch?v=abc123"


def test_scan_keeps_creator_probe_when_scraper_backed_role_is_not_top(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    learned = learn_download({}, "https://www.youtube.com/watch?v=abc123", "abc123")
    media_root = tmp_path / "media"
    platform_dir = media_root / "youtube"
    creator_dir = platform_dir / "Scraped Artist"
    creator_dir.mkdir(parents=True)
    (creator_dir / "Soft Light [abc123].mp4").write_bytes(b"video")

    saved: dict[str, dict] = {}
    _patch_scan_common(monkeypatch, saved, platform_dir)
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "_scan_token_role_map", lambda: {"youtube": {"artist": "username"}})
    monkeypatch.setattr(scan_module, "_scan_scrape_rule_tokens", lambda: {"youtube": {"artist"}})
    monkeypatch.setattr(
        scan_module,
        "_scan_field_roles_map",
        lambda: {"youtube": {"username": ["channel", "scraper[artist]"]}},
    )
    probed: list[str] = []
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: probed.append(url) or {"channel": "Probed Channel"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["creator"] == "Probed Channel"
    assert probed == ["https://www.youtube.com/watch?v=abc123"]


def test_scan_skips_probe_when_creator_already_resolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Details already in the system: keep the resolved creator/url, never re-probe.
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    def _fail_probe(url, *, with_cookies=False):
        raise AssertionError("probe must not run for an already-resolved file")

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", _fail_probe)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed_history.append(task_id))

    result = scan_module.scan_media_library([tmp_path])

    assert result == {
        "checked": 1,
        "missing": 1,
        "added": 0,
        "unchanged": 0,
        "renamed": 0,
        "rename_failed": 0,
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed.append(task_id))

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "renamed": 0,
        "rename_failed": 0,
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed.append(task_id))

    result = scan_module.scan_media_library([tmp_path])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "renamed": 0,
        "rename_failed": 0,
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed.append(task_id))

    result = scan_module.scan_media_library([media_root])

    assert result == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "renamed": 0,
        "rename_failed": 0,
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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed.append(task_id))
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


def test_count_tasks_and_by_menu():
    tasks = [
        {"status": "pending", "source_key": "youtube"},
        {"status": "running", "source_key": "youtube"},
        {"status": "completed", "source_key": "tiktok"},
        {"status": "failed", "source_key": ""},
    ]
    counts = count_tasks(tasks)
    assert counts == {"queued": 1, "running": 1, "completed": 1, "failed": 1}
    by_menu = counts_by_menu(tasks)
    assert by_menu["all"]["queued"] == 1
    assert by_menu["all"]["failed"] == 1
    assert by_menu["youtube"]["running"] == 1
    assert by_menu["tiktok"]["completed"] == 1
    assert "" not in by_menu
    assert "others" not in by_menu


def test_counts_by_menu_does_not_seed_empty_unresolved(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        serializers_module,
        "get_effective_source_profiles",
        lambda: [],
    )

    by_menu = counts_by_menu([{"status": "completed", "source_key": "youtube"}])

    assert "others" not in by_menu
    assert by_menu["youtube"]["completed"] == 1


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

    path, filename, cleanup_path = operations_module.resolve_task_file("gallerydl:abc123")

    assert path == media_file
    assert filename == "clip [abc123].jpg"
    assert cleanup_path is None


def test_resolve_task_file_zips_numbered_gallerydl_siblings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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

    archive_path, filename, cleanup_path = operations_module.resolve_task_file(task_id)

    try:
        assert filename == "Creator - Cap [abc123].zip"
        assert cleanup_path == archive_path
        with zipfile.ZipFile(archive_path) as archive:
            assert archive.namelist() == [first.name, second.name]
    finally:
        archive_path.unlink(missing_ok=True)


def test_clean_social_title_default_strips_hashtags_and_metrics():
    result = clean_social_title("Cool clip #fun #viral 1.2M views", creator="alice")
    assert "#" not in result
    assert "views" not in result
    assert result.startswith("Cool clip")


def test_clean_social_title_respects_disabled_flags():
    result = clean_social_title(
        "Cool clip #fun 1.2M views",
        creator="alice",
        cleaning={"strip_hashtags": False, "strip_metrics": False},
    )
    assert "#fun" in result
    assert "views" in result


def test_clean_template_filename_shorten_defaults_off_and_keeps_long_title():
    long_title = "This is a very long title that would normally be shortened. " * 5
    name = f"alice - {long_title} [abc123].mp4"
    template = "{{username}} - {{title}} [{{id}}]"
    default = clean_template_filename(name, template, creator="alice", title=long_title)
    shortened = clean_template_filename(name, template, creator="alice", title=long_title, cleaning={"shorten": True})
    full = clean_template_filename(name, template, creator="alice", title=long_title, cleaning={"shorten": False})
    assert default == full
    assert len(shortened) < len(full)
    assert long_title in full


def test_sanitize_lookalike_slashes():
    assert sanitize_filename_component("AC⧸DC") == "AC_DC"
    assert sanitize_filename_component("Folder⧹Subfolder") == "Folder_Subfolder"
    assert sanitize_path_literal("AC⧸DC") == "AC_DC"
    assert sanitize_path_literal("Folder⧹Subfolder") == "Folder_Subfolder"


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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

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


def _incremental_scan_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A scan wired to a real history store, so a rescan sees the first pass's rows."""
    media_root = tmp_path / "media"
    media_root.mkdir()
    rows: dict[str, dict] = {}
    resolved: list[str] = []

    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(rows)})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda batch: rows.update(dict(batch)))
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: rows.pop(task_id, None))
    monkeypatch.setattr(scan_module, "resolution_revision", lambda: "rev-1")
    seeded: set[str] = set()
    monkeypatch.setattr(scan_module, "seeded_download_ids", lambda: set(seeded))
    monkeypatch.setattr(scan_module, "mark_downloads_seeded", seeded.update)

    real_parse = scan_module._parse_media_fields

    def counting_parse(path, pattern):
        resolved.append(str(path))
        return real_parse(path, pattern)

    monkeypatch.setattr(scan_module, "_parse_media_fields", counting_parse)
    return media_root, rows, resolved


def test_rescan_skips_files_that_did_not_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, resolved = _incremental_scan_env(tmp_path, monkeypatch)
    for index in range(4):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")

    first = scan_module.scan_media_library([media_root])
    resolved.clear()
    second = scan_module.scan_media_library([media_root])

    assert first["added"] == 4
    assert first["unchanged"] == 0
    # Second pass resolves nothing: same bytes, same rules.
    assert second == {
        "checked": 4,
        "missing": 0,
        "added": 0,
        "unchanged": 4,
        "renamed": 0,
        "rename_failed": 0,
        "needs_resolve": 0,
    }
    assert resolved == []
    assert len(rows) == 4


def test_rescan_reresolves_a_file_whose_contents_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, _rows, resolved = _incremental_scan_env(tmp_path, monkeypatch)
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


def test_rescan_reresolves_everything_when_the_rules_improve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Learning a new format or changing settings is exactly when a disk row may
    # resolve better than last time, so that is when the work is worth redoing.
    media_root, _rows, resolved = _incremental_scan_env(tmp_path, monkeypatch)
    for index in range(3):
        (media_root / f"Creator - Clip [vid{index}].mp4").write_bytes(b"video")

    scan_module.scan_media_library([media_root])
    resolved.clear()
    monkeypatch.setattr(scan_module, "resolution_revision", lambda: "rev-2")
    result = scan_module.scan_media_library([media_root])

    assert len(resolved) == 3
    assert result["unchanged"] == 0


def test_seeded_learning_is_not_reanalyzed_when_nothing_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Re-deriving route templates from every past download is the most expensive
    # part of a scan and returns the same answer until the downloads change.
    media_root, _rows, _resolved = _incremental_scan_env(tmp_path, monkeypatch)
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")
    history = {
        f"gallerydl:{index}": {
            "source_url": f"https://example.test/@creator/video/{index}",
            "media_id": str(index),
            "status": "completed",
        }
        for index in range(5)
    }
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(history)})

    analyzed: list[str] = []
    real_update = scan_module.update_learned_formats_with_download

    def counting_update(learned, source_url, media_id):
        analyzed.append(source_url)
        return real_update(learned, source_url, media_id)

    monkeypatch.setattr(scan_module, "update_learned_formats_with_download", counting_update)
    monkeypatch.setattr(scan_module, "_drop_missing_records", lambda records, seen_paths, pacer=None: (0, 0))

    scan_module.scan_media_library([media_root])
    first_pass = len(analyzed)
    analyzed.clear()
    scan_module.scan_media_library([media_root])

    assert first_pass == 5
    assert analyzed == []


def test_seeded_learning_reads_only_the_download_that_was_added(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root, _rows, _resolved = _incremental_scan_env(tmp_path, monkeypatch)
    (media_root / "Creator - Clip [vid1].mp4").write_bytes(b"video")
    history = {
        "gallerydl:1": {"source_url": "https://example.test/@creator/video/1", "media_id": "1"},
    }
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(history)})
    monkeypatch.setattr(scan_module, "_drop_missing_records", lambda records, seen_paths, pacer=None: (0, 0))

    analyzed: list[str] = []
    real_update = scan_module.update_learned_formats_with_download
    monkeypatch.setattr(
        scan_module,
        "update_learned_formats_with_download",
        lambda learned, url, media_id: (analyzed.append(url), real_update(learned, url, media_id))[1],
    )

    scan_module.scan_media_library([media_root])
    analyzed.clear()
    history["gallerydl:2"] = {"source_url": "https://example.test/@other/video/2", "media_id": "2"}
    scan_module.scan_media_library([media_root])

    # Only the new download is analyzed; the one already folded in is not re-read.
    assert analyzed == ["https://example.test/@other/video/2"]


@pytest.mark.parametrize("placeholder", ["None", "unknown"])
def test_a_download_filed_under_a_placeholder_creator_moves_to_the_root(tmp_path: Path, placeholder: str):
    # The engine's own folder token came back null, so it invented a directory.
    stranded = tmp_path / placeholder
    stranded.mkdir()
    path = stranded / "Clip [abc123].mp4"
    path.write_bytes(b"video")

    final_path = completion_module._move_group_to_template_folder(
        path,
        tmp_path,
        {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        "",
        "abc123",
    )

    assert final_path == tmp_path / path.name
    assert final_path.is_file()
    assert not stranded.exists()


def test_a_real_creator_folder_is_left_alone(tmp_path: Path):
    kept = tmp_path / "Creator"
    kept.mkdir()
    path = kept / "Clip [abc123].mp4"
    path.write_bytes(b"video")

    final_path = completion_module._move_group_to_template_folder(
        path,
        tmp_path,
        {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        "",
        "abc123",
    )

    assert final_path == path
    assert path.is_file()
