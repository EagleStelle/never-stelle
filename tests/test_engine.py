from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.app.domains.downloads.enrich as enrich
import backend.app.domains.downloads.formats as formats
import backend.app.domains.downloads.gallerydl as gallerydl
import backend.app.domains.downloads.ytdlp as ytdlp
from backend.app.domains.downloads import engine_by_name, engine_for_task, select_engine
from backend.app.domains.downloads.constants import (
    PROGRESS_RE,
    audio_format_selector,
    container_acodec_filter,
    container_vcodec_filter,
    default_quality_selection,
    merge_output_format,
    normalize_post_processing,
    normalize_quality_selection,
    post_processing_requested,
    quality_options,
    template_tokens,
    video_format_selector,
)
from backend.app.domains.downloads.engine import all_engines
from backend.app.domains.downloads.workers.execution import _looks_unsupported, _should_try_next_engine
from backend.app.domains.downloads.workers.progress import (
    DOWNLOAD_END,
    FINALIZE_END,
    PREPARE_END,
    TaskProgress,
)
from backend.app.domains.downloads.workers.runner import _count_progress


def _gallerydl_postprocessors(cmd: list[str]) -> list[dict[str, object]]:
    for index, value in enumerate(cmd[:-1]):
        if value == "-o" and cmd[index + 1].startswith("postprocessors="):
            return json.loads(cmd[index + 1].partition("=")[2])
    return []


def _gallerydl_raw_option(cmd: list[str], key: str) -> object:
    prefix = f"{key}="
    for index, value in enumerate(cmd[:-1]):
        if value == "-o" and cmd[index + 1].startswith(prefix):
            return json.loads(cmd[index + 1].partition("=")[2])
    return None


def test_normalize_quality_selection_defaults_and_validates():
    assert default_quality_selection() == {
        "mode": "video",
        "video_quality": "best",
        "video_container": "auto",
        "video_codec": "auto",
        "video_audio_codec": "auto",
        "audio_format": "auto",
        "audio_bitrate": "best",
    }
    assert normalize_quality_selection(None) == default_quality_selection()
    assert normalize_quality_selection(
        {"mode": "bogus", "video_quality": "bogus", "video_container": "iso", "video_codec": "xyz"}
    ) == default_quality_selection()
    selection = normalize_quality_selection(
        {
            "mode": "audio",
            "audio_format": "OPUS",
            "audio_bitrate": "192",
            "video_quality": "720p",
            "video_container": "MKV",
            "video_codec": "AV1",
            "video_audio_codec": "OPUS",
        }
    )
    assert selection == {
        "mode": "audio",
        "video_quality": "720p",
        "video_container": "mkv",
        "video_codec": "av1",
        "video_audio_codec": "opus",
        "audio_format": "opus",
        "audio_bitrate": "192",
    }
    # A container-compatible codec is kept; an incompatible one (VP9 can't play in MP4)
    # falls back to Auto so it never muxes an unplayable stream.
    assert normalize_quality_selection({"video_container": "mkv", "video_codec": "vp9"})["video_codec"] == "vp9"
    assert normalize_quality_selection({"video_container": "mp4", "video_codec": "vp9"})["video_codec"] == "auto"
    assert normalize_quality_selection({"video_container": "webm", "video_codec": "h264"})["video_codec"] == "auto"
    assert (
        normalize_quality_selection({"video_container": "mkv", "video_audio_codec": "opus"})["video_audio_codec"]
        == "opus"
    )
    assert (
        normalize_quality_selection({"video_container": "mp4", "video_audio_codec": "opus"})["video_audio_codec"]
        == "auto"
    )


def test_normalize_post_processing_defaults_and_validates():
    assert normalize_post_processing(None) == {
        "metadata": "off",
        "subtitles": "off",
        "automatic_subtitles": "off",
        "chapters": "off",
        "thumbnail": "off",
        "subtitle_languages": [],
    }
    assert normalize_post_processing(
        {"metadata": "embed", "chapters": "both", "thumbnail": "nonsense"}
    ) == {
        "metadata": "embed",
        "subtitles": "off",
        "automatic_subtitles": "off",
        "chapters": "both",
        "thumbnail": "off",
        "subtitle_languages": [],
    }
    assert normalize_post_processing({"subtitle_languages": " EN , ja ,en, "}) == {
        "metadata": "off",
        "subtitles": "off",
        "automatic_subtitles": "off",
        "chapters": "off",
        "thumbnail": "off",
        "subtitle_languages": ["en", "ja"],
    }
    assert not post_processing_requested({"metadata": "off"})
    assert post_processing_requested({"automatic_subtitles": "sidecar"})
    assert post_processing_requested({"chapters": "both"})


def test_normalize_post_processing_rejects_the_retired_boolean_shape():
    # m0005 converts stored rows; nothing reads the old form at runtime.
    retired = {"metadata": True, "subtitles": True, "save_as": "embed"}
    assert not post_processing_requested(retired)
    assert normalize_post_processing(retired)["metadata"] == "off"
    assert "save_as" not in normalize_post_processing(retired)


def test_container_ffmpeg_build_includes_chapter_metadata_demuxer():
    dockerfile = (Path(__file__).parents[1] / "Dockerfile").read_text(encoding="utf-8")
    demuxer_line = next(
        line for line in dockerfile.splitlines() if line.strip().startswith("--enable-demuxer=")
    )
    enabled = set(demuxer_line.partition("=")[2].rstrip(" \\").split(","))
    assert "ffmetadata" in enabled


def test_metadata_sidecar_options_cover_gallerydl_and_integrated_ytdlp():
    processing = {"metadata": "sidecar"}
    cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        "\x1f{id}.{extension}",
        post_processing=processing,
    )

    postprocessors = _gallerydl_postprocessors(cmd)
    assert postprocessors[-1]["name"] == "metadata"
    assert postprocessors[-1]["private"] is True
    assert not any("writeinfojson" in part for part in cmd)
    assert not any("getcomments" in part for part in cmd)

    fallback = ytdlp.build_ytdlp_command(
        "https://example.test/post/1",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        post_processing=processing,
    )
    assert "--write-info-json" in fallback
    assert "--no-clean-info-json" in fallback
    assert "--write-comments" not in fallback


def test_metadata_embed_waits_for_the_app_finalization_stage():
    processing = {"metadata": "embed"}
    cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        "\x1f{id}.{extension}",
        post_processing=processing,
    )
    assert not any("FFmpegMetadata" in part for part in cmd)
    assert not any("writeinfojson" in part for part in cmd)
    postprocessors = _gallerydl_postprocessors(cmd)
    assert postprocessors[-1]["name"] == "metadata"
    assert postprocessors[-1]["private"] is True

    fallback = ytdlp.build_ytdlp_command(
        "https://example.test/post/1",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        post_processing=processing,
    )
    assert "--write-info-json" in fallback
    assert "--no-clean-info-json" in fallback
    assert "--embed-metadata" not in fallback
    assert "--embed-chapters" not in fallback
    assert "--embed-info-json" not in fallback


def test_thumbnail_options_capture_extractor_payload_for_finalization():
    processing = {"thumbnail": "sidecar"}
    cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        "\x1f{id}.{extension}",
        post_processing=processing,
    )
    postprocessors = _gallerydl_postprocessors(cmd)
    assert postprocessors[-1]["name"] == "metadata"
    assert postprocessors[-1]["private"] is True

    fallback = ytdlp.build_ytdlp_command(
        "https://example.test/post/1",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        post_processing=processing,
    )
    assert "--write-info-json" in fallback
    assert "--no-clean-info-json" in fallback
    assert "--write-thumbnail" not in fallback
    assert "--embed-thumbnail" not in fallback


def test_audio_metadata_and_thumbnail_request_youtube_music_cover_metadata():
    processing = {"metadata": "embed", "thumbnail": "embed"}
    quality = {"mode": "audio", "audio_format": "mp3"}
    both = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=Yb9FzUPpk0Y",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        quality=quality,
        post_processing=processing,
    )
    assert both[both.index("--extractor-args") + 1] == "youtube:player_client=default,web_music"

    delegated = gallerydl.build_gallerydl_command(
        "https://www.youtube.com/watch?v=Yb9FzUPpk0Y",
        "/media",
        "\x1f{id}.{extension}",
        metadata_sidecar="/scratch/task/downloads.tsv",
        quality=quality,
        post_processing=processing,
    )
    expected_args = {"youtube": {"player_client": ["default", "web_music"]}}
    assert _gallerydl_raw_option(
        delegated, "downloader.ytdl.raw-options.extractor_args"
    ) == expected_args
    assert _gallerydl_raw_option(
        delegated, "extractor.ytdl.raw-options.extractor_args"
    ) == expected_args
    delegated_processors = _gallerydl_raw_option(
        delegated, "downloader.ytdl.raw-options.postprocessors"
    )
    assert all(processor.get("key") != "NeverStelleCapture" for processor in delegated_processors)
    assert _gallerydl_postprocessors(delegated)[-1]["private"] is True

    thumbnail_only = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=Yb9FzUPpk0Y",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        quality={"mode": "audio", "audio_format": "mp3"},
        post_processing={"thumbnail": "embed"},
    )
    assert "--extractor-args" not in thumbnail_only


def test_subtitle_options_capture_extractor_payload_for_finalization():
    processing = {"subtitles": "embed", "automatic_subtitles": "embed"}
    cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        "\x1f{id}.{extension}",
        metadata_sidecar="/scratch/task/downloads.tsv",
        post_processing=processing,
    )
    postprocessors = _gallerydl_postprocessors(cmd)
    assert postprocessors[-1]["name"] == "metadata"
    assert postprocessors[-1]["private"] is True
    expected_capture = {
        "key": "NeverStelleCapture",
        "directory": "/scratch/task/extractor",
    }
    assert _gallerydl_raw_option(cmd, "downloader.ytdl.raw-options.postprocessors")[-1] == expected_capture
    assert _gallerydl_raw_option(cmd, "extractor.ytdl.raw-options.postprocessors")[-1] == expected_capture

    fallback = ytdlp.build_ytdlp_command(
        "https://example.test/post/1",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        post_processing=processing,
    )
    assert "--write-info-json" in fallback
    assert "--no-clean-info-json" in fallback
    assert "--write-subs" not in fallback
    assert "--write-auto-subs" not in fallback
    assert "--embed-subs" not in fallback


def test_chapter_options_capture_extractor_payload_for_finalization():
    processing = {"chapters": "embed"}
    cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        "\x1f{id}.{extension}",
        metadata_sidecar="/scratch/task/downloads.tsv",
        post_processing=processing,
    )
    expected_capture = {
        "key": "NeverStelleCapture",
        "directory": "/scratch/task/extractor",
    }
    assert _gallerydl_raw_option(cmd, "downloader.ytdl.raw-options.postprocessors")[-1] == expected_capture
    assert _gallerydl_raw_option(cmd, "extractor.ytdl.raw-options.postprocessors")[-1] == expected_capture

    fallback = ytdlp.build_ytdlp_command(
        "https://example.test/post/1",
        "/usr/bin/ffmpeg",
        "/media/%(id)s.%(ext)s",
        post_processing=processing,
    )
    assert "--write-info-json" in fallback
    assert "--no-clean-info-json" in fallback
    assert "--embed-chapters" not in fallback


def test_quality_options_expose_all_pickers():
    options = quality_options()
    assert {o["key"] for o in options["video"]} == {
        "best",
        "2160p60",
        "1440p60",
        "1080p60",
        "1080p",
        "720p",
        "480p",
    }
    assert {o["key"] for o in options["video_containers"]} == {"auto", "mp4", "mkv", "webm"}
    # Containers expose their playable codecs so the UI can hide incompatible picks (e.g. VP9 in MP4).
    mp4 = next(o for o in options["video_containers"] if o["key"] == "mp4")
    assert "vp9" not in mp4["codecs"] and "h264" in mp4["codecs"]
    webm = next(o for o in options["video_containers"] if o["key"] == "webm")
    assert set(webm["embed_capabilities"]) == {
        "metadata",
        "subtitles",
        "automatic_subtitles",
        "chapters",
    }
    aac = next(o for o in options["audio_formats"] if o["key"] == "aac")
    assert aac["embed_capabilities"] == []
    mp3 = next(o for o in options["audio_formats"] if o["key"] == "mp3")
    assert set(mp3["embed_capabilities"]) == {"metadata", "chapters", "thumbnail"}
    assert "aac" in mp4["audio_codecs"] and "opus" not in mp4["audio_codecs"]
    assert {o["key"] for o in options["video_codecs"]} == {"auto", "av1", "vp9", "h264", "h265"}
    assert {o["key"] for o in options["video_audio_codecs"]} == {"auto", "aac", "opus", "mp3", "flac"}
    assert {o["key"] for o in options["audio_formats"]} == {"auto", "mp3", "m4a", "opus", "aac", "flac", "wav"}
    assert all("codecs" not in o for o in options["audio_formats"])
    assert "audio_codecs" not in options
    assert {o["key"] for o in options["audio_bitrates"]} == {"best", "320", "192", "128"}


def test_template_tokens_expose_supported_public_tokens_only():
    assert [token["key"] for token in template_tokens()] == [
        "username",
        "nickname",
        "title",
        "id",
        "quality",
    ]


def test_container_vcodec_filter_restricts_incompatible_codecs():
    mp4 = container_vcodec_filter("mp4")
    assert "vp09" not in mp4 and "vp9" not in mp4
    assert "av01" in mp4 and "avc1" in mp4
    webm = container_vcodec_filter("webm")
    assert "avc1" not in webm and "vp09" in webm
    assert container_vcodec_filter("mkv") == ""


def test_merge_output_format_falls_back_to_mkv():
    # Every container prefers itself but drops to MKV (plays any codec) when the delivered
    # streams can't go in playably — so Auto/recovery paths never yield an unplayable file.
    assert merge_output_format("auto") == "mp4/mkv/webm"
    assert merge_output_format("mp4") == "mp4/mkv"
    assert merge_output_format("webm") == "webm/mkv"
    assert merge_output_format("mkv") == "mkv"
    assert merge_output_format("bogus") == "mp4/mkv/webm"


def test_container_acodec_filter_restricts_incompatible_audio():
    assert "opus" in container_acodec_filter("webm")
    assert "mp4a" not in container_acodec_filter("webm")
    assert "mp4a" in container_acodec_filter("mp4")
    assert container_acodec_filter("mkv") == ""


def test_video_format_selector_carries_filters_on_every_branch():
    fmt = video_format_selector("720p", "mp4")
    strict = (
        f"bestvideo*[height<=720]{container_vcodec_filter('mp4')}"
        f"+bestaudio{container_acodec_filter('mp4')}"
        f"/best[height<=720]{container_vcodec_filter('mp4')}{container_acodec_filter('mp4')}"
    )
    assert fmt.startswith(strict)
    assert "/best[height<=720][ext=mp4]/" in fmt
    assert fmt.endswith("/best")
    assert video_format_selector("best", "mkv") == "bestvideo*+bestaudio/best"


def test_audio_format_selector_uses_native_audio_filters():
    assert audio_format_selector("opus", "192") == (
        "bestaudio[acodec~='^opus'][abr<=192]/"
        "bestaudio[ext=opus][abr<=192]/"
        "bestaudio[ext=webm][acodec~='^opus'][abr<=192]/"
        "bestaudio[abr<=192]"
    )
    assert audio_format_selector("flac", "320") == "bestaudio[ext=flac]/bestaudio[acodec~='^flac']/bestaudio"


def test_video_format_selector_leads_with_separate_streams():
    # A muxed `best` first would cap YouTube at its 720p progressive rendition.
    assert video_format_selector("best", "auto") == "bestvideo*+bestaudio/best"
    assert video_format_selector("1080p", "auto").startswith("bestvideo*[height<=1080]+bestaudio/")


def test_video_format_selector_falls_back_from_frame_rate_then_resolution():
    fmt = video_format_selector("2160p60", "auto")
    assert fmt == (
        "bestvideo*[height<=2160][fps>=59]+bestaudio/"
        "best[height<=2160][fps>=59]/"
        "bestvideo*[height<=2160]+bestaudio/"
        "best[height<=2160]/"
        "bestvideo*+bestaudio/"
        "best"
    )


def test_auto_selectors_prefer_available_low_processing_outputs():
    assert audio_format_selector("auto", "best") == (
        "bestaudio[ext=m4a]/"
        "bestaudio[acodec~='^(mp4a|aac)']/"
        "bestaudio[acodec~='^opus']/"
        "bestaudio[ext=opus]/"
        "bestaudio[ext=webm][acodec~='^opus']/"
        "bestaudio[ext=mp3]/"
        "bestaudio[acodec~='^(mp3|mpga)']/"
        "bestaudio[ext=flac]/"
        "bestaudio[ext=wav]/"
        "bestaudio"
    )


def test_ytdlp_command_filters_format_to_container_codecs():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_container": "webm", "video_codec": "vp9"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "mkv"
    assert cmd[cmd.index("--format") + 1] == video_format_selector("best", "webm", "vp9")
    assert cmd[cmd.index("-S") + 1] == "vcodec:vp09"
    assert cmd[cmd.index("--recode-video") + 1] == "webm"
    assert cmd[cmd.index("--postprocessor-args") + 1] == "VideoConvertor+ffmpeg_o:-c:v libvpx-vp9"


def test_ytdlp_command_transcodes_video_file_audio_codec():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_audio_codec": "opus"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "mkv"
    assert cmd[cmd.index("--format") + 1] == video_format_selector("best", "auto", "auto", "opus")
    assert cmd[cmd.index("--recode-video") + 1] == "mkv"
    postprocessor_args = [
        right
        for left, right in zip(cmd, cmd[1:], strict=False)
        if left == "--postprocessor-args"
    ]
    assert "Merger+ffmpeg_o:-c:v copy -c:a libopus" in postprocessor_args
    assert "VideoConvertor+ffmpeg_o:-c:v copy -c:a libopus" in postprocessor_args


def test_ytdlp_command_drops_codec_incompatible_with_container():
    # VP9 can't play in MP4, so the codec preference is dropped rather than forcing an
    # unplayable stream: no -S sort is emitted (equivalent to Auto).
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_container": "mp4", "video_codec": "vp9"},
    )

    # Explicit MP4 is the requested final format; MKV is only the safe intermediate.
    assert cmd[cmd.index("--merge-output-format") + 1] == "mkv"
    assert cmd[cmd.index("--recode-video") + 1] == "mp4"
    assert "-S" not in cmd


def test_ytdlp_username_field_uses_configured_list_authoritatively():
    # Unconfigured: the built-in chain (handles first, opaque channel_id last).
    assert (
        ytdlp.ytdlp_username_field()
        == "%(uploader_id,playlist_uploader_id,uploader,channel,creator,channel_id|Unknown)s"
    )
    # A configured list is authoritative: only those fields, in order, no chain trailing;
    # `user[name]` is a gallery-dl-only spelling and must be dropped from the yt-dlp spec.
    spec = ytdlp.ytdlp_username_field(["scraper[artist]", "channel", "user[name]", "uploader_id"])
    assert spec == "%(channel,uploader_id|Unknown)s"


def test_gallerydl_nickname_field_uses_configured_list_authoritatively():
    spec = gallerydl.gallerydl_nickname_field(["scraper[artist]", "display[name]"])
    assert spec == '{display[name]|"unknown"}'


def test_build_output_template_applies_per_source_fields(monkeypatch):
    monkeypatch.setattr(formats, "get_effective_fields", lambda url: {"username": ["channel"]})
    template = ytdlp.build_output_template(
        "https://example.com/watch?v=x",
        "/media/out",
        {"folder_template": "{{username}}", "filename_template": "{{username}} [{{id}}]"},
    )
    # The configured `channel` field is the whole username spec, no chain trailing.
    assert "%(channel|Unknown)s" in template


def test_ytdlp_creator_sidecar_uses_per_source_nickname_fields(monkeypatch):
    monkeypatch.setattr(ytdlp, "get_effective_fields", lambda url: {"nickname": ["channel"]})
    cmd = ytdlp.build_ytdlp_command(
        "https://example.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        creator_sidecar="/tmp/creator.txt",
    )

    assert "after_move:%(channel|Unknown)s" in cmd


def test_convert_template_to_gallerydl_uses_field_roles():
    rendered = gallerydl.convert_template_to_gallerydl(
        "{{nickname}}", "https://example.com/x", field_roles={"nickname": ["fullname"]}
    )
    assert rendered == '{fullname|"unknown"}'


def test_resolve_scraped_tokens_returns_role_keyed_overrides(monkeypatch):
    rules = {
        "rule34video": {
            "rules": [
                {"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"},
                {"token": "caption", "xpath": "//*[@id='caption']", "attr": "text"},
            ]
        }
    }
    roles = {"rule34video": {"artist": "username", "caption": "title"}}
    monkeypatch.setattr(
        enrich,
        "fetch_html",
        lambda *args: "<main><b id='artist'>Trace Artist</b><h1 id='caption'>Scraped Title</h1></main>",
    )

    result = enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        rules,
        roles,
        field_roles={
            "username": ["scraper[artist]", "uploader"],
            "title": ["scraper[caption]", "title"],
        },
    )

    assert result == {"username": "Trace Artist", "title": "Scraped Title"}


def test_resolve_scraped_tokens_creator_role_feeds_username_and_nickname(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"}]}}
    roles = {"rule34video": {"artist": "creator"}}
    monkeypatch.setattr(
        enrich,
        "fetch_html",
        lambda *args: "<main><b id='artist'>Trace Artist</b></main>",
    )

    # A Creator-role token leads both role lists, so a template using either resolves.
    result = enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{username}}", "filename_template": "{{nickname}} [{{id}}]"},
        rules,
        roles,
        field_roles={
            "username": ["scraper[artist]", "uploader"],
            "nickname": ["scraper[artist]", "uploader"],
        },
    )

    assert result == {"username": "Trace Artist", "nickname": "Trace Artist"}


def test_resolve_scraped_tokens_uses_first_top_scraper_field(monkeypatch):
    rules = {
        "rule34video": {
            "rules": [
                {"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"},
                {"token": "alt_artist", "xpath": "//*[@id='alt']", "attr": "text"},
            ]
        }
    }
    roles = {"rule34video": {"artist": "username", "alt_artist": "username"}}
    monkeypatch.setattr(
        enrich,
        "fetch_html",
        lambda *args: "<main><b id='artist'>Rule Order Artist</b><b id='alt'>Top Artist</b></main>",
    )

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{username}}", "filename_template": "{{id}}"},
        rules,
        roles,
        field_roles={"username": ["scraper[alt_artist]", "scraper[artist]", "uploader"]},
    ) == {"username": "Top Artist"}


def test_resolve_scraped_tokens_uses_first_top_title_scraper_field(monkeypatch):
    rules = {
        "rule34video": {
            "rules": [
                {"token": "caption", "xpath": "//*[@id='caption']", "attr": "text"},
                {"token": "alt_caption", "xpath": "//*[@id='alt']", "attr": "text"},
            ]
        }
    }
    roles = {"rule34video": {"caption": "title", "alt_caption": "title"}}
    monkeypatch.setattr(
        enrich,
        "fetch_html",
        lambda *args: "<main><h1 id='caption'>Rule Order Title</h1><h2 id='alt'>Top Title</h2></main>",
    )

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "", "filename_template": "{{title}} [{{id}}]"},
        rules,
        roles,
        field_roles={"title": ["scraper[alt_caption]", "scraper[caption]", "title"]},
    ) == {"title": "Top Title"}


def test_resolve_scraped_tokens_ignores_role_rule_when_scraper_field_is_not_top(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"}]}}
    roles = {"rule34video": {"artist": "username"}}

    def fail_fetch(*args):
        raise AssertionError("non-top scraper field should not trigger a fetch")

    monkeypatch.setattr(enrich, "fetch_html", fail_fetch)

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{username}}", "filename_template": "{{id}}"},
        rules,
        roles,
        field_roles={"username": ["uploader", "scraper[artist]"]},
    ) == {}


def test_resolve_scraped_tokens_ignores_title_rule_when_scraper_field_is_not_top(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "caption", "xpath": "//*[@id='caption']", "attr": "text"}]}}
    roles = {"rule34video": {"caption": "title"}}

    def fail_fetch(*args):
        raise AssertionError("non-top scraper title field should not trigger a fetch")

    monkeypatch.setattr(enrich, "fetch_html", fail_fetch)

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "", "filename_template": "{{title}} [{{id}}]"},
        rules,
        roles,
        field_roles={"title": ["title", "scraper[caption]"]},
    ) == {}


def test_resolve_scraped_tokens_ignores_raw_template_token_without_public_role(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"}]}}
    roles = {"rule34video": {"artist": "username"}}

    def fail_fetch(*args):
        raise AssertionError("raw scraper token should not trigger a fetch")

    monkeypatch.setattr(enrich, "fetch_html", fail_fetch)

    assert (
        enrich.resolve_scraped_tokens(
            "https://rule34video.com/video/1/post",
            "rule34video",
            {"folder_template": "{{artist}}", "filename_template": "{{id}}"},
            rules,
            roles,
        )
        == {}
    )


def test_resolve_scraped_tokens_keeps_none_role_as_custom_template_token(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"}]}}
    monkeypatch.setattr(
        enrich,
        "fetch_html",
        lambda *args: "<main><b id='artist'>Trace Artist</b></main>",
    )

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{artist}}", "filename_template": "{{id}}"},
        rules,
        {"rule34video": {}},
    ) == {"artist": "Trace Artist"}


def test_ytdlp_template_uses_role_keyed_scraped_title_and_username():
    template = ytdlp.build_output_template(
        "https://rule34video.com/video/1/post",
        "/media",
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        extra_tokens={"username": "@Trace Artist", "title": "Scraped / Title"},
    )

    assert "Trace Artist" in template
    assert "Scraped _ Title" in template
    assert "%(title|Unknown)s" not in template


def _has_cli_pair(cmd: list[str], option: str, value: str) -> bool:
    return any(left == option and right == value for left, right in zip(cmd, cmd[1:], strict=False))


@pytest.mark.parametrize(
    "url",
    [
        "https://www.youtube.com/watch?v=1",
        "https://www.pixiv.net/en/artworks/12345",
        "https://i.imgur.com/abc.jpg",
        "not a url",
    ],
)
def test_select_engine_always_defaults_to_gallerydl(url):
    assert select_engine(url).name == "gallerydl"


def test_all_engines_includes_both_backends():
    assert {engine.name for engine in all_engines()} == {"ytdlp", "gallerydl"}


def test_engine_by_name_falls_back_to_gallerydl():
    assert engine_by_name("gallerydl").name == "gallerydl"
    assert engine_by_name("ytdlp").name == "ytdlp"
    assert engine_by_name("bogus").name == "gallerydl"


def test_engine_for_task_prefers_explicit_engine_over_url():
    task = {"engine": "gallerydl", "source_url": "https://www.youtube.com/watch?v=1"}
    assert engine_for_task(task).name == "gallerydl"


def test_engine_for_task_defaults_to_gallerydl_when_untagged():
    assert engine_for_task({"source_url": "https://www.pixiv.net/en/artworks/1"}).name == "gallerydl"


def test_looks_unsupported_flags_wrong_engine_errors():
    assert _looks_unsupported({"last_log_lines": ["ERROR: Unsupported URL: https://tiktok.com/@x/photo/1"]}) is True
    assert _looks_unsupported({"last_log_lines": ["yt_dlp.utils.UnsupportedError: Unsupported URL: ..."]}) is True
    assert _looks_unsupported({"last_log_lines": ["No suitable extractor found"]}) is True
    assert _looks_unsupported({"last_log_lines": ["ERROR: [site] id: No video formats found!"]}) is True
    assert _looks_unsupported({"last_log_lines": ["ERROR: Video unavailable"]}) is False
    assert _looks_unsupported({}) is False


def test_should_try_next_engine_after_empty_failure(tmp_path: Path):
    media = tmp_path / "a.mp4"
    media.write_bytes(b"video")

    assert _should_try_next_engine(1, {"last_log_lines": ["ERROR: Video unavailable"]}, "", []) is True
    assert _should_try_next_engine(0, {}, "", []) is False
    assert _should_try_next_engine(1, {}, str(media), [str(media)]) is False
    assert _should_try_next_engine(
        1,
        {"last_log_lines": ["ERROR: [site] child: No video formats found!"]},
        str(media),
        [str(media)],
    ) is False


def test_ytdlp_engine_progress_and_path_parsing():
    engine = engine_by_name("ytdlp")
    assert engine.parse_progress("[download]  50.0% of 10MiB") == 50.0
    assert engine.parse_progress("[info] not a progress line") is None
    assert engine.extract_output_path("[download] Destination: /media/a.mp4") == "/media/a.mp4"


def test_gallerydl_engine_progress_and_path_parsing():
    engine = engine_by_name("gallerydl")
    assert engine.parse_progress("no percentages here") is None
    assert engine.parse_progress("[download]  50.0% of 10MiB") == 50.0
    assert engine.extract_output_path("/media/imgur/artist/photo.jpg") == "/media/imgur/artist/photo.jpg"
    assert engine.extract_output_path('"/media/imgur/artist/photo.png"') == "/media/imgur/artist/photo.png"
    assert engine.extract_output_path("[download] Destination: /media/imgur/artist/clip.mp4") == (
        "/media/imgur/artist/clip.mp4"
    )
    assert engine.extract_output_path("[download] skipping existing file") == ""
    assert engine.extract_output_path("# /media/imgur/artist/photo.jpg") == "/media/imgur/artist/photo.jpg"
    assert engine.extract_output_path("/media/imgur/notes.txt") == ""


def test_convert_template_to_gallerydl_maps_fields_and_resolves_creator():
    result = gallerydl.convert_template_to_gallerydl(
        "{{username}} - {{title}} [{{id}}]",
        "https://twitter.com/DohaVT/status/2073635724684054528",
    )
    gallery_username = '{username|author[uniqueId]|user[name]|user[username]|user[uniqueId]|account|author|"unknown"}'
    assert result.startswith(f"{gallery_username} - ")
    assert '{title|content|"untitled"}' in result
    assert "[2073635724684054528]" in result


def test_convert_template_to_gallerydl_falls_back_to_metadata_creator():
    # No creator segment in the URL -> emit a gallery-dl field with fallbacks.
    result = gallerydl.convert_template_to_gallerydl("{{username}}", "https://imgur.com/abc")
    assert result == '{username|author[uniqueId]|user[name]|user[username]|user[uniqueId]|account|author|"unknown"}'


def test_engine_progress_style_flags():
    assert engine_by_name("ytdlp").emits_progress is True
    assert engine_by_name("gallerydl").emits_progress is False


def test_gallerydl_counts_a_media_url_as_one_item_without_a_second_pass():
    engine = engine_by_name("gallerydl")
    # A link naming one item is the common case, and the bar can track its bytes exactly.
    assert engine.count_items("https://www.tiktok.com/@someone/video/7493558766131039489") == 1
    # A profile could be any number of items, so the unknown-total curve still applies.
    assert engine.count_items("https://www.tiktok.com/@someone") == 0


def test_count_progress_known_total_caps_below_100():
    assert _count_progress(0, 4) == 0.0
    assert _count_progress(2, 4) == 50.0
    assert _count_progress(4, 4) == 99.0
    assert _count_progress(9, 4) == 99.0


def test_count_progress_unknown_total_is_monotonic_below_100():
    first = _count_progress(1, 0)
    second = _count_progress(2, 0)
    later = _count_progress(20, 0)
    assert 0 < first < second < later < 100


def test_count_progress_fills_the_gap_with_the_file_in_flight():
    # Half of the third of four files done reads as seven eighths of the run.
    assert _count_progress(3, 4, 50.0) == 87.5
    assert _count_progress(1, 0) < _count_progress(1, 0, 50.0) < _count_progress(2, 0)
    # A finished file lands exactly where its own percentage was heading.
    assert _count_progress(1, 0, 100.0) == _count_progress(2, 0)


def test_gallerydl_command_asks_for_parsable_byte_progress():
    cmd = gallerydl.build_gallerydl_command("https://x.test/gallery", "/media", "dir\x1ffile.{extension}")

    assert f"output.mode={gallerydl._PROGRESS_OUTPUT_MODE}" in cmd
    assert "output.shorten=false" in cmd
    assert f"downloader.http.chunk-size={gallerydl._PROGRESS_CHUNK_SIZE}" in cmd


def test_gallerydl_output_mode_writes_what_the_worker_parses(monkeypatch):
    # Pins the contract with gallery-dl's own writer: a version that changed these
    # formats would silently leave the bar with nothing to read.
    from gallery_dl import config, output

    written: list[str] = []
    monkeypatch.setattr(output, "stderr_write", written.append)
    monkeypatch.setattr(output, "stdout_write", written.append)
    config.set(("output",), "shorten", False)
    try:
        writer = output.CustomOutput(json.loads(gallerydl._PROGRESS_OUTPUT_MODE))
        writer.success("/media/clip.mp4")
        writer.progress(1000, 421, 90)
        writer.progress(None, 421, 90)
    finally:
        config.unset(("output",), "shorten")

    assert written[0] == "/media/clip.mp4\n"
    assert PROGRESS_RE.search(written[1]).group(1) == "42"
    assert written[1].endswith("\n")
    # No total to report against, so nothing the reader has to wake up for.
    assert "\n" not in written[2]


def test_task_progress_gives_every_step_its_own_slice():
    progress = TaskProgress()
    assert progress.prepare(0.25) == 2.0
    assert progress.prepare(1.0) == PREPARE_END
    assert PREPARE_END < progress.download(50.0) < DOWNLOAD_END
    assert progress.finalize(0.0) == DOWNLOAD_END
    assert progress.finalize(1.0) == FINALIZE_END


def test_task_progress_never_rewinds_when_a_stream_restarts():
    progress = TaskProgress()
    # yt-dlp finishes the video stream, then starts the audio stream back at zero.
    progress.download(100.0, partial=True)
    after_video = progress.value
    assert after_video < DOWNLOAD_END
    assert progress.download(0.0, partial=True) == after_video
    assert progress.download(50.0, partial=True) > after_video
    assert progress.download(100.0, partial=True) < DOWNLOAD_END


def test_task_progress_spends_the_whole_slice_on_a_single_run_percentage():
    progress = TaskProgress()
    # gallery-dl's count curve already covers the whole run, so nothing is held back.
    assert progress.download(50.0) == (PREPARE_END + DOWNLOAD_END) / 2
    assert progress.download(100.0) == DOWNLOAD_END


def test_task_progress_holds_the_line_through_a_failed_retry():
    progress = TaskProgress()
    progress.download(40.0)
    stalled = progress.value
    # The cookies file failed and the next attempt starts over from nothing.
    assert progress.download(0.0) == stalled
    assert progress.prepare(1.0) == stalled
    assert progress.download(100.0) <= DOWNLOAD_END


def test_task_progress_stays_below_the_download_slice_after_many_streams():
    progress = TaskProgress()
    for _ in range(12):
        progress.download(0.0, partial=True)
        progress.download(100.0, partial=True)
    assert progress.value <= DOWNLOAD_END


def test_build_gallerydl_output_template_adds_num_for_slideshows():
    template = gallerydl.build_gallerydl_output_template("https://www.tiktok.com/@x/photo/1", "/media/tiktok")
    _, _, filename = template.partition(gallerydl._TEMPLATE_SEP)
    assert filename.endswith("_{num}.{extension}")


def test_engines_build_output_templates_from_same_settings_snapshot():
    settings = {
        "folder_template": "{{username}}/{{id}}",
        "filename_template": "{{username}} - {{title}} [{{id}}]",
    }
    url = "https://twitter.com/DohaVT/status/2073635724684054528"

    ytdlp_template = engine_by_name("ytdlp").build_output_template(url, "/media/twitter", settings)
    gallery_template = engine_by_name("gallerydl").build_output_template(url, "/media/twitter", settings)
    gallery_folder, _, gallery_filename = gallery_template.partition(gallerydl._TEMPLATE_SEP)
    gallery_username = '{username|author[uniqueId]|user[name]|user[username]|user[uniqueId]|account|author|"unknown"}'

    assert "%(uploader_id,playlist_uploader_id,uploader,channel,creator,channel_id|Unknown)s" in ytdlp_template
    assert "%(id|NA)s" in ytdlp_template
    assert gallery_folder == f"{gallery_username}/2073635724684054528"
    assert gallery_filename.startswith(
        f'{gallery_username} - {{title|content|"untitled"}} [2073635724684054528]'
    )


def test_build_gallerydl_command_layout():
    sep = gallerydl._TEMPLATE_SEP
    output_template = f"artist{sep}Clip [id].{{extension}}"
    cmd = gallerydl.build_gallerydl_command(
        "https://imgur.com/a/x",
        "/media/imgur",
        output_template,
    )
    assert cmd[0] == "gallery-dl"
    assert cmd[cmd.index("--destination") + 1] == str(Path("/media/imgur"))
    assert _has_cli_pair(cmd, "-o", "extractor.tiktok.audio=false")
    assert _has_cli_pair(cmd, "-o", 'directory=["artist"]')
    assert cmd[cmd.index("--filename") + 1] == "Clip [id].{extension}"
    assert "--cookies" not in cmd
    assert cmd[-1] == "https://imgur.com/a/x"


def test_build_gallerydl_command_can_filter_extensions():
    sep = gallerydl._TEMPLATE_SEP
    cmd = gallerydl.build_gallerydl_command(
        "https://imgur.com/a/x",
        "/media/imgur",
        f"artist{sep}Clip [id].{{extension}}",
        excluded_extensions={".mp4", "webm"},
    )

    filter_expr = cmd[cmd.index("--filter") + 1]
    assert "extension not in" in filter_expr
    assert "mp4" in filter_expr
    assert "webm" in filter_expr


def test_build_gallerydl_command_can_write_metadata_sidecar():
    sep = gallerydl._TEMPLATE_SEP
    cmd = gallerydl.build_gallerydl_command(
        "https://imgur.com/a/x",
        "/media/imgur",
        f"artist{sep}Clip [id].{{extension}}",
        metadata_sidecar="/tmp/meta.tsv",
    )

    postprocessors = _gallerydl_postprocessors(cmd)
    assert len(postprocessors) == 1
    metadata_processor = postprocessors[0]
    metadata_format = metadata_processor["content-format"]
    assert metadata_processor["event"] == "after"
    assert metadata_processor["filename"] == "meta.tsv"
    assert metadata_processor["base-directory"] == "/tmp"
    assert metadata_processor["open"] == "a"
    assert metadata_format.startswith("\fE ")
    assert "std.json.dumps(dict(locals()" in metadata_format
    assert "sidecar" not in metadata_format
    assert "post_shortcode" not in metadata_format


def test_build_gallerydl_command_routes_streams_through_ytdlp(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.module=yt_dlp")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.enabled=true")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.module=yt_dlp")
    assert _has_cli_pair(cmd, "-o", 'downloader.ytdl.raw-options.js_runtimes={"node":{}}')
    assert _has_cli_pair(cmd, "-o", 'extractor.ytdl.raw-options.js_runtimes={"node":{}}')
    assert _has_cli_pair(cmd, "-o", 'downloader.ytdl.raw-options.remote_components=["ejs:github"]')
    assert _has_cli_pair(cmd, "-o", 'extractor.ytdl.raw-options.remote_components=["ejs:github"]')
    assert _has_cli_pair(cmd, "-o", f"downloader.ytdl.format={video_format_selector('best', 'auto')}")
    assert _has_cli_pair(cmd, "-o", f"extractor.ytdl.format={video_format_selector('best', 'auto')}")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mp4/mkv/webm")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.merge_output_format=mp4/mkv/webm")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.ffmpeg_location=/usr/bin/ffmpeg")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.ffmpeg_location=/usr/bin/ffmpeg")


def test_build_gallerydl_command_omits_ffmpeg_option_when_absent(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.module=yt_dlp")
    assert not any(str(arg).startswith("downloader.ytdl.raw-options.ffmpeg_location=") for arg in cmd)
    assert not any(str(arg).startswith("extractor.ytdl.raw-options.ffmpeg_location=") for arg in cmd)


def test_build_gallerydl_command_normalizes_windows_ffmpeg_path(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: r"C:\tools\ffmpeg\ffmpeg.exe")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.ffmpeg_location=C:/tools/ffmpeg/ffmpeg.exe")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.ffmpeg_location=C:/tools/ffmpeg/ffmpeg.exe")


def test_ytdlp_command_defaults_to_best_video_merge():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
    )

    assert cmd[cmd.index("--format") + 1] == video_format_selector("best", "auto")
    assert cmd[cmd.index("--merge-output-format") + 1] == "mp4/mkv/webm"
    assert "--extract-audio" not in cmd


def test_ytdlp_command_caps_resolution():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_quality": "720p"},
    )

    assert cmd[cmd.index("--format") + 1] == video_format_selector("720p", "auto")
    assert cmd[cmd.index("--merge-output-format") + 1] == "mp4/mkv/webm"
    assert "-S" not in cmd


def test_ytdlp_command_sets_container_and_codec_preference():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_quality": "best", "video_container": "mkv", "video_codec": "av1"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "mkv"
    assert cmd[cmd.index("-S") + 1] == "vcodec:av01"
    assert cmd[cmd.index("--recode-video") + 1] == "mkv"
    postprocessor_args = [
        right
        for left, right in zip(cmd, cmd[1:], strict=False)
        if left == "--postprocessor-args"
    ]
    assert "Merger+ffmpeg_o:-c:v libaom-av1" in postprocessor_args
    assert "VideoConvertor+ffmpeg_o:-c:v libaom-av1" in postprocessor_args


def test_ytdlp_command_auto_codec_omits_sort():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_codec": "auto", "video_container": "webm"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "mkv"
    assert cmd[cmd.index("--recode-video") + 1] == "webm"
    assert "-S" not in cmd


def test_ytdlp_command_auto_audio_uses_native_selector_without_postprocessing():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "auto", "audio_bitrate": "320"},
    )

    assert cmd[cmd.index("--format") + 1] == audio_format_selector("auto", "320")
    assert "--audio-format" not in cmd
    assert "--audio-quality" not in cmd
    assert "--extract-audio" not in cmd
    assert "--ffmpeg-location" not in cmd


def test_ytdlp_command_lossless_audio_transcodes_without_bitrate():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "flac", "audio_bitrate": "320"},
    )

    assert cmd[cmd.index("--format") + 1] == audio_format_selector("flac", "320")
    assert cmd[cmd.index("--audio-format") + 1] == "flac"
    assert "--extract-audio" in cmd
    assert "--audio-quality" not in cmd


def test_ytdlp_command_audio_mode_transcodes_explicit_format_and_bitrate():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "opus", "audio_bitrate": "192"},
    )

    assert cmd[cmd.index("--format") + 1] == audio_format_selector("opus", "192")
    assert cmd[cmd.index("--audio-format") + 1] == "opus"
    assert cmd[cmd.index("--audio-quality") + 1] == "192"
    assert "--extract-audio" in cmd
    assert "--merge-output-format" not in cmd
    assert cmd[cmd.index("--ffmpeg-location") + 1] == "/usr/bin/ffmpeg"


def test_ytdlp_command_audio_mode_best_bitrate_omits_bitrate_filter():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "mp3", "audio_bitrate": "best"},
    )

    assert cmd[cmd.index("--format") + 1] == audio_format_selector("mp3", "best")
    assert cmd[cmd.index("--audio-format") + 1] == "mp3"
    assert cmd[cmd.index("--audio-quality") + 1] == "0"
    assert "abr<=" not in cmd[cmd.index("--format") + 1]


def test_gallerydl_command_applies_capped_quality_to_ytdl_downloader(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_quality": "480p"},
    )

    assert _has_cli_pair(cmd, "-o", f"downloader.ytdl.format={video_format_selector('480p', 'auto')}")
    assert _has_cli_pair(cmd, "-o", f"extractor.ytdl.format={video_format_selector('480p', 'auto')}")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mp4/mkv/webm")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.merge_output_format=mp4/mkv/webm")


def test_gallerydl_command_honors_video_container(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_quality": "best", "video_container": "mkv"},
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mkv")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.merge_output_format=mkv")
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessors=[{"key":"FFmpegVideoConvertor","preferedformat":"mkv"}]',
    )
    assert _has_cli_pair(
        cmd,
        "-o",
        'extractor.ytdl.raw-options.postprocessors=[{"key":"FFmpegVideoConvertor","preferedformat":"mkv"}]',
    )


def test_gallerydl_command_honors_video_codec(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_container": "mkv", "video_codec": "vp9"},
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.format_sort=vcodec:vp09")
    assert _has_cli_pair(cmd, "-o", "extractor.ytdl.raw-options.format_sort=vcodec:vp09")
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessors=[{"key":"FFmpegVideoConvertor","preferedformat":"mkv"}]',
    )
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessor_args={"VideoConvertor+ffmpeg_o":["-c:v","libvpx-vp9"],"Merger+ffmpeg_o":["-c:v","libvpx-vp9"]}',
    )


def test_gallerydl_command_honors_video_audio_codec(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_audio_codec": "aac"},
    )

    assert _has_cli_pair(
        cmd,
        "-o",
        f"downloader.ytdl.format={video_format_selector('best', 'auto', 'auto', 'aac')}",
    )
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mkv")
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessors=[{"key":"FFmpegVideoConvertor","preferedformat":"mkv"}]',
    )
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessor_args={"VideoConvertor+ffmpeg_o":["-c:v","copy","-c:a","aac"],"Merger+ffmpeg_o":["-c:v","copy","-c:a","aac"]}',
    )


def test_gallerydl_command_drops_codec_incompatible_with_container(monkeypatch):
    # VP9 can't play in MP4: no format_sort is forwarded to the ytdl sub-downloader,
    # so gallery-dl never muxes an unplayable VP9 stream into an MP4 container.
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_container": "mp4", "video_codec": "vp9"},
    )

    assert not any("format_sort" in str(arg) for arg in cmd)


def test_gallerydl_audio_auto_mode_stays_native(monkeypatch):
    ffmpeg_checked = False

    def fake_detect_ffmpeg_location():
        nonlocal ffmpeg_checked
        ffmpeg_checked = True
        return "/usr/bin/ffmpeg"

    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", fake_detect_ffmpeg_location)
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "audio", "audio_format": "auto", "audio_bitrate": "320"},
    )

    assert _has_cli_pair(cmd, "-o", f"downloader.ytdl.format={audio_format_selector('auto', '320')}")
    assert _has_cli_pair(cmd, "-o", f"extractor.ytdl.format={audio_format_selector('auto', '320')}")
    assert not ffmpeg_checked
    assert not any("postprocessors" in str(arg) for arg in cmd)
    assert not any("merge_output_format" in str(arg) for arg in cmd)
    assert not any("ffmpeg_location" in str(arg) for arg in cmd)


def test_gallerydl_audio_mode_transcodes_explicit_format_and_bitrate(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "audio", "audio_format": "mp3", "audio_bitrate": "320"},
    )

    assert _has_cli_pair(cmd, "-o", f"downloader.ytdl.format={audio_format_selector('mp3', '320')}")
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessors=[{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"320"}]',
    )
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.ffmpeg_location=/usr/bin/ffmpeg")
    assert not any("merge_output_format" in str(arg) for arg in cmd)


def test_gallerydl_audio_mode_lossless_omits_bitrate_filter(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "audio", "audio_format": "flac", "audio_bitrate": "320"},
    )

    assert _has_cli_pair(cmd, "-o", f"downloader.ytdl.format={audio_format_selector('flac', '320')}")
    assert _has_cli_pair(cmd, "-o", f"extractor.ytdl.format={audio_format_selector('flac', '320')}")
    assert _has_cli_pair(
        cmd,
        "-o",
        'downloader.ytdl.raw-options.postprocessors=[{"key":"FFmpegExtractAudio","preferredcodec":"flac"}]',
    )
    assert not any("abr<=" in str(arg) for arg in cmd if "ytdl.format" in str(arg))


def test_count_gallerydl_items_disables_tiktok_audio(monkeypatch):
    captured: dict[str, list[str]] = {}

    class Result:
        returncode = 0
        stdout = "https://example.test/1.jpg\nhttps://example.test/2.jpg\n"

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return Result()

    monkeypatch.setattr(gallerydl.subprocess, "run", fake_run)

    assert gallerydl.count_gallerydl_items("https://www.tiktok.com/@x/photo/1") == 2
    assert _has_cli_pair(captured["cmd"], "-o", "extractor.tiktok.audio=false")


def test_ytdlp_command_enables_youtube_js_solver():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=Rh8dLAeeEsQ",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
    )

    assert cmd[cmd.index("--js-runtimes") + 1] == "node"
    assert cmd[cmd.index("--remote-components") + 1] == "ejs:github"


def test_ytdlp_command_includes_js_runtimes_universally():
    cmd = ytdlp.build_ytdlp_command(
        "https://twitter.com/DohaVT/status/1",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
    )

    assert "--js-runtimes" in cmd
    assert "node" in cmd
    assert "--remote-components" in cmd


def test_downloader_commands_use_the_leased_cookie_file():
    ytdlp_cmd = ytdlp.build_ytdlp_command(
        "https://twitter.com/DohaVT/status/1",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        cookies_file="/cookies/twitter-2.txt",
    )
    gallery_cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        cookies_file="/cookies/twitter-2.txt",
    )

    assert ytdlp_cmd[ytdlp_cmd.index("--cookies") + 1] == "/cookies/twitter-2.txt"
    assert gallery_cmd[gallery_cmd.index("--cookies") + 1] == "/cookies/twitter-2.txt"


def test_downloader_commands_route_intermediates_and_extractor_payloads_to_task_scratch():
    ytdlp_cmd = ytdlp.build_ytdlp_command(
        "https://example.test/watch/1",
        "/usr/bin/ffmpeg",
        "/media/creator/clip.%(ext)s",
        output_dir="/media",
        metadata_sidecar="/scratch/nvs-download-task-1/downloads.tsv",
        post_processing={"metadata": "sidecar"},
    )
    gallery_cmd = gallerydl.build_gallerydl_command(
        "https://example.test/post/1",
        "/media",
        f"creator{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        metadata_sidecar="/scratch/nvs-download-task-1/downloads.tsv",
        post_processing={"metadata": "sidecar"},
    )

    ytdlp_paths = [ytdlp_cmd[index + 1] for index, value in enumerate(ytdlp_cmd) if value == "--paths"]
    assert "home:/media" in ytdlp_paths
    assert "temp:/scratch/nvs-download-task-1/parts" in ytdlp_paths
    assert "infojson:/scratch/nvs-download-task-1/extractor" in ytdlp_paths
    assert ytdlp_cmd[ytdlp_cmd.index("--output") + 1] == "creator/clip.%(ext)s"
    assert _has_cli_pair(
        gallery_cmd,
        "-o",
        "downloader.part-directory=/scratch/nvs-download-task-1/parts",
    )
    gallery_postprocessors = _gallerydl_postprocessors(gallery_cmd)
    assert gallery_postprocessors[0]["base-directory"] == "/scratch/nvs-download-task-1"
    assert gallery_postprocessors[0]["filename"] == "downloads.tsv"
    assert gallery_postprocessors[1]["directory"] == "/scratch/nvs-download-task-1/extractor"
    assert "--postprocessor-option" not in gallery_cmd


def test_downloader_commands_and_templates_obey_naming_limits():
    # 1. yt-dlp command includes --trim-filenames with safe default or configured stem limit
    default_cmd = ytdlp.build_ytdlp_command("https://youtu.be/x", "/usr/bin/ffmpeg", "/media/out.%(ext)s")
    assert "--trim-filenames" in default_cmd
    assert default_cmd[default_cmd.index("--trim-filenames") + 1] == "160"

    custom_cmd = ytdlp.build_ytdlp_command(
        "https://youtu.be/x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        cleaning={"stem_max_chars": 50},
    )
    assert custom_cmd[custom_cmd.index("--trim-filenames") + 1] == "50"

    # 2. gallery-dl command includes raw-options.trim_file_names
    gallery_default = gallerydl.build_gallerydl_command("https://example.com/1", "/media", "clip.{extension}")
    assert _has_cli_pair(gallery_default, "-o", "downloader.ytdl.raw-options.trim_file_names=160")

    gallery_custom = gallerydl.build_gallerydl_command(
        "https://example.com/1",
        "/media",
        "clip.{extension}",
        cleaning={"stem_max_chars": 45},
    )
    assert _has_cli_pair(gallery_custom, "-o", "downloader.ytdl.raw-options.trim_file_names=45")

    # 3. Output template converters respect shorten=True and max_chars
    ytdlp_tmpl = ytdlp.convert_template_to_ytdlp(
        "{{username}} - {{title}} [{{id}}]",
        cleaning={"shorten": True, "max_chars": 80},
    )
    assert "%(title|Unknown).80s" in ytdlp_tmpl

    gallery_tmpl = gallerydl.convert_template_to_gallerydl(
        "{{username}} - {{title}} [{{id}}]",
        cleaning={"shorten": True, "max_chars": 75},
    )
    assert '{title[:75]|content[:75]|"untitled"}' in gallery_tmpl
