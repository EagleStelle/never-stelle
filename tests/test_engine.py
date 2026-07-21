from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.services.tasks.enrich as enrich
import backend.app.services.tasks.gallerydl as gallerydl
import backend.app.services.tasks.ytdlp as ytdlp
from backend.app.services.tasks import engine_by_name, engine_for_task, select_engine
from backend.app.services.tasks.constants import (
    codec_allowed_for_container,
    default_quality_selection,
    normalize_quality_selection,
    quality_options,
    template_tokens,
)
from backend.app.services.tasks.engine import all_engines
from backend.app.services.tasks.worker import _count_progress, _looks_unsupported, _should_try_next_engine


def test_normalize_quality_selection_defaults_and_validates():
    assert default_quality_selection() == {
        "mode": "video",
        "video_quality": "best",
        "video_container": "mp4",
        "video_codec": "auto",
        "audio_format": "mp3",
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
        }
    )
    assert selection == {
        "mode": "audio",
        "video_quality": "720p",
        "video_container": "mkv",
        "video_codec": "av1",
        "audio_format": "opus",
        "audio_bitrate": "192",
    }
    assert normalize_quality_selection({"video_container": "mp4", "video_codec": "vp9"})["video_codec"] == "auto"


def test_quality_options_expose_all_pickers():
    options = quality_options()
    assert {o["key"] for o in options["video"]} == {"best", "1080p", "720p", "480p"}
    assert {o["key"] for o in options["video_containers"]} == {"mp4", "mkv", "webm"}
    assert {o["key"] for o in options["video_codecs"]} == {"auto", "av1", "vp9", "h264", "h265"}
    assert {o["key"] for o in options["audio_formats"]} == {"mp3", "m4a", "opus", "aac", "flac", "wav"}
    assert {o["key"] for o in options["audio_bitrates"]} == {"best", "320", "192", "128"}
    by_key = {o["key"]: o for o in options["video_containers"]}
    assert set(by_key["webm"]["codecs"]) == {"av1", "vp9"}
    assert "vp9" not in by_key["mp4"]["codecs"]


def test_template_tokens_expose_supported_public_tokens_only():
    assert [token["key"] for token in template_tokens()] == [
        "username",
        "nickname",
        "title",
        "id",
        "quality",
    ]


def test_codec_allowed_for_container_matrix():
    assert codec_allowed_for_container("auto", "webm") is True
    assert codec_allowed_for_container("av1", "mp4") is True
    assert codec_allowed_for_container("vp9", "mp4") is False
    assert codec_allowed_for_container("h264", "webm") is False
    assert codec_allowed_for_container("vp9", "mkv") is True


def test_ytdlp_command_drops_codec_pref_incompatible_with_container():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_container": "webm", "video_codec": "h264"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "webm"
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


def test_build_output_template_applies_per_source_creator_fields(monkeypatch):
    monkeypatch.setattr(ytdlp, "get_effective_creator_fields", lambda url: {"username": ["channel"]})
    template = ytdlp.build_output_template(
        "https://example.com/watch?v=x",
        "/media/out",
        {"folder_template": "{{username}}", "filename_template": "{{username}} [{{id}}]"},
    )
    # The configured `channel` field is the whole username spec, no chain trailing.
    assert "%(channel|Unknown)s" in template


def test_ytdlp_creator_sidecar_uses_per_source_nickname_fields(monkeypatch):
    monkeypatch.setattr(ytdlp, "get_effective_creator_fields", lambda url: {"nickname": ["channel"]})
    cmd = ytdlp.build_ytdlp_command(
        "https://example.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        creator_sidecar="/tmp/creator.txt",
    )

    assert "after_move:%(channel|Unknown)s" in cmd


def test_convert_template_to_gallerydl_uses_creator_fields():
    rendered = gallerydl.convert_template_to_gallerydl(
        "{{nickname}}", "https://example.com/x", creator_fields={"nickname": ["fullname"]}
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
        creator_fields={"username": ["scraper[artist]", "uploader"]},
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
        creator_fields={
            "username": ["scraper[artist]", "uploader"],
            "nickname": ["scraper[artist]", "uploader"],
        },
    )

    assert result == {"username": "Trace Artist", "nickname": "Trace Artist"}


def test_resolve_scraped_tokens_uses_first_top_scraper_creator_field(monkeypatch):
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
        creator_fields={"username": ["scraper[alt_artist]", "scraper[artist]", "uploader"]},
    ) == {"username": "Top Artist"}


def test_resolve_scraped_tokens_ignores_role_rule_when_scraper_field_is_not_top(monkeypatch):
    rules = {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='artist']", "attr": "text"}]}}
    roles = {"rule34video": {"artist": "username"}}

    def fail_fetch(*args):
        raise AssertionError("non-top scraper creator field should not trigger a fetch")

    monkeypatch.setattr(enrich, "fetch_html", fail_fetch)

    assert enrich.resolve_scraped_tokens(
        "https://rule34video.com/video/1/post",
        "rule34video",
        {"folder_template": "{{username}}", "filename_template": "{{id}}"},
        rules,
        roles,
        creator_fields={"username": ["uploader", "scraper[artist]"]},
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
def test_select_engine_always_defaults_to_ytdlp(url):
    assert select_engine(url).name == "ytdlp"


def test_all_engines_includes_both_backends():
    assert {engine.name for engine in all_engines()} == {"ytdlp", "gallerydl"}


def test_engine_by_name_falls_back_to_ytdlp():
    assert engine_by_name("gallerydl").name == "gallerydl"
    assert engine_by_name("ytdlp").name == "ytdlp"
    assert engine_by_name("bogus").name == "ytdlp"


def test_engine_for_task_prefers_explicit_engine_over_url():
    task = {"engine": "gallerydl", "source_url": "https://www.youtube.com/watch?v=1"}
    assert engine_for_task(task).name == "gallerydl"


def test_engine_for_task_defaults_to_ytdlp_when_untagged():
    assert engine_for_task({"source_url": "https://www.pixiv.net/en/artworks/1"}).name == "ytdlp"


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
    ) is True


def test_ytdlp_engine_progress_and_path_parsing():
    engine = engine_by_name("ytdlp")
    assert engine.parse_progress("[download]  50.0% of 10MiB") == 50.0
    assert engine.parse_progress("[info] not a progress line") is None
    assert engine.extract_output_path("[download] Destination: /media/a.mp4") == "/media/a.mp4"


def test_gallerydl_engine_progress_and_path_parsing():
    engine = engine_by_name("gallerydl")
    assert engine.parse_progress("no percentages here") is None
    assert engine.extract_output_path("/media/imgur/artist/photo.jpg") == "/media/imgur/artist/photo.jpg"
    assert engine.extract_output_path('"/media/imgur/artist/photo.png"') == "/media/imgur/artist/photo.png"
    assert engine.extract_output_path("[download] skipping existing file") == ""
    assert engine.extract_output_path("/media/imgur/notes.txt") == ""


def test_convert_template_to_gallerydl_maps_fields_and_resolves_creator():
    result = gallerydl.convert_template_to_gallerydl(
        "{{username}} - {{title}} [{{id}}]",
        "https://twitter.com/DohaVT/status/2073635724684054528",
    )
    assert result.startswith("DohaVT - ")
    assert '{title|content|"untitled"}' in result
    assert '{id|num|"NA"}' in result


def test_convert_template_to_gallerydl_falls_back_to_metadata_creator():
    # No creator segment in the URL -> emit a gallery-dl field with fallbacks.
    result = gallerydl.convert_template_to_gallerydl("{{username}}", "https://imgur.com/abc")
    assert result == '{username|user[name]|user[username]|account|author|"unknown"}'


def test_engine_progress_style_flags():
    assert engine_by_name("ytdlp").emits_progress is True
    assert engine_by_name("gallerydl").emits_progress is False


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

    assert "DohaVT" in ytdlp_template
    assert "%(id|NA)s" in ytdlp_template
    assert gallery_folder == 'DohaVT/{id|num|"NA"}'
    assert gallery_filename.startswith('DohaVT - {title|content|"untitled"} [{id|num|"NA"}]')


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


def test_build_gallerydl_command_routes_streams_through_ytdlp(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.module=yt_dlp")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.format=bestvideo*+bestaudio/best")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mp4")
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.ffmpeg_location=/usr/bin/ffmpeg")


def test_build_gallerydl_command_omits_ffmpeg_option_when_absent(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.module=yt_dlp")
    assert not any(str(arg).startswith("downloader.ytdl.raw-options.ffmpeg_location=") for arg in cmd)


def test_build_gallerydl_command_normalizes_windows_ffmpeg_path(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: r"C:\tools\ffmpeg\ffmpeg.exe")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.ffmpeg_location=C:/tools/ffmpeg/ffmpeg.exe")


def test_ytdlp_command_defaults_to_best_video_merge():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
    )

    assert cmd[cmd.index("--format") + 1] == "bestvideo*+bestaudio/best"
    assert cmd[cmd.index("--merge-output-format") + 1] == "mp4"
    assert "--extract-audio" not in cmd


def test_ytdlp_command_caps_resolution():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_quality": "720p"},
    )

    assert cmd[cmd.index("--format") + 1] == "bestvideo*[height<=720]+bestaudio/best[height<=720]/best"
    assert cmd[cmd.index("--merge-output-format") + 1] == "mp4"
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


def test_ytdlp_command_auto_codec_omits_sort():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "video", "video_codec": "auto", "video_container": "webm"},
    )

    assert cmd[cmd.index("--merge-output-format") + 1] == "webm"
    assert "-S" not in cmd


def test_ytdlp_command_lossless_audio_omits_bitrate():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "flac", "audio_bitrate": "320"},
    )

    assert cmd[cmd.index("--audio-format") + 1] == "flac"
    assert "--audio-quality" not in cmd


def test_ytdlp_command_audio_mode_extracts_chosen_format_and_bitrate():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "opus", "audio_bitrate": "192"},
    )

    assert cmd[cmd.index("--format") + 1] == "bestaudio/best"
    assert cmd[cmd.index("--audio-format") + 1] == "opus"
    assert cmd[cmd.index("--audio-quality") + 1] == "192K"
    assert "--extract-audio" in cmd
    assert "--merge-output-format" not in cmd


def test_ytdlp_command_audio_mode_best_bitrate_uses_vbr_zero():
    cmd = ytdlp.build_ytdlp_command(
        "https://www.youtube.com/watch?v=x",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        quality={"mode": "audio", "audio_format": "mp3", "audio_bitrate": "best"},
    )

    assert cmd[cmd.index("--audio-quality") + 1] == "0"


def test_gallerydl_command_applies_capped_quality_to_ytdl_downloader(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_quality": "480p"},
    )

    assert _has_cli_pair(
        cmd, "-o", "downloader.ytdl.format=bestvideo*[height<=480]+bestaudio/best[height<=480]/best"
    )
    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mp4")


def test_gallerydl_command_honors_video_container(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_quality": "best", "video_container": "mkv"},
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.merge_output_format=mkv")


def test_gallerydl_command_honors_video_codec(monkeypatch):
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "video", "video_container": "mkv", "video_codec": "vp9"},
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.raw-options.format_sort=vcodec:vp09")


def test_gallerydl_audio_mode_still_downloads_best_video(monkeypatch):
    # gallery-dl handles images/galleries, not audio extraction: audio mode must
    # not leave the ytdl downloader empty, so it maps to best video there.
    monkeypatch.setattr(gallerydl, "detect_ffmpeg_location", lambda: "")
    cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        quality={"mode": "audio", "audio_format": "mp3", "audio_bitrate": "320"},
    )

    assert _has_cli_pair(cmd, "-o", "downloader.ytdl.format=bestvideo*+bestaudio/best")


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


def test_ytdlp_command_keeps_youtube_solver_off_for_other_sites():
    cmd = ytdlp.build_ytdlp_command(
        "https://twitter.com/DohaVT/status/1",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
    )

    assert "--js-runtimes" not in cmd
    assert "--remote-components" not in cmd


def test_downloader_commands_use_resolved_source_cookie(monkeypatch):
    monkeypatch.setattr(ytdlp, "find_cookies_file_for_source", lambda source_key: f"/cookies/{source_key}.txt")
    monkeypatch.setattr(gallerydl, "find_cookies_file_for_source", lambda source_key: f"/cookies/{source_key}.txt")
    monkeypatch.setattr(ytdlp, "find_cookies_file_for_url", lambda source_url: "")
    monkeypatch.setattr(gallerydl, "find_cookies_file_for_url", lambda source_url: "")

    ytdlp_cmd = ytdlp.build_ytdlp_command(
        "https://twitter.com/DohaVT/status/1",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        with_cookies=True,
        cookie_source_key="twitter",
    )
    gallery_cmd = gallerydl.build_gallerydl_command(
        "https://twitter.com/DohaVT/status/1",
        "/media/twitter",
        f"DohaVT{gallerydl._TEMPLATE_SEP}clip.{{extension}}",
        with_cookies=True,
        cookie_source_key="twitter",
    )

    assert ytdlp_cmd[ytdlp_cmd.index("--cookies") + 1] == "/cookies/twitter.txt"
    assert gallery_cmd[gallery_cmd.index("--cookies") + 1] == "/cookies/twitter.txt"
