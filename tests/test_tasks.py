from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

import backend.app.services.tasks.gallerydl as gallerydl_module
import backend.app.services.tasks.history as history_module
import backend.app.services.tasks.operations as operations_module
import backend.app.services.tasks.scan as scan_module
import backend.app.services.tasks.serializers as serializers_module
import backend.app.services.tasks.urls as urls_module
import backend.app.services.tasks.worker as worker_module
from backend.app.core.sources import source_label_from_key
from backend.app.services.tasks import (
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
from backend.app.services.tasks.formats import (
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
from backend.app.services.tasks.naming import (
    clean_gallerydl_disk_filename,
    clean_gallerydl_display_filename,
    clean_template_filename,
    strip_numbered_suffix,
    strip_placeholder_title,
)
from backend.app.services.tasks.serializers import history_to_api, task_to_api
from backend.app.services.tasks.ytdlp import (
    YTDLP_NICKNAME_FIELD,
    YTDLP_USERNAME_FIELD,
    clean_filename_title,
    clean_social_title,
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


def test_convert_template_prefers_creator_from_url():
    result = convert_template_to_ytdlp(
        "{{username}} - {{title}} [{{id}}]",
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
    )

    assert result.startswith("fzyahoo.com - ")
    assert "%(creator" not in result


def test_convert_template_can_keep_url_handle_at_sign():
    url = "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489"

    ytdlp_result = convert_template_to_ytdlp("{{username}}", url, cleaning={"strip_handle_at": False})
    gallerydl_result = gallerydl_module.convert_template_to_gallerydl(
        "{{username}}",
        url,
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


def test_parse_filename_media_id_uses_last_bracketed_id():
    assert parse_filename_media_id("Creator - Soft Light [Abc_123-xy].mp4") == (
        "Abc_123-xy",
        "Creator - Soft Light",
    )


def test_queue_task_stores_quality_and_falls_back_to_saved_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from backend.app.services.tasks.planning import ResolvedTaskSettings

    resolved = ResolvedTaskSettings(
        source_key="example",
        source_profile={"key": "example", "label": "Example"},
        source_profiles=[{"key": "example", "label": "Example", "hosts": []}],
        site_locations={"example": str(tmp_path)},
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
    assert captured["task"]["quality"] == {
        "mode": "audio",
        "video_quality": "best",
        "video_container": "mp4",
        "video_codec": "auto",
        "audio_format": "opus",
        "audio_bitrate": "320",
    }

    operations_module.queue_task("https://example.test/watch?v=2", quality=None)
    assert captured["task"]["quality"] == saved_default

    operations_module.queue_task(
        "https://example.test/watch?v=3",
        quality={"mode": "video", "video_container": "webm", "video_codec": "h264"},
    )
    assert captured["task"]["quality"]["video_container"] == "webm"
    assert captured["task"]["quality"]["video_codec"] == "auto"


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


def test_clean_gallerydl_display_filename_drops_tiktok_placeholder_and_num():
    filename = "fzyahoo.com - TikTok photo #7420705673542978833 [7420705673542978833]_1.jpg"

    assert clean_gallerydl_display_filename(filename) == "fzyahoo.com - [7420705673542978833].jpg"


def test_clean_gallerydl_disk_filename_keeps_num_but_drops_tiktok_placeholder():
    filename = "fzyahoo.com - TikTok photo #7420705673542978833 [7420705673542978833]_1.jpg"

    assert clean_gallerydl_disk_filename(filename) == "fzyahoo.com - [7420705673542978833]_1.jpg"


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
    result = clean_template_filename(
        "Charess - Bakit kadiri pag ako？ ｜ Charess [891576008993182].mp4",
        "{{username}} - {{title}} [{{id}}]",
        creator="charechii",
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
        media_id="ABC123",
    )
    assert result == "NASA - Cool Rocket [ABC123].jpg"


def test_clean_template_filename_handle_at_cleanup_can_be_disabled():
    name = "@alice - Nice clip [abc123].mp4"
    template = "{{username}} - {{title}} [{{id}}]"

    assert clean_template_filename(name, template) == "alice - Nice clip [abc123].mp4"
    assert clean_template_filename(name, template, cleaning={"strip_handle_at": False}) == name


def test_filename_creator_ignores_nickname_token_for_handle():
    # A {{nickname}} filename must NOT feed the {{username}} handle with the display name.
    path = Path("/media/instagram/nasa/NASA - Cool Rocket [ABC123].jpg")
    creator = worker_module._filename_creator(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        {},
        "https://www.instagram.com/p/Cxyz/",
        "ABC123",
    )
    assert creator == ""


def test_username_folder_not_clobbered_by_nickname_filename():
    # No handle known -> {{username}} folder unresolved -> no move, keeping the engine's handle folder.
    folder = worker_module._render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{username}}"},
        creator="",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder is None


def test_render_template_folder_renders_nickname_distinct_from_username():
    folder = worker_module._render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{nickname}}"},
        creator="nasa",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder == Path("/media/instagram/NASA")


def test_render_template_folder_handle_at_cleanup_can_be_disabled():
    root = Path("/media/tiktok")
    template = {"folder_template": "{{username}}"}

    assert worker_module._render_template_folder(root, template, "@alice", "abc123") == root / "alice"
    assert (
        worker_module._render_template_folder(
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
    nickname = worker_module._filename_nickname(
        path,
        "{{username}} - {{title}} [{{id}}]",
        "{{nickname}}",
        worker_module._template_folder_text(root, path),
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

    nickname = worker_module._filename_nickname(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        "{{username}}",
        worker_module._template_folder_text(root, path),
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

    creator = worker_module._filename_creator(
        raw_path,
        template_settings["filename_template"],
        metadata,
        source_url,
        media_id,
    )
    nickname = worker_module._filename_nickname(
        raw_path,
        template_settings["filename_template"],
        template_settings["folder_template"],
        worker_module._template_folder_text(tmp_path, raw_path),
        metadata,
        creator,
    )
    final_path, display_filename = worker_module._clean_resolved_filename(
        source_url,
        raw_path,
        template_settings,
        "tiktok",
        creator_hint=creator,
        media_id_hint=media_id,
        nickname_hint=nickname,
    )
    final_path = worker_module._move_group_to_template_folder(
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
    assert worker_module._metadata_creator(metadata, "891576008993182") == "charechii"


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
    assert worker_module._metadata_creator(metadata, "1727302008412891") == "charechii"


def test_metadata_creator_prefers_at_handle_metadata():
    metadata = {
        "channel": "Mili",
        "uploader": "Mili",
        "creator": "Mili",
        "uploader_id": "@mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    assert worker_module._metadata_creator(metadata, "In5Du5x6MZM") == "mili"


def test_metadata_creator_rejects_opaque_id_metadata():
    metadata = {
        "channel": "UC-wNqHVYS82PF4mkaQb0Alg",
        "uploader": "",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    assert worker_module._metadata_creator(metadata, "In5Du5x6MZM") == ""


def test_filename_creator_uses_handle_metadata_without_at():
    metadata = {
        "channel": "Mili",
        "uploader": "Mili",
        "creator": "Mili",
        "uploader_id": "@mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
        "webpage_url": "https://video.example/watch?v=In5Du5x6MZM",
    }

    creator = worker_module._filename_creator(
        Path("@mili - Iron Lotus [In5Du5x6MZM].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://video.example/watch?v=In5Du5x6MZM",
        "In5Du5x6MZM",
    )

    assert creator == "mili"


def test_filename_creator_strips_at_from_filename_username():
    creator = worker_module._filename_creator(
        Path("@mili - Iron Lotus [In5Du5x6MZM].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        {},
        "https://video.example/watch?v=In5Du5x6MZM",
        "In5Du5x6MZM",
    )

    assert creator == "mili"


def test_role_creator_uses_scraped_token_role():
    creator = worker_module._role_creator(
        {"username": "Trace Artist"},
        {"rule34video": {"artist": "username"}},
        "rule34video",
    )

    assert creator == "Trace Artist"


def test_clean_filename_title_drops_empty_title_sentinels():
    assert clean_filename_title("None") == ""
    assert clean_filename_title(" untitled ") == ""
    assert clean_gallerydl_display_filename("Poster - None [abc123]_1.jpg", "Poster") == "Poster - [abc123].jpg"


def test_clean_template_filename_drops_none_title_segment():
    result = clean_template_filename(
        "Poster - None [abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        media_id="abc123",
    )

    assert result == "Poster - [abc123]_1.jpg"


def test_clean_template_filename_truncates_long_title_when_enabled():
    title = "A" * 140
    result = clean_template_filename(
        f"Poster - {title} [abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        media_id="abc123",
        cleaning={"shorten": True},
    )

    assert result == f"Poster - {'A' * 100} [abc123]_1.jpg"


def test_clean_template_filename_preserves_slug_quality_tokens_without_title():
    result = clean_template_filename(
        "daiwa-scarlet-suokanawer_source - [4483553].mp4",
        "{{slug}}_{{quality}} - [{{id}}]",
        media_id="4483553",
    )

    assert result == "daiwa-scarlet-suokanawer_source - [4483553].mp4"


def test_clean_resolved_filename_renames_real_file_using_settings_template(tmp_path: Path):
    source_url = "https://twitter.com/DohaVT/status/2073635724684054528"
    media_file = tmp_path / "DohaVT - 2073635724684054528 - Video by DohaVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = worker_module._clean_resolved_filename(
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


def test_clean_resolved_filename_title_only_template_falls_back_to_media_id(tmp_path: Path):
    source_url = "https://twitter.com/DohaVT/status/2073635724684054528"
    media_file = tmp_path / "Video by DohaVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = worker_module._clean_resolved_filename(
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

    final_path, display_filename = worker_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "",
        creator_hint="mili",
        media_id_hint="In5Du5x6MZM",
        nickname_hint="Mili",
    )

    expected = tmp_path / "mili - Iron Lotus [In5Du5x6MZM].mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_configured_creator_field_honors_opaque_id_in_priority_order(monkeypatch):
    # channel_id first in the configured order must win, even though the handle
    # heuristics reject it as an opaque identifier.
    monkeypatch.setattr(
        worker_module,
        "get_effective_creator_fields",
        lambda url: {"username": ["channel_id", "uploader"]},
    )
    metadata = {
        "uploader": "Mili",
        "channel": "Mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
    }

    assert (
        worker_module._configured_creator_field(metadata, "https://video.example/watch?v=x", "username")
        == "UC-wNqHVYS82PF4mkaQb0Alg"
    )


def test_configured_creator_field_empty_order_defers_to_heuristics(monkeypatch):
    monkeypatch.setattr(worker_module, "get_effective_creator_fields", lambda url: {})
    metadata = {"channel_id": "UC-wNqHVYS82PF4mkaQb0Alg", "uploader": "Mili"}

    assert worker_module._configured_creator_field(metadata, "https://video.example/watch?v=x", "username") == ""


def test_clean_resolved_filename_keeps_authoritative_creator_over_url_handle(tmp_path: Path):
    # An authoritative (configured) creator must not be clobbered by a URL-derived handle.
    source_url = "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489"
    media_file = tmp_path / "UC1234567890 - Clip [7493558766131039489].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = worker_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "tiktok",
        creator_hint="UC1234567890",
        media_id_hint="7493558766131039489",
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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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

    monkeypatch.setattr(worker_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
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
    assert completed["title"] == "Creator - [1234567890]"
    assert saved[task_id]["resolved_full_path"] == str(first_clean)
    assert saved[task_id]["resolved_filename"] == "Creator - [1234567890].jpg"


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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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

    monkeypatch.setattr(worker_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
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
        worker_module._path_key(first): {"webpage_url": "https://www.example.test/item/asset-a"},
        worker_module._path_key(second): {"webpage_url": "https://www.example.test/item/asset-b"},
    }

    groups = worker_module._download_groups(
        [first, second],
        engine_by_name("gallerydl"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://www.example.test/post/root123",
    )

    assert len(groups) == 2
    assert {group["media_id"] for group in groups} == {"asset-a", "asset-b"}


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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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

    monkeypatch.setattr(worker_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)
    monkeypatch.setattr(gallerydl_module, "count_gallerydl_items", lambda *args, **kwargs: 1)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert commands == ["yt-dlp", "gallery-dl"]
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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[list[str]] = []
    count_kwargs: list[dict] = []

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

    monkeypatch.setattr(worker_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: False)

    def fake_count_gallerydl_items(*args, **kwargs):
        count_kwargs.append(kwargs)
        return 3

    monkeypatch.setattr(gallerydl_module, "count_gallerydl_items", fake_count_gallerydl_items)
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_video = tmp_path / "love.rizzzz - [DOS-dVRkUK3].mp4"
    clean_image = tmp_path / "love.rizzzz - [DOS-dVRkUK3]_2.jpg"
    completed = store["tasks"][task_id]
    assert [cmd[0] for cmd in commands] == ["yt-dlp", "gallery-dl"]
    gallery_cmd = commands[1]
    filter_expr = gallery_cmd[gallery_cmd.index("--filter") + 1]
    assert "mp4" in filter_expr
    assert "webm" in filter_expr
    assert ".mp4" in count_kwargs[0]["excluded_extensions"]
    assert set(saved) == {task_id}
    assert clean_video.is_file()
    assert clean_image.is_file()
    assert not ytdlp_video.exists()
    assert not gallery_video.exists()
    assert not gallery_image.exists()
    assert not stale_wrong_video.exists()
    assert completed["status"] == "completed"
    assert completed["engine"] == "ytdlp"
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
                "template_settings": {
                    "folder_template": "{{username}}",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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

    monkeypatch.setattr(worker_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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

    monkeypatch.setattr(worker_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "load_task_store", lambda: store)
    monkeypatch.setattr(worker_module, "update_task", fake_update_task)
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


def test_task_to_api_cleans_existing_raw_gallerydl_display_filename(tmp_path: Path):
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

    assert api_task["resolved_filename"] == "fzyahoo.com - [7420705673542978833].jpg"


def test_task_to_api_prefers_tiktok_url_creator_over_stored_display_name(tmp_path: Path):
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

    assert api_task["creator"] == "fzyahoo.com"


def test_worker_prefers_tiktok_url_creator_over_ytdlp_sidecar(tmp_path: Path):
    sidecar = tmp_path / "creator.txt"
    sidecar.write_text("Some Display Name\n", encoding="utf-8")

    class FakeEngine:
        def read_creator(self, sidecar_path: str, source_url: str) -> str:
            return "Some Display Name"

    creator = worker_module._resolved_task_creator(
        FakeEngine(),
        str(sidecar),
        "https://www.tiktok.com/@fzyahoo.com/video/7420705673542978833",
        "fzyahoo.com - Clip [7420705673542978833].mp4",
    )

    assert creator == "fzyahoo.com"


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
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    result = scan_module.scan_media_library([media_root])

    assert result == {"checked": 0, "missing": 0, "added": 1}
    assert saved["disk:abc123"]["resolved_full_path"] == str(media_file)
    assert saved["disk:abc123"]["resolved_filename"] == media_file.name
    assert saved["disk:abc123"]["source_key"] == ""


def test_scan_media_library_infers_source_from_named_platform_folder(
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: set())
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: set())
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
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


def test_learn_download_marks_creator_segment():
    learned = learn_download({}, "https://twitter.com/DohaVT/status/2073635724684054528", "2073635724684054528")
    learned = learn_download(learned, "https://twitter.com/Other/status/1111111111111111111", "1111111111111111111")
    assert learned["twitter"]["templates"][0] == "https://twitter.com/{creator}/status/{id}"
    assert learned["twitter"]["creator_part"] == "path:0"


def test_learn_download_trims_seo_query_and_keeps_creator_token():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489?lang=en&q=fzyahoo&t=1781279478413",
        "7493558766131039489",
    )

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert reconstruct_url(learned, "tiktok", "7493558766131039489") == ""


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

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert set(learned["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@{creator}/video/{id}",
        "https://www.tiktok.com/@{creator}/photo/{id}",
    }


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


def test_learn_download_keeps_descriptive_segment_literal_for_single_sample():
    # Slug is no longer auto-detected: a lone descriptive segment stays literal in the
    # learned shape; a configured slug part overrides it at reconstruction time.
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


def test_reconstruct_url_replaces_literal_segment_with_configured_slug_value():
    learned = {
        "rule34video": {
            "templates": ["https://rule34video.com/video/{id}/cocolia-rand-sutekimeppou"],
        }
    }

    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds - minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_reconstruct_url_candidates_needs_creator_and_id():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489",
        "7493558766131039489",
    )
    assert reconstruct_url_candidates(learned, "tiktok", "123") == []
    assert reconstruct_url_candidates(learned, "tiktok", "") == []


def test_creator_from_url_uses_handle_segment_without_at_sign():
    assert creator_from_url("https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489") == "fzyahoo.com"
    assert (
        creator_from_url("https://www.tiktok.com/@fzyahoo.com/video/7493558766131039489", strip_at=False)
        == "@fzyahoo.com"
    )
    assert creator_from_url("https://x.com/ININIinNINI/status/2073390288501166083") == "ININIinNINI"


def test_extract_url_part_reads_configured_path_segment():
    # A configured slug part reads its value straight from the (canonical) URL, generically.
    url = "https://rule34video.com/video/3056158/84-minus8/"
    assert media_id_from_url(url) == "3056158"
    assert extract_url_part(url, "path:2") == "84-minus8"
    assert extract_url_part(url, "path:0") == "video"
    assert extract_url_part(url, "path:9") == ""


def test_extract_url_part_reads_query_value():
    url = "https://example.com/watch?v=dQw4w9WgXcQ&list=PL123"
    assert extract_url_part(url, "query:list") == "PL123"
    assert extract_url_part(url, "query:missing") == ""


def test_describe_learned_segments_marks_id_reserved_and_slug_selectable():
    learned = learn_download(
        {},
        "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/",
        "4483553",
    )
    described = describe_learned_segments(learned["rule34video"])
    parts = {seg["part"]: seg for seg in described["segments"]}
    assert parts["path:1"]["kind"] == "id" and parts["path:1"]["reserved"] is True
    # The descriptive segment is selectable so the user can name a slug token for it.
    assert parts["path:2"]["reserved"] is False
    assert parts["path:2"]["label"] == "daiwa-scarlet-suokanawer"


def test_convert_template_quality_uses_selected_label_best_reads_source():
    tmpl = "{{id}}_{{quality}}"
    url = "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/"
    assert convert_template_to_ytdlp(tmpl, url, {"mode": "video", "video_quality": "best"}).endswith("_source")
    assert convert_template_to_ytdlp(tmpl, url, {"mode": "video", "video_quality": "1080p"}).endswith("_1080p")


def test_convert_template_source_token_is_removed():
    tmpl = "{{id}}_{{source}}"
    url = "https://rule34video.com/video/3238394/wsds-minus8/"

    result = convert_template_to_ytdlp(tmpl, url, {"mode": "video", "video_quality": "best"})

    assert result == "%(id|NA)s_"


def test_convert_template_ext_token_is_removed():
    assert convert_template_to_ytdlp("{{ext}}", "https://example.com/x") == ""
    assert gallerydl_module.convert_template_to_gallerydl("{{ext}}", "https://example.com/x") == ""


def test_convert_template_quality_without_selection_keeps_metadata_specifier():
    # Direct callers with no quality threaded through fall back to the delivered format.
    assert "%(format_id" in convert_template_to_ytdlp("{{quality}}", "https://example.com/x")
    assert convert_template_to_ytdlp("{{source}}", "https://example.com/x") == ""


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
    entry = {"task_type": "disk", "source_url": "https://www.tiktok.com/@a/photo/7615077542189337873"}
    real_url = "https://www.tiktok.com/@a/video/7615077542189337873"

    out = operations_module._correct_reconstructed_url("disk:7615077542189337873", entry, real_url)

    assert out["source_url"] == real_url
    assert saved["disk:7615077542189337873"]["source_url"] == real_url


def test_history_source_lookup_matches_filename_media_id_without_stored_url(monkeypatch):
    entry = {
        "task_type": "disk",
        "source_url": "",
        "source_key": "rule34video",
        "media_id": "3238394",
        "resolved_filename": "wsds-minus8_source [3238394].mp4",
    }
    monkeypatch.setattr(history_module, "load_history", lambda: {"entries": {"disk:3238394": entry}})

    task_id, found = history_module.find_history_by_source("https://rule34video.com/video/3238394/wsds-minus8/")

    assert task_id == "disk:3238394"
    assert found is entry


def test_correct_reconstructed_url_leaves_real_download_untouched(monkeypatch):
    saved: dict[str, dict] = {}
    monkeypatch.setattr(operations_module, "save_history_entry_row", lambda tid, p: saved.update({tid: p}))
    real_url = "https://www.tiktok.com/@a/video/7615077542189337873"
    entry = {"task_type": "ytdlp", "source_url": real_url}

    out = operations_module._correct_reconstructed_url("ytdlp:abc", entry, "https://www.tiktok.com/@a/photo/7615077542189337873")

    assert out["source_url"] == real_url  # a real download's link is authoritative; never overwritten
    assert saved == {}


def test_prune_disk_shadows_drops_disk_duplicate_of_real_download(monkeypatch):
    removed: list[str] = []
    monkeypatch.setattr(scan_module, "remove_history_record", lambda tid: removed.append(tid))
    records = {
        "ytdlp:abc": {"source_url": "https://www.tiktok.com/@a/video/7615077542189337873", "media_id": ""},
        "disk:7615077542189337873": {
            "task_type": "disk",
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
    index = scan_module._source_location_index({"youtube": str(folder)})

    source_key, pending, _ = scan_module.infer_disk_source(
        media_file, "2073635724684054528", index, _learned_youtube_twitter()
    )

    assert source_key == "twitter"
    assert pending is False


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

    source_key, pending, candidates = scan_module.infer_disk_source(
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
    index = scan_module._source_location_index({"youtube": str(folder)})

    source_key, pending, candidates = scan_module.infer_disk_source(
        media_file, "dQw4w9WgXcQ", index, _learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert pending is False
    assert candidates == []


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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {"youtube": str(platform_dir)})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Cool Channel"


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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {"youtube": str(platform_dir)})
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Cool Channel"
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {"rule34video": str(platform_dir)})
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: (
            {"folder_template": "", "filename_template": "{{artist}} - {{title}} [{{id}}]"},
            {"rule34video": {"folder_template": "", "filename_template": "{{artist}} - {{title}} [{{id}}]"}},
        ),
    )
    monkeypatch.setattr(scan_module, "_scan_token_role_map", lambda: {"rule34video": {"artist": "username"}})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Trace Artist"
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: ({"folder_template": "{{quality}}", "filename_template": "{{title}} [{{id}}]"}, {}),
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == ""


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


def test_scan_media_library_reconstructs_slug_url_from_filename_template(
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
    monkeypatch.setattr(scan_module, "_scan_source_profile_keys", lambda: {"rule34video"})
    monkeypatch.setattr(
        scan_module,
        "_scan_template_map",
        lambda: (
            {"folder_template": "", "filename_template": "{{slug}}_{{quality}} [{{id}}]"},
            {"rule34video": {"folder_template": "", "filename_template": "{{slug}}_{{quality}} [{{id}}]"}},
        ),
    )
    # The user mapped path segment 2 to a slug token named "slug"; capture + reconstruct.
    monkeypatch.setattr(
        scan_module,
        "_scan_slug_tokens_map",
        lambda: {"rule34video": [{"part": "path:2", "token": "slug"}]},
    )
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    entry = saved["disk:3238394"]
    assert entry["source_key"] == "rule34video"
    assert entry["source_pending"] is False
    assert entry["source_url"] == "https://rule34video.com/video/3238394/wsds-minus8"


def _patch_scan_common(monkeypatch: pytest.MonkeyPatch, saved: dict[str, dict], platform_dir: Path) -> None:
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {"youtube": str(platform_dir)})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
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
        "_scan_creator_fields_map",
        lambda: {"youtube": {"username": ["channel_id", "uploader"]}},
    )
    probed: list[str] = []
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: probed.append(url) or {"uploader": "Mili", "channel_id": "UCopaque123"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "UCopaque123"
    assert saved["disk:abc123"]["source_url"] == "https://www.youtube.com/watch?v=abc123"
    assert probed == ["https://www.youtube.com/watch?v=abc123"]


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
        "_scan_creator_fields_map",
        lambda: {"youtube": {"username": ["channel_id"], "nickname": ["uploader", "channel"]}},
    )
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: {"channel_id": "UCopaque123", "uploader": "Mili Display", "channel": "Mili"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Mili Display"


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
        "_scan_creator_fields_map",
        lambda: {"youtube": {"username": ["scraper[artist]", "channel"]}},
    )

    def fail_probe(url, *, with_cookies=False):
        raise AssertionError("scraper-backed username must not probe engine metadata")

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", fail_probe)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Scraped Artist"
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
        "_scan_creator_fields_map",
        lambda: {"youtube": {"username": ["channel", "scraper[artist]"]}},
    )
    probed: list[str] = []
    monkeypatch.setattr(
        scan_module,
        "_scan_probe_metadata",
        lambda url, *, with_cookies=False: probed.append(url) or {"channel": "Probed Channel"},
    )

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Probed Channel"
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
        "task_id": "disk:abc123",
        "media_id": "abc123",
        "task_type": "disk",
        "artist": "AlreadyResolved",
        "source_url": "https://www.youtube.com/watch?v=abc123",
        "resolved_full_path": str(media_file),
    }
    saved: dict[str, dict] = {}
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {"disk:abc123": prior}})
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {"youtube": str(platform_dir)})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    def _fail_probe(url, *, with_cookies=False):
        raise AssertionError("probe must not run for an already-resolved file")

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", _fail_probe)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "AlreadyResolved"
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

    assert saved["gallerydl:abc123"]["task_type"] == "gallerydl"
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
            "task_type": "gallerydl",
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
            "task_type": "gallerydl",
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
    default = clean_template_filename(name, template, creator="alice")
    shortened = clean_template_filename(name, template, creator="alice", cleaning={"shorten": True})
    full = clean_template_filename(name, template, creator="alice", cleaning={"shorten": False})
    assert default == full
    assert len(shortened) < len(full)
    assert long_title in full
