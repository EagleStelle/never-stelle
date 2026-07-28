from __future__ import annotations

import json
import time
import zipfile
from pathlib import Path

import pytest

import backend.app.domains.downloads.files as files_module
import backend.app.domains.downloads.gallerydl as gallerydl_module
import backend.app.domains.downloads.history as history_module
import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.scan as scan_module
import backend.app.domains.downloads.serializers as serializers_module
import backend.app.domains.downloads.urls as urls_module
import backend.app.domains.downloads.workers.completion as completion_module
import backend.app.domains.downloads.workers.completion_learning as completion_learning_module
import backend.app.domains.downloads.workers.completion_metadata as completion_metadata_module
import backend.app.domains.downloads.workers.execution as worker_module
import backend.app.domains.downloads.workers.runner as runner_module
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
    sanitize_filename_component,
    sanitize_path_literal,
    strip_numbered_suffix,
    strip_placeholder_title,
)
from backend.app.domains.downloads.serializers import history_to_api, task_to_api
from backend.app.domains.downloads.ytdlp import (
    YTDLP_NICKNAME_FIELD,
    YTDLP_USERNAME_FIELD,
    clean_filename_title,
    clean_social_title,
)


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
        site_locations={"example": {"https://example.com/{id}": str(tmp_path)}},
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
    assert captured["task"]["engine"] == "gallerydl"
    assert "engine_policy" not in captured["task"]
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
        quality={"mode": "video", "video_container": "webm", "video_codec": "vp9"},
    )
    assert captured["task"]["quality"]["video_container"] == "webm"
    assert captured["task"]["quality"]["video_codec"] == "vp9"


def test_retry_task_migrates_to_gallerydl_engine(monkeypatch: pytest.MonkeyPatch):
    store = {
        "tasks": {
            "ytdlp:failed": {
                "status": "failed",
                "engine": "ytdlp",
                "source_url": "https://example.test/watch?v=1",
                "output_dir": "/media/example",
                "template_settings": {"folder_template": "", "filename_template": "{{title}} [{{id}}]"},
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
        "task_type": "ytdlp",
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


NAMING_TEMPLATE = "{{username}} - {{title}} [{{id}}]"
NAMING_SAMPLE = "nasa - Café Rocket Launch [aB3dK9x].jpg"


def _named(**cleaning: object) -> str:
    return clean_template_filename(
        NAMING_SAMPLE,
        NAMING_TEMPLATE,
        creator="nasa",
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
    result = clean_template_filename(
        "nasa - naïve don't stop [aB3dK9x].jpg",
        NAMING_TEMPLATE,
        creator="nasa",
        media_id="aB3dK9x",
        cleaning={"case": "capitalized"},
    )

    assert result == "Nasa - Naïve Don't Stop [aB3dK9x].jpg"


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


def test_clean_template_filename_drops_none_title_segment():
    result = clean_template_filename(
        "Poster - None [abc123]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="Poster",
        media_id="abc123",
    )

    assert result == "Poster - [abc123]_1.jpg"


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


def test_clean_resolved_filename_rerenders_selected_quality(tmp_path: Path):
    source_url = "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/"
    media_file = tmp_path / "source - Video by Artist [4483553].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = worker_module._clean_resolved_filename(
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


def test_clean_resolved_filename_rebuilds_sparse_gallerydl_name_from_title_hint(tmp_path: Path):
    source_url = "https://example.com/alice/post/abc123"
    media_file = tmp_path / "[abc123]_1.jpg"
    media_file.write_bytes(b"image")

    final_path, display_filename = worker_module._clean_resolved_filename(
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


def test_configured_field_value_honors_opaque_id_in_priority_order(monkeypatch):
    # channel_id first in the configured order must win, even though the handle
    # heuristics reject it as an opaque identifier.
    monkeypatch.setattr(
        worker_module,
        "get_effective_fields",
        lambda url: {"username": ["channel_id", "uploader"]},
    )
    metadata = {
        "uploader": "Mili",
        "channel": "Mili",
        "channel_id": "UC-wNqHVYS82PF4mkaQb0Alg",
    }

    assert (
        worker_module._configured_field_value(metadata, "https://video.example/watch?v=x", "username")
        == "UC-wNqHVYS82PF4mkaQb0Alg"
    )


def test_configured_field_value_empty_order_defers_to_heuristics(monkeypatch):
    monkeypatch.setattr(worker_module, "get_effective_fields", lambda url: {})
    metadata = {"channel_id": "UC-wNqHVYS82PF4mkaQb0Alg", "uploader": "Mili"}

    assert worker_module._configured_field_value(metadata, "https://video.example/watch?v=x", "username") == ""


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


def test_gallerydl_sparse_single_output_uses_probe_metadata_for_template(
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
                "template_settings": {
                    "folder_template": "{{username}}",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
            }
        }
    }
    saved: dict[str, dict] = {}

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
        lambda url, source_key="": {
            "id": "abc123",
            "webpage_url": source_url,
            "channel": "ChannelHandle",
            "uploader": "Channel Name",
            "title": "Nice clip",
        },
    )
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "_learn_field_roles_from_download", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    clean_video = tmp_path / "ChannelHandle" / "ChannelHandle - Nice clip [abc123].mp4"
    assert completed["status"] == "completed"
    assert clean_video.is_file()
    assert not raw_video.exists()
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_folder"] == str(clean_video.parent)
    assert completed["resolved_filename"] == clean_video.name
    assert completed["creator"] == "ChannelHandle"
    assert saved[task_id]["template_settings"] == store["tasks"][task_id]["template_settings"]


def test_gallerydl_cookie_probe_repairs_none_creator_and_duplicate_id_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_id = "DZwrrifkye4"
    raw_video = tmp_path / f"None - [{media_id}] [{media_id}].mp4"
    raw_video.write_bytes(b"video")
    source_url = f"https://www.instagram.com/reel/{media_id}/"
    task_id = "gallerydl:instagram-cookie-metadata"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
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
    monkeypatch.setattr(worker_module, "get_effective_fields", lambda url: {"username": ["username"]})
    monkeypatch.setattr(
        completion_metadata_module,
        "_probe_output_metadata",
        lambda url, source_key="": {
            "id": media_id,
            "webpage_url": source_url,
            "username": "real.creator",
            "title": f"None - [{media_id}]",
        },
    )
    monkeypatch.setattr(worker_module, "_learn_source_format", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "_learn_field_roles_from_download", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    clean_video = tmp_path / f"real.creator - [{media_id}].mp4"
    assert completed["status"] == "completed"
    assert clean_video.is_file()
    assert not raw_video.exists()
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_filename"] == clean_video.name
    assert completed["creator"] == "real.creator"
    assert saved[task_id]["resolved_filename"] == clean_video.name


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
        worker_module._path_key(first): {"webpage_url": "https://www.example.test/item/asset-a"},
        worker_module._path_key(second): {"webpage_url": "https://www.example.test/item/asset-b"},
    }

    groups = worker_module._download_groups(
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
        worker_module._path_key(first): {"webpage_url": "https://www.example.test/item/child-a"},
        worker_module._path_key(second): {"webpage_url": "https://www.example.test/item/child-b"},
    }

    groups = worker_module._download_groups(
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
        worker_module._item_source_url(source_url, "example", "root123", "poster", metadata)
        == source_url
    )


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

    row = metadata[worker_module._path_key(media_file)]
    assert row["id"] == "child-a"
    assert row["user[name]"] == "poster"
    assert row["tags"] == "one, two"


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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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
                "template_settings": {
                    "folder_template": "",
                    "filename_template": "{{username}} - {{title}} [{{id}}]",
                },
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
    assert not stale_wrong_video.exists()
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
            "template_settings": {
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            },
        },
    )

    assert api_task["resolved_filename"] == "ChannelHandle - Nice clip [abc123].mp4"


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
        "creator": "ChannelHandle",
        "media_id": "abc123",
        "title": "Nice clip",
        "resolved_full_path": str(media_file),
        "resolved_filename": media_file.name,
        "template_settings": {
            "folder_template": "{{username}}",
            "filename_template": "{{username}} - {{title}} [{{id}}]",
        },
    }

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": {"gallerydl:test": task}})
    monkeypatch.setattr(operations_module, "find_history_by_id", lambda task_id: None)
    monkeypatch.setattr(
        operations_module,
        "recover_task_path",
        lambda task_id, task, persist=True: (str(media_file), str(tmp_path), media_file.name),
    )
    monkeypatch.setattr(operations_module, "find_numbered_media_siblings", lambda path: [path])

    path, filename, archive = operations_module.resolve_task_file("gallerydl:test")

    assert path == media_file
    assert filename == "ChannelHandle - Nice clip [abc123].mp4"
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

    creator = worker_module._resolved_task_creator(
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

    assert result == {"checked": 0, "missing": 0, "added": 1, "unchanged": 0}
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
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


def test_worker_reprobes_field_roles_only_when_format_is_new(monkeypatch):
    calls: list[tuple[str, str]] = []
    url = "https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833"

    monkeypatch.setattr(completion_learning_module, "persist_source_format", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        completion_learning_module,
        "learn_missing_fields_for_format",
        lambda source_url, source_key: calls.append((source_url, source_key)),
    )

    completion_learning_module._learn_source_format(url, "fzyahoo.com - [7420705673542978833].jpg", source_key="tiktok")

    monkeypatch.setattr(completion_learning_module, "persist_source_format", lambda *args, **kwargs: False)
    completion_learning_module._learn_source_format(url, "fzyahoo.com - [7420705673542978833].jpg", source_key="tiktok")

    assert calls == [(url, "tiktok")]


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
    index = scan_module._source_location_index(
        {"youtube": {"https://www.youtube.com/watch?v={id}": str(folder)}}
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
        {"youtube": {"https://www.youtube.com/watch?v={id}": str(folder)}}
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
        {
            "youtube": {
                "https://www.youtube.com/watch?v={id}": str(tmp_path / "yt"),
                "https://www.youtube.com/shorts/{id}": str(shorts),
            }
        }
    )

    source_key, _, _, format_template = scan_module.infer_disk_source(
        media_file, "dQw4w9WgXcQ", index, _learned_youtube_twitter()
    )

    assert source_key == "youtube"
    assert format_template == "https://www.youtube.com/shorts/{id}"


def test_source_location_index_keeps_one_source_sharing_a_folder(tmp_path: Path):
    shared = str(tmp_path / "yt")
    index = scan_module._source_location_index(
        {
            "youtube": {
                "https://www.youtube.com/watch?v={id}": shared,
                "https://www.youtube.com/shorts/{id}": shared,
            }
        }
    )

    # One source, two formats: the source is still unambiguous, the format is not.
    assert [(key, fmt) for _, key, fmt in index] == [("youtube", "")]


def test_source_location_index_drops_a_folder_two_sources_share(tmp_path: Path):
    shared = str(tmp_path / "shared")
    index = scan_module._source_location_index(
        {
            "youtube": {"https://www.youtube.com/watch?v={id}": shared},
            "twitter": {"https://twitter.com/{creator}/status/{id}": shared},
        }
    )

    assert index == []


def test_source_folder_keys_covers_every_format_folder(tmp_path: Path):
    watch = tmp_path / "yt"
    shorts = tmp_path / "yt-shorts"
    keys = scan_module._source_folder_keys(
        {
            "youtube": {
                "https://www.youtube.com/watch?v={id}": str(watch),
                "https://www.youtube.com/shorts/{id}": str(shorts),
            }
        }
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: _scan_locations("youtube", platform_dir))
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(
        scan_module,
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
    )
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: None)

    scan_module.scan_media_library([media_root])

    assert saved["disk:abc123"]["artist"] == "Cool Channel"


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
        "_scan_location_map",
        lambda: {"youtube": {watch_format: str(watch_dir), shorts_format: str(shorts_dir)}},
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
    assert entry["template_settings"] == per_source["youtube"][shorts_format]
    assert entry["title"] == "Soft Light"
    assert entry["artist"] == "Cool Channel"


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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: _scan_locations("youtube", platform_dir))
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
    monkeypatch.setattr(
        scan_module,
        "_scan_location_map",
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
        "save_history_entry_rows",
        lambda rows: saved.update(dict(rows)),
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: {})
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
    return {source_key: {format_template: str(folder)}}


def _patch_scan_common(monkeypatch: pytest.MonkeyPatch, saved: dict[str, dict], platform_dir: Path) -> None:
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": {}})
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: _scan_locations("youtube", platform_dir))
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

    assert saved["disk:abc123"]["artist"] == "UCopaque123"
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
        "_scan_location_map",
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

    assert saved[f"disk:{media_id}"]["artist"] == "moli0n"
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
        "_scan_location_map",
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

    assert saved[f"disk:{media_id}"]["artist"] == "wrongname"
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
        "_scan_field_roles_map",
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
    monkeypatch.setattr(scan_module, "_scan_location_map", lambda: _scan_locations("youtube", platform_dir))
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
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda rows: None)
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: removed_tasks.append(task_id))
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: removed_history.append(task_id))

    result = scan_module.scan_media_library([tmp_path])

    assert result == {"checked": 1, "missing": 1, "added": 0, "unchanged": 0}
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
                    "task_type": "disk",
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

    assert result == {"checked": 1, "missing": 0, "added": 0, "unchanged": 0}
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
                    "task_type": "disk",
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

    assert result == {"checked": 1, "missing": 0, "added": 0, "unchanged": 0}
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
                    "task_type": "disk",
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

    assert result == {"checked": 1, "missing": 0, "added": 0, "unchanged": 0}
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
                    "task_type": "disk",
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
    assert second == {"checked": 4, "missing": 0, "added": 0, "unchanged": 4}
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
