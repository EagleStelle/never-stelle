from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.domains.downloads.engines.ytdlp import (
    clean_filename_title,
    clean_social_title,
)
from backend.app.domains.downloads.naming.filenames import (
    parse_filename_media_id,
    sanitize_filename_component,
    sanitize_path_literal,
    shorten_filename_title,
    strip_numbered_suffix,
)
from backend.app.domains.downloads.naming.render import (
    clean_template_filename,
    filed_creator,
    filename_template_title,
)
from backend.app.domains.downloads.naming.titles import strip_placeholder_title

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


def test_parse_filename_media_id_uses_last_bracketed_id():
    assert parse_filename_media_id("Creator - Soft Light [Abc_123-xy].mp4") == (
        "Abc_123-xy",
        "Creator - Soft Light",
    )


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
    assert clean_social_title("Soft Light ｜ AB Demo on Reels") == "Soft Light"
    assert clean_social_title("AB Demo - Video by AB Demo", "AB Demo") == "AB Demo"
    assert clean_social_title("Video by AB Demo", "AB Demo") == ""
    assert clean_social_title("Photo by AB Demo - 12K likes", "AB Demo") == ""


def test_clean_filename_title_removes_duplicate_social_display_name():
    title = (
        "DEMOinARTIST - "
        "\u5c71\u7530 \u30c7\u30e2 \u2726\u2726 - "
        "Blender\u3067\u3064\u304f\u308b\u3001 \u79cb\u30a4\u30e9\u30b9\u30c8"
        "\u306e\u30e1\u30a4\u30ad\u30f3\u30b0\u898b\u3066\u2026\uff01\uff01"
    )

    assert clean_filename_title(title, "DEMOinARTIST") == (
        "DEMOinARTIST - "
        "Blender\u3067\u3064\u304f\u308b\u3001 \u79cb\u30a4\u30e9\u30b9\u30c8"
        "\u306e\u30e1\u30a4\u30ad\u30f3\u30b0\u898b\u3066\u2026\uff01\uff01"
    )


def test_clean_filename_title_keeps_content_like_leading_segment():
    title = "DEMOinARTIST - Part 1 - Blender autumn sketch process"

    assert clean_filename_title(title, "DEMOinARTIST") == title


def test_clean_social_title_strips_trailing_creator_byline():
    raw = "6.9M views · 66K reactions | Is this real life? | Pagename"
    assert clean_social_title(raw, "demopage", ("Pagename",)) == "Is this real life?"
    # A plain-space trailing name is left intact; only strong separators mark a byline.
    assert clean_social_title("A letter to Pagename", "demopage", ("Pagename",)) == "A letter to Pagename"


def test_clean_social_title_drops_generic_post_caption():
    assert clean_social_title("Photos from Pagename's post", "demopage") == ""
    assert clean_social_title("Video from Pagename’s timeline", "demopage") == ""
    # Real captions that merely start with a media word survive.
    assert clean_social_title("Photos from my trip to Japan", "demopage") == "Photos from my trip to Japan"


def test_clean_template_filename_redacts_duplicate_display_name():
    name = "Pagename - Is this real life？ ｜ Pagename [800000000000001].mp4"
    template = "{{username}} - {{title}} [{{id}}]"
    result = clean_template_filename(
        name,
        template,
        creator="demopage",
        title=filename_template_title(name, template),
        media_id="800000000000001",
    )
    assert result == "demopage - Is this real life？ [800000000000001].mp4"


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


def test_clean_template_filename_drops_generic_post_caption():
    result = clean_template_filename(
        "demopage - Photos from Pagename's post [pfbid02DemoPostAa]_1.jpg",
        "{{username}} - {{title}} [{{id}}]",
        creator="demopage",
        media_id="pfbid02DemoPostAa",
    )
    assert result == "demopage - [pfbid02DemoPostAa]_1.jpg"


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
        quality={"mode": "merged", "video_quality": "1080p"},
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


def test_multibyte_title_shortening_and_byte_safety():
    # Long Japanese ASMR title with emojis and symbols (92 chars, 221 bytes)
    japanese_title = (
        "【ASMR】オノマトペとよしよし♡こちょこちょ♡猫の尻尾でもふもふ🐾ぎゅー…♡意識がとろけてゾクゾク気持ちいい囁き┊︎耳ふー_睡眠導入【Handmovement_Ear.Massag】"
    )
    # 1. Shorten with max_chars=60
    shortened_60 = shorten_filename_title(japanese_title, max_chars=60)
    assert len(shortened_60) <= 60
    assert len(shortened_60.encode("utf-8")) <= 200

    # 2. Shorten with default max_chars=100 ensures byte safety (<= 200 bytes)
    shortened_100 = shorten_filename_title(japanese_title, max_chars=100)
    assert len(shortened_100.encode("utf-8")) <= 200

    # 3. clean_template_filename with shorten=True caps title while keeping id
    template = "{{username}} - {{title}} [{{id}}]"
    name = f"@runa_asmr_c1r - {japanese_title} [f4O9COxuIgo].mp4"
    cleaned_shorten = clean_template_filename(
        name,
        template,
        creator="@runa_asmr_c1r",
        title=japanese_title,
        media_id="f4O9COxuIgo",
        cleaning={"shorten": True, "max_chars": 40},
    )
    assert len(cleaned_shorten.encode("utf-8")) <= 240
    assert "f4O9COxuIgo" in cleaned_shorten

    # 4. clean_template_filename with stem_max_chars caps whole stem length safely
    cleaned_stem = clean_template_filename(
        name,
        template,
        creator="@runa_asmr_c1r",
        title=japanese_title,
        media_id="f4O9COxuIgo",
        cleaning={"stem_max_chars": 50},
    )
    assert len(cleaned_stem.encode("utf-8")) <= 240
    assert len(Path(cleaned_stem).stem) <= 50


@pytest.mark.parametrize(
    ("templates", "nickname", "expected"),
    [
        ({"folder_template": "{{username}}"}, "Alice Films", "alice_handle"),
        ({"folder_template": "{{nickname}}"}, "Alice Films", "Alice Films"),
        # A row that recorded no nickname was named by its username.
        ({"folder_template": "{{nickname}}"}, "", "alice_handle"),
        # The folder names no creator, so the first template that does decides.
        ({"folder_template": "{{quality}}", "filename_template": "{{nickname}}"}, "Alice Films", "Alice Films"),
        ({"folder_template": "{{id}}", "filename_template": "{{title}}"}, "Alice Films", "alice_handle"),
    ],
)
def test_filed_creator_takes_the_creator_token_the_row_is_filed_by(templates, nickname, expected):
    row = {**templates, "creator": "alice_handle", "resolved_tokens": {"nickname": nickname}}

    assert filed_creator(row) == expected


def test_filed_creator_cleans_the_handle_like_a_folder_token():
    row = {"folder_template": "{{username}}", "creator": "@alice_handle"}

    assert filed_creator(row) == "alice_handle"
    assert filed_creator(row, {"strip_handle_at": False}) == "@alice_handle"
