from __future__ import annotations

import pytest

import backend.app.services.tasks.probe as probe_module
from backend.app.services.tasks.probe import (
    _creator_probe_fields,
    _entry_url,
    _flatten_metadata,
    _gallerydl_richest_metadata,
    _radio_single_url,
    _strip_playlist_param,
    probe_creator_fields,
    probe_url,
)


def test_strip_playlist_param_drops_only_list():
    stripped = _strip_playlist_param("https://www.youtube.com/watch?v=abc&list=RD123&t=5")
    assert "list=" not in stripped
    assert "v=abc" in stripped
    assert "t=5" in stripped


@pytest.mark.parametrize(
    "url,expected",
    [
        # Anchored radio link -> download the single video it points at.
        ("https://www.youtube.com/watch?v=abc&list=RD123", "https://www.youtube.com/watch?v=abc"),
        # Bare radio playlist page (no v=) is not radio-single; must fall through.
        ("https://www.youtube.com/playlist?list=RD123", ""),
        # Normal playlist id (not RD) is not a radio.
        ("https://www.youtube.com/watch?v=abc&list=PL123", ""),
        # Non-youtube host never counts as radio.
        ("https://vimeo.com/watch?v=abc&list=RD123", ""),
    ],
)
def test_radio_single_url(url, expected):
    assert _radio_single_url(url) == expected


@pytest.mark.parametrize(
    "entry,expected",
    [
        ({"url": "https://youtu.be/xyz"}, "https://youtu.be/xyz"),
        ({"id": "xyz"}, "https://www.youtube.com/watch?v=xyz"),
        ({"url": "xyz"}, "https://www.youtube.com/watch?v=xyz"),
        ({}, ""),
    ],
)
def test_entry_url(entry, expected):
    assert _entry_url(entry) == expected


def test_flat_playlist_includes_js_runtimes_flags(monkeypatch):
    recorded_cmd = []

    def fake_run(cmd, **kwargs):
        recorded_cmd.extend(cmd)
        class Dummy:
            returncode = 0
            stdout = '{"_type": "video"}'
            stderr = ""
        return Dummy()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    probe_module._flat_playlist("https://www.youtube.com/watch?v=abc")

    assert "--js-runtimes" in recorded_cmd
    assert "node" in recorded_cmd
    assert "--remote-components" in recorded_cmd
    assert "ejs:github" in recorded_cmd


def test_probe_url_rejects_empty():
    with pytest.raises(ValueError):
        probe_url("   ")


def test_probe_url_classifies_radio_without_spawning_ytdlp(monkeypatch):
    def _fail(_url):
        raise AssertionError("radio must resolve before probing yt-dlp")

    monkeypatch.setattr(probe_module, "_flat_playlist", _fail)

    result = probe_url("https://www.youtube.com/watch?v=abc&list=RD123")

    assert result == {
        "kind": "radio",
        "url": "https://www.youtube.com/watch?v=abc",
        "title": "",
        "entries": [],
    }


def test_probe_url_classifies_single_video(monkeypatch):
    monkeypatch.setattr(probe_module, "_flat_playlist", lambda url: {"_type": "video"})

    result = probe_url("https://www.youtube.com/watch?v=abc")

    assert result["kind"] == "video"
    assert result["entries"] == []


def test_probe_url_expands_playlist_entries(monkeypatch):
    payload = {
        "_type": "playlist",
        "title": "My List",
        "entries": [
            {"id": "aaa", "title": "First", "uploader": "Chan", "duration": 10},
            {"url": "https://youtu.be/bbb", "title": "Second", "channel": "Other"},
            {"title": "no id or url -> skipped"},
        ],
    }
    monkeypatch.setattr(probe_module, "_flat_playlist", lambda url: payload)

    result = probe_url("https://www.youtube.com/playlist?list=PL123")

    assert result["kind"] == "playlist"
    assert result["title"] == "My List"
    assert [entry["url"] for entry in result["entries"]] == [
        "https://www.youtube.com/watch?v=aaa",
        "https://youtu.be/bbb",
    ]
    assert result["entries"][0] == {
        "index": 1,
        "url": "https://www.youtube.com/watch?v=aaa",
        "title": "First",
        "creator": "Chan",
        "duration": 10,
        "id": "aaa",
    }


def test_probe_url_playlist_with_no_usable_entries_falls_back_to_video(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_flat_playlist",
        lambda url: {"_type": "playlist", "entries": [{"title": "junk"}]},
    )

    result = probe_url("https://www.youtube.com/playlist?list=PL123")

    assert result["kind"] == "video"
    assert result["entries"] == []


def test_probe_url_bad_link_raises_value_error(monkeypatch):
    def _reject(_url):
        raise ValueError("Video unavailable")

    monkeypatch.setattr(probe_module, "_flat_playlist", _reject)

    with pytest.raises(ValueError, match="Video unavailable"):
        probe_url("https://www.youtube.com/watch?v=dead")


def test_flatten_metadata_expands_one_level_and_drops_non_scalars():
    flat = _flatten_metadata(
        {
            "uploader": "Alice",
            "view_count": 12,
            "is_live": True,  # bool must not become a value
            "user": {"name": "alice_handle", "nested": {"deep": 1}},
            "tags": ["a", "b"],  # lists are skipped
        }
    )
    assert flat["uploader"] == "Alice"
    assert flat["view_count"] == "12"
    assert flat["user[name]"] == "alice_handle"
    assert "is_live" not in flat
    assert "tags" not in flat


def test_creator_probe_fields_returns_only_catalog_fields_with_values():
    flat = {"uploader": "Alice", "uploader_id": "", "zzz": "last", "title": "Hi"}
    fields = [item["field"] for item in _creator_probe_fields(flat, "ytdlp")]
    # Only creator catalog fields with values; empty uploader_id and non-catalog title/zzz dropped.
    assert fields == ["uploader"]


def test_gallerydl_richest_metadata_finds_largest_dict():
    data = [["directory"], [3, "http://x", {"id": 1, "user": "a", "title": "t"}], [2, {"small": 1}]]
    best = _gallerydl_richest_metadata(data)
    assert best == {"id": 1, "user": "a", "title": "t"}


def test_gallerydl_tiktok_photo_author_fields_are_creator_candidates(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url: (None, "unsupported url"))
    monkeypatch.setattr(
        probe_module,
        "_gallerydl_dump",
        lambda url: {
            "id": "7420705673542978833",
            "author": {
                "id": "6673617364291994625",
                "nickname": "FZ Yahoo",
                "secUid": "MS4wLjABAAAAC0QSwXXGjf1xr3FVnQxnr33V3X5v-QJrnH8KaGbJ5tQQlt8cyC_9OrrBOdb_NMhe",
                "uniqueId": "fzyahoo.com",
            },
        },
    )
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_creator_fields("https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833")

    assert [field["field"] for field in result["fields"]] == ["author[uniqueId]", "author[nickname]"]
    assert result["creator_fields"] == {
        "username": ["author[uniqueId]"],
        "nickname": ["author[nickname]"],
    }
    assert result["url_creator_fields"] == {}


def test_gallerydl_dump_uses_tiktok_no_audio_probe_option(monkeypatch):
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd

        class Result:
            returncode = 0
            stdout = "[[2, {\"author\": {\"uniqueId\": \"bob\"}}]]"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    monkeypatch.setattr(probe_module, "find_cookies_file_for_url", lambda url: "")

    assert probe_module._gallerydl_dump("https://www.tiktok.com/@bob/photo/1") == {
        "author": {"uniqueId": "bob"}
    }
    assert captured["cmd"][:4] == ["gallery-dl", "-j", "-o", "extractor.tiktok.audio=false"]


def test_probe_creator_fields_merges_both_engines(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url: ({"uploader_id": "bob_h", "title": "hey"}, ""))
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: {"username": "bob", "title": "hey"})
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")
    result = probe_creator_fields("https://example.com/x")
    assert result["source_key"] == "example"
    fields = [f["field"] for f in result["fields"]]
    # Merged creator fields from both engines; title (non-creator) excluded.
    assert "uploader_id" in fields
    assert "username" in fields
    assert "title" not in fields
    assert result["creator_fields"]["username"] == ["uploader_id", "username"]
    assert result["creator_fields"]["nickname"] == ["username"]


def test_probe_creator_fields_keeps_bare_facebook_reel_uploader(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url: (
            {
                "id": "849162654788919",
                "uploader": "Tomet Fonn",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_creator_fields("https://www.facebook.com/reel/849162654788919")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["username"] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["nickname"] == ["uploader"]
    assert result["url_creator_fields"] == {}


def test_probe_creator_fields_keeps_facebook_share_post_uploader(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url: (
            {
                "id": "194bUYA419",
                "uploader": "Tomet Fonn",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_creator_fields("https://www.facebook.com/share/p/194bUYA419/")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["username"] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["nickname"] == ["uploader"]
    assert result["url_creator_fields"] == {}


def test_probe_creator_fields_keeps_live_fields_without_url_owner_filter(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url: (
            {
                "id": "7487436336081734913",
                "uploader": "wrong-owner",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_creator_fields("https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["username"] == ["uploader_id", "uploader"]
    assert result["creator_fields"]["nickname"] == ["uploader"]
    assert result["url_creator_fields"] == {}


def test_probe_creator_fields_uses_engine_order_not_url_creator_promotion(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url: (
            {
                "id": "7487436336081734913",
                "uploader_id": "6673617364291994625",
                "uploader": "fzyahoo.com",
                "channel": "❤️",
                "channel_id": "MS4wLjABAAAAC0QSwXXGjf1xr3FVnQxnr33V3X5v-QJrnH8KaGbJ5tQQlt8cyC_9OrrBOdb_NMhe",
                "artist": "spidey",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_creator_fields("https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913")

    assert [field["field"] for field in result["fields"]][:2] == ["uploader_id", "uploader"]
    assert result["url_creator_fields"] == {}
    assert result["creator_fields"]["username"][:2] == ["uploader_id", "uploader"]


def test_probe_creator_fields_skips_engine_that_returns_nothing(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url: (None, "unsupported url"))
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: {"username": "bob"})
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")
    result = probe_creator_fields("https://example.com/x")
    assert [f["field"] for f in result["fields"]] == ["username"]
    assert result["creator_fields"]["username"] == ["username"]


def test_probe_creator_fields_raises_when_both_engines_fail(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url: (None, "bad link line"))
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url: None)
    with pytest.raises(ValueError):
        probe_creator_fields("https://example.com/x")
