from __future__ import annotations

import pytest

import backend.app.domains.downloads.probe as probe_module
from backend.app.domains.downloads.probe import (
    _candidate_probe_fields,
    _entry_url,
    _flatten_metadata,
    _gallerydl_richest_metadata,
    _radio_single_url,
    _strip_playlist_param,
    probe_fields,
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


def test_candidate_probe_fields_returns_only_catalog_fields_with_values():
    flat = {"uploader": "Alice", "uploader_id": "", "zzz": "last", "title": "Hi"}
    fields = [item["field"] for item in _candidate_probe_fields(flat, "ytdlp")]
    # Catalog fields with values; empty uploader_id and non-catalog zzz dropped.
    assert fields == ["uploader", "title"]


def test_gallerydl_richest_metadata_finds_largest_dict():
    data = [["directory"], [3, "http://x", {"id": 1, "user": "a", "title": "t"}], [2, {"small": 1}]]
    best = _gallerydl_richest_metadata(data)
    assert best == {"id": 1, "user": "a", "title": "t"}


def test_gallerydl_tiktok_photo_author_fields_are_creator_candidates(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url, **kwargs: (None, "unsupported url"))
    monkeypatch.setattr(
        probe_module,
        "_gallerydl_dump",
        lambda url, **kwargs: {
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

    result = probe_fields("https://www.tiktok.com/@fzyahoo.com/photo/7420705673542978833")

    assert [field["field"] for field in result["fields"]] == ["author[uniqueId]", "author[nickname]"]
    assert result["field_roles"] == {
        "username": ["author[uniqueId]"],
        "nickname": ["author[nickname]"],
    }
    assert result["url_field_roles"] == {}


def test_gallerydl_dump_uses_tiktok_no_audio_probe_option(monkeypatch):
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd

        class Result:
            returncode = 0
            stdout = "[[2, {\"author\": {\"uniqueId\": \"bob\"}}]]"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)

    assert probe_module._gallerydl_dump("https://www.tiktok.com/@bob/photo/1") == {
        "author": {"uniqueId": "bob"}
    }
    assert captured["cmd"][:4] == ["gallery-dl", "-j", "-o", "extractor.tiktok.audio=false"]


def _stub_rotation(monkeypatch, paths, source_key="instagram"):
    """Hand the probe a fixed list of jars, in order, like the real rotation does."""
    from backend.app.domains.settings import CookieLease

    leases = [
        CookieLease(cookie_id=f"jar-{index}", source_key=source_key, path=path, filename=f"jar{index}.txt")
        for index, path in enumerate(paths, start=1)
    ]

    def fake_rotation(url, key=""):
        yield from leases

    monkeypatch.setattr(probe_module, "has_cookies_for_source", lambda key: key == source_key)
    monkeypatch.setattr(probe_module, "_probe_cookie_rotation", fake_rotation)
    return leases


def test_ytdlp_dump_falls_back_to_a_leased_cookie_after_anonymous_fails(monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        anonymous = "--cookies" not in cmd

        class Result:
            returncode = 1 if anonymous else 0
            stdout = "" if anonymous else '{"id": "abc123", "uploader": "Cookie Creator"}\n'
            stderr = "HTTP Error 429: Too Many Requests" if anonymous else ""

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    (lease,) = _stub_rotation(monkeypatch, ["/tmp/instagram-jar1.txt"])

    info, error = probe_module._ytdlp_dump(
        "https://www.instagram.com/reel/abc123/",
        cookie_source_key="instagram",
    )

    assert error == ""
    assert info == {"id": "abc123", "uploader": "Cookie Creator"}
    assert "--cookies" not in calls[0]
    assert calls[1][-3:] == ["--cookies", "/tmp/instagram-jar1.txt", "https://www.instagram.com/reel/abc123/"]
    assert lease.banned is False


def test_ytdlp_dump_low_priority_uses_windows_priority_flag(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": cmd, "kwargs": kwargs})

        class Result:
            returncode = 0
            stdout = '{"id": "abc123", "uploader": "Creator"}\n'
            stderr = ""

        return Result()

    monkeypatch.setattr(probe_module.os, "name", "nt", raising=False)
    monkeypatch.setattr(probe_module.subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0x4000, raising=False)
    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)

    info, _ = probe_module._ytdlp_dump("https://example.test/watch/abc123", with_cookies=False, low_priority=True)

    assert info == {"id": "abc123", "uploader": "Creator"}
    assert calls[0]["kwargs"]["creationflags"] == 0x4000


def test_ytdlp_dump_marks_the_leased_cookie_banned_on_a_rate_limit(monkeypatch):
    def fake_run(cmd, **kwargs):
        class Result:
            returncode = 1
            stdout = ""
            stderr = "HTTP Error 429: Too Many Requests"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    leases = _stub_rotation(monkeypatch, ["/tmp/jar1.txt", "/tmp/jar2.txt", "/tmp/jar3.txt"])

    info, _ = probe_module._ytdlp_dump(
        "https://www.instagram.com/reel/abc123/",
        cookie_source_key="instagram",
    )

    assert info is None
    # Every jar in the list is tried, and each blocked one is marked for a rest.
    assert [lease.banned for lease in leases] == [True, True, True]


def test_gallerydl_dump_falls_back_to_a_leased_cookie_after_anonymous_fails(monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        anonymous = "--cookies" not in cmd

        class Result:
            returncode = 1 if anonymous else 0
            stdout = "" if anonymous else '[[2, {"id": "abc123", "username": "cookie.creator"}]]'
            stderr = ""

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    _stub_rotation(monkeypatch, ["/tmp/instagram-jar1.txt"])

    metadata = probe_module._gallerydl_dump(
        "https://www.instagram.com/reel/abc123/",
        cookie_source_key="instagram",
    )

    assert metadata == {"id": "abc123", "username": "cookie.creator"}
    assert "--cookies" not in calls[0]
    assert calls[1][-3:] == ["--cookies", "/tmp/instagram-jar1.txt", "https://www.instagram.com/reel/abc123/"]


def test_probe_cookie_source_uses_profile_resolved_source(monkeypatch):
    checked: list[str] = []

    def fake_has_cookies(source_key):
        checked.append(source_key)
        return source_key == "saved-profile"

    monkeypatch.setattr(probe_module, "detect_cookie_source", lambda url: "saved-profile")
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "domain-stem")
    monkeypatch.setattr(probe_module, "has_cookies_for_source", fake_has_cookies)

    assert probe_module._probe_cookie_source("https://cdn.example.test/post/abc123") == "saved-profile"
    assert checked == ["saved-profile"]


def test_probe_fields_uses_profile_cookie_source_when_source_key_is_blank(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_ytdlp_dump(url, **kwargs):
        calls.append(dict(kwargs))
        cookie_source = probe_module._probe_cookie_source(url, str(kwargs.get("cookie_source_key") or ""))
        if kwargs.get("with_cookies") and cookie_source:
            return {"id": "abc123", "uploader": "Cookie Creator"}, ""
        return {}, ""

    monkeypatch.setattr(probe_module, "detect_cookie_source", lambda url: "saved-profile")
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "domain-stem")
    monkeypatch.setattr(probe_module, "has_cookies_for_source", lambda key: key == "saved-profile")
    monkeypatch.setattr(probe_module, "_ytdlp_dump", fake_ytdlp_dump)
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)

    result = probe_fields("https://cdn.example.test/post/abc123")

    assert result["source_key"] == "saved-profile"
    assert [field["field"] for field in result["fields"]] == ["uploader"]
    assert [call["with_cookies"] for call in calls] == [False, True]
    assert all(call["cookie_source_key"] == "saved-profile" for call in calls)


def test_probe_fields_does_not_use_cookies_when_anonymous_has_field_roles(monkeypatch):
    calls: list[bool] = []

    def fake_ytdlp_dump(url, **kwargs):
        with_cookies = bool(kwargs.get("with_cookies"))
        calls.append(with_cookies)
        if with_cookies:
            return {"id": "abc123", "description": "Cookie metadata without creator"}, ""
        return {"id": "abc123", "uploader": "Anonymous Creator"}, ""

    monkeypatch.setattr(
        probe_module,
        "_probe_cookie_source",
        lambda url, source_key="": (_ for _ in ()).throw(
            AssertionError("cookies should not be checked after anonymous fields")
        ),
    )
    monkeypatch.setattr(probe_module, "_ytdlp_dump", fake_ytdlp_dump)
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "detect_cookie_source", lambda url: "")
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")

    result = probe_fields("https://example.com/post/abc123")

    assert [field["field"] for field in result["fields"]] == ["uploader"]
    assert calls == [False]


def test_probe_fields_merges_both_engines(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: ({"uploader_id": "bob_h", "title": "hey"}, ""),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: {"username": "bob", "title": "hey"})
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")
    result = probe_fields("https://example.com/x")
    assert result["source_key"] == "example"
    fields = [f["field"] for f in result["fields"]]
    # Merged fields from both engines including title token field.
    assert "uploader_id" in fields
    assert "username" in fields
    assert "title" in fields
    assert result["field_roles"]["username"] == ["uploader_id", "username"]
    assert result["field_roles"]["nickname"] == ["username"]
    assert result["field_roles"]["title"] == ["title"]


def test_probe_fields_fast_mode_skips_second_engine_when_first_has_roles(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: ({"uploader_id": "bob_h", "title": "hey"}, ""),
    )
    monkeypatch.setattr(
        probe_module,
        "_gallerydl_dump",
        lambda url, **kwargs: (_ for _ in ()).throw(AssertionError("gallery-dl should not be probed")),
    )
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")

    result = probe_fields("https://example.com/x", stop_after_first_with_roles=True)

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "title"]
    assert result["field_roles"]["username"] == ["uploader_id"]
    assert result["field_roles"]["title"] == ["title"]


def test_probe_fields_keeps_bare_facebook_reel_uploader(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: (
            {
                "id": "849162654788919",
                "uploader": "Tomet Fonn",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_fields("https://www.facebook.com/reel/849162654788919")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]
    assert result["url_field_roles"] == {}


def test_probe_fields_keeps_facebook_share_post_uploader(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: (
            {
                "id": "194bUYA419",
                "uploader": "Tomet Fonn",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_fields("https://www.facebook.com/share/p/194bUYA419/")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]
    assert result["url_field_roles"] == {}


def test_probe_fields_keeps_live_fields_without_url_owner_filter(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: (
            {
                "id": "7487436336081734913",
                "uploader": "wrong-owner",
                "uploader_id": "100035730073475",
            },
            "",
        ),
    )
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_fields("https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]
    assert result["url_field_roles"] == {}


def test_probe_fields_promotes_exact_url_creator_match(monkeypatch):
    monkeypatch.setattr(
        probe_module,
        "_ytdlp_dump",
        lambda url, **kwargs: (
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
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_fields("https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913")

    assert [field["field"] for field in result["fields"]][:2] == ["uploader_id", "uploader"]
    assert result["url_field_roles"] == {}
    assert result["field_roles"]["username"][:2] == ["uploader", "uploader_id"]


def test_probe_fields_skips_engine_that_returns_nothing(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url, **kwargs: (None, "unsupported url"))
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: {"username": "bob"})
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")
    result = probe_fields("https://example.com/x")
    assert [f["field"] for f in result["fields"]] == ["username"]
    assert result["field_roles"]["username"] == ["username"]


def test_probe_fields_raises_when_both_engines_fail(monkeypatch):
    monkeypatch.setattr(probe_module, "_ytdlp_dump", lambda url, **kwargs: (None, "bad link line"))
    monkeypatch.setattr(probe_module, "_gallerydl_dump", lambda url, **kwargs: None)
    with pytest.raises(ValueError):
        probe_fields("https://example.com/x")
