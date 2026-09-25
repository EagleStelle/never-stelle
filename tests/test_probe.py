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


def _stub_engines(monkeypatch, ytdlp=None, gallerydl=None):
    """Answer every link of a batch as the one-link fakes do: yt-dlp ``(info, error)``, gallery-dl ``metadata``."""
    if ytdlp:

        def ytdlp_dumps(urls, **kwargs):
            answers = {url: ytdlp(url, **kwargs) for url in urls}
            found = {url: info for url, (info, _) in answers.items() if info}
            return found, next((error for info, error in answers.values() if not info and error), "")

        monkeypatch.setattr(probe_module, "_ytdlp_dumps", ytdlp_dumps)
    if gallerydl:

        def gallerydl_dumps(urls, **kwargs):
            return {url: metadata for url in urls if (metadata := gallerydl(url, **kwargs))}

        monkeypatch.setattr(probe_module, "_gallerydl_dumps", gallerydl_dumps)


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
    _stub_engines(monkeypatch, ytdlp=lambda url, **kwargs: (None, "unsupported url"))
    _stub_engines(
        monkeypatch,
        gallerydl=lambda url, **kwargs: {
            "id": "7100000000000000002",
            "author": {
                "id": "6600000000000000001",
                "nickname": "Clip Demo",
                "secUid": "MS4wLjABAAAADemoSecUidDemoSecUidDemoSecUidDemoSecUidDemoSecUidDemoSecUidDemo",
                "uniqueId": "fakeacc.com",
            },
        },
    )
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_fields("https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002")

    assert [field["field"] for field in result["fields"]] == ["author[uniqueId]", "author[nickname]"]
    assert result["field_roles"] == {
        "username": ["author[uniqueId]"],
        "nickname": ["author[nickname]"],
    }


def test_gallerydl_dumps_uses_tiktok_no_audio_probe_option(monkeypatch):
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd

        class Result:
            returncode = 0
            stdout = "[[2, {\"author\": {\"uniqueId\": \"bob\"}}]]"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)

    url = "https://www.tiktok.com/@bob/photo/1"
    assert probe_module._gallerydl_dumps([url]) == {url: {"author": {"uniqueId": "bob"}}}
    assert captured["cmd"][:4] == ["gallery-dl", "-j", "-o", "extractor.tiktok.audio=false"]


def _stub_rotation(monkeypatch, paths, source_key="instagram"):
    """Hand the probe a fixed list of jars, in order, like the real rotation does."""
    from backend.app.domains.settings import CookieLease

    leases = [
        CookieLease(cookie_id=f"jar-{index}", source_key=source_key, path=path, filename=f"jar{index}.txt")
        for index, path in enumerate(paths, start=1)
    ]

    def fake_rotation(key, **kwargs):
        assert key == source_key
        yield from leases

    import backend.app.domains.downloads.access as access_module

    monkeypatch.setattr(probe_module, "has_cookies_for_source", lambda key: key == source_key)
    monkeypatch.setattr(access_module, "has_cookies_for_source", lambda key: key == source_key)
    monkeypatch.setattr(access_module, "cookie_rotation", fake_rotation)
    monkeypatch.setattr(access_module, "_impersonation_families", lambda: ())
    return leases


def test_ytdlp_dumps_falls_back_to_a_leased_cookie_after_anonymous_fails(monkeypatch):
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

    url = "https://www.instagram.com/reel/abc123/"
    found, error = probe_module._ytdlp_dumps([url], cookie_source_key="instagram")

    assert error == ""
    assert found == {url: {"id": "abc123", "uploader": "Cookie Creator"}}
    assert "--cookies" not in calls[0]
    assert calls[1][calls[1].index("--cookies") + 1] == "/tmp/instagram-jar1.txt"
    assert calls[1][-1] == url
    # The jar's run presents its browser; the anonymous one keeps the engine's own.
    assert "--add-headers" in calls[1] and "--add-headers" not in calls[0]
    assert lease.banned is False


def test_ytdlp_dumps_loads_the_fingerprint_backend_only_when_impersonating(monkeypatch):
    calls: list[tuple[bool, str]] = []

    def fake_run(cmd, **kwargs):
        impersonating = "--impersonate" in cmd
        calls.append((impersonating, (kwargs.get("env") or {}).get("PYTHONPATH", "")))

        class Result:
            returncode = 0 if impersonating else 1
            stdout = '{"id": "abc123"}\n' if impersonating else ""
            stderr = "" if impersonating else "ERROR: Got HTTP Error 403 caused by Cloudflare anti-bot challenge"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    _stub_rotation(monkeypatch, [])
    import backend.app.domains.downloads.access as access_module

    monkeypatch.setattr(access_module, "_impersonation_families", lambda: ("chrome",))
    monkeypatch.setenv("NEVER_STELLE_IMPERSONATE_PATH", "/opt/impersonate")
    monkeypatch.delenv("PYTHONPATH", raising=False)

    found, _ = probe_module._ytdlp_dumps(["https://example.test/v/1"], cookie_source_key="instagram")

    assert found == {"https://example.test/v/1": {"id": "abc123"}}
    assert calls == [(False, ""), (True, "/opt/impersonate")]


def test_ytdlp_dumps_low_priority_uses_windows_priority_flag(monkeypatch):
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

    url = "https://example.test/watch/abc123"
    found, _ = probe_module._ytdlp_dumps([url], with_cookies=False, low_priority=True)

    assert found == {url: {"id": "abc123", "uploader": "Creator"}}
    assert calls[0]["kwargs"]["creationflags"] == 0x4000


def test_ytdlp_dumps_marks_the_leased_cookie_banned_on_a_rate_limit(monkeypatch):
    def fake_run(cmd, **kwargs):
        class Result:
            returncode = 1
            stdout = ""
            stderr = "HTTP Error 429: Too Many Requests"

        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    leases = _stub_rotation(monkeypatch, ["/tmp/jar1.txt", "/tmp/jar2.txt", "/tmp/jar3.txt"])

    found, _ = probe_module._ytdlp_dumps(["https://www.instagram.com/reel/abc123/"], cookie_source_key="instagram")

    assert found == {}
    # Every jar in the list is tried, and each blocked one is marked for a rest.
    assert [lease.banned for lease in leases] == [True, True, True]


def test_gallerydl_dumps_falls_back_to_a_leased_cookie_after_anonymous_fails(monkeypatch):
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

    url = "https://www.instagram.com/reel/abc123/"
    found = probe_module._gallerydl_dumps([url], cookie_source_key="instagram")

    assert found == {url: {"id": "abc123", "username": "cookie.creator"}}
    assert "--cookies" not in calls[0]
    assert calls[1][calls[1].index("--cookies") + 1] == "/tmp/instagram-jar1.txt"
    assert calls[1][-1] == url
    assert any(arg.startswith("headers=") for arg in calls[1])
    assert not any(arg.startswith("headers=") for arg in calls[0])


def _fake_engine_runs(monkeypatch, answer):
    """Fake subprocess.run: ``answer(cmd, urls)`` gives stdout, stderr; returns each run's links."""
    runs: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        urls = [part for part in cmd if part.startswith("https://")]
        runs.append(urls)
        stdout, stderr = answer(cmd, urls)

        class Result:
            returncode = 0 if not stderr else 1

        Result.stdout, Result.stderr = stdout, stderr
        return Result()

    monkeypatch.setattr(probe_module.subprocess, "run", fake_run)
    return runs


def test_ytdlp_dumps_reads_several_links_in_one_run_and_retries_only_the_unread(monkeypatch):
    links = [f"https://www.instagram.com/reel/{n}/" for n in ("a1", "b2", "c3")]
    _stub_rotation(monkeypatch, ["/tmp/jar1.txt"])

    def answer(cmd, urls):
        if "--cookies" in cmd:
            return "".join(f'{{"id": "{url[-3:-1]}"}}\n' for url in urls), ""
        # The middle link needs a cookie; the others read anonymously.
        return '{"id": "a1"}\nnull\n{"id": "c3"}\n', "ERROR: [instagram] b2: login required"

    runs = _fake_engine_runs(monkeypatch, answer)

    found, error = probe_module._ytdlp_dumps(links, cookie_source_key="instagram")

    assert runs == [links, [links[1]]]
    assert found == {links[0]: {"id": "a1"}, links[1]: {"id": "b2"}, links[2]: {"id": "c3"}}
    assert error == ""


def test_ytdlp_dumps_takes_a_playlists_first_video(monkeypatch):
    url = "https://example.test/post/1"
    playlist = '{"_type": "playlist", "entries": [{"_type": "playlist", "entries": [{"id": "v1"}]}]}\n'
    _fake_engine_runs(monkeypatch, lambda cmd, urls: (playlist, ""))

    found, _ = probe_module._ytdlp_dumps([url], with_cookies=False)

    assert found == {url: {"id": "v1"}}


def test_ytdlp_dumps_run_cut_short_leaves_only_the_links_after_it_unread(monkeypatch):
    links = ["https://example.test/v/1", "https://example.test/v/2", "https://example.test/v/3"]
    _fake_engine_runs(monkeypatch, lambda cmd, urls: ('{"id": "1"}\n', "Traceback: crashed"))

    found, error = probe_module._ytdlp_dumps(links, with_cookies=False)

    assert found == {links[0]: {"id": "1"}}
    assert error == "Traceback: crashed"


def test_gallerydl_dumps_maps_documents_by_position(monkeypatch):
    links = ["https://www.instagram.com/p/AAA/", "https://www.instagram.com/p/BBB/"]
    documents = '[\n  [2, {"id": "AAA", "username": "a"}]\n]\n[\n  [2, {"id": "BBB", "username": "b"}]\n]\n'
    runs = _fake_engine_runs(monkeypatch, lambda cmd, urls: (documents, ""))

    found = probe_module._gallerydl_dumps(links, with_cookies=False)

    assert runs == [links]
    assert found == {links[0]: {"id": "AAA", "username": "a"}, links[1]: {"id": "BBB", "username": "b"}}


def test_gallerydl_dumps_reads_each_link_alone_when_the_documents_do_not_line_up(monkeypatch):
    links = ["https://www.instagram.com/p/AAA/", "https://www.instagram.com/p/BBB/"]

    def answer(cmd, urls):
        if len(urls) > 1:
            # One link printed nothing, so which document is whose cannot be told.
            return '[[2, {"id": "BBB"}]]', ""
        return ('[[2, {"id": "BBB"}]]', "") if urls[0].endswith("BBB/") else ("", "stopped")

    runs = _fake_engine_runs(monkeypatch, answer)

    found = probe_module._gallerydl_dumps(links, with_cookies=False)

    assert runs == [links, [links[0]], [links[1]]]
    assert found == {links[1]: {"id": "BBB"}}


def test_gallerydl_dumps_tries_the_next_identity_for_a_link_it_reported_an_error_for(monkeypatch):
    url = "https://www.instagram.com/p/AAA/"
    (lease,) = _stub_rotation(monkeypatch, ["/tmp/jar1.txt"])

    def answer(cmd, urls):
        if "--cookies" in cmd:
            return '[[2, {"id": "AAA", "username": "a"}]]', ""
        # Exit code 0 all the same; only the document says the link failed.
        return '[[-1, {"error": "HttpError", "message": "429 Too Many Requests"}]]', ""

    runs = _fake_engine_runs(monkeypatch, answer)

    found = probe_module._gallerydl_dumps([url], cookie_source_key="instagram")

    assert runs == [[url], [url]]
    assert found == {url: {"id": "AAA", "username": "a"}}
    assert lease.banned is False


def test_gallerydl_richest_metadata_skips_error_messages():
    document = [[2, {"id": "AAA"}], [-1, {"error": "HttpError", "message": "404 Not Found", "extra": 1}]]

    assert _gallerydl_richest_metadata(document) == {"id": "AAA"}
    assert _gallerydl_richest_metadata(document[1:]) == {}


def test_gallerydl_dumps_never_passes_a_link_it_has_no_extractor_for(monkeypatch):
    runs = _fake_engine_runs(monkeypatch, lambda cmd, urls: ("", ""))
    monkeypatch.setattr(probe_module, "gallerydl_reads", lambda url: None)

    assert probe_module._gallerydl_dumps(["https://example.test/x"], with_cookies=False) == {}
    assert runs == []


def test_probe_metadata_asks_gallerydl_once_for_every_link_ytdlp_could_not_read(monkeypatch):
    links = [f"https://example.test/video/{n}" for n in range(1, 4)]
    runs: list[tuple[str, list[str]]] = []

    def ytdlp(urls, **kwargs):
        runs.append(("yt-dlp", urls))
        return {links[0]: {"uploader": "bob"}}, "ERROR: no video"

    def gallerydl(urls, **kwargs):
        runs.append(("gallery-dl", urls))
        return {url: {"username": "bob"} for url in urls}

    monkeypatch.setattr(probe_module, "_ytdlp_dumps", ytdlp)
    monkeypatch.setattr(probe_module, "_gallerydl_dumps", gallerydl)
    monkeypatch.setattr(probe_module, "gallerydl_reads", lambda url: False)

    metadata = probe_module.probe_metadata(links, with_cookies=False)

    assert runs == [("yt-dlp", links), ("gallery-dl", links[1:])]
    assert metadata == {
        links[0]: {"uploader": "bob"},
        links[1]: {"username": "bob"},
        links[2]: {"username": "bob"},
    }


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

    def fake_ytdlp(url, **kwargs):
        calls.append(dict(kwargs))
        cookie_source = probe_module._probe_cookie_source(url, str(kwargs.get("cookie_source_key") or ""))
        if kwargs.get("with_cookies") and cookie_source:
            return {"id": "abc123", "uploader": "Cookie Creator"}, ""
        return {}, ""

    monkeypatch.setattr(probe_module, "detect_cookie_source", lambda url: "saved-profile")
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "domain-stem")
    monkeypatch.setattr(probe_module, "has_cookies_for_source", lambda key: key == "saved-profile")
    _stub_engines(monkeypatch, ytdlp=fake_ytdlp)
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)

    result = probe_fields("https://cdn.example.test/post/abc123")

    assert result["source_key"] == "saved-profile"
    assert [field["field"] for field in result["fields"]] == ["uploader"]
    assert [call["with_cookies"] for call in calls] == [False, True]
    assert all(call["cookie_source_key"] == "saved-profile" for call in calls)


def test_probe_fields_does_not_use_cookies_when_anonymous_has_field_roles(monkeypatch):
    calls: list[bool] = []

    def fake_ytdlp(url, **kwargs):
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
    _stub_engines(monkeypatch, ytdlp=fake_ytdlp)
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "detect_cookie_source", lambda url: "")
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")

    result = probe_fields("https://example.com/post/abc123")

    assert [field["field"] for field in result["fields"]] == ["uploader"]
    assert calls == [False]


def test_probe_fields_merges_both_engines(monkeypatch):
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: ({"uploader_id": "bob_h", "title": "hey"}, ""),
    )
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: {"username": "bob", "title": "hey"})
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
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: ({"uploader_id": "bob_h", "title": "hey"}, ""),
    )
    _stub_engines(
        monkeypatch,
        gallerydl=lambda url, **kwargs: (_ for _ in ()).throw(AssertionError("gallery-dl should not be probed")),
    )
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")

    result = probe_fields("https://example.com/x", stop_after_first_with_roles=True)

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "title"]
    assert result["field_roles"]["username"] == ["uploader_id"]
    assert result["field_roles"]["title"] == ["title"]


def test_probe_fields_keeps_bare_facebook_reel_uploader(monkeypatch):
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: (
            {
                "id": "800000000000003",
                "uploader": "Demo Poster",
                "uploader_id": "100000000000002",
            },
            "",
        ),
    )
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_fields("https://www.facebook.com/reel/800000000000003")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]


def test_probe_fields_keeps_facebook_share_post_uploader(monkeypatch):
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: (
            {
                "id": "1cDemoCc3d",
                "uploader": "Demo Poster",
                "uploader_id": "100000000000002",
            },
            "",
        ),
    )
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "facebook")

    result = probe_fields("https://www.facebook.com/share/p/1cDemoCc3d/")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]


def test_probe_fields_keeps_live_fields_without_url_owner_filter(monkeypatch):
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: (
            {
                "id": "7100000000000000003",
                "uploader": "wrong-owner",
                "uploader_id": "100000000000002",
            },
            "",
        ),
    )
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_fields("https://www.tiktok.com/@fakeacc.com/video/7100000000000000003")

    assert [field["field"] for field in result["fields"]] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"] == ["uploader_id", "uploader"]
    assert result["field_roles"]["nickname"] == ["uploader"]


def test_probe_fields_promotes_exact_url_creator_match(monkeypatch):
    _stub_engines(
        monkeypatch,
        ytdlp=lambda url, **kwargs: (
            {
                "id": "7100000000000000003",
                "uploader_id": "6600000000000000001",
                "uploader": "fakeacc.com",
                "channel": "❤️",
                "channel_id": "MS4wLjABAAAADemoSecUidDemoSecUidDemoSecUidDemoSecUidDemoSecUidDemoSecUidDemo",
                "artist": "spidey",
            },
            "",
        ),
    )
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "tiktok")

    result = probe_fields("https://www.tiktok.com/@fakeacc.com/video/7100000000000000003")

    assert [field["field"] for field in result["fields"]][:2] == ["uploader_id", "uploader"]
    assert result["field_roles"]["username"][:2] == ["uploader", "uploader_id"]


def test_probe_fields_skips_engine_that_returns_nothing(monkeypatch):
    _stub_engines(monkeypatch, ytdlp=lambda url, **kwargs: (None, "unsupported url"))
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: {"username": "bob"})
    monkeypatch.setattr(probe_module, "source_key_from_url", lambda url: "example")
    result = probe_fields("https://example.com/x")
    assert [f["field"] for f in result["fields"]] == ["username"]
    assert result["field_roles"]["username"] == ["username"]


def test_probe_metadata_asks_gallerydl_first_for_a_link_only_it_has_an_extractor_for(monkeypatch):
    calls: list[str] = []

    def ytdlp(url, **kwargs):
        calls.append("yt-dlp")
        return {"uploader": "bob"}, ""

    def gallerydl(url, **kwargs):
        calls.append("gallery-dl")
        return {"username": "bob"}

    _stub_engines(monkeypatch, ytdlp=ytdlp)
    _stub_engines(monkeypatch, gallerydl=gallerydl)
    monkeypatch.setattr(probe_module, "gallerydl_reads", lambda url: "/photo/" in url)
    monkeypatch.setattr(probe_module, "ytdlp_single_video", lambda url: "/video/" in url)

    photo, video = "https://example.test/photo/1", "https://example.test/video/1"
    assert probe_module.probe_metadata([photo]) == {photo: {"username": "bob"}}
    assert probe_module.probe_metadata([video]) == {video: {"uploader": "bob"}}
    assert calls == ["gallery-dl", "yt-dlp"]


def test_probe_fields_raises_when_both_engines_fail(monkeypatch):
    _stub_engines(monkeypatch, ytdlp=lambda url, **kwargs: (None, "bad link line"))
    _stub_engines(monkeypatch, gallerydl=lambda url, **kwargs: None)
    with pytest.raises(ValueError):
        probe_fields("https://example.com/x")
