from __future__ import annotations

import pytest

import backend.app.domains.downloads.links.urls as urls_module
from backend.app.core.sources import source_label_from_key
from backend.app.domains.downloads.links.urls import canonicalize_source_url, detect_source_key
from tests.support import patch_head


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
            "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001?lang=en&q=fakeacc&t=1781279478413",
            "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001",
        ),
        ("https://youtube.com/watch?v=1&feature=share", "https://youtube.com/watch?v=1"),
    ],
)
def test_canonicalize_source_url(raw, expected):
    assert canonicalize_source_url(raw) == expected


def test_canonicalize_trailing_slash_idempotent():
    once = canonicalize_source_url("instagram.com/reel/abc")
    assert canonicalize_source_url(once) == once


def test_resolve_redirect_expands_share_link(monkeypatch):
    patch_head(monkeypatch, "https://www.facebook.com/demopage/posts/pfbid02DemoPostAaDemoPostDemoPostDemoPostDemoPostDemoPostDemoPostDemoPos")
    resolved = urls_module.resolve_redirect_url("https://www.facebook.com/share/p/1aDemoAa1b/")
    assert resolved.endswith("pfbid02DemoPostAaDemoPostDemoPostDemoPostDemoPostDemoPostDemoPostDemoPos")


def test_resolve_redirect_keeps_original_when_target_loses_id(monkeypatch):
    patch_head(monkeypatch, "https://www.instagram.com/accounts/login/")
    original = "https://www.instagram.com/reel/DDemoReel01/"
    assert urls_module.resolve_redirect_url(original) == original


def test_resolve_redirect_survives_network_error(monkeypatch):
    patch_head(monkeypatch, exc=RuntimeError("boom"))
    original = "https://www.facebook.com/share/p/1aDemoAa1b/"
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


def test_resolve_creator_handle_extracts_vanity_without_network(monkeypatch):
    patch_head(monkeypatch, exc=RuntimeError("should not be called"))
    assert urls_module.resolve_creator_handle("https://www.facebook.com/demopage") == "demopage"
    assert urls_module.resolve_creator_handle("https://www.tiktok.com/@fakeacc.com") == "fakeacc.com"


def test_resolve_creator_handle_follows_numeric_id_redirect(monkeypatch):
    patch_head(monkeypatch, "https://www.facebook.com/demopage")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100000000000001") == "demopage"


def test_resolve_creator_handle_rejects_media_and_walls(monkeypatch):
    # Media URLs are rejected pre-network (multi-segment); auth walls redirect with a query string.
    patch_head(monkeypatch, "https://www.facebook.com/login/?next=https%3A%2F%2Fwww.facebook.com%2F100000000000001")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/reel/800000000000001") == ""
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100000000000001") == ""


def test_resolve_creator_handle_rejects_cross_host_redirect(monkeypatch):
    # An off-site consent/login host must never supply a handle for the source's creator.
    patch_head(monkeypatch, "https://login.example.com/demopage")
    assert urls_module.resolve_creator_handle("https://www.facebook.com/100000000000001") == ""
