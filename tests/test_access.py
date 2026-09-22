import os
from contextlib import closing

import httpx
import pytest

import backend.app.domains.downloads.access as access_module
import backend.app.domains.downloads.enrich as enrich
import backend.app.domains.settings.cookie_pool as pool
from backend.app.domains.settings import CookieLease

_CLOUDFLARE = "ERROR: [generic] Got HTTP Error 403 caused by Cloudflare anti-bot challenge; try again"
_DDOS_GUARD = "[example][error] ChallengeError: DDoS-Guard challenge (403 Forbidden) for 'https://example.test/'"
_SCRIPT_CHALLENGE = (
    "[example][info] Solving JavaScript challenge\n"
    "[example][error] https://example.test/post/1: Failed to extract post (HttpError: '403 Forbidden')"
)


def _stub(monkeypatch, *, jars=(), target="chrome", has_cookies=True):
    leases = [
        CookieLease(cookie_id=f"jar-{index}", source_key="example", path=f"/tmp/{name}.txt", filename=f"{name}.txt")
        for index, name in enumerate(jars, start=1)
    ]
    monkeypatch.setattr(access_module, "has_cookies_for_source", lambda key: has_cookies and bool(key))

    def fake_rotation(key, **kwargs):
        yield from leases

    monkeypatch.setattr(access_module, "cookie_rotation", fake_rotation)
    monkeypatch.setattr(access_module, "impersonation_target", lambda: target)
    return leases


def _walk(outputs, cookie_source_key="example", **options):
    """Drive the rotation, failing every attempt with the next output."""
    seen = []
    with closing(access_module.access_rotation(cookie_source_key, **options)) as rotation:
        for access, output in zip(rotation, outputs, strict=False):
            seen.append(access)
            access.report(output)
    return seen


def test_antibot_walls_are_told_apart_from_rate_limits():
    assert pool.looks_antibot_walled(_CLOUDFLARE)
    assert pool.looks_antibot_walled(_DDOS_GUARD)
    assert pool.looks_antibot_walled(_SCRIPT_CHALLENGE)
    assert not pool.looks_antibot_walled("ERROR: HTTP Error 429: Too Many Requests")
    assert not pool.looks_antibot_walled("ERROR: HTTP Error 403: Forbidden")


def test_a_wall_adds_one_fingerprinted_attempt_before_cookies(monkeypatch):
    _stub(monkeypatch, jars=["a", "b"])

    seen = _walk([_CLOUDFLARE] * 4)

    assert [(access.impersonate, access.cookies_file) for access in seen] == [
        ("", ""),
        ("chrome", ""),
        ("chrome", "/tmp/a.txt"),
        ("chrome", "/tmp/b.txt"),
    ]
    assert [access.lease.banned for access in seen[2:]] == [False, False]


def test_no_fingerprint_without_a_wall(monkeypatch):
    _stub(monkeypatch, jars=["a"])

    seen = _walk(["ERROR: HTTP Error 429: Too Many Requests"] * 2)

    assert [(access.impersonate, access.cookies_file) for access in seen] == [("", ""), ("", "/tmp/a.txt")]
    assert seen[1].lease.banned is True


def test_a_wall_behind_a_cookie_fingerprints_the_remaining_jars(monkeypatch):
    _stub(monkeypatch, jars=["a", "b"])

    seen = _walk(["ERROR: Unsupported URL", _DDOS_GUARD, "ERROR: Unsupported URL"])

    assert [access.impersonate for access in seen] == ["", "", "chrome"]


@pytest.mark.parametrize(("walled", "impersonate"), [(True, "chrome"), (False, "")])
def test_a_known_wall_state_starts_at_the_jars(monkeypatch, walled, impersonate):
    _stub(monkeypatch, jars=["a", "b"])

    seen = _walk(["ERROR: Unsupported URL"] * 2, walled=walled)

    assert [(access.impersonate, access.cookies_file) for access in seen] == [
        (impersonate, "/tmp/a.txt"),
        (impersonate, "/tmp/b.txt"),
    ]


def test_the_fingerprint_step_is_skipped_without_an_impersonation_backend(monkeypatch):
    _stub(monkeypatch, jars=["a"], target="")

    seen = _walk([_CLOUDFLARE] * 3)

    assert [(access.impersonate, access.cookies_file) for access in seen] == [("", ""), ("", "/tmp/a.txt")]


def test_callers_without_a_fingerprint_go_straight_to_cookies_after_a_wall(monkeypatch):
    _stub(monkeypatch, jars=["a"])
    monkeypatch.setattr(access_module, "impersonation_target", lambda: pytest.fail("no fingerprint lookup"))

    seen = []
    with closing(access_module.access_rotation("example", fingerprint=False)) as rotation:
        for access in rotation:
            seen.append(access)
            access.report(_CLOUDFLARE)

    assert [(access.impersonate, access.cookies_file) for access in seen] == [("", ""), ("", "/tmp/a.txt")]


@pytest.mark.parametrize(
    ("headers", "banned"),
    [({"cf-mitigated": "challenge"}, False), ({}, True)],
)
def test_fetch_html_rests_a_jar_on_a_block_but_not_on_a_cloudflare_challenge(monkeypatch, headers, banned):
    leases = _stub(monkeypatch, jars=["a"])
    monkeypatch.setattr(enrich, "_load_cookie_jar", lambda path: {"session": "1"} if path else None)
    requests = []
    monkeypatch.setattr(
        enrich.httpx,
        "get",
        lambda url, **kwargs: requests.append(kwargs.get("cookies"))
        or httpx.Response(403, headers=headers, request=httpx.Request("GET", url)),
    )

    assert enrich.fetch_html("https://example.test/post/1", "example") == ""
    assert [cookies is not None for cookies in requests] == [False, True]
    assert leases[0].banned is banned


def test_a_lazy_cookie_source_is_not_resolved_when_anonymous_works(monkeypatch):
    _stub(monkeypatch, jars=["a"])
    resolved = []

    with closing(access_module.access_rotation(lambda: resolved.append(True) or "example")) as rotation:
        next(rotation)

    assert resolved == []


def test_impersonate_targets_parse_to_the_preferred_usable_family():
    listing = "\n".join(
        [
            "[info] Available impersonate targets",
            "Client          OS           Source",
            "--------------------------------------",
            "Chrome-133      Macos-15     curl_cffi",
            "Tor-14.5        Macos-14     curl_cffi",
            "Safari-26.0.1   Macos-26     curl_cffi",
            "Chrome-150      Macos-26     curl_cffi",
            "Edge            -            curl_cffi (unavailable)",
        ]
    )

    assert access_module.parse_impersonate_targets(listing) == "chrome"


def test_impersonate_targets_parse_empty_when_every_target_is_unavailable():
    listing = "\n".join(
        [
            "[info] Available impersonate targets",
            "Client    OS   Source",
            "--------------------------------------------",
            "Edge      -    curl_cffi (unavailable)",
            "Chrome    -    curl_cffi (unavailable)",
        ]
    )

    assert access_module.parse_impersonate_targets(listing) == ""


def test_access_env_only_adds_the_fingerprint_backend_when_impersonating(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_IMPERSONATE_PATH", "/opt/impersonate")
    monkeypatch.setenv("PYTHONPATH", "/app")

    assert access_module.access_env(access_module.AccessIdentity(cookies_file="/tmp/a.txt")) is None
    env = access_module.access_env(access_module.AccessIdentity(impersonate="chrome"))
    assert env is not None
    assert env["PYTHONPATH"] == os.pathsep.join(["/opt/impersonate", "/app"])


def test_access_env_inherits_when_the_backend_is_on_the_default_path(monkeypatch):
    monkeypatch.delenv("NEVER_STELLE_IMPERSONATE_PATH", raising=False)

    assert access_module.access_env(access_module.AccessIdentity(impersonate="chrome")) is None
