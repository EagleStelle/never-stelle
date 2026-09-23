from __future__ import annotations

import pytest

from backend.app.domains.settings.browser_identity import browser_identity, desktop_chrome_user_agent

_ENGINE = "AppleWebKit/537.36 (KHTML, like Gecko)"
_WINDOWS = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) {_ENGINE} Chrome/{{}}.0.0.0 Safari/537.36"
_MAC = f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) {_ENGINE} Chrome/140.0.0.0 Safari/537.36"
_LINUX = f"Mozilla/5.0 (X11; Linux x86_64) {_ENGINE} Chrome/140.0.0.0 Safari/537.36"
_CHROMEOS = f"Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) {_ENGINE} Chrome/140.0.0.0 Safari/537.36"
_EDGE = _WINDOWS.format(140) + " Edg/140.0.0.0"
_FIREFOX = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"
_ANDROID = f"Mozilla/5.0 (Linux; Android 10; K) {_ENGINE} Chrome/140.0.0.0 Mobile Safari/537.36"
_TABLET = f"Mozilla/5.0 (Linux; Android 10; K) {_ENGINE} Chrome/140.0.0.0 Safari/537.36"


@pytest.mark.parametrize("agent", [_WINDOWS.format(140), _MAC, _LINUX, _CHROMEOS, _EDGE])
def test_desktop_chromium_browsers_are_kept(agent):
    assert desktop_chrome_user_agent(f"  {agent} ") == agent


@pytest.mark.parametrize("agent", [_FIREFOX, _ANDROID, _TABLET, "", None, "curl/8.0"])
def test_other_browsers_are_refused(agent):
    assert desktop_chrome_user_agent(agent) == ""


@pytest.mark.parametrize(
    ("major", "brands"),
    [
        # What those Chrome versions really send.
        (120, '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'),
        (131, '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'),
        (140, '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"'),
    ],
)
def test_client_hints_match_the_chrome_version(major, brands):
    assert browser_identity(_WINDOWS.format(major)).headers()["sec-ch-ua"] == brands


@pytest.mark.parametrize(
    ("agent", "platform", "navigator", "label"),
    [
        (_WINDOWS.format(140), "Windows", "Win32", "Chrome 140 Windows"),
        (_MAC, "macOS", "MacIntel", "Chrome 140 macOS"),
        (_LINUX, "Linux", "Linux x86_64", "Chrome 140 Linux"),
        (_CHROMEOS, "Chrome OS", "Linux x86_64", "Chrome 140 ChromeOS"),
    ],
)
def test_the_platform_follows_the_user_agent(agent, platform, navigator, label):
    identity = browser_identity(agent)

    assert identity.headers()["sec-ch-ua-platform"] == f'"{platform}"'
    assert identity.cdp_override()["platform"] == navigator
    assert identity.cdp_override()["userAgentMetadata"]["platform"] == platform
    assert identity.label == label


def test_edge_sends_its_own_brand():
    identity = browser_identity(_EDGE)

    assert '"Microsoft Edge";v="140"' in identity.headers()["sec-ch-ua"]
    assert identity.label == "Edge 140 Windows"


@pytest.mark.parametrize("agent", ["", _FIREFOX, _ANDROID])
def test_any_other_browser_presents_the_default_chrome(agent):
    from gallery_dl.util import USERAGENT_CHROME

    identity = browser_identity(agent)

    assert identity.user_agent == USERAGENT_CHROME
    assert identity.label == "Default Chrome"
    assert '"Google Chrome"' in identity.headers()["sec-ch-ua"]


def test_the_browser_override_agrees_with_the_headers():
    identity = browser_identity(_MAC)
    override = identity.cdp_override()
    metadata = override["userAgentMetadata"]

    assert override["userAgent"] == identity.headers()["User-Agent"] == _MAC
    sent = ", ".join(f'"{brand["brand"]}";v="{brand["version"]}"' for brand in metadata["brands"])
    assert sent == identity.headers()["sec-ch-ua"]
    assert metadata["fullVersion"] == "140.0.0.0"
    assert metadata["mobile"] is False
