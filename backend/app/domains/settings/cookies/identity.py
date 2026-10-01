from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from typing import Any

# gallery-dl's own test for a desktop Chrome; some extractors lower media quality for any other browser.
_DESKTOP_CHROME_RE = re.compile(r"Chrome/(\d{3,})\.\d+\.\d+\.\d+(?!\d* Mobile)")
# (user agent marker, sec-ch-ua-platform, navigator.platform, platformVersion, label); ChromeOS says X11 too.
_PLATFORMS = (
    ("CrOS", "Chrome OS", "Linux x86_64", "", "ChromeOS"),
    ("Windows NT", "Windows", "Win32", "10.0.0", "Windows"),
    ("Macintosh", "macOS", "MacIntel", "15.5.0", "macOS"),
    ("X11", "Linux", "Linux x86_64", "", "Linux"),
)
# (version marker, client hint brand, label); plain Chrome when none matches.
_PRODUCTS = (
    (re.compile(r"Edg/(\d+)"), "Microsoft Edge", "Edge"),
    (re.compile(r"OPR/(\d+)"), "Opera", "Opera"),
)
# Chromium's GREASE brand, seeded by the major version (components/embedder_support/user_agent_utils.cc).
_GREASEY_CHARS = " (:-./);=?_"
_GREASED_VERSIONS = ("8", "99", "24")
_BRAND_ORDERS = ((0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0))
ACCEPT_LANGUAGE = "en-US,en;q=0.9"
_DEFAULT_LABEL = "Default Chrome"


@dataclass(frozen=True)
class BrowserIdentity:
    """The browser every request carrying one cookie jar presents."""

    user_agent: str
    chromium: str
    # (brand, major version), in the order the browser sends them.
    brands: tuple[tuple[str, str], ...]
    platform: str
    navigator_platform: str
    platform_version: str
    label: str

    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "sec-ch-ua": ", ".join(f'"{brand}";v="{major}"' for brand, major in self.brands),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": f'"{self.platform}"',
            "Accept-Language": ACCEPT_LANGUAGE,
        }

    def cdp_override(self) -> dict[str, Any]:
        """Parameters for ``Emulation.setUserAgentOverride``."""
        # Reduced user agents carry only the major version, so the full ones are too.
        return {
            "userAgent": self.user_agent,
            "acceptLanguage": "en-US,en",
            "platform": self.navigator_platform,
            "userAgentMetadata": {
                "brands": [{"brand": brand, "version": major} for brand, major in self.brands],
                "fullVersionList": [{"brand": brand, "version": f"{major}.0.0.0"} for brand, major in self.brands],
                "fullVersion": f"{self.chromium}.0.0.0",
                "platform": self.platform,
                "platformVersion": self.platform_version,
                "architecture": "x86",
                "model": "",
                "mobile": False,
                "bitness": "64",
                "wow64": False,
            },
        }


def _platform(user_agent: str) -> tuple[str, str, str, str, str] | None:
    return next((platform for platform in _PLATFORMS if platform[0] in user_agent), None)


def desktop_chrome_user_agent(value: Any) -> str:
    """The user agent when it is a desktop Chromium browser's, else ""."""
    text = str(value or "").strip()
    if not _DESKTOP_CHROME_RE.search(text) or "Android" in text or not _platform(text):
        return ""
    return text


def _default_user_agent() -> str:
    from gallery_dl.util import USERAGENT_CHROME

    return USERAGENT_CHROME


def _brands(product: tuple[str, str], chromium: int) -> tuple[tuple[str, str], ...]:
    grease = (
        f"Not{_GREASEY_CHARS[chromium % len(_GREASEY_CHARS)]}A"
        f"{_GREASEY_CHARS[(chromium + 1) % len(_GREASEY_CHARS)]}Brand",
        _GREASED_VERSIONS[chromium % len(_GREASED_VERSIONS)],
    )
    ordered = [grease, grease, grease]
    _, second, third = _BRAND_ORDERS[chromium % len(_BRAND_ORDERS)]
    ordered[second] = ("Chromium", str(chromium))
    ordered[third] = product
    return tuple(ordered)


@cache
def browser_identity(user_agent: str = "") -> BrowserIdentity:
    """The identity a jar uploaded from ``user_agent`` presents; the default Chrome for any other browser."""
    own = desktop_chrome_user_agent(user_agent)
    agent = own or _default_user_agent()
    chrome = _DESKTOP_CHROME_RE.search(agent)
    chromium = int(chrome[1]) if chrome else 0
    brand, major, product = "Google Chrome", str(chromium), "Chrome"
    for marker, name, label in _PRODUCTS:
        if found := marker.search(agent):
            brand, major, product = name, found[1], label
            break
    _, platform, navigator_platform, platform_version, platform_label = _platform(agent) or _PLATFORMS[1]
    return BrowserIdentity(
        user_agent=agent,
        chromium=str(chromium),
        brands=_brands((brand, major), chromium),
        platform=platform,
        navigator_platform=navigator_platform,
        platform_version=platform_version,
        label=f"{product} {major} {platform_label}" if own else _DEFAULT_LABEL,
    )
