from __future__ import annotations

import json
import subprocess
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from backend.app.services.settings import find_cookies_file_for_url

from .formats import _prepare_url

# YouTube mix/radio playlists carry an ``RD`` list id and are endless, so we
# never expand them; we download only the video the link points at.
_RADIO_PREFIX = "RD"
_PROBE_TIMEOUT_SECONDS = 90
_MAX_ENTRIES = 500

# Reconstructed links are verified with a light HTTP request that follows
# redirects; a wrong path redirects to the real one, so the final URL is the
# authoritative link. Works for any host, with no per-platform route knowledge.
_VERIFY_TIMEOUT_SECONDS = 8
_VERIFY_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; never-stelle/1.0)"}


def _strip_playlist_param(url: str) -> str:
    parsed = urlparse(url)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key != "list"]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query), ""))


def _radio_single_url(url: str) -> str:
    parsed = urlparse(url)
    if "youtu" not in parsed.netloc.lower():
        return ""
    query = dict(parse_qsl(parsed.query))
    if not query.get("list", "").startswith(_RADIO_PREFIX):
        return ""
    # Bare radio page (no v=) would strip to a dead URL; let it fall through.
    if not query.get("v"):
        return ""
    return _strip_playlist_param(url)


def _entry_url(entry: dict[str, Any]) -> str:
    raw = str(entry.get("url") or "").strip()
    if raw.startswith(("http://", "https://")):
        return raw
    video_id = str(entry.get("id") or raw).strip()
    return f"https://www.youtube.com/watch?v={video_id}" if video_id else ""


def _flat_playlist(url: str) -> dict[str, Any]:
    cmd = ["yt-dlp", "--flat-playlist", "--dump-single-json", "--no-warnings"]
    cookies_file = find_cookies_file_for_url(url)
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.extend(["--playlist-end", str(_MAX_ENTRIES), url])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=_PROBE_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        # Rejected link is client input: ValueError -> route maps to 400, not 502.
        raise ValueError(detail[-1] if detail else "Could not read that link.")
    return json.loads(result.stdout or "{}")


def _resolve_live_url(url: str, media_id: str = "") -> str:
    """Follow redirects to the real landing URL; the id must survive it, else "" (a dead/bounced link)."""
    try:
        request = Request(url, headers=_VERIFY_HEADERS, method="GET")
        with urlopen(request, timeout=_VERIFY_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", 200) or 200
            final_url = response.geturl()
    except Exception:
        return ""
    if status >= 400:
        return ""
    media_id = str(media_id or "").strip()
    if media_id and media_id not in final_url:
        return ""
    return final_url


def verify_source_url(candidates: list[str], media_id: str = "") -> str:
    """Probe candidates and return the first that resolves to its real landing URL."""
    seen: set[str] = set()
    for candidate in candidates:
        url = str(candidate or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        resolved = _resolve_live_url(url, media_id)
        if resolved:
            return resolved
    return ""


def resolve_source_url(candidates: list[str], media_id: str = "") -> str:
    """Pick the real URL among reconstructed candidates, falling back when offline."""
    if not candidates:
        return ""
    return verify_source_url(candidates, media_id) or candidates[0]


def probe_url(source_url: str) -> dict[str, Any]:
    """Classify a URL as ``video``, ``playlist``, or ``radio`` without downloading.

    Radios resolve to just their current video. Playlists return their ordered
    entries so the UI can ask which to download.
    """
    url = _prepare_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")

    radio_url = _radio_single_url(url)
    if radio_url:
        return {"kind": "radio", "url": radio_url, "title": "", "entries": []}

    data = _flat_playlist(url)
    if data.get("_type") != "playlist":
        return {"kind": "video", "url": url, "title": "", "entries": []}

    entries: list[dict[str, Any]] = []
    for index, entry in enumerate(data.get("entries") or [], start=1):
        entry_url = _entry_url(entry)
        if not entry_url:
            continue
        entries.append(
            {
                "index": index,
                "url": entry_url,
                "title": str(entry.get("title") or "Untitled"),
                "creator": str(entry.get("uploader") or entry.get("channel") or ""),
                "duration": entry.get("duration"),
                "id": str(entry.get("id") or ""),
            }
        )

    if not entries:
        return {"kind": "video", "url": url, "title": "", "entries": []}
    return {"kind": "playlist", "url": url, "title": str(data.get("title") or "Playlist"), "entries": entries}
