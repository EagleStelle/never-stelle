from __future__ import annotations

import json
import subprocess
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from backend.app.services.settings import find_cookies_file_for_url

from .formats import _prepare_url

# YouTube mix/radio playlists carry an ``RD`` list id and are endless, so we
# never expand them; we download only the video the link points at.
_RADIO_PREFIX = "RD"
_PROBE_TIMEOUT_SECONDS = 90
_MAX_ENTRIES = 500


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
        raise RuntimeError(detail[-1] if detail else "Could not read that link.")
    return json.loads(result.stdout or "{}")


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
