from __future__ import annotations

import json
import subprocess
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import normalize_source_key, source_key_from_url
from backend.app.domains.settings import (
    detect_cookie_source,
    find_cookies_file_for_source,
    find_cookies_file_for_url,
)

from .constants import (
    CREATOR_FIELD_CANDIDATES,
    creator_roles_from_probe_fields,
    promote_creator_field_roles,
)
from .formats import _prepare_url

# YouTube mix/radio playlists carry an ``RD`` list id and are endless, so we
# never expand them; we download only the video the link points at.
_RADIO_PREFIX = "RD"
_PROBE_TIMEOUT_SECONDS = 90
_MAX_ENTRIES = 500
_GALLERYDL_TIKTOK_NO_AUDIO_OPTION = "extractor.tiktok.audio=false"


def _probe_cookie_source_keys(url: str, source_key: str = "") -> list[str]:
    keys: list[str] = []

    def add(raw: Any) -> None:
        key = normalize_source_key(raw)
        if key and key not in keys:
            keys.append(key)

    add(source_key)
    try:
        add(detect_cookie_source(url))
    except Exception:
        pass
    add(source_key_from_url(url))
    return keys


def _resolved_probe_source_key(url: str, source_key: str = "") -> str:
    keys = _probe_cookie_source_keys(url, source_key)
    return keys[0] if keys else source_key_from_url(url)


def _probe_cookies_file(url: str, source_key: str = "") -> str:
    for key in _probe_cookie_source_keys(url, source_key):
        cookies_file = find_cookies_file_for_source(key)
        if cookies_file:
            return cookies_file
    try:
        return find_cookies_file_for_url(url)
    except Exception:
        return ""


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
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-single-json",
        "--no-warnings",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
    ]
    cookies_file = _probe_cookies_file(url)
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


# --- Field probe ---
def _scalar(value: Any) -> str:
    # bool is an int subclass; keep it out so True/False don't masquerade as values.
    if isinstance(value, bool) or value is None:
        return ""
    return str(value).strip() if isinstance(value, str | int | float) else ""


def _flatten_metadata(data: Any) -> dict[str, str]:
    # Expose one level of nesting as key[sub] to match how the template addresses fields.
    out: dict[str, str] = {}
    if not isinstance(data, dict):
        return out
    for key, value in data.items():
        if isinstance(value, dict):
            for sub, sub_value in value.items():
                scalar = _scalar(sub_value)
                if scalar:
                    out[f"{key}[{sub}]"] = scalar
        else:
            scalar = _scalar(value)
            if scalar:
                out[key] = scalar
    return out


def _creator_probe_fields(flat: dict[str, str], engine: str) -> list[dict[str, str]]:
    # Only the handle/display-name candidates for this engine, in stable order.
    fields: list[dict[str, str]] = []
    seen: set[str] = set()
    for field in CREATOR_FIELD_CANDIDATES.get(engine, ()):
        if field in seen:
            continue
        seen.add(field)
        value = flat.get(field, "")
        if value:
            fields.append({"field": field, "value": value})
    return fields


def _url_exact_values(source_url: str) -> set[str]:
    try:
        parsed = urlparse(_prepare_url(source_url))
    except Exception:
        return set()
    values: set[str] = set()
    for raw in [part for part in str(parsed.path or "").split("/") if part.strip()]:
        value = unquote(raw).strip().strip("/").lstrip("@").strip()
        if value:
            values.add(value)
    for _, raw in parse_qsl(parsed.query, keep_blank_values=False):
        value = unquote(str(raw or "")).strip().strip("/").lstrip("@").strip()
        if value:
            values.add(value)
    return values


def _exact_url_creator_fields(
    source_url: str,
    fields_by_role: dict[str, list[str]],
    values_by_field: dict[str, str],
) -> dict[str, list[str]]:
    url_values = _url_exact_values(source_url)
    if not url_values:
        return {}
    promoted: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        fields: list[str] = []
        for field in fields_by_role.get(role) or []:
            value = str(values_by_field.get(field) or "").strip().strip("/").lstrip("@").strip()
            if value and value in url_values and field not in fields:
                fields.append(field)
        if fields:
            promoted[role] = fields
    return promoted


def _ytdlp_dump(
    url: str, *, with_cookies: bool = True, cookie_source_key: str = ""
) -> tuple[dict[str, Any] | None, str]:
    cmd = [
        "yt-dlp",
        "--dump-json",
        "--no-warnings",
        "--no-download",
        "--playlist-items",
        "1",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
    ]
    cookies_file = _probe_cookies_file(url, cookie_source_key) if with_cookies else ""

    def _exec(extra_args: list[str]) -> tuple[dict[str, Any] | None, str]:
        try:
            result = subprocess.run(
                cmd + extra_args + [url],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.SubprocessError):
            return None, ""
        if result.returncode != 0:
            return None, (result.stderr or result.stdout or "")
        line = next((row for row in (result.stdout or "").splitlines() if row.strip().startswith("{")), "")
        if not line:
            return None, ""
        try:
            return json.loads(line), ""
        except json.JSONDecodeError:
            return None, ""

    if cookies_file:
        res, err = _exec(["--cookies", cookies_file])
        if res is not None:
            return res, ""
        return _exec([])
    return _exec([])


def _gallerydl_richest_metadata(node: Any) -> dict[str, Any]:
    # gallery-dl -j nests the file metadata dict inside its message list; return the largest dict.
    best: dict[str, Any] = {}
    stack: list[Any] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if len(current) > len(best):
                best = current
        elif isinstance(current, list):
            stack.extend(current)
    return best


def _gallerydl_dump(url: str, *, with_cookies: bool = True, cookie_source_key: str = "") -> dict[str, Any] | None:
    cmd = ["gallery-dl", "-j", "-o", _GALLERYDL_TIKTOK_NO_AUDIO_OPTION]
    cookies_file = _probe_cookies_file(url, cookie_source_key) if with_cookies else ""

    def _exec(extra_args: list[str]) -> dict[str, Any] | None:
        try:
            result = subprocess.run(
                cmd + extra_args + [url],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        try:
            data = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            return None
        metadata = _gallerydl_richest_metadata(data)
        return metadata or None

    if cookies_file:
        res = _exec(["--cookies", cookies_file])
        if res is not None:
            return res
        return _exec([])
    return _exec([])


def probe_metadata(source_url: str, *, with_cookies: bool = True, cookie_source_key: str = "") -> dict[str, str]:
    """Flat metadata for a URL from whichever engine answers first; ``{}`` on failure.

    The library scan uses this to resolve a manually-placed file's creator without a
    download. Unlike ``probe_creator_fields`` it returns every scalar field (flattened
    to ``key[sub]``) so the caller can walk its own configured field-priority order.
    """
    url = _prepare_url(source_url)
    if not url:
        return {}
    info, _ = _ytdlp_dump(url, with_cookies=with_cookies, cookie_source_key=cookie_source_key)
    if isinstance(info, dict) and info:
        return _flatten_metadata(info)
    metadata = _gallerydl_dump(url, with_cookies=with_cookies, cookie_source_key=cookie_source_key)
    if isinstance(metadata, dict) and metadata:
        return _flatten_metadata(metadata)
    return {}


def _probe_creator_metadata(
    url: str,
    source_key: str,
    *,
    with_cookies: bool,
) -> tuple[list[tuple[str, dict[str, str]]], list[str]]:
    probed: list[tuple[str, dict[str, str]]] = []
    errors: list[str] = []
    info, error = _ytdlp_dump(url, with_cookies=with_cookies, cookie_source_key=source_key)
    if isinstance(info, dict) and info:
        probed.append(("ytdlp", _flatten_metadata(info)))
    elif error:
        errors.append(error)

    metadata = _gallerydl_dump(url, with_cookies=with_cookies, cookie_source_key=source_key)
    if isinstance(metadata, dict) and metadata:
        probed.append(("gallerydl", _flatten_metadata(metadata)))
    return probed, errors


def _creator_field_count(probed: list[tuple[str, dict[str, str]]]) -> int:
    return sum(len(_creator_probe_fields(flat, engine)) for engine, flat in probed)


def probe_creator_fields(source_url: str, source_key: str = "") -> dict[str, Any]:
    """List the handle/display-name candidate fields for a link, no download.

    Probes both yt-dlp and gallery-dl and merges the username/nickname catalog fields
    each returns, so the user sees whichever engine's fields apply to this source.
    """
    url = _prepare_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")
    resolved_key = _resolved_probe_source_key(url, source_key)

    probed: list[tuple[str, dict[str, str]]] = []
    errors: list[str] = []
    for with_cookies in (False, True):
        if with_cookies:
            if _creator_field_count(probed) or not _probe_cookies_file(url, resolved_key):
                break
        attempt_probed, attempt_errors = _probe_creator_metadata(
            url,
            resolved_key,
            with_cookies=with_cookies,
        )
        errors.extend(attempt_errors)
        if not attempt_probed:
            continue
        probed.extend(attempt_probed)
        if _creator_field_count(attempt_probed):
            break

    if not probed:
        detail = (errors[-1] if errors else "").strip().splitlines()
        raise ValueError(detail[-1] if detail else "Could not read that link.")

    fields: list[dict[str, str]] = []
    fields_by_engine: dict[str, list[str]] = {}
    values_by_field: dict[str, str] = {}
    seen: set[str] = set()
    for engine, flat in probed:
        engine_fields = _creator_probe_fields(flat, engine)
        fields_by_engine[engine] = [item["field"] for item in engine_fields]
        for item in engine_fields:
            values_by_field.setdefault(item["field"], item["value"])
            if item["field"] in seen:
                continue
            seen.add(item["field"])
            fields.append(item)
    creator_fields = creator_roles_from_probe_fields(fields_by_engine)
    creator_fields = promote_creator_field_roles(
        creator_fields,
        _exact_url_creator_fields(url, creator_fields, values_by_field),
    )
    return {
        "source_key": resolved_key,
        "fields": fields,
        "creator_fields": creator_fields,
        "url_creator_fields": {},
    }
