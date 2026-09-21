from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Iterator
from contextlib import closing, suppress
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import normalize_source_key, source_key_from_url
from backend.app.domains.downloads.workers.processes import run_task_subprocess
from backend.app.domains.settings import detect_cookie_source, has_cookies_for_source

from .access import AccessIdentity, access_env, access_rotation
from .constants import (
    FIELD_CANDIDATES,
    field_roles_from_probe_fields,
    promote_field_roles,
)
from .formats import _prepare_url
from .gallerydl import gallerydl_access_args
from .ytdlp import ytdlp_access_args

# YouTube mix/radio playlists carry an ``RD`` list id and are endless, so we
# never expand them; we download only the video the link points at.
_RADIO_PREFIX = "RD"
_PROBE_TIMEOUT_SECONDS = 90
_MAX_ENTRIES = 500
_GALLERYDL_TIKTOK_NO_AUDIO_OPTION = "extractor.tiktok.audio=false"
_BLANK_RE = re.compile(r"\s*")


def low_priority_command(cmd: list[str], kwargs: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    """The command and its process options, lowered below the downloads' CPU priority."""
    run_cmd = list(cmd)
    run_kwargs = dict(kwargs)
    if os.name == "nt":
        priority = getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
        if priority:
            run_kwargs["creationflags"] = int(run_kwargs.get("creationflags", 0)) | priority
    else:
        nice = shutil.which("nice")
        if nice:
            run_cmd = [nice, "-n", "10", *run_cmd]
    return run_cmd, run_kwargs


def gallerydl_reads(url: str) -> bool | None:
    """True when a gallery-dl extractor reads the link, False for a dispatcher, None when none matches."""
    try:
        from gallery_dl import extractor
        from gallery_dl.extractor.common import Dispatch

        found = extractor.find(url)
    except Exception:
        return None
    return None if found is None else not isinstance(found, Dispatch)


def ytdlp_single_video(url: str, ie_key: str = "") -> bool | None:
    """Whether yt-dlp's extractor, ``ie_key`` or the one matching the link, reads one video.

    None when it does not know, as broad and generic extractors never do.
    """
    try:
        from yt_dlp.extractor import gen_extractor_classes, get_info_extractor

        suitable = (
            get_info_extractor(ie_key)
            if ie_key
            else next((ie for ie in gen_extractor_classes() if ie.suitable(url)), None)
        )
        return suitable.is_single_video(url) if suitable else None
    except Exception:
        return None


def _run_probe_command(
    cmd: list[str],
    *,
    low_priority: bool = False,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    if low_priority:
        cmd, kwargs = low_priority_command(cmd, kwargs)
    return run_task_subprocess(cmd, **kwargs)


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


def _probe_cookie_source(url: str, source_key: str = "") -> str:
    for key in _probe_cookie_source_keys(url, source_key):
        try:
            if has_cookies_for_source(key):
                return key
        except Exception:
            continue
    return ""


def _probe_rotation(url: str, source_key: str = "", *, with_cookies: bool = True) -> Iterator[AccessIdentity]:
    return access_rotation(lambda: _probe_cookie_source(url, source_key) if with_cookies else "")


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

    def _exec(access: AccessIdentity) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            cmd + ytdlp_access_args(access) + ["--playlist-end", str(_MAX_ENTRIES), url],
            env=access_env(access),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_PROBE_TIMEOUT_SECONDS,
        )

    # Anonymous read first; a fingerprint or cookie is only spent when the public listing fails.
    with closing(_probe_rotation(url)) as rotation:
        for access in rotation:
            result = _exec(access)
            if result.returncode == 0:
                break
            access.report(result.stderr or result.stdout)
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


def _candidate_probe_fields(flat: dict[str, str], engine: str) -> list[dict[str, str]]:
    # Only the candidate fields for this engine, in stable order.
    fields: list[dict[str, str]] = []
    seen: set[str] = set()
    for field in FIELD_CANDIDATES.get(engine, ()):
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


def _exact_url_field_roles(
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


def _run_probe_batch(
    cmd: list[str], urls: list[str], access: AccessIdentity, low_priority: bool
) -> subprocess.CompletedProcess[str] | None:
    """One engine process reading every link; None when it could not run or ran out of time."""
    try:
        return _run_probe_command(
            cmd + urls,
            low_priority=low_priority,
            env=access_env(access),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_PROBE_TIMEOUT_SECONDS * len(urls),
        )
    except (OSError, subprocess.SubprocessError):
        return None


def _probe_output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stderr or result.stdout or ""


def _walk_probe_rotation(
    urls: list[str],
    read: Callable[[AccessIdentity, list[str]], tuple[dict[str, dict[str, Any]], str]],
    cookie_source_key: str,
    with_cookies: bool,
) -> tuple[dict[str, dict[str, Any]], str]:
    """What ``read`` found for each link, each identity trying only the links still unread; the last failure."""
    found: dict[str, dict[str, Any]] = {}
    error = ""
    if not urls:
        return found, error
    with closing(_probe_rotation(urls[0], cookie_source_key, with_cookies=with_cookies)) as rotation:
        for access in rotation:
            attempt, attempt_error = read(access, [url for url in urls if url not in found])
            found.update(attempt)
            if len(found) == len(urls):
                return found, ""
            access.report(attempt_error)
            error = attempt_error or error
    return found, error


def _ytdlp_video(result: Any) -> dict[str, Any] | None:
    """The first video of a result, as a playlist's first entry; None when it holds none."""
    while isinstance(result, dict) and result.get("_type") in ("playlist", "multi_video"):
        result = next(iter(result.get("entries") or []), None)
    return result if isinstance(result, dict) and result else None


def _ytdlp_dumps(
    urls: list[str],
    *,
    with_cookies: bool = True,
    cookie_source_key: str = "",
    low_priority: bool = False,
    extra_args: tuple[str, ...] = (),
) -> tuple[dict[str, dict[str, Any]], str]:
    """yt-dlp's info for each link it read, one process per attempt; the last failed attempt's output."""
    cmd = [
        "yt-dlp",
        # One line per link, in order: its info, or null for a link it could not read.
        "--dump-single-json",
        "--ignore-errors",
        "--no-warnings",
        "--no-download",
        "--playlist-items",
        "1",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
        *extra_args,
    ]

    def _read(access: AccessIdentity, pending: list[str]) -> tuple[dict[str, dict[str, Any]], str]:
        result = _run_probe_batch(cmd + ytdlp_access_args(access), pending, access, low_priority)
        if result is None:
            return {}, ""
        found: dict[str, dict[str, Any]] = {}
        # A run cut short leaves only the links after its last line unread.
        for url, line in zip(pending, filter(str.strip, (result.stdout or "").splitlines()), strict=False):
            with suppress(json.JSONDecodeError):
                if info := _ytdlp_video(json.loads(line)):
                    found[url] = info
        return found, "" if len(found) == len(pending) else _probe_output(result)

    return _walk_probe_rotation(urls, _read, cookie_source_key, with_cookies)


def _gallerydl_messages(document: Any) -> list[Any]:
    return document if isinstance(document, list) else [document]


def _is_gallerydl_error(message: Any) -> bool:
    # gallery-dl -j reports what stopped a link as a message of kind -1.
    return isinstance(message, list) and message[:1] == [-1]


def _gallerydl_errors(document: Any) -> str:
    """What gallery-dl reported stopping a link, "" when nothing did."""
    return " ".join(
        json.dumps(message[1:]) for message in _gallerydl_messages(document) if _is_gallerydl_error(message)
    )


def _gallerydl_richest_metadata(document: Any) -> dict[str, Any]:
    # gallery-dl -j nests the file metadata dict inside its message list; return the largest dict.
    best: dict[str, Any] = {}
    stack: list[Any] = [message for message in _gallerydl_messages(document) if not _is_gallerydl_error(message)]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if len(current) > len(best):
                best = current
        elif isinstance(current, list):
            stack.extend(current)
    return best


def _json_documents(text: str) -> list[Any] | None:
    """Every JSON document in ``text``, in order; None when it holds anything else."""
    decoder = json.JSONDecoder()
    documents: list[Any] = []
    index = _BLANK_RE.match(text).end()
    while index < len(text):
        try:
            document, index = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            return None
        documents.append(document)
        index = _BLANK_RE.match(text, index).end()
    return documents


def _gallerydl_dumps(
    urls: list[str],
    *,
    with_cookies: bool = True,
    cookie_source_key: str = "",
    low_priority: bool = False,
) -> dict[str, dict[str, Any]]:
    """gallery-dl's richest metadata for each link it read, one process per attempt.

    gallery-dl prints one document per link it has an extractor for, so only those links are passed and
    the documents follow them in order. A run printing another count is read again one link at a time.
    """
    cmd = ["gallery-dl", "-j", "-o", _GALLERYDL_TIKTOK_NO_AUDIO_OPTION]

    def _read(access: AccessIdentity, pending: list[str]) -> tuple[dict[str, dict[str, Any]], str]:
        result = _run_probe_batch(cmd + gallerydl_access_args(access), pending, access, low_priority)
        if result is None:
            return {}, ""
        documents = _json_documents(result.stdout or "")
        if documents is None or len(documents) != len(pending):
            if len(pending) == 1:
                return {}, _probe_output(result)
            attempts = [_read(access, [url]) for url in pending]
            return (
                {url: metadata for attempt, _ in attempts for url, metadata in attempt.items()},
                " ".join(error for _, error in attempts if error),
            )
        found = {
            url: metadata
            for url, document in zip(pending, documents, strict=True)
            if (metadata := _gallerydl_richest_metadata(document))
        }
        if len(found) == len(pending):
            return found, ""
        # Its exit code stays 0 even then, so only the document tells a link failed.
        reported = [
            _gallerydl_errors(document) for url, document in zip(pending, documents, strict=True) if url not in found
        ]
        return found, " ".join(filter(None, [*reported, _probe_output(result)]))

    readable = [url for url in urls if gallerydl_reads(url) is not None]
    return _walk_probe_rotation(readable, _read, cookie_source_key, with_cookies)[0]


def probe_metadata(
    source_urls: list[str],
    *,
    with_cookies: bool = True,
    cookie_source_key: str = "",
    low_priority: bool = False,
) -> dict[str, dict[str, str]]:
    """Flat metadata for each URL from whichever engine answers first; a URL no engine read is left out.

    The library scan uses this to resolve a manually-placed file's creator without a
    download, and a tracker to read the items its pages show. Unlike ``probe_fields`` it
    returns every scalar field (flattened to ``key[sub]``) so the caller can walk its own
    configured field-priority order. Links sharing their engine order and cookie source
    are read together, one process per engine.
    """
    prepared = {source_url: _prepare_url(source_url) for source_url in source_urls}
    options: dict[str, Any] = {
        "with_cookies": with_cookies,
        "cookie_source_key": cookie_source_key,
        "low_priority": low_priority,
    }
    groups: dict[tuple[Any, ...], list[str]] = {}
    for url in dict.fromkeys(filter(None, prepared.values())):
        # yt-dlp only reads a link gallery-dl has its own extractor for generically, failing once per jar.
        gallerydl_first = bool(gallerydl_reads(url)) and not ytdlp_single_video(url)
        groups.setdefault((gallerydl_first, *_probe_cookie_source_keys(url, cookie_source_key)), []).append(url)
    dumps: tuple[Callable[[list[str]], dict[str, dict[str, Any]]], ...] = (
        lambda urls: _ytdlp_dumps(urls, **options)[0],
        lambda urls: _gallerydl_dumps(urls, **options),
    )
    found: dict[str, dict[str, Any]] = {}
    for (gallerydl_first, *_), group in groups.items():
        for dump in dumps[::-1] if gallerydl_first else dumps:
            if pending := [url for url in group if url not in found]:
                found.update(dump(pending))
    return {source_url: _flatten_metadata(found[url]) for source_url, url in prepared.items() if url in found}


def probe_media_info(
    source_url: str,
    *,
    with_cookies: bool = True,
    cookie_source_key: str = "",
) -> dict[str, Any]:
    """yt-dlp's full info dict for one item, subtitles included; ``{}`` on failure."""
    url = _prepare_url(source_url)
    if not url:
        return {}
    # Extractors only collect subtitles when asked to write them; --no-download still writes nothing.
    found, _ = _ytdlp_dumps(
        [url],
        with_cookies=with_cookies,
        cookie_source_key=cookie_source_key,
        extra_args=("--write-subs", "--write-auto-subs"),
    )
    return found.get(url, {})


def _probe_field_metadata(
    url: str,
    source_key: str,
    *,
    with_cookies: bool,
    low_priority: bool = False,
    stop_after_first_with_roles: bool = False,
) -> tuple[list[tuple[str, dict[str, str]]], list[str]]:
    probed: list[tuple[str, dict[str, str]]] = []
    errors: list[str] = []
    found, error = _ytdlp_dumps(
        [url],
        with_cookies=with_cookies,
        cookie_source_key=source_key,
        low_priority=low_priority,
    )
    if info := found.get(url):
        flat = _flatten_metadata(info)
        probed.append(("ytdlp", flat))
        if stop_after_first_with_roles and _candidate_probe_fields(flat, "ytdlp"):
            return probed, errors
    elif error:
        errors.append(error)

    metadata = _gallerydl_dumps(
        [url],
        with_cookies=with_cookies,
        cookie_source_key=source_key,
        low_priority=low_priority,
    ).get(url)
    if metadata:
        probed.append(("gallerydl", _flatten_metadata(metadata)))
    return probed, errors


def _candidate_field_count(probed: list[tuple[str, dict[str, str]]]) -> int:
    return sum(len(_candidate_probe_fields(flat, engine)) for engine, flat in probed)


def probe_fields(
    source_url: str,
    source_key: str = "",
    *,
    low_priority: bool = False,
    stop_after_first_with_roles: bool = False,
) -> dict[str, Any]:
    """List candidate metadata fields for a link, no download.

    Probes both yt-dlp and gallery-dl and merges the username/nickname/title catalog fields
    each returns, so the user sees whichever engine's fields apply to this source.
    ``metadata`` holds every flat field the engines returned, first engine first.
    """
    url = _prepare_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")
    resolved_key = _resolved_probe_source_key(url, source_key)

    probed: list[tuple[str, dict[str, str]]] = []
    errors: list[str] = []
    for with_cookies in (False, True):
        if with_cookies:
            if _candidate_field_count(probed) or not _probe_cookie_source(url, resolved_key):
                break
        attempt_probed, attempt_errors = _probe_field_metadata(
            url,
            resolved_key,
            with_cookies=with_cookies,
            low_priority=low_priority,
            stop_after_first_with_roles=stop_after_first_with_roles,
        )
        errors.extend(attempt_errors)
        if not attempt_probed:
            continue
        probed.extend(attempt_probed)
        if _candidate_field_count(attempt_probed):
            break

    if not probed:
        detail = (errors[-1] if errors else "").strip().splitlines()
        raise ValueError(detail[-1] if detail else "Could not read that link.")

    fields: list[dict[str, str]] = []
    fields_by_engine: dict[str, list[str]] = {}
    values_by_field: dict[str, str] = {}
    metadata: dict[str, str] = {}
    seen: set[str] = set()
    for engine, flat in probed:
        for name, value in flat.items():
            metadata.setdefault(name, value)
        engine_fields = _candidate_probe_fields(flat, engine)
        fields_by_engine[engine] = [item["field"] for item in engine_fields]
        for item in engine_fields:
            values_by_field.setdefault(item["field"], item["value"])
            if item["field"] in seen:
                continue
            seen.add(item["field"])
            fields.append(item)
    field_roles = field_roles_from_probe_fields(fields_by_engine)
    field_roles = promote_field_roles(
        field_roles,
        _exact_url_field_roles(url, field_roles, values_by_field),
    )
    return {
        "source_key": resolved_key,
        "fields": fields,
        "field_roles": field_roles,
        "metadata": metadata,
    }
