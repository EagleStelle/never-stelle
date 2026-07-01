from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import UploadFile

from backend.app.core.config import (
    SITE_KEYS,
    get_site_default_locations,
    load_app_config,
    normalize_allowed_location,
    normalize_download_locations,
)
from backend.app.db.repositories import (
    delete_file_blob,
    get_file_blob,
    get_file_blob_metadata,
    load_settings_payload,
    save_file_blob,
    save_settings_payload,
)

# One cookies.txt per platform. yt-dlp is given the jar that matches the URL
# being downloaded; an "others" jar acts as a catch-all fallback.
COOKIE_PLATFORMS = (
    "youtube",
    "tiktok",
    "instagram",
    "twitter",
    "facebook",
    "reddit",
    "twitch",
    "pinterest",
    "bluesky",
    "linkedin",
    "others",
)
COOKIE_BLOB_PREFIX = "ytdlp_cookies::"
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024
_cookie_file_lock = threading.RLock()

_COOKIE_HOST_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("youtube", ("youtube.com", "youtu.be")),
    ("tiktok", ("tiktok.com",)),
    ("instagram", ("instagram.com",)),
    ("twitter", ("twitter.com", "x.com")),
    ("facebook", ("facebook.com", "fb.com", "fb.watch")),
    ("reddit", ("reddit.com", "redd.it")),
    ("twitch", ("twitch.tv",)),
    ("pinterest", ("pinterest.com", "pin.it")),
    ("bluesky", ("bsky.app", "bsky.social")),
    ("linkedin", ("linkedin.com",)),
)


def normalize_cookie_platform(platform: Any) -> str:
    candidate = str(platform or "").strip().lower()
    return candidate if candidate in COOKIE_PLATFORMS else "others"


def _cookie_key(platform: Any) -> str:
    return f"{COOKIE_BLOB_PREFIX}{normalize_cookie_platform(platform)}"


def detect_cookie_platform(source_url: str) -> str:
    try:
        host = (urlparse(source_url).hostname or "").lower()
    except Exception:
        host = ""
    for platform, suffixes in _COOKIE_HOST_RULES:
        if host.endswith(suffixes):
            return platform
    return "others"


def _cookie_blob_metadata(platform: str) -> dict[str, Any] | None:
    return get_file_blob_metadata(_cookie_key(platform))


def _cookie_blob_content(platform: str) -> dict[str, Any] | None:
    return get_file_blob(_cookie_key(platform))

BUILTIN_FOLDER_TEMPLATE = "{{creator}}"
BUILTIN_FILENAME_TEMPLATE = "{{creator}} - {{title}} [{{id}}]"


def load_saved_settings_file() -> dict[str, Any]:
    payload = load_settings_payload()
    return payload if isinstance(payload, dict) else {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_settings_payload(payload if isinstance(payload, dict) else {})


def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder_template = str(source.get("folder_template") or "").strip() or BUILTIN_FOLDER_TEMPLATE
    filename_template = str(source.get("filename_template") or "").strip() or BUILTIN_FILENAME_TEMPLATE
    return {
        "folder_template": folder_template,
        "filename_template": filename_template,
    }


def get_effective_template_settings() -> dict[str, str]:
    payload = load_saved_settings_file()
    return normalize_template_settings(payload.get("template_settings"))


def normalize_site_location_selection(raw: Any, cfg: dict[str, Any]) -> dict[str, str]:
    defaults = get_site_default_locations(cfg)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for site in SITE_KEYS:
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "")
    return out


def get_cookie_platform_status(platform: str) -> dict[str, Any]:
    uploaded = _cookie_blob_metadata(platform)
    if uploaded:
        return {
            "configured": True,
            "source": "uploaded",
            "filename": str(uploaded.get("filename") or "cookies.txt"),
            "uploaded_at": str(uploaded.get("created_at") or uploaded.get("updated_at") or ""),
        }

    return {"configured": False, "source": "none", "filename": "", "uploaded_at": ""}


def get_ytdlp_cookies_status() -> dict[str, dict[str, Any]]:
    return {platform: get_cookie_platform_status(platform) for platform in COOKIE_PLATFORMS}


def materialize_cookie_blob(platform: str) -> str:
    platform = normalize_cookie_platform(platform)
    uploaded = _cookie_blob_content(platform)
    if not uploaded:
        return ""
    content = uploaded.get("content")
    if not isinstance(content, bytes) or not content:
        return ""

    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    target = runtime_dir / f"ytdlp-cookies-{platform}.txt"
    with _cookie_file_lock:
        temp_target = target.with_suffix(".tmp")
        temp_target.write_bytes(content)
        temp_target.replace(target)
    return str(target)


def find_cookies_file_for_url(source_url: str) -> str:
    # Use the jar uploaded for this URL's platform; fall back to the catch-all
    # "others" jar so a generic cookies.txt still helps unmapped sites.
    platform = detect_cookie_platform(source_url)
    candidate = materialize_cookie_blob(platform)
    if candidate and Path(candidate).is_file():
        return candidate
    if platform != "others":
        fallback = materialize_cookie_blob("others")
        if fallback and Path(fallback).is_file():
            return fallback
    return ""


def has_cookies_for_url(source_url: str) -> bool:
    platform = detect_cookie_platform(source_url)
    if _cookie_blob_metadata(platform):
        return True
    return platform != "others" and bool(_cookie_blob_metadata("others"))


async def save_ytdlp_cookies_upload(uploaded: UploadFile, platform: str) -> None:
    raw = await uploaded.read()
    if not raw:
        raise ValueError("Cookies file is empty.")
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        raise ValueError("Cookies file is too large.")

    save_file_blob(
        _cookie_key(platform),
        Path(uploaded.filename or "cookies.txt").name,
        raw,
        "text/plain",
    )


def clear_ytdlp_cookies_upload(platform: str) -> None:
    platform = normalize_cookie_platform(platform)
    delete_file_blob(_cookie_key(platform))
    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    for filename in (f"ytdlp-cookies-{platform}.txt", f"ytdlp-cookies-{platform}.tmp"):
        try:
            (runtime_dir / filename).unlink(missing_ok=True)
        except Exception:
            pass


def get_effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = cfg or load_app_config()
    payload = load_saved_settings_file()
    return {
        "site_locations": normalize_site_location_selection(payload.get("site_locations"), cfg),
        "template_settings": normalize_template_settings(payload.get("template_settings")),
        "ytdlp_cookies": get_ytdlp_cookies_status(),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_site_locations: Any,
    raw_template_settings: Any = None,
) -> dict[str, Any]:
    existing = load_saved_settings_file()
    existing.update(
        {
            "site_locations": normalize_site_location_selection(raw_site_locations, cfg),
            "template_settings": normalize_template_settings(raw_template_settings),
        }
    )
    save_saved_settings_file(existing)
    return get_effective_saved_settings(cfg)


def build_settings_response(
    cfg: dict[str, Any] | None = None,
    saved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = cfg or load_app_config()
    saved = saved or get_effective_saved_settings(cfg)
    return {
        "download_locations": normalize_download_locations(cfg),
        "site_default_locations": saved.get("site_locations", {}),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "ytdlp_cookies": saved.get("ytdlp_cookies", get_ytdlp_cookies_status()),
        "settings_loaded_at": int(time.time()),
    }
