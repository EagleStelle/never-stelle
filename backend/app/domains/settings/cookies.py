from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from backend.app.core.sources import normalize_source_key
from backend.app.db.repositories import delete_file_blob, get_file_blob, get_file_blob_metadata, save_file_blob

from .profiles import (
    get_effective_source_profiles,
    get_source_profile_for_url,
    require_settings_managed_source,
    settings_managed_profiles,
)

# One cookies.txt per resolved source.
COOKIE_BLOB_PREFIX = "ytdlp_cookies::"
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024
_cookie_file_lock = threading.RLock()


def normalize_cookie_source(source_key: Any) -> str:
    return normalize_source_key(source_key)


def _cookie_key(source_key: Any) -> str:
    return f"{COOKIE_BLOB_PREFIX}{normalize_cookie_source(source_key)}"


def _cookie_blob_metadata(source_key: str) -> dict[str, Any] | None:
    return get_file_blob_metadata(_cookie_key(source_key))


def _cookie_blob_content(source_key: str) -> dict[str, Any] | None:
    return get_file_blob(_cookie_key(source_key))


def detect_cookie_source(source_url: str) -> str:
    return get_source_profile_for_url(source_url)["key"]


def get_cookie_source_status(source_key: str) -> dict[str, Any]:
    uploaded = _cookie_blob_metadata(source_key)
    if uploaded:
        return {
            "configured": True,
            "source": "uploaded",
            "filename": str(uploaded.get("filename") or "cookies.txt"),
            "uploaded_at": str(uploaded.get("created_at") or uploaded.get("updated_at") or ""),
        }
    return {"configured": False, "source": "none", "filename": "", "uploaded_at": ""}


def get_ytdlp_cookies_status(source_profiles: list[dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    profiles = settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles()
    )
    keys = [key for key in (normalize_source_key(profile.get("key")) for profile in profiles) if key]
    return {key: get_cookie_source_status(key) for key in dict.fromkeys(keys)}


def materialize_cookie_blob(source_key: str) -> str:
    source_key = normalize_cookie_source(source_key)
    if not source_key:
        return ""
    uploaded = _cookie_blob_content(source_key)
    if not uploaded:
        return ""
    content = uploaded.get("content")
    if not isinstance(content, bytes) or not content:
        return ""

    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    target = runtime_dir / f"ytdlp-cookies-{source_key}.txt"
    with _cookie_file_lock:
        temp_target = target.with_suffix(".tmp")
        temp_target.write_bytes(content)
        temp_target.replace(target)
    return str(target)


def find_cookies_file_for_source(source_key: str) -> str:
    source_key = normalize_cookie_source(source_key)
    candidate = materialize_cookie_blob(source_key)
    if candidate and Path(candidate).is_file():
        return candidate
    return ""


def find_cookies_file_for_url(source_url: str) -> str:
    return find_cookies_file_for_source(detect_cookie_source(source_url))


def has_cookies_for_source(source_key: str) -> bool:
    source_key = normalize_cookie_source(source_key)
    return bool(source_key and _cookie_blob_metadata(source_key))


def has_cookies_for_url(source_url: str) -> bool:
    return has_cookies_for_source(detect_cookie_source(source_url))


async def save_ytdlp_cookies_upload(uploaded: UploadFile, source_key: str) -> None:
    require_settings_managed_source(source_key)
    raw = await uploaded.read()
    if not raw:
        raise ValueError("Cookies file is empty.")
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        raise ValueError("Cookies file is too large.")
    save_file_blob(
        _cookie_key(source_key),
        Path(uploaded.filename or "cookies.txt").name,
        raw,
        "text/plain",
    )


def clear_ytdlp_cookies_upload(source_key: str) -> None:
    require_settings_managed_source(source_key)
    source_key = normalize_cookie_source(source_key)
    delete_file_blob(_cookie_key(source_key))
    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    for filename in (f"ytdlp-cookies-{source_key}.txt", f"ytdlp-cookies-{source_key}.tmp"):
        try:
            (runtime_dir / filename).unlink(missing_ok=True)
        except Exception:
            pass
