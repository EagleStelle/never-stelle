from __future__ import annotations

import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now_datetime
from backend.app.db.repositories import (
    add_source_cookie,
    delete_source_cookie,
    delete_source_cookies,
    get_source_cookie,
    list_source_cookies,
    reorder_source_cookies,
)

from .profiles import (
    get_effective_source_profiles,
    get_source_profile_for_url,
    require_settings_managed_source,
    settings_managed_profiles,
)

# A source holds any number of cookie jars; the pool rotates between them.
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024
_cookie_file_lock = threading.RLock()


def _cookie_runtime_dir() -> Path:
    return Path(tempfile.gettempdir()) / "never-stelle" / "cookies"


def normalize_cookie_source(source_key: Any) -> str:
    return normalize_source_key(source_key)


def detect_cookie_source(source_url: str) -> str:
    return get_source_profile_for_url(source_url)["key"]


def _cookie_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "filename": str(row.get("filename") or "cookies.txt"),
        "uploaded_at": str(row.get("created_at") or row.get("updated_at") or ""),
    }


def list_cookies_for_source(source_key: str) -> list[dict[str, Any]]:
    source_key = normalize_cookie_source(source_key)
    if not source_key:
        return []
    return [_cookie_entry(row) for row in list_source_cookies(source_key)]


def cookie_ids_for_source(source_key: str) -> list[str]:
    return [entry["id"] for entry in list_cookies_for_source(source_key) if entry["id"]]


def get_cookie_source_status(source_key: str) -> dict[str, Any]:
    cookies = list_cookies_for_source(source_key)
    return {"configured": bool(cookies), "count": len(cookies), "cookies": cookies}


def get_ytdlp_cookies_status(source_profiles: list[dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    profiles = settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles()
    )
    keys = [key for key in (normalize_source_key(profile.get("key")) for profile in profiles) if key]
    return {key: get_cookie_source_status(key) for key in dict.fromkeys(keys)}


def has_cookies_for_source(source_key: str) -> bool:
    source_key = normalize_cookie_source(source_key)
    return bool(source_key and list_source_cookies(source_key))


def has_cookies_for_url(source_url: str) -> bool:
    return has_cookies_for_source(detect_cookie_source(source_url))


def materialize_cookie(cookie_id: str) -> str:
    """Write one stored jar to a runtime cookies.txt yt-dlp/gallery-dl can read."""
    cookie_id = str(cookie_id or "").strip()
    if not cookie_id:
        return ""
    stored = get_source_cookie(cookie_id)
    if not stored:
        return ""
    content = stored.get("content")
    if not isinstance(content, bytes) or not content:
        return ""

    runtime_dir = _cookie_runtime_dir()
    runtime_dir.mkdir(parents=True, exist_ok=True)
    target = runtime_dir / f"{cookie_id}.txt"
    with _cookie_file_lock:
        temp_target = target.with_suffix(".tmp")
        temp_target.write_bytes(content)
        temp_target.replace(target)
    return str(target)


def _drop_materialized_cookie(cookie_id: str) -> None:
    runtime_dir = _cookie_runtime_dir()
    for name in (f"{cookie_id}.txt", f"{cookie_id}.tmp"):
        try:
            (runtime_dir / name).unlink(missing_ok=True)
        except Exception:
            pass


def _stored_cookie_filename(source_key: str) -> str:
    """Uploads are renamed to ``<date> <time> <source>.txt``; the browser's name is dropped."""
    # Same-day uploads are common, so the clock disambiguates them. Dashes, not
    # colons, keep the name usable as a filename on Windows.
    stem = f"{utc_now_datetime().strftime('%Y-%m-%d %H-%M-%S')} {source_key}".strip()
    taken = {entry["filename"] for entry in list_cookies_for_source(source_key)}
    candidate = f"{stem}.txt"
    suffix = 2
    while candidate in taken:
        candidate = f"{stem} ({suffix}).txt"
        suffix += 1
    return candidate


async def save_ytdlp_cookies_upload(uploaded: UploadFile, source_key: str) -> dict[str, Any]:
    require_settings_managed_source(source_key)
    source_key = normalize_cookie_source(source_key)
    raw = await uploaded.read()
    if not raw:
        raise ValueError("Cookies file is empty.")
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        raise ValueError("Cookies file is too large.")
    cookie_id = uuid.uuid4().hex
    filename = _stored_cookie_filename(source_key)
    add_source_cookie(cookie_id, source_key, filename, raw)
    from .cookie_pool import invalidate_cookie_pool

    invalidate_cookie_pool(source_key)
    return _cookie_entry({"id": cookie_id, "filename": filename})


def reorder_ytdlp_cookies(source_key: str, cookie_ids: list[str]) -> None:
    """Set the order jars are tried in; the top entry leads a fresh rotation."""
    require_settings_managed_source(source_key)
    source_key = normalize_cookie_source(source_key)
    reorder_source_cookies(source_key, [str(cookie_id or "").strip() for cookie_id in cookie_ids])


def clear_ytdlp_cookie(source_key: str, cookie_id: str) -> None:
    require_settings_managed_source(source_key)
    source_key = normalize_cookie_source(source_key)
    stored = get_source_cookie(str(cookie_id or "").strip())
    if not stored or normalize_cookie_source(stored.get("source_key")) != source_key:
        raise ValueError("That cookies file no longer exists.")
    delete_source_cookie(stored["id"])
    _drop_materialized_cookie(stored["id"])
    from .cookie_pool import invalidate_cookie_pool

    invalidate_cookie_pool(source_key)


def clear_ytdlp_cookies_upload(source_key: str) -> None:
    require_settings_managed_source(source_key)
    source_key = normalize_cookie_source(source_key)
    for entry in list_cookies_for_source(source_key):
        _drop_materialized_cookie(entry["id"])
    delete_source_cookies(source_key)
    from .cookie_pool import invalidate_cookie_pool

    invalidate_cookie_pool(source_key)
