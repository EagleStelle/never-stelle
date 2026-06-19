from __future__ import annotations

import os
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


COOKIE_BLOB_KEY = "instagram_ytdlp_cookies"
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024
_cookie_file_lock = threading.RLock()

DEFAULT_FOLDER_TEMPLATE = os.environ.get("DEFAULT_FOLDER_TEMPLATE", "{{creator}}").strip() or "{{creator}}"
DEFAULT_FILENAME_TEMPLATE = (
    os.environ.get("DEFAULT_FILENAME_TEMPLATE", "{{creator}} - {{title}} [{{id}}]").strip()
    or "{{creator}} - {{title}} [{{id}}]"
)


def load_saved_settings_file() -> dict[str, Any]:
    payload = load_settings_payload()
    return payload if isinstance(payload, dict) else {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_settings_payload(payload if isinstance(payload, dict) else {})


def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder_template = str(source.get("folder_template") or "").strip() or DEFAULT_FOLDER_TEMPLATE
    filename_template = str(source.get("filename_template") or "").strip() or DEFAULT_FILENAME_TEMPLATE
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


def get_ytdlp_cookies_status() -> dict[str, Any]:
    uploaded = get_file_blob_metadata(COOKIE_BLOB_KEY)
    if uploaded:
        return {
            "configured": True,
            "source": "uploaded",
            "filename": str(uploaded.get("filename") or "cookies.txt"),
            "uploaded_at": str(uploaded.get("created_at") or uploaded.get("updated_at") or ""),
        }

    mounted = str(os.environ.get("YTDLP_INSTAGRAM_COOKIES") or os.environ.get("YTDLP_COOKIES") or "").strip()
    if mounted and Path(mounted).is_file():
        try:
            uploaded_at = datetime.fromtimestamp(Path(mounted).stat().st_mtime, tz=timezone.utc).isoformat()
        except Exception:
            uploaded_at = ""
        return {
            "configured": True,
            "source": "mounted",
            "filename": Path(mounted).name,
            "uploaded_at": uploaded_at,
        }

    return {"configured": False, "source": "none", "filename": "", "uploaded_at": ""}


def materialize_cookie_blob() -> str:
    uploaded = get_file_blob(COOKIE_BLOB_KEY)
    if not uploaded:
        return ""
    content = uploaded.get("content")
    if not isinstance(content, bytes) or not content:
        return ""

    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    target = runtime_dir / "instagram-ytdlp-cookies.txt"
    with _cookie_file_lock:
        temp_target = target.with_suffix(".tmp")
        temp_target.write_bytes(content)
        temp_target.replace(target)
    return str(target)


def find_cookies_file_for_url(source_url: str) -> str:
    host = ""
    try:
        from urllib.parse import urlparse

        host = (urlparse(source_url).hostname or "").lower()
    except Exception:
        host = ""

    candidates: list[str] = []
    if host.endswith("instagram.com"):
        candidates.append(materialize_cookie_blob())
    if host.endswith("instagram.com"):
        candidates.append(str(os.environ.get("YTDLP_INSTAGRAM_COOKIES") or "").strip())
    candidates.append(str(os.environ.get("YTDLP_COOKIES") or "").strip())

    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return ""


async def save_ytdlp_cookies_upload(uploaded: UploadFile) -> None:
    raw = await uploaded.read()
    if not raw:
        raise ValueError("Cookies file is empty.")
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        raise ValueError("Cookies file is too large.")

    save_file_blob(
        COOKIE_BLOB_KEY,
        Path(uploaded.filename or "cookies.txt").name,
        raw,
        "text/plain",
    )


def clear_ytdlp_cookies_upload() -> None:
    delete_file_blob(COOKIE_BLOB_KEY)
    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    for filename in ("instagram-ytdlp-cookies.txt", "instagram-ytdlp-cookies.tmp"):
        try:
            (runtime_dir / filename).unlink(missing_ok=True)
        except Exception:
            pass


def get_effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = cfg or load_app_config()
    payload = load_saved_settings_file()
    return {
        "site_locations": normalize_site_location_selection(payload.get("site_locations"), cfg),
        "save_mode": "device" if str(payload.get("save_mode") or "").lower() == "device" else "nas",
        "template_settings": normalize_template_settings(payload.get("template_settings")),
        "instagram_ytdlp_cookies": get_ytdlp_cookies_status(),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_site_locations: Any,
    raw_save_mode: Any,
    raw_template_settings: Any = None,
) -> dict[str, Any]:
    existing = load_saved_settings_file()
    existing.update(
        {
            "site_locations": normalize_site_location_selection(raw_site_locations, cfg),
            "save_mode": "device" if str(raw_save_mode or "").lower() == "device" else "nas",
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
        "save_mode": saved.get("save_mode", "nas"),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "instagram_ytdlp_cookies": saved.get("instagram_ytdlp_cookies", get_ytdlp_cookies_status()),
        "settings_loaded_at": int(time.time()),
    }
