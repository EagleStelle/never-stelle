from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from backend.app.core.config import (
    DATA_DIR,
    SITE_KEYS,
    get_site_default_locations,
    load_app_config,
    normalize_allowed_location,
    normalize_download_locations,
)
from backend.app.core.storage import file_lock, load_json, save_json


SETTINGS_FILE = DATA_DIR / "settings.json"
COOKIE_FILE = DATA_DIR / "instagram-ytdlp-cookies.txt"
RUNTIME_COOKIE_FILE = DATA_DIR / "instagram-ytdlp-cookies.runtime.txt"
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024

DEFAULT_FOLDER_TEMPLATE = os.environ.get("DEFAULT_FOLDER_TEMPLATE", "{{creator}}").strip() or "{{creator}}"
DEFAULT_FILENAME_TEMPLATE = (
    os.environ.get("DEFAULT_FILENAME_TEMPLATE", "{{creator}} - {{title}} [{{id}}]").strip()
    or "{{creator}} - {{title}} [{{id}}]"
)


def _settings_default() -> dict[str, Any]:
    return {}


def load_saved_settings_file() -> dict[str, Any]:
    payload = load_json(SETTINGS_FILE, _settings_default)
    return payload if isinstance(payload, dict) else {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_json(SETTINGS_FILE, payload if isinstance(payload, dict) else {})


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
    filename = str(load_saved_settings_file().get("instagram_cookies_filename") or "").strip()
    uploaded_at = str(load_saved_settings_file().get("instagram_cookies_uploaded_at") or "").strip()
    if COOKIE_FILE.exists() and COOKIE_FILE.is_file():
        return {
            "configured": True,
            "source": "uploaded",
            "filename": filename or COOKIE_FILE.name,
            "uploaded_at": uploaded_at,
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


def find_cookies_file_for_url(source_url: str) -> str:
    host = ""
    try:
        from urllib.parse import urlparse

        host = (urlparse(source_url).hostname or "").lower()
    except Exception:
        host = ""

    candidates: list[str] = []
    if host.endswith("instagram.com") and COOKIE_FILE.exists() and COOKIE_FILE.is_file():
        candidates.append(str(COOKIE_FILE))
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

    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(COOKIE_FILE):
        temp_path = COOKIE_FILE.with_suffix(".tmp")
        temp_path.write_bytes(raw)
        temp_path.replace(COOKIE_FILE)

    try:
        RUNTIME_COOKIE_FILE.unlink(missing_ok=True)
    except Exception:
        pass

    existing = load_saved_settings_file()
    existing["instagram_cookies_filename"] = Path(uploaded.filename or "cookies.txt").name
    existing["instagram_cookies_uploaded_at"] = datetime.now(timezone.utc).isoformat()
    save_saved_settings_file(existing)


def clear_ytdlp_cookies_upload() -> None:
    for path in (COOKIE_FILE, RUNTIME_COOKIE_FILE):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
    existing = load_saved_settings_file()
    existing.pop("instagram_cookies_filename", None)
    existing.pop("instagram_cookies_uploaded_at", None)
    save_saved_settings_file(existing)


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
