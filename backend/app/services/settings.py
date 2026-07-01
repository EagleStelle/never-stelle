from __future__ import annotations

import tempfile
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from backend.app.core.config import (
    get_config_source_profiles,
    get_site_default_locations,
    load_app_config,
    normalize_allowed_location,
    normalize_download_locations,
)
from backend.app.core.sources import (
    FALLBACK_SOURCE_KEY,
    favicon_url_for_host,
    host_from_url,
    merge_source_profiles,
    normalize_source_key,
    source_key_from_url,
    source_label_from_key,
    source_profile_for_url,
)
from backend.app.db.repositories import (
    activity_revision,
    delete_file_blob,
    get_file_blob,
    get_file_blob_metadata,
    load_history_payload,
    load_settings_payload,
    load_task_store_payload,
    save_file_blob,
    save_settings_payload,
)

BUILTIN_FOLDER_TEMPLATE = "{{creator}}"
BUILTIN_FILENAME_TEMPLATE = "{{creator}} - {{title}} [{{id}}]"

# One cookies.txt per source; the FALLBACK_SOURCE_KEY jar is the catch-all.
COOKIE_BLOB_PREFIX = "ytdlp_cookies::"
MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024
_cookie_file_lock = threading.RLock()

_activity_cache_lock = threading.RLock()
_activity_cache: tuple[tuple[Any, ...], list[dict[str, Any]]] | None = None


# --- Saved settings file I/O ---
def load_saved_settings_file() -> dict[str, Any]:
    payload = load_settings_payload()
    return payload if isinstance(payload, dict) else {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_settings_payload(payload if isinstance(payload, dict) else {})


# --- Source profiles ---
def _profiles_from_keys(keys: Iterable[str]) -> list[dict[str, str]]:
    return [{"key": normalize_source_key(key)} for key in keys if normalize_source_key(key)]


def _profile_keys(source: Iterable[dict[str, Any]]) -> set[str]:
    return {normalize_source_key(item.get("key")) for item in source if normalize_source_key(item.get("key"))}


def _filter_profiles(source: Any, allowed_keys: set[str]) -> list[dict[str, Any]]:
    # Keep visible keys or any profile the user gave real hosts to; a hosted
    # profile is an intentional creation and must survive reload even without
    # config/activity backing, while hostless activity echoes still prune.
    return [
        profile
        for profile in merge_source_profiles(source)
        if normalize_source_key(profile.get("key")) in allowed_keys or profile.get("hosts")
    ]


def _configured_source_profiles(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return {}
    return raw.get("source_profiles") or raw.get("site_profiles") or {}


def _config_fingerprint(config_profiles: list[dict[str, Any]]) -> tuple[Any, ...]:
    return tuple((profile.get("key"), tuple(profile.get("hosts") or [])) for profile in config_profiles)


def _activity_source_profiles(config_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Profiles inferred from downloaded URLs. Cached against a cheap DB revision
    # so repeated resolutions in one request skip re-decoding the whole store.
    global _activity_cache
    signature = (activity_revision(), _config_fingerprint(config_profiles))
    with _activity_cache_lock:
        if _activity_cache is not None and _activity_cache[0] == signature:
            return _activity_cache[1]

    payloads: list[dict[str, Any]] = []
    for store in (load_task_store_payload(), load_history_payload()):
        collection = store.get("tasks") or store.get("entries") or {}
        if isinstance(collection, dict):
            payloads.extend(item for item in collection.values() if isinstance(item, dict))

    profiles: list[dict[str, Any]] = []
    for payload in payloads:
        source_url = str(payload.get("source_url") or "")
        if source_url:
            profiles.append(source_profile_for_url(source_url, config_profiles))
        elif payload.get("source_key") or payload.get("site_category"):
            profiles.append({"key": normalize_source_key(payload.get("source_key") or payload.get("site_category"))})

    result = merge_source_profiles(profiles)
    with _activity_cache_lock:
        _activity_cache = (signature, result)
    return result


def get_effective_source_profiles(
    cfg: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    extra_keys: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    cfg = cfg or load_app_config()
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    config_profiles = get_config_source_profiles(cfg)
    activity_profiles = _activity_source_profiles(config_profiles)
    extra_profiles = _profiles_from_keys(normalize_source_key(key) for key in (extra_keys or []))
    visible_keys = _profile_keys(config_profiles) | _profile_keys(activity_profiles) | _profile_keys(extra_profiles)
    saved_profiles = _filter_profiles(_configured_source_profiles(payload), visible_keys)
    return merge_source_profiles(config_profiles, activity_profiles, saved_profiles, extra_profiles)


def get_source_profile_for_url(
    source_url: str,
    cfg: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return source_profile_for_url(source_url, get_effective_source_profiles(cfg, payload))


def get_source_profile_by_key(
    source_key: str,
    cfg: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    key = normalize_source_key(source_key)
    profiles = get_effective_source_profiles(cfg, payload, [key])
    return next((profile for profile in profiles if profile.get("key") == key), source_profile_for_url(key, profiles))


def ensure_source_profile_for_url(url_or_host: str) -> str:
    """Derive a source key from a URL/host and persist a matching profile.

    Lets the user attach cookies (or settings) to any site by pasting its link,
    with no predefined profile. If the derived host maps to an existing profile
    the key is returned unchanged; otherwise a new profile carrying that host is
    saved so future downloads from the same site match the same jar.
    """
    cfg = load_app_config()
    payload = load_saved_settings_file()
    profiles = get_effective_source_profiles(cfg, payload)
    key = source_key_from_url(url_or_host, profiles)
    host = host_from_url(url_or_host)
    if any(normalize_source_key(profile.get("key")) == key for profile in profiles):
        return key

    saved_profiles = merge_source_profiles(_configured_source_profiles(payload))
    saved_profiles.append(
        {
            "key": key,
            "label": source_label_from_key(key),
            "hosts": [host] if host else [],
            "icon_url": favicon_url_for_host(host) if host else "",
        }
    )
    payload["source_profiles"] = saved_profiles
    save_saved_settings_file(payload)
    return key


# --- Templates ---
def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder_template = str(source.get("folder_template") or "").strip() or BUILTIN_FOLDER_TEMPLATE
    filename_template = str(source.get("filename_template") or "").strip() or BUILTIN_FILENAME_TEMPLATE
    return {"folder_template": folder_template, "filename_template": filename_template}


def normalize_source_template_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
    default_template: dict[str, str] | None = None,
) -> dict[str, dict[str, str]]:
    source = raw if isinstance(raw, dict) else {}
    default_template = normalize_template_settings(default_template or {})
    profiles = source_profiles or get_effective_source_profiles(cfg)
    out: dict[str, dict[str, str]] = {}
    for profile in profiles:
        key = normalize_source_key(profile.get("key"))
        profile_template = {
            "folder_template": str(profile.get("folder_template") or default_template["folder_template"]),
            "filename_template": str(profile.get("filename_template") or default_template["filename_template"]),
        }
        out[key] = normalize_template_settings(
            source.get(key) or source.get(str(profile.get("label") or "")) or profile_template
        )
    return out


def get_effective_template_settings(source_url: str = "") -> dict[str, str]:
    cfg = load_app_config()
    payload = load_saved_settings_file()
    base = normalize_template_settings(payload.get("template_settings"))
    if not source_url:
        return base
    profile = get_source_profile_for_url(source_url, cfg, payload)
    source_templates = normalize_source_template_selection(
        payload.get("source_templates") or payload.get("source_template_settings"),
        cfg,
        get_effective_source_profiles(cfg, payload, [profile["key"]]),
        base,
    )
    return source_templates.get(profile["key"], base)


# --- Locations ---
def normalize_source_location_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    source_profiles = source_profiles or get_effective_source_profiles(cfg)
    source_keys = [normalize_source_key(profile.get("key")) for profile in source_profiles]
    defaults = get_site_default_locations(cfg, source_keys)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for site in source_keys:
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "") or defaults.get(FALLBACK_SOURCE_KEY, "")
    return out


# --- Cookies ---
def normalize_cookie_source(source_key: Any) -> str:
    return normalize_source_key(source_key or FALLBACK_SOURCE_KEY)


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
    profiles = source_profiles or get_effective_source_profiles()
    keys = [normalize_source_key(profile.get("key")) for profile in profiles]
    if FALLBACK_SOURCE_KEY not in keys:
        keys.append(FALLBACK_SOURCE_KEY)
    return {key: get_cookie_source_status(key) for key in dict.fromkeys(keys)}


def materialize_cookie_blob(source_key: str) -> str:
    source_key = normalize_cookie_source(source_key)
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


def find_cookies_file_for_url(source_url: str) -> str:
    # Prefer the jar for this URL's source; fall back to the catch-all jar so a
    # generic cookies.txt still helps unmapped sites.
    source_key = detect_cookie_source(source_url)
    candidate = materialize_cookie_blob(source_key)
    if candidate and Path(candidate).is_file():
        return candidate
    if source_key != FALLBACK_SOURCE_KEY:
        fallback = materialize_cookie_blob(FALLBACK_SOURCE_KEY)
        if fallback and Path(fallback).is_file():
            return fallback
    return ""


def has_cookies_for_url(source_url: str) -> bool:
    source_key = detect_cookie_source(source_url)
    if _cookie_blob_metadata(source_key):
        return True
    return source_key != FALLBACK_SOURCE_KEY and bool(_cookie_blob_metadata(FALLBACK_SOURCE_KEY))


async def save_ytdlp_cookies_upload(uploaded: UploadFile, source_key: str) -> None:
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
    source_key = normalize_cookie_source(source_key)
    delete_file_blob(_cookie_key(source_key))
    runtime_dir = Path(tempfile.gettempdir()) / "never-stelle"
    for filename in (f"ytdlp-cookies-{source_key}.txt", f"ytdlp-cookies-{source_key}.tmp"):
        try:
            (runtime_dir / filename).unlink(missing_ok=True)
        except Exception:
            pass


# --- Aggregate settings API ---
def get_effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = cfg or load_app_config()
    payload = load_saved_settings_file()
    source_profiles = get_effective_source_profiles(cfg, payload)
    template_settings = normalize_template_settings(payload.get("template_settings"))
    return {
        "source_profiles": source_profiles,
        "site_locations": normalize_source_location_selection(
            payload.get("source_locations") or payload.get("site_locations"),
            cfg,
            source_profiles,
        ),
        "template_settings": template_settings,
        "source_templates": normalize_source_template_selection(
            payload.get("source_templates") or payload.get("source_template_settings"),
            cfg,
            source_profiles,
            template_settings,
        ),
        "ytdlp_cookies": get_ytdlp_cookies_status(source_profiles),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_site_locations: Any,
    raw_template_settings: Any = None,
    raw_source_profiles: Any = None,
    raw_source_templates: Any = None,
) -> dict[str, Any]:
    existing = load_saved_settings_file()
    source_profiles = get_effective_source_profiles(
        cfg,
        {
            **existing,
            "source_profiles": (
                raw_source_profiles
                or existing.get("source_profiles")
                or existing.get("site_profiles")
                or {}
            ),
            "site_locations": raw_site_locations,
            "source_templates": raw_source_templates
            or existing.get("source_templates")
            or existing.get("source_template_settings")
            or {},
        },
    )
    template_settings = normalize_template_settings(raw_template_settings)
    existing.update(
        {
            "source_profiles": source_profiles,
            "site_locations": normalize_source_location_selection(raw_site_locations, cfg, source_profiles),
            "template_settings": template_settings,
            "source_templates": normalize_source_template_selection(
                raw_source_templates,
                cfg,
                source_profiles,
                template_settings,
            ),
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
        "source_profiles": saved.get("source_profiles", get_effective_source_profiles(cfg)),
        "source_default_locations": saved.get("site_locations", {}),
        "site_default_locations": saved.get("site_locations", {}),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "source_templates": saved.get("source_templates", {}),
        "ytdlp_cookies": saved.get("ytdlp_cookies", get_ytdlp_cookies_status(saved.get("source_profiles"))),
        "settings_loaded_at": int(time.time()),
    }
