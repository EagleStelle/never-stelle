from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

from backend.app.core.config import APP_CONFIG_KEY, get_config_source_profiles, load_app_config
from backend.app.core.resolution import is_scoped, resolved
from backend.app.core.sources import (
    favicon_url_for_host,
    host_from_url,
    merge_source_profiles,
    normalize_source_key,
    source_key_from_url,
    source_label_from_key,
    source_profile_for_url,
    source_profile_settings_managed,
)
from backend.app.db.repositories import load_history_payload, load_task_store_payload, source_activity_revision
from backend.app.integrations.swaratelle import client as swaratelle

from .storage import SAVED_SETTINGS_KEY, load_saved_settings_file, save_saved_settings_file

_activity_cache_lock = threading.RLock()
_activity_cache: tuple[tuple[Any, ...], list[dict[str, Any]]] | None = None


def _profiles_from_keys(keys: Iterable[str]) -> list[dict[str, str]]:
    return [{"key": normalize_source_key(key)} for key in keys if normalize_source_key(key)]


def _profile_keys(source: Iterable[dict[str, Any]]) -> set[str]:
    return {normalize_source_key(item.get("key")) for item in source if normalize_source_key(item.get("key"))}


def _filter_profiles(source: Any, allowed_keys: set[str]) -> list[dict[str, Any]]:
    # A saved hosted profile is an intentional user-created source and should
    # survive reload even if it has no config or activity yet.
    return [
        profile
        for profile in merge_source_profiles(source)
        if normalize_source_key(profile.get("key")) in allowed_keys or profile.get("hosts")
    ]


def settings_managed_profiles(source_profiles: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [profile for profile in source_profiles if source_profile_settings_managed(profile)]


def configured_source_profiles(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return {}
    return raw.get("source_profiles") or {}


def _config_fingerprint(config_profiles: list[dict[str, Any]]) -> tuple[Any, ...]:
    return tuple((profile.get("key"), tuple(profile.get("hosts") or [])) for profile in config_profiles)


def _activity_source_profiles(config_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Profiles inferred from downloaded URLs. Cached against a source-only DB
    # revision: a running download rewrites its row twice a second, and keying on
    # a generic "anything changed" fingerprint made every poll re-decode the whole
    # task and history tables. Which sources exist only changes when rows appear,
    # disappear, or get re-keyed.
    global _activity_cache
    revision = resolved("settings.activity_revision", source_activity_revision)
    signature = (revision, _config_fingerprint(config_profiles))
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
    # Resolved per output item during a download and per file during a scan. Callers
    # pass the config and settings payload they already hold, so the memo applies
    # whenever those are the objects this operation resolved. `extra_keys` widens the
    # result, so it always rebuilds.
    if not extra_keys and is_scoped(APP_CONFIG_KEY, cfg) and is_scoped(SAVED_SETTINGS_KEY, payload):
        return resolved("settings.source_profiles", _effective_source_profiles)
    return _effective_source_profiles(cfg, payload, extra_keys)


def _effective_source_profiles(
    cfg: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    extra_keys: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    cfg = cfg or load_app_config()
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    config_profiles = get_config_source_profiles(cfg)
    activity_profiles = _activity_source_profiles(config_profiles)
    extra_profiles = _profiles_from_keys(normalize_source_key(key) for key in (extra_keys or []))
    swaratelle_profiles = [swaratelle.source_profile()] if swaratelle.is_configured() else []
    visible_keys = (
        _profile_keys(config_profiles)
        | _profile_keys(activity_profiles)
        | _profile_keys(extra_profiles)
        | _profile_keys(swaratelle_profiles)
    )
    saved_profiles = _filter_profiles(configured_source_profiles(payload), visible_keys)
    return merge_source_profiles(
        config_profiles,
        activity_profiles,
        swaratelle_profiles,
        saved_profiles,
        extra_profiles,
    )


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


def require_settings_managed_source(source_key: Any) -> None:
    key = normalize_source_key(source_key)
    if not key:
        raise ValueError("Choose or type a source.")
    profile = get_source_profile_by_key(key)
    if source_profile_settings_managed(profile):
        return
    label = str(profile.get("label") or source_key or "Source").strip()
    backend = str(profile.get("external_backend") or "the external service").strip()
    raise ValueError(f"{label} settings are managed in {backend}.")


def ensure_source_profile_for_url(url_or_host: str) -> str:
    """Derive a source key from a URL/host and persist a matching profile."""
    cfg = load_app_config()
    payload = load_saved_settings_file()
    profiles = get_effective_source_profiles(cfg, payload)
    key = source_key_from_url(url_or_host, profiles)
    host = host_from_url(url_or_host)
    if not key:
        raise ValueError("Paste a valid link or domain first.")
    if any(normalize_source_key(profile.get("key")) == key for profile in profiles):
        return key

    saved_profiles = merge_source_profiles(configured_source_profiles(payload))
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
