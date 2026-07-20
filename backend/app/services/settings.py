from __future__ import annotations

import re
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
    favicon_url_for_host,
    host_from_url,
    merge_source_profiles,
    normalize_source_key,
    source_key_from_url,
    source_label_from_key,
    source_profile_for_url,
    source_profile_settings_managed,
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
from backend.app.services import swaratelle

BUILTIN_FOLDER_TEMPLATE = "{{username}}"
BUILTIN_FILENAME_TEMPLATE = "{{username}} - {{title}} [{{id}}]"

# One cookies.txt per resolved source.
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


def _settings_managed_profiles(source_profiles: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [profile for profile in source_profiles if source_profile_settings_managed(profile)]


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
    swaratelle_profiles = [swaratelle.source_profile()] if swaratelle.is_configured() else []
    visible_keys = (
        _profile_keys(config_profiles)
        | _profile_keys(activity_profiles)
        | _profile_keys(extra_profiles)
        | _profile_keys(swaratelle_profiles)
    )
    saved_profiles = _filter_profiles(_configured_source_profiles(payload), visible_keys)
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
    if not key:
        raise ValueError("Paste a valid link or domain first.")
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
    profiles = _settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles(cfg)
    )
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


# --- Scrape rules ---
TOKEN_ROLES = {"creator", "nickname", "title", "slug", "id", "ignore"}
_TOKEN_NAME_RE = re.compile(r"[^a-zA-Z0-9_]+")


def normalize_token_name(value: Any) -> str:
    token = _TOKEN_NAME_RE.sub("_", str(value or "").strip()).strip("_")
    if not token or not re.match(r"[a-zA-Z_]", token):
        return ""
    return token.lower()


def normalize_source_token_roles(raw: Any) -> dict[str, dict[str, str]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, str]] = {}
    for raw_key, raw_roles in source.items():
        key = normalize_source_key(raw_key)
        roles: dict[str, str] = {}
        if isinstance(raw_roles, dict):
            for raw_token, raw_role in raw_roles.items():
                token = normalize_token_name(raw_token)
                role = str(raw_role or "").strip().lower()
                if token and role in TOKEN_ROLES:
                    roles[token] = role
        if key and roles:
            out[key] = roles
    return out


def get_effective_token_roles(payload: dict[str, Any] | None = None) -> dict[str, dict[str, str]]:
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_source_token_roles(payload.get("source_token_roles") or payload.get("token_roles"))


def load_token_roles() -> dict[str, dict[str, str]]:
    return get_effective_token_roles()


def get_effective_scrape_rules(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    # Per-platform, user-defined HTML extraction rules. Normalized on read so a
    # hand-edited or legacy payload can never feed the scraper malformed rules.
    from backend.app.services.tasks.enrich import normalize_scrape_rules

    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_scrape_rules(payload.get("source_scrape_rules") or payload.get("scrape_rules"))


def load_scrape_rules() -> dict[str, Any]:
    return get_effective_scrape_rules()


# --- Creator fields & title cleaning ---
_CREATOR_FIELD_RE = re.compile(r"[^A-Za-z0-9_\[\]]+")


def normalize_creator_field(value: Any) -> str:
    # Keep identifier chars plus gallery-dl [sub] nesting; strip everything else.
    return _CREATOR_FIELD_RE.sub("", str(value or "").strip())


def normalize_source_creator_fields(raw: Any) -> dict[str, dict[str, list[str]]]:
    from backend.app.services.tasks.constants import CREATOR_FIELDS

    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, list[str]]] = {}
    for raw_key, raw_roles in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_roles, dict):
            continue
        roles: dict[str, list[str]] = {}
        for role, values in raw_roles.items():
            if role not in CREATOR_FIELDS or not isinstance(values, list):
                continue
            fields: list[str] = []
            for value in values:
                field = normalize_creator_field(value)
                if field and field not in fields:
                    fields.append(field)
            if fields:
                roles[role] = fields
        if roles:
            out[key] = roles
    return out


def normalize_source_title_cleaning(raw: Any) -> dict[str, dict[str, Any]]:
    from backend.app.services.tasks.constants import normalize_title_cleaning

    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for raw_key, raw_flags in source.items():
        # An empty dict means untouched; skip it so the source keeps defaults.
        if not isinstance(raw_flags, dict) or not raw_flags:
            continue
        key = normalize_source_key(raw_key)
        if key:
            out[key] = normalize_title_cleaning(raw_flags)
    return out


def get_effective_creator_fields(source_url: str = "") -> dict[str, list[str]]:
    payload = load_saved_settings_file()
    mapping = normalize_source_creator_fields(payload.get("source_creator_fields"))
    if not source_url or not mapping:
        return {}
    profile = get_source_profile_for_url(source_url, payload=payload)
    return mapping.get(profile["key"], {})


def get_effective_title_cleaning(source_url: str = "") -> dict[str, Any]:
    payload = load_saved_settings_file()
    mapping = normalize_source_title_cleaning(payload.get("source_title_cleaning"))
    if not source_url or not mapping:
        return {}
    profile = get_source_profile_for_url(source_url, payload=payload)
    return mapping.get(profile["key"], {})


# --- Locations ---
def normalize_source_location_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    source_profiles = _settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles(cfg)
    )
    source_keys = [
        key
        for key in (normalize_source_key(profile.get("key")) for profile in source_profiles)
        if key
    ]
    defaults = get_site_default_locations(cfg, source_keys)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for site in source_keys:
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "")
    return out


# --- Cookies ---
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
    profiles = _settings_managed_profiles(
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


# --- Aggregate settings API ---
def get_effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    # Lazy import breaks the tasks<->settings module cycle.
    from backend.app.services.tasks.constants import normalize_quality_selection

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
        "default_quality": normalize_quality_selection(payload.get("default_quality")),
        "ytdlp_cookies": get_ytdlp_cookies_status(source_profiles),
        "source_scrape_rules": get_effective_scrape_rules(payload),
        "source_token_roles": get_effective_token_roles(payload),
        "source_creator_fields": normalize_source_creator_fields(payload.get("source_creator_fields")),
        "source_title_cleaning": normalize_source_title_cleaning(payload.get("source_title_cleaning")),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_site_locations: Any,
    raw_template_settings: Any = None,
    raw_source_profiles: Any = None,
    raw_source_templates: Any = None,
    raw_default_quality: Any = None,
    raw_scrape_rules: Any = None,
    raw_token_roles: Any = None,
    raw_creator_fields: Any = None,
    raw_title_cleaning: Any = None,
) -> dict[str, Any]:
    from backend.app.services.tasks.constants import normalize_quality_selection
    from backend.app.services.tasks.enrich import normalize_scrape_rules

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
    managed_profiles = _settings_managed_profiles(source_profiles)
    template_settings = normalize_template_settings(raw_template_settings)
    existing.update(
        {
            "source_profiles": managed_profiles,
            "site_locations": normalize_source_location_selection(raw_site_locations, cfg, managed_profiles),
            "template_settings": template_settings,
            "source_templates": normalize_source_template_selection(
                raw_source_templates,
                cfg,
                managed_profiles,
                template_settings,
            ),
            "default_quality": normalize_quality_selection(
                raw_default_quality if raw_default_quality is not None else existing.get("default_quality")
            ),
            "source_scrape_rules": normalize_scrape_rules(
                raw_scrape_rules
                if raw_scrape_rules is not None
                else existing.get("source_scrape_rules") or existing.get("scrape_rules")
            ),
            "source_token_roles": normalize_source_token_roles(
                raw_token_roles
                if raw_token_roles is not None
                else existing.get("source_token_roles") or existing.get("token_roles")
            ),
            "source_creator_fields": normalize_source_creator_fields(
                raw_creator_fields if raw_creator_fields is not None else existing.get("source_creator_fields")
            ),
            "source_title_cleaning": normalize_source_title_cleaning(
                raw_title_cleaning if raw_title_cleaning is not None else existing.get("source_title_cleaning")
            ),
        }
    )
    save_saved_settings_file(existing)
    return get_effective_saved_settings(cfg)


def build_settings_response(
    cfg: dict[str, Any] | None = None,
    saved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from backend.app.services.auth import auth_public_payload
    from backend.app.services.tasks.constants import (
        creator_field_defaults,
        default_quality_selection,
        quality_options,
        template_tokens,
        title_cleaning_rules,
    )

    cfg = cfg or load_app_config()
    saved = saved or get_effective_saved_settings(cfg)
    return {
        "auth": auth_public_payload(),
        "download_locations": normalize_download_locations(cfg),
        "source_profiles": saved.get("source_profiles", get_effective_source_profiles(cfg)),
        "source_default_locations": saved.get("site_locations", {}),
        "site_default_locations": saved.get("site_locations", {}),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "source_templates": saved.get("source_templates", {}),
        "default_quality": saved.get("default_quality", default_quality_selection()),
        "quality_options": quality_options(),
        "template_tokens": template_tokens(),
        "ytdlp_cookies": saved.get("ytdlp_cookies", get_ytdlp_cookies_status(saved.get("source_profiles"))),
        "source_scrape_rules": saved.get("source_scrape_rules", get_effective_scrape_rules()),
        "source_token_roles": saved.get("source_token_roles", get_effective_token_roles()),
        "source_creator_fields": saved.get("source_creator_fields", {}),
        "source_title_cleaning": saved.get("source_title_cleaning", {}),
        "creator_field_defaults": creator_field_defaults(),
        "title_cleaning_rules": title_cleaning_rules(),
        "settings_loaded_at": int(time.time()),
    }
