from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import normalize_source_key

from .profiles import get_source_profile_for_url
from .scraping import get_effective_scrape_rules
from .slug_tokens import get_effective_slug_tokens
from .storage import load_saved_settings_file
from .tokens import get_effective_token_roles, normalize_token_name

_CREATOR_FIELD_RE = re.compile(r"[^A-Za-z0-9_\[\]]+")
_SCRAPER_CREATOR_FIELD_RE = re.compile(r"^scraper\[([A-Za-z_][A-Za-z0-9_]*)\]$")


def scraper_creator_field(token: Any) -> str:
    token_name = normalize_token_name(token)
    return f"scraper[{token_name}]" if token_name else ""


def scraper_token_from_creator_field(value: Any) -> str:
    match = _SCRAPER_CREATOR_FIELD_RE.fullmatch(str(value or "").strip())
    return match.group(1) if match else ""


def is_scraper_creator_field(value: Any) -> bool:
    return bool(scraper_token_from_creator_field(value))


def normalize_creator_field(value: Any) -> str:
    # Keep identifier chars plus gallery-dl [sub] nesting; strip everything else.
    raw = str(value or "").strip()
    if raw.lower().startswith("scraper[") and raw.endswith("]"):
        return scraper_creator_field(raw[8:-1])
    return _CREATOR_FIELD_RE.sub("", str(value or "").strip())


def normalize_source_creator_fields(raw: Any) -> dict[str, dict[str, list[str]]]:
    from backend.app.domains.downloads.constants import CREATOR_FIELDS

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


def _has_creator_field_roles(value: Any) -> bool:
    roles = value if isinstance(value, dict) else {}
    return any(bool(roles.get(role)) for role in ("username", "nickname"))


def saved_creator_fields(
    payload: dict[str, Any],
) -> dict[str, dict[str, list[str]]]:
    return normalize_source_creator_fields((payload if isinstance(payload, dict) else {}).get("source_creator_fields"))


def get_source_creator_field_defaults(
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[str]]]:
    return {}


def get_effective_source_creator_fields_map(
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[str]]]:
    return saved_creator_fields(load_saved_settings_file())


def normalize_source_title_cleaning(raw: Any) -> dict[str, dict[str, Any]]:
    from backend.app.domains.downloads.constants import normalize_title_cleaning

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


def _assigned_scraper_creator_fields(source_key: str, payload: dict[str, Any]) -> list[str]:
    # Scraper and URL-part tokens assigned the Creator role lead both username and
    # nickname field lists without being persisted into either list.
    key = normalize_source_key(source_key)
    roles = get_effective_token_roles(payload).get(key) or {}
    if not roles:
        return []
    tokens = [
        *((get_effective_scrape_rules(payload).get(key) or {}).get("rules") or []),
        *(get_effective_slug_tokens(payload).get(key) or []),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for item in tokens:
        token = normalize_token_name((item or {}).get("token"))
        if not token or token in seen:
            continue
        seen.add(token)
        if roles.get(token) != "creator":
            continue
        field = scraper_creator_field(token)
        if field:
            out.append(field)
    return out


def _with_assigned_scraper_creator_fields(
    source_key: str, base: dict[str, list[str]], payload: dict[str, Any]
) -> dict[str, list[str]]:
    assigned = _assigned_scraper_creator_fields(source_key, payload)
    if not assigned and not any(is_scraper_creator_field(field) for fields in base.values() for field in fields):
        return base
    assigned_set = set(assigned)
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname"):
        existing = list(base.get(role) or [])
        merged: list[str] = [field for field in assigned if field not in existing]
        for field in existing:
            if is_scraper_creator_field(field) and field not in assigned_set:
                continue
            if field not in merged:
                merged.append(field)
        if merged:
            out[role] = merged
    return out


def get_effective_creator_fields(source_url: str = "") -> dict[str, list[str]]:
    payload = load_saved_settings_file()
    if not source_url:
        return {}
    profile = get_source_profile_for_url(source_url, payload=payload)
    key = normalize_source_key(profile.get("key"))
    mapping = saved_creator_fields(payload)
    saved = mapping.get(key) or {}
    base = saved if _has_creator_field_roles(saved) else {}
    return _with_assigned_scraper_creator_fields(key, base, payload)


def get_effective_title_cleaning(source_url: str = "") -> dict[str, Any]:
    payload = load_saved_settings_file()
    mapping = normalize_source_title_cleaning(payload.get("source_title_cleaning"))
    if not source_url or not mapping:
        return {}
    profile = get_source_profile_for_url(source_url, payload=payload)
    return mapping.get(profile["key"], {})
