from __future__ import annotations

import re
from typing import Any

from backend.app.core.resolution import resolved
from backend.app.core.sources import normalize_source_key

from . import storage
from .profiles import get_source_profile_for_url
from .slug_tokens import get_effective_slug_tokens
from .tokens import get_effective_token_roles, normalize_token_name

_FIELD_RE = re.compile(r"[^A-Za-z0-9_\[\]]+")
_SCRAPER_FIELD_RE = re.compile(r"^scraper\[([A-Za-z_][A-Za-z0-9_]*)\]$")
FIELD_ROLES = ("username", "nickname", "title")


def load_saved_settings_file() -> dict[str, Any]:
    return storage.load_saved_settings_file()


def scraper_field(token: Any) -> str:
    token_name = normalize_token_name(token)
    return f"scraper[{token_name}]" if token_name else ""


def scraper_token_from_field(value: Any) -> str:
    match = _SCRAPER_FIELD_RE.fullmatch(str(value or "").strip())
    return match.group(1) if match else ""


def is_scraper_field(value: Any) -> bool:
    return bool(scraper_token_from_field(value))


def normalize_field(value: Any) -> str:
    # Keep identifier chars plus gallery-dl [sub] nesting; strip everything else.
    raw = str(value or "").strip()
    if raw.lower().startswith("scraper[") and raw.endswith("]"):
        return scraper_field(raw[8:-1])
    return _FIELD_RE.sub("", str(value or "").strip())


def normalize_source_fields(raw: Any, defaults: Any = None) -> dict[str, dict[str, list[str]]]:
    source = raw if isinstance(raw, dict) else {}
    default_roles = normalize_default_fields(defaults) if defaults is not None else {}
    out: dict[str, dict[str, list[str]]] = {}
    for raw_key, raw_roles in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_roles, dict):
            continue
        roles: dict[str, list[str]] = {}
        for role, values in raw_roles.items():
            if role not in FIELD_ROLES or not isinstance(values, list):
                continue
            fields: list[str] = []
            for value in values:
                field = normalize_field(value)
                if field and field not in fields:
                    fields.append(field)
            if fields and fields != default_roles.get(role, []):
                roles[role] = fields
        if roles:
            out[key] = roles
    return out


def saved_fields(
    payload: dict[str, Any],
    defaults: Any = None,
) -> dict[str, dict[str, list[str]]]:
    return normalize_source_fields((payload if isinstance(payload, dict) else {}).get("source_fields"), defaults)


def get_source_field_defaults(
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[str]]]:
    return {}


def normalize_default_fields(raw: Any) -> dict[str, list[str]]:
    """The global field order per role. Scraper fields are per-source, so they are dropped."""
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, list[str]] = {}
    for role in FIELD_ROLES:
        fields: list[str] = []
        for value in source.get(role) if isinstance(source.get(role), list) else []:
            field = normalize_field(value)
            if field and not is_scraper_field(field) and field not in fields:
                fields.append(field)
        out[role] = fields
    return out


def get_effective_field_defaults(payload: dict[str, Any] | None = None) -> dict[str, list[str]]:
    """Configured global order per role, falling back to the built-in role priors."""
    from backend.app.domains.downloads.constants import field_defaults

    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    configured = normalize_default_fields(payload.get("default_fields"))
    builtin = field_defaults()
    return {role: configured.get(role) or builtin.get(role, []) for role in FIELD_ROLES}


def get_effective_source_fields_map(
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[str]]]:
    payload = load_saved_settings_file()
    return saved_fields(payload, normalize_default_fields(payload.get("default_fields")))


def normalize_default_naming(raw: Any) -> dict[str, Any]:
    """The global naming defaults, kept sparse: only what differs from the built-ins."""
    from backend.app.domains.downloads.constants import normalize_naming_overrides

    return normalize_naming_overrides(raw)


def get_effective_naming_defaults(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Every naming flag at the value an unconfigured source inherits."""
    from backend.app.domains.downloads.constants import normalize_title_cleaning

    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_title_cleaning(normalize_default_naming(payload.get("default_naming")))


def normalize_source_title_cleaning(raw: Any, defaults: Any = None) -> dict[str, dict[str, Any]]:
    from backend.app.domains.downloads.constants import normalize_naming_overrides

    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for raw_key, raw_flags in source.items():
        # An empty dict means untouched; skip it so the source keeps defaults.
        if not isinstance(raw_flags, dict) or not raw_flags:
            continue
        key = normalize_source_key(raw_key)
        # Flags matching the global default are dropped, so the source keeps following it.
        overrides = normalize_naming_overrides(raw_flags, defaults)
        if key and overrides:
            out[key] = overrides
    return out


def _is_token_role_matching_field_role(token_role: str, role: str) -> bool:
    if token_role == role:
        return True
    if token_role == "creator" and role in ("username", "nickname"):
        return True
    return False


def _scrape_rule_tokens(source_key: str, payload: dict[str, Any]) -> list[dict[str, str]]:
    rules_map = payload.get("source_scrape_rules")
    platform = (rules_map if isinstance(rules_map, dict) else {}).get(normalize_source_key(source_key))
    if not isinstance(platform, dict):
        return []
    out: list[dict[str, str]] = []
    for item in platform.get("rules") or []:
        if isinstance(item, dict):
            out.append({"token": normalize_token_name(item.get("token"))})
    return out


def _assigned_scraper_fields(source_key: str, role: str, payload: dict[str, Any]) -> list[str]:
    # Scraper and URL-part tokens lead creator fields (if creator/username/nickname role)
    # or title fields (if title role) without being persisted into the lists.
    key = normalize_source_key(source_key)
    roles = get_effective_token_roles(payload).get(key) or {}
    if not roles:
        return []
    tokens = [
        *_scrape_rule_tokens(key, payload),
        *(get_effective_slug_tokens(payload).get(key) or []),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for item in tokens:
        token = normalize_token_name((item or {}).get("token"))
        if not token or token in seen:
            continue
        seen.add(token)
        token_role = str(roles.get(token) or "ignore")
        if not _is_token_role_matching_field_role(token_role, role):
            continue
        field = scraper_field(token)
        if field:
            out.append(field)
    return out


def _with_assigned_scraper_fields(
    source_key: str, base: dict[str, list[str]], payload: dict[str, Any]
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for role in FIELD_ROLES:
        assigned = _assigned_scraper_fields(source_key, role, payload)
        assigned_set = set(assigned)
        existing = list(base.get(role) or [])
        merged: list[str] = [field for field in assigned if field not in existing]
        for field in existing:
            if is_scraper_field(field) and field not in assigned_set:
                continue
            if field not in merged:
                merged.append(field)
        if merged:
            out[role] = merged
    return out


def get_effective_fields(source_url: str = "") -> dict[str, list[str]]:
    # Resolved per output item during a download and per file during a scan; the
    # answer only depends on the URL and the saved settings, so cache it per scope.
    return resolved(f"settings.fields:{source_url}", lambda: _effective_fields(source_url))


def _effective_fields(source_url: str = "") -> dict[str, list[str]]:
    payload = load_saved_settings_file()
    if not source_url:
        return {}
    profile = get_source_profile_for_url(source_url, payload=payload)
    key = normalize_source_key(profile.get("key"))
    configured_defaults = normalize_default_fields(payload.get("default_fields"))
    mapping = saved_fields(payload, configured_defaults)
    saved = mapping.get(key) or {}
    base: dict[str, list[str]] = {}
    for role in FIELD_ROLES:
        values = saved.get(role) or configured_defaults.get(role) or []
        if values:
            base[role] = values
    return _with_assigned_scraper_fields(key, base, payload)


def get_effective_title_cleaning(source_url: str = "") -> dict[str, Any]:
    return resolved(f"settings.cleaning:{source_url}", lambda: _effective_title_cleaning(source_url))


def _effective_title_cleaning(source_url: str = "") -> dict[str, Any]:
    from backend.app.domains.downloads.constants import normalize_title_cleaning

    payload = load_saved_settings_file()
    defaults = get_effective_naming_defaults(payload)
    mapping = normalize_source_title_cleaning(payload.get("source_title_cleaning"), defaults)
    if not source_url or not mapping:
        return defaults
    profile = get_source_profile_for_url(source_url, payload=payload)
    # A full set, so downstream re-normalization can't fall back past the configured defaults.
    return normalize_title_cleaning(mapping.get(profile["key"], {}), defaults)
