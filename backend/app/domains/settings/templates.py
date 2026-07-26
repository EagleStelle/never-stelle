from __future__ import annotations

import re
from typing import Any

from backend.app.core.config import load_app_config
from backend.app.core.sources import normalize_source_key

from .profiles import get_effective_source_profiles, get_source_profile_for_url, settings_managed_profiles
from .storage import load_saved_settings_file
from .tokens import get_effective_token_roles, normalize_token_name

BUILTIN_FOLDER_TEMPLATE = "{{username}}"
BUILTIN_FILENAME_TEMPLATE = "{{username}} - {{title}} [{{id}}]"


def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder_template = str(source.get("folder_template") or "").strip() or BUILTIN_FOLDER_TEMPLATE
    filename_template = str(source.get("filename_template") or "").strip() or BUILTIN_FILENAME_TEMPLATE
    return {"folder_template": folder_template, "filename_template": filename_template}


def _template_role_replacements(token_roles: dict[str, str] | None) -> dict[str, str]:
    roles = token_roles if isinstance(token_roles, dict) else {}
    return {
        token: role
        for token, role in roles.items()
        if token and role in {"title"} and token != role
    }


def _migrate_template_tokens(template: str, token_roles: dict[str, str] | None = None) -> str:
    replacements = _template_role_replacements(token_roles)
    if not replacements:
        return str(template or "").strip()

    def replace(match: re.Match[str]) -> str:
        token = normalize_token_name(match.group(1))
        return f"{{{{{replacements[token]}}}}}" if token in replacements else match.group(0)

    return re.sub(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}", replace, str(template or "").strip())


def migrate_template_settings_tokens(
    template_settings: dict[str, str] | None,
    token_roles: dict[str, str] | None = None,
) -> dict[str, str]:
    settings = normalize_template_settings(template_settings or {})
    return {
        "folder_template": _migrate_template_tokens(settings["folder_template"], token_roles),
        "filename_template": _migrate_template_tokens(settings["filename_template"], token_roles),
    }


def normalize_source_template_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
    default_template: dict[str, str] | None = None,
    token_roles: dict[str, dict[str, str]] | None = None,
) -> dict[str, dict[str, dict[str, str]]]:
    source = raw if isinstance(raw, dict) else {}
    profiles = settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles(cfg)
    )

    out: dict[str, dict[str, dict[str, str]]] = {}
    for profile in profiles:
        key = normalize_source_key(profile.get("key"))
        raw_val = source.get(key) or source.get(str(profile.get("label") or ""))

        out[key] = {}
        if isinstance(raw_val, dict):
            for format_template, val in raw_val.items():
                if isinstance(val, dict):
                    normalized = normalize_template_settings(val)
                    out[key][format_template] = migrate_template_settings_tokens(
                        normalized, (token_roles or {}).get(key)
                    )
    return out


def get_effective_template_settings(source_url: str = "") -> dict[str, str]:
    cfg = load_app_config()
    payload = load_saved_settings_file()
    token_roles = get_effective_token_roles(payload)
    base = normalize_template_settings(payload.get("template_settings"))
    if not source_url:
        return base
    profile = get_source_profile_for_url(source_url, cfg, payload)
    source_templates = normalize_source_template_selection(
        payload.get("source_templates"),
        cfg,
        get_effective_source_profiles(cfg, payload, [profile["key"]]),
        base,
        token_roles,
    )

    platform_templates = source_templates.get(profile["key"]) or {}

    from backend.app.domains.downloads.formats import _canonical_shape, match_template
    from backend.app.domains.downloads.store import load_learned_formats

    learned = load_learned_formats()
    matched = match_template(learned, profile["key"], source_url)
    canonical_matched = _canonical_shape(matched)

    matched_template = None
    for fmt, settings_dict in platform_templates.items():
        if _canonical_shape(fmt) == canonical_matched:
            matched_template = settings_dict
            break

    if matched_template is None:
        matched_template = base

    return normalize_template_settings(matched_template)
