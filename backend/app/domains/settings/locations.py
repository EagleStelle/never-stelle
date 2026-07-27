from __future__ import annotations

import os
from typing import Any

from backend.app.core.config import get_default_site_location, get_site_default_locations, normalize_allowed_location
from backend.app.core.paths import real_path_key as _path_key
from backend.app.core.sources import normalize_source_key

from .profiles import get_effective_source_profiles, settings_managed_profiles


def _learned_templates(source_key: str) -> list[str]:
    # Lazy import so a settings read never hard-depends on the downloads domain.
    from backend.app.domains.downloads.formats import learned_templates_for
    from backend.app.domains.downloads.store import load_learned_formats

    return learned_templates_for(load_learned_formats(), source_key)


def _is_same_or_child_path(candidate: str, parent: str) -> bool:
    candidate_key = _path_key(candidate)
    parent_key = _path_key(parent)
    if not candidate_key or not parent_key:
        return False
    try:
        return os.path.commonpath([candidate_key, parent_key]) == parent_key
    except ValueError:
        return False


def _normalize_source_allowed_location(raw_path: str, source_default: str) -> str:
    candidate = normalize_allowed_location(str(raw_path or "").strip())
    if not candidate:
        return ""
    return candidate if _is_same_or_child_path(candidate, source_default) else ""


def normalize_source_location_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, str]]:
    """Per-source, per-format download locations.

    Keyed by learned URL template like ``source_templates``; a format with nothing saved
    falls back to the source default (``<media>/<source_key>``).
    """
    from backend.app.domains.downloads.formats import select_for_format

    source_profiles = settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles(cfg)
    )
    source_keys = [
        key
        for key in (normalize_source_key(profile.get("key")) for profile in source_profiles)
        if key
    ]
    defaults = get_site_default_locations(cfg, source_keys)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, str]] = {}
    for site in source_keys:
        raw_value = source.get(site)
        saved = raw_value if isinstance(raw_value, dict) else {}
        default = defaults.get(site, "")
        out[site] = {}
        for template in _learned_templates(site):
            saved_path = select_for_format(saved, template) if saved else ""
            candidate = _normalize_source_allowed_location(str(saved_path or "").strip(), default)
            out[site][template] = candidate or default
    return out


def resolve_source_location(
    locations: Any,
    cfg: dict[str, Any],
    source_key: str,
    format_template: str = "",
) -> str:
    """The download folder for one source/format pair, else the source default."""
    from backend.app.domains.downloads.formats import select_for_format

    key = normalize_source_key(source_key)
    if not key:
        return ""
    per_source = (locations or {}).get(key) if isinstance(locations, dict) else None
    default = get_default_site_location(cfg, key)
    selected = _normalize_source_allowed_location(
        str(select_for_format(per_source, format_template) or "").strip(),
        default,
    )
    return selected or default
