from __future__ import annotations

from typing import Any

from backend.app.core.config import get_site_default_locations, normalize_allowed_location
from backend.app.core.sources import normalize_source_key

from .profiles import get_effective_source_profiles, settings_managed_profiles


def normalize_source_location_selection(
    raw: Any,
    cfg: dict[str, Any],
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
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
    out: dict[str, str] = {}
    for site in source_keys:
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "")
    return out
