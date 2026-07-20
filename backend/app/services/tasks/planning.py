from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.app.core.config import load_app_config
from backend.app.core.sources import (
    merge_source_profiles,
    normalize_source_key,
    source_profile_for_url,
)
from backend.app.services.settings import (
    get_effective_saved_settings,
    normalize_source_location_selection,
    normalize_source_template_selection,
    normalize_template_settings,
)


@dataclass(frozen=True)
class ResolvedTaskSettings:
    source_key: str
    source_profile: dict[str, Any]
    source_profiles: list[dict[str, Any]]
    site_locations: dict[str, str]
    output_dir: str
    template_settings: dict[str, str]


def resolve_task_settings(
    source_url: str,
    *,
    site_locations: Any = None,
    template_settings: Any = None,
    source_profiles: Any = None,
    source_templates: Any = None,
    cfg: dict[str, Any] | None = None,
) -> ResolvedTaskSettings:
    cfg = cfg or load_app_config()
    effective = get_effective_saved_settings(cfg)

    profiles = merge_source_profiles(
        effective.get("source_profiles") or [],
        source_profiles or [],
    )
    source_profile = source_profile_for_url(source_url, profiles)
    source_key = normalize_source_key(source_profile.get("key"))
    if not any(normalize_source_key(profile.get("key")) == source_key for profile in profiles):
        profiles.append(source_profile)

    selected_locations = normalize_source_location_selection(
        site_locations if site_locations is not None else effective.get("site_locations"),
        cfg,
        profiles,
    )
    output_dir = selected_locations.get(source_key) or ""

    base_template = normalize_template_settings(
        template_settings if template_settings is not None else effective.get("template_settings")
    )
    selected_templates = normalize_source_template_selection(
        source_templates if source_templates is not None else effective.get("source_templates"),
        cfg,
        profiles,
        base_template,
        effective.get("source_token_roles"),
    )
    selected_template = normalize_template_settings(selected_templates.get(source_key) or base_template)

    return ResolvedTaskSettings(
        source_key=source_key,
        source_profile=source_profile,
        source_profiles=profiles,
        site_locations=selected_locations,
        output_dir=output_dir,
        template_settings=selected_template,
    )
