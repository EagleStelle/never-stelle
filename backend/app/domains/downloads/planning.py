from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.app.core.config import load_app_config
from backend.app.core.sources import (
    merge_source_profiles,
    normalize_source_key,
    source_profile_for_url,
)
from backend.app.domains.settings import (
    get_effective_saved_settings,
    normalize_source_location_selection,
    normalize_source_template_selection,
    normalize_template_settings,
    resolve_source_location,
)


@dataclass(frozen=True)
class ResolvedTaskSettings:
    source_key: str
    source_profile: dict[str, Any]
    source_profiles: list[dict[str, Any]]
    source_locations: dict[str, dict[str, str]]
    output_dir: str
    template_settings: dict[str, str]


def resolve_task_settings(
    source_url: str,
    *,
    source_locations: dict[str, dict[str, str]] | None = None,
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

    raw_locations = source_locations if source_locations is not None else effective.get("source_locations")
    selected_locations = normalize_source_location_selection(raw_locations, cfg, profiles)

    base_template = normalize_template_settings(
        template_settings if template_settings is not None else effective.get("template_settings")
    )
    selected_templates = normalize_source_template_selection(
        source_templates if source_templates is not None else effective.get("source_templates"),
        cfg,
        profiles,
        effective.get("source_token_roles"),
    )

    from backend.app.domains.downloads.formats import match_template, select_for_format
    from backend.app.domains.downloads.store import load_learned_formats

    # One format match drives both the folder and the naming templates for this link.
    matched = match_template(load_learned_formats(), source_key, source_url)
    output_dir = resolve_source_location(selected_locations, source_key, matched)

    matched_template = select_for_format(selected_templates.get(source_key), matched)
    selected_template = normalize_template_settings(
        matched_template if matched_template is not None else base_template
    )

    return ResolvedTaskSettings(
        source_key=source_key,
        source_profile=source_profile,
        source_profiles=profiles,
        source_locations=selected_locations,
        output_dir=output_dir,
        template_settings=selected_template,
    )
