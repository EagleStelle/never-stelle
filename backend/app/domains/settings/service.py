from __future__ import annotations

import time
from typing import Any

from backend.app.core.config import load_app_config, normalize_download_locations

from .cookies import get_ytdlp_cookies_status
from .creator_fields import (
    get_source_creator_field_defaults,
    normalize_source_creator_fields,
    normalize_source_title_cleaning,
    saved_creator_fields,
)
from .formats import get_learned_formats_for_ui
from .locations import normalize_source_location_selection
from .profiles import (
    get_effective_source_profiles,
    settings_managed_profiles,
)
from .scraping import get_effective_scrape_rules, normalize_source_scrape_rules
from .slug_tokens import get_effective_slug_tokens, normalize_source_slug_tokens
from .storage import load_saved_settings_file, save_saved_settings_file
from .templates import (
    normalize_source_template_selection,
    normalize_template_settings,
)
from .tokens import get_effective_token_roles, normalize_source_token_roles


def get_effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    from backend.app.domains.downloads.constants import normalize_quality_selection

    cfg = cfg or load_app_config()
    payload = load_saved_settings_file()
    source_profiles = get_effective_source_profiles(cfg, payload)
    token_roles = get_effective_token_roles(payload)
    template_settings = normalize_template_settings(payload.get("template_settings"))
    return {
        "source_profiles": source_profiles,
        "site_locations": normalize_source_location_selection(
            payload.get("site_locations"),
            cfg,
            source_profiles,
        ),
        "template_settings": template_settings,
        "source_templates": normalize_source_template_selection(
            payload.get("source_templates"),
            cfg,
            source_profiles,
            template_settings,
            token_roles,
        ),
        "default_quality": normalize_quality_selection(payload.get("default_quality")),
        "ytdlp_cookies": get_ytdlp_cookies_status(source_profiles),
        "source_scrape_rules": get_effective_scrape_rules(payload),
        "source_token_roles": token_roles,
        "source_slug_tokens": get_effective_slug_tokens(payload),
        "source_creator_fields": saved_creator_fields(payload),
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
    raw_slug_tokens: Any = None,
) -> dict[str, Any]:
    from backend.app.domains.downloads.constants import normalize_quality_selection

    existing = load_saved_settings_file()
    source_profiles = get_effective_source_profiles(
        cfg,
        {
            **existing,
            "source_profiles": (
                raw_source_profiles
                or existing.get("source_profiles")
                or {}
            ),
            "site_locations": raw_site_locations,
            "source_templates": raw_source_templates
            or existing.get("source_templates")
            or {},
        },
    )
    managed_profiles = settings_managed_profiles(source_profiles)
    normalized_scrape_rules = normalize_source_scrape_rules(
        raw_scrape_rules
        if raw_scrape_rules is not None
        else existing.get("source_scrape_rules")
    )
    normalized_token_roles = normalize_source_token_roles(
        raw_token_roles
        if raw_token_roles is not None
        else existing.get("source_token_roles")
    )
    normalized_slug_tokens = normalize_source_slug_tokens(
        raw_slug_tokens if raw_slug_tokens is not None else existing.get("source_slug_tokens")
    )
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
                normalized_token_roles,
            ),
            "default_quality": normalize_quality_selection(
                raw_default_quality if raw_default_quality is not None else existing.get("default_quality")
            ),
            "source_scrape_rules": normalized_scrape_rules,
            "source_token_roles": normalized_token_roles,
            "source_slug_tokens": normalized_slug_tokens,
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
    from backend.app.domains.auth import auth_public_payload
    from backend.app.domains.downloads.constants import (
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
        "source_slug_tokens": saved.get("source_slug_tokens", get_effective_slug_tokens()),
        "learned_formats": get_learned_formats_for_ui(),
        "source_creator_fields": saved.get("source_creator_fields", {}),
        "source_creator_field_defaults": get_source_creator_field_defaults(saved.get("source_profiles")),
        "source_title_cleaning": saved.get("source_title_cleaning", {}),
        "creator_field_defaults": creator_field_defaults(),
        "title_cleaning_rules": title_cleaning_rules(),
        "settings_loaded_at": int(time.time()),
    }
