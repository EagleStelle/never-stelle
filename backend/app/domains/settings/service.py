from __future__ import annotations

import time
from typing import Any

from backend.app.core.config import (
    APP_CONFIG_KEY,
    MEDIA_DIR,
    load_app_config,
    source_location_options,
)
from backend.app.core.resolution import is_scoped, resolved

from .cookie_policy import (
    builtin_cookie_policy_defaults,
    get_effective_cookie_policies,
    normalize_default_cookie_policy,
    normalize_source_cookie_policies,
)
from .cookies import get_ytdlp_cookies_status
from .fields import (
    get_effective_naming_defaults,
    normalize_default_fields,
    normalize_default_naming,
    normalize_source_fields,
    normalize_source_title_cleaning,
    saved_fields,
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
    # Rebuilding this normalizes every settings section at once, and almost every
    # caller hands back the config it just loaded rather than omitting it, so the
    # memo has to recognize that config as the scoped one. Keying only on the
    # no-argument form left queueing a batch of URLs rebuilding this twice per URL.
    if is_scoped(APP_CONFIG_KEY, cfg):
        return resolved("settings.effective", _effective_saved_settings)
    return _effective_saved_settings(cfg)


def _effective_saved_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    from backend.app.domains.downloads.constants import normalize_post_processing, normalize_quality_selection

    cfg = cfg or load_app_config()
    payload = load_saved_settings_file()
    source_profiles = get_effective_source_profiles(cfg, payload)
    token_roles = get_effective_token_roles(payload)
    template_settings = normalize_template_settings(payload.get("template_settings"))
    naming_defaults = get_effective_naming_defaults(payload)
    default_fields = normalize_default_fields(payload.get("default_fields"))
    return {
        "source_profiles": source_profiles,
        "source_locations": normalize_source_location_selection(
            payload.get("source_locations"),
            cfg,
            source_profiles,
        ),
        "template_settings": template_settings,
        "source_templates": normalize_source_template_selection(
            payload.get("source_templates"),
            cfg,
            source_profiles,
            token_roles,
        ),
        "default_quality": normalize_quality_selection(payload.get("default_quality")),
        "default_post_processing": normalize_post_processing(payload.get("default_post_processing")),
        "ytdlp_cookies": get_ytdlp_cookies_status(source_profiles),
        "source_scrape_rules": get_effective_scrape_rules(payload),
        "source_token_roles": token_roles,
        "source_slug_tokens": get_effective_slug_tokens(payload),
        "source_fields": saved_fields(payload, default_fields),
        "source_title_cleaning": normalize_source_title_cleaning(
            payload.get("source_title_cleaning"), naming_defaults
        ),
        "source_cookie_policies": get_effective_cookie_policies(payload),
        # Global defaults every source inherits until it overrides them.
        "default_cookie_policy": normalize_default_cookie_policy(payload.get("default_cookie_policy")),
        "default_fields": default_fields,
        "default_naming": normalize_default_naming(payload.get("default_naming")),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_source_locations: Any,
    raw_template_settings: Any = None,
    raw_source_profiles: Any = None,
    raw_source_templates: Any = None,
    raw_default_quality: Any = None,
    raw_scrape_rules: Any = None,
    raw_token_roles: Any = None,
    raw_fields: Any = None,
    raw_title_cleaning: Any = None,
    raw_slug_tokens: Any = None,
    raw_cookie_policies: Any = None,
    raw_default_cookie_policy: Any = None,
    raw_default_fields: Any = None,
    raw_default_naming: Any = None,
    raw_default_post_processing: Any = None,
) -> dict[str, Any]:
    from backend.app.domains.downloads.constants import normalize_post_processing, normalize_quality_selection

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
            "source_locations": raw_source_locations,
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
    raw_field_payload = raw_fields if raw_fields is not None else existing.get("source_fields")
    template_settings = normalize_template_settings(raw_template_settings)
    default_cookie_policy = normalize_default_cookie_policy(
        raw_default_cookie_policy
        if raw_default_cookie_policy is not None
        else existing.get("default_cookie_policy")
    )
    default_fields = normalize_default_fields(
        raw_default_fields if raw_default_fields is not None else existing.get("default_fields")
    )
    default_naming = normalize_default_naming(
        raw_default_naming if raw_default_naming is not None else existing.get("default_naming")
    )
    # Per-source naming is stored against the defaults saved in this same write, so a
    # source only keeps the flags it really overrides.
    naming_defaults = get_effective_naming_defaults({"default_naming": default_naming})
    existing.update(
        {
            "source_profiles": managed_profiles,
            "source_locations": normalize_source_location_selection(raw_source_locations, cfg, managed_profiles),
            "template_settings": template_settings,
            "source_templates": normalize_source_template_selection(
                raw_source_templates,
                cfg,
                managed_profiles,
                normalized_token_roles,
            ),
            "default_quality": normalize_quality_selection(
                raw_default_quality if raw_default_quality is not None else existing.get("default_quality")
            ),
            "default_post_processing": normalize_post_processing(
                raw_default_post_processing
                if raw_default_post_processing is not None
                else existing.get("default_post_processing")
            ),
            "source_scrape_rules": normalized_scrape_rules,
            "source_token_roles": normalized_token_roles,
            "source_slug_tokens": normalized_slug_tokens,
            "source_fields": normalize_source_fields(raw_field_payload, default_fields),
            "source_title_cleaning": normalize_source_title_cleaning(
                raw_title_cleaning if raw_title_cleaning is not None else existing.get("source_title_cleaning"),
                naming_defaults,
            ),
            "source_cookie_policies": normalize_source_cookie_policies(
                raw_cookie_policies
                if raw_cookie_policies is not None
                else existing.get("source_cookie_policies")
            ),
            "default_cookie_policy": default_cookie_policy,
            "default_fields": default_fields,
            "default_naming": default_naming,
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
        default_post_processing,
        default_quality_selection,
        field_defaults,
        naming_choices,
        quality_options,
        template_tokens,
        title_cleaning_rules,
    )

    cfg = cfg or load_app_config()
    saved = saved or get_effective_saved_settings(cfg)
    source_profiles = saved.get("source_profiles", get_effective_source_profiles(cfg))
    return {
        "auth": auth_public_payload(),
        "media_root": str(MEDIA_DIR),
        "source_profiles": source_profiles,
        "source_locations": saved.get("source_locations", {}),
        "source_location_options": source_location_options(
            profile.get("key") for profile in source_profiles if isinstance(profile, dict)
        ),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "source_templates": saved.get("source_templates", {}),
        "default_quality": saved.get("default_quality", default_quality_selection()),
        "default_post_processing": saved.get("default_post_processing", default_post_processing()),
        "quality_options": quality_options(),
        "template_tokens": template_tokens(),
        "ytdlp_cookies": saved.get("ytdlp_cookies", get_ytdlp_cookies_status(saved.get("source_profiles"))),
        "source_scrape_rules": saved.get("source_scrape_rules", get_effective_scrape_rules()),
        "source_token_roles": saved.get("source_token_roles", get_effective_token_roles()),
        "source_slug_tokens": saved.get("source_slug_tokens", get_effective_slug_tokens()),
        "learned_formats": get_learned_formats_for_ui(),
        "source_fields": saved.get("source_fields", {}),
        "source_title_cleaning": saved.get("source_title_cleaning", {}),
        "source_cookie_policies": saved.get("source_cookie_policies", get_effective_cookie_policies()),
        "default_cookie_policy": saved.get("default_cookie_policy", {}),
        "default_fields": saved.get("default_fields", normalize_default_fields({})),
        "default_naming": saved.get("default_naming", {}),
        # Built-ins: what the defaults above fall back to, and what the UI offers as a reset.
        "cookie_policy_defaults": builtin_cookie_policy_defaults(),
        "field_defaults": field_defaults(),
        "title_cleaning_rules": title_cleaning_rules(),
        "naming_choices": naming_choices(),
        "settings_loaded_at": int(time.time()),
    }
