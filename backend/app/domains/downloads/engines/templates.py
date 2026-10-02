from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from backend.app.domains.downloads.constants import TEMPLATE_RE
from backend.app.domains.downloads.metadata.values import strip_handle_at_enabled
from backend.app.domains.formats.analysis import clean_creator
from backend.app.domains.options.quality import quality_label
from backend.app.domains.settings import (
    get_effective_fields,
    get_effective_template_settings,
    get_effective_title_cleaning,
    is_scraper_field,
    normalize_template_settings,
)


def derived_token_value(
    field: str,
    source_url: str = "",
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str | None:
    """Value for a filename token that comes from the URL, the quality selection,
    or scraped page metadata rather than the engine's own fields. Returns None when
    the engine should resolve the token from its metadata fields instead ("" is a
    real, empty value). Both engines share this so the logic lives in one place."""
    field = str(field or "").strip().lower()
    if extra_tokens:
        # User scrape rules win over engine fields, so a broken/absent extractor
        # value (uploader/artist) can be overridden with the page's own markup.
        override = extra_tokens.get(field)
        if override is not None and str(override).strip():
            if field == "username":
                return clean_creator(str(override), strip_at=strip_handle_at_enabled(cleaning))
            return str(override)
    if field == "quality":
        # Selected combo label when threaded; None falls back to delivered format.
        return quality_label(quality) if quality is not None else None
    return None


def field_role_list(field_roles: dict[str, Any] | None, role: str) -> list[str] | None:
    """Configured fields for a creator role, minus scraper tokens no engine can fill."""
    if not isinstance(field_roles, dict):
        return None
    values = field_roles.get(role)
    if not isinstance(values, list) or not values:
        return None
    fields = [str(value) for value in values if not is_scraper_field(value)]
    return fields or None


def field_spec_parts(fields: tuple[str, ...] | list[str], pattern: re.Pattern[str]) -> list[str]:
    """Ordered, deduplicated fields an engine's own template syntax accepts."""
    clean = [
        str(field).strip()
        for field in fields
        if not is_scraper_field(field) and pattern.match(str(field or "").strip())
    ]
    return list(dict.fromkeys(clean))


def substitute_template(template: str, resolve: Callable[[str], str]) -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: resolve(match.group(1)), value)


def rendered_template_parts(
    source_url: str,
    template_settings: dict[str, str] | None,
    quality: dict[str, str] | None,
    extra_tokens: dict[str, str] | None,
    convert: Callable[..., str],
) -> tuple[str, str]:
    """Folder and filename templates rendered through one engine's converter."""
    settings = (
        normalize_template_settings(template_settings)
        if template_settings is not None
        else get_effective_template_settings(source_url)
    )
    field_roles = get_effective_fields(source_url)
    cleaning = get_effective_title_cleaning(source_url)

    def render(template: str) -> str:
        return convert(template, source_url, quality, extra_tokens, field_roles, cleaning)

    return render(settings["folder_template"]), render(settings["filename_template"])
