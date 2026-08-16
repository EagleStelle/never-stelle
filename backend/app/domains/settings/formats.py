from __future__ import annotations

from typing import Any

from backend.app.core.config import load_app_config
from backend.app.core.sources import normalize_source_key, source_key_from_url

from .fields import normalize_source_fields
from .profiles import ensure_source_profile_for_url, get_effective_source_profiles
from .storage import load_saved_settings_file, save_saved_settings_file


def get_learned_formats_for_ui() -> dict[str, dict[str, Any]]:
    # Read-only per-source learned URL shape + selectable segments for the Slug pane.
    from backend.app.domains.downloads.formats import describe_learned_segments
    from backend.app.domains.downloads.store import load_learned_formats

    out: dict[str, dict[str, Any]] = {}
    for raw_key, entry in (load_learned_formats() or {}).items():
        key = normalize_source_key(raw_key)
        described = describe_learned_segments(entry if isinstance(entry, dict) else {})
        # Any learned shape belongs in the map; the Format pane lists templates even
        # when a source has no user-nameable segments.
        if key and (described.get("templates") or described.get("segments")):
            out[key] = described
    return out


def add_source_and_learn_format(url_or_link: str) -> dict[str, Any]:
    """Add a platform from a pasted link and learn its URL format in one step."""
    from backend.app.domains.downloads.formats import match_template, media_id_from_url
    from backend.app.domains.downloads.learning import learn_missing_fields_for_format, learn_source_format
    from backend.app.domains.downloads.store import load_learned_formats

    url = str(url_or_link or "").strip()
    if not url:
        raise ValueError("Paste a link first.")
    cfg = load_app_config()
    payload = load_saved_settings_file()
    profiles = get_effective_source_profiles(cfg, payload)
    key = source_key_from_url(url, profiles)
    if not key:
        raise ValueError("Paste a valid link or domain first.")
    existed = any(normalize_source_key(profile.get("key")) == key for profile in profiles)
    ensure_source_profile_for_url(url)
    media_id = media_id_from_url(url)
    learned = learn_source_format(url, media_id) if media_id else False
    field_roles = learn_missing_fields_for_format(url, key) if learned else {}
    format_template = match_template(load_learned_formats(), key, url, media_id) if media_id else ""
    return {
        "source_key": key,
        "created": not existed,
        "learned": bool(learned),
        "media_id": media_id,
        "format_template": format_template,
        "field_roles": field_roles,
    }


def set_learned_format_templates(source_key: str, templates: Any) -> dict[str, Any]:
    """Reorder or delete a source's learned URL templates."""
    from backend.app.domains.downloads.store import (
        forget_learned_format,
        forget_seeded_downloads,
        load_learned_formats,
        save_learned_formats,
    )

    key = normalize_source_key(source_key)
    if not key:
        raise ValueError("Choose a source first.")
    learned = load_learned_formats() or {}
    entry = learned.get(key)
    if not isinstance(entry, dict):
        raise ValueError("That source has no learned format.")
    existing = [str(item or "").strip() for item in (entry.get("templates") or []) if str(item or "").strip()]
    ordered: list[str] = []
    for item in templates if isinstance(templates, list) else []:
        value = str(item or "").strip()
        if value in existing and value not in ordered:
            ordered.append(value)
    if ordered:
        new_entry = dict(entry)
        new_entry["templates"] = ordered
        save_learned_formats({key: new_entry})
    else:
        forget_learned_format(key)
    # The user just edited or deleted learned templates by hand. What was folded in
    # from history no longer describes the saved state, so the next scan rebuilds it.
    forget_seeded_downloads()
    if not ordered:
        payload = load_saved_settings_file()
        field_roles = normalize_source_fields(payload.get("source_fields"))
        if key in field_roles:
            field_roles.pop(key, None)
            if field_roles:
                payload["source_fields"] = field_roles
            else:
                payload.pop("source_fields", None)
            save_saved_settings_file(payload)
    return {"source_key": key, "templates": ordered}
