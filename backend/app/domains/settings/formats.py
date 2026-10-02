from __future__ import annotations

from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.formats.learning import describe_learned_segments
from backend.app.domains.formats.matching import learned_templates_for
from backend.app.domains.formats.store import forget_learned_format, load_learned_formats, save_learned_formats

from .fields import normalize_source_fields
from .storage import load_saved_settings_file, save_saved_settings_file


def get_learned_formats_for_ui() -> dict[str, dict[str, Any]]:
    # Read-only per-source learned URL shape + selectable segments for the Slug pane.

    out: dict[str, dict[str, Any]] = {}
    for raw_key, entry in (load_learned_formats() or {}).items():
        key = normalize_source_key(raw_key)
        described = describe_learned_segments(entry if isinstance(entry, dict) else {})
        # Any learned shape belongs in the map; the Format pane lists templates even
        # when a source has no user-nameable segments.
        if key and (described.get("templates") or described.get("segments")):
            out[key] = described
    return out


def set_learned_format_templates(source_key: str, templates: Any) -> dict[str, Any]:
    """Reorder or delete a source's learned URL templates."""
    key = normalize_source_key(source_key)
    if not key:
        raise ValueError("Choose a source first.")
    learned = load_learned_formats() or {}
    entry = learned.get(key)
    if not isinstance(entry, dict):
        raise ValueError("That source has no learned format.")
    existing = learned_templates_for(learned, key)
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
