from __future__ import annotations

from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.settings import (
    get_source_profile_for_url,
    load_saved_settings_file,
    normalize_default_fields,
    normalize_source_fields,
    save_saved_settings_file,
)

from .formats import learn_download, learn_media_id, media_id_from_url
from .store import load_learned_formats, save_learned_formats


def _resolved_field_source_key(
    source_url: str = "",
    source_key: str = "",
    payload: dict[str, Any] | None = None,
) -> str:
    key = normalize_source_key(source_key) if str(source_key or "").strip() else ""
    if key:
        return key
    if not str(source_url or "").strip():
        return ""
    try:
        return normalize_source_key(get_source_profile_for_url(source_url, payload=payload or {}).get("key"))
    except Exception:
        return ""


def _field_defaults(payload: dict[str, Any]) -> dict[str, list[str]]:
    return normalize_default_fields(payload.get("default_fields"))


def _normalized_field_roles_for_key(
    source_key: str,
    field_roles: Any,
    defaults: Any = None,
) -> dict[str, list[str]]:
    key = normalize_source_key(source_key)
    return normalize_source_fields({key: field_roles}, defaults).get(key, {})


def get_learned_fields(source_url: str = "", source_key: str = "") -> dict[str, list[str]]:
    payload = load_saved_settings_file()
    key = _resolved_field_source_key(source_url, source_key, payload)
    if not key:
        return {}
    defaults = _field_defaults(payload)
    return normalize_source_fields(payload.get("source_fields"), defaults).get(key, {})


def has_learned_fields(source_url: str = "", source_key: str = "") -> bool:
    return bool(get_learned_fields(source_url, source_key))


def _merge_field_roles(
    existing: dict[str, list[str]],
    learned: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        fields: list[str] = []
        for source in (existing, learned):
            for field in source.get(role, []):
                if field not in fields:
                    fields.append(field)
        if fields:
            merged[role] = fields
    return merged


def _append_missing_field_roles(
    existing: dict[str, list[str]],
    learned: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        fields = list(existing.get(role) or [])
        for field in learned.get(role, []):
            if field and field not in fields:
                fields.append(field)
        if fields:
            merged[role] = fields
    return merged


def save_missing_learned_fields(
    source_url: str = "",
    source_key: str = "",
    field_roles: Any = None,
) -> dict[str, list[str]]:
    """Persist only probed fields missing from a source's saved order.

    Automatic format learning uses this path so a new URL shape can add extractor
    fields it needs without reordering fields the user already has configured.
    """
    payload = load_saved_settings_file()
    key = _resolved_field_source_key(source_url, source_key, payload)
    if not key:
        return {}

    defaults = _field_defaults(payload)
    learned = _normalized_field_roles_for_key(key, field_roles, defaults)
    if not learned:
        return {}

    mapping = normalize_source_fields(payload.get("source_fields"), defaults)
    existing = mapping.get(key, {})
    updated = _append_missing_field_roles(existing, learned) if existing else learned
    updated = _normalized_field_roles_for_key(key, updated, defaults)
    if existing == updated:
        return existing

    if updated:
        mapping[key] = updated
    else:
        mapping.pop(key, None)
    payload["source_fields"] = mapping
    save_saved_settings_file(payload)
    return updated


def save_learned_fields(
    source_url: str = "",
    source_key: str = "",
    field_roles: Any = None,
    *,
    only_when_missing: bool = True,
    merge: bool = True,
) -> dict[str, list[str]]:
    """Persist probed fields for one source.

    Nothing is saved when the probe produced no usable username/nickname/title fields.
    Automatic callers pass ``only_when_missing`` so the first successful probe
    teaches the source without repeatedly hitting downloader metadata endpoints.
    """
    payload = load_saved_settings_file()
    key = _resolved_field_source_key(source_url, source_key, payload)
    if not key:
        return {}

    defaults = _field_defaults(payload)
    learned = _normalized_field_roles_for_key(key, field_roles, defaults)
    if not learned:
        return {}
    mapping = normalize_source_fields(payload.get("source_fields"), defaults)
    existing = mapping.get(key, {})
    if existing and only_when_missing:
        return existing

    updated = _merge_field_roles(existing, learned) if merge else learned
    updated = _normalized_field_roles_for_key(key, updated, defaults)
    if existing == updated:
        return existing

    if updated:
        mapping[key] = updated
    else:
        mapping.pop(key, None)
    payload["source_fields"] = mapping
    save_saved_settings_file(payload)
    return updated


def _metadata_from_probe_fields(fields: Any) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for item in fields if isinstance(fields, list) else []:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        value = str(item.get("value") or "").strip()
        if field and value and field not in metadata:
            metadata[field] = value
    return metadata


def promote_learned_format_from_probe(source_url: str, fields: Any) -> bool:
    """Use probed field values to upgrade literal URL creators to role tokens."""
    media_id = media_id_from_url(source_url)
    metadata = _metadata_from_probe_fields(fields)
    if not media_id or not metadata:
        return False
    learned = load_learned_formats()
    updated = learn_download(learned, source_url, media_id, metadata)
    if updated == learned:
        return False
    save_learned_formats(updated)
    return True


def learn_missing_fields_for_format(
    source_url: str,
    source_key: str = "",
    *,
    low_priority: bool = False,
) -> dict[str, list[str]]:
    """Probe a newly learned URL format and append any missing fields."""
    if not str(source_url or "").strip():
        return {}
    try:
        from .probe import probe_fields

        if low_priority:
            result = probe_fields(
                source_url,
                source_key,
                low_priority=True,
                stop_after_first_with_roles=True,
            )
        else:
            result = probe_fields(source_url, source_key)
    except Exception:
        return {}

    promote_learned_format_from_probe(source_url, result.get("fields"))
    key = str(result.get("source_key") or source_key)
    return save_missing_learned_fields(source_url, key, result.get("field_roles"))


def ensure_fields_learned(source_url: str, source_key: str = "") -> dict[str, list[str]]:
    """Probe fields once for a source that has no learned record yet."""
    key = normalize_source_key(source_key) if str(source_key or "").strip() else ""
    if not key:
        return {}
    if not str(source_url or "").strip() or has_learned_fields(source_url, source_key):
        return {}
    try:
        from .probe import probe_fields

        result = probe_fields(source_url, source_key)
    except Exception:
        return {}
    promote_learned_format_from_probe(source_url, result.get("fields"))
    return save_learned_fields(
        source_url,
        str(result.get("source_key") or source_key),
        result.get("field_roles"),
        only_when_missing=True,
    )


def learn_source_format(source_url: str, media_id: str, metadata: dict[str, Any] | None = None) -> bool:
    learned = load_learned_formats()
    updated = learn_download(learned, source_url, media_id, metadata)
    if updated == learned:
        return False
    save_learned_formats(updated)
    return True


def learn_source_id_signature(source_key: str, media_id: str) -> bool:
    learned = load_learned_formats()
    updated = learn_media_id(learned, source_key, media_id)
    if updated == learned:
        return False
    save_learned_formats(updated)
    return True


def update_learned_formats_with_download(
    learned: dict[str, Any],
    source_url: str,
    media_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return learn_download(learned, source_url, media_id, metadata)
