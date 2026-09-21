from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.settings import (
    get_effective_fields,
    get_source_profile_for_url,
    load_saved_settings_file,
    normalize_default_fields,
    normalize_source_fields,
    save_saved_settings_file,
)

from .formats import learn_download, learn_media_id
from .store import merge_learned_formats


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


def probe_link_fields(source_url: str, source_key: str = "", *, low_priority: bool = False) -> dict[str, Any]:
    """One probe of a link's fields and metadata; ``{}`` when no engine reads it."""
    if not str(source_url or "").strip():
        return {}
    try:
        from .probe import probe_fields

        if low_priority:
            return probe_fields(source_url, source_key, low_priority=True, stop_after_first_with_roles=True)
        return probe_fields(source_url, source_key)
    except Exception:
        return {}


def learn_missing_fields_for_format(
    source_url: str,
    source_key: str = "",
    *,
    low_priority: bool = False,
) -> dict[str, list[str]]:
    """Probe a newly learned URL format and append any missing fields."""
    result = probe_link_fields(source_url, source_key, low_priority=low_priority)
    if not result:
        return {}
    key = str(result.get("source_key") or source_key)
    return save_missing_learned_fields(source_url, key, result.get("field_roles"))


def _templates(formats: dict[str, Any]) -> dict[str, Any]:
    return {key: entry.get("templates") for key, entry in formats.items()}


def learn_formats(samples: Iterable[tuple[str, str, dict[str, Any] | None]]) -> bool:
    """Fold item links into the stored formats in one write; True when a template changed.

    Each sample is ``(source_url, media_id, metadata)`` from a link that was saved by hand
    or downloaded successfully. The fields holding the creator are resolved first, since
    the write holds the database lock.
    """
    prepared: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, list[str]]]] = {}
    for source_url, media_id, metadata in samples:
        source_url, media_id = str(source_url or "").strip(), str(media_id or "").strip()
        if source_url and media_id and (source_url, media_id) not in prepared:
            prepared[(source_url, media_id)] = (metadata, get_effective_fields(source_url))
    if not prepared:
        return False

    def update(learned: dict[str, Any]) -> dict[str, Any]:
        for (source_url, media_id), (metadata, roles) in prepared.items():
            learned = learn_download(learned, source_url, media_id, metadata, roles)
        return learned

    before, after = merge_learned_formats(update)
    return _templates(before) != _templates(after)


def learn_source_id_signature(source_key: str, media_id: str) -> None:
    merge_learned_formats(lambda learned: learn_media_id(learned, source_key, media_id))
